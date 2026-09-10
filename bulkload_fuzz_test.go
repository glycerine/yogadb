//go:build memfs

package yogadb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"testing"

	"github.com/glycerine/vfs"
)

type bulkLoadFuzzOp struct {
	key    string
	value  []byte
	vtyp   uint64
	delete bool
}

type bulkLoadFuzzWant struct {
	value  []byte
	vtyp   uint64
	hlc    HLC
	delete bool
}

func FuzzBulkLoadBeforeAllowReads(f *testing.F) {
	// Sorted value-is-key SetBytes batches: the fastest initial-load path.
	f.Add([]byte{
		0, 0, 0, 0, 0,
		0, 0, 1, 0, 0,
		0, 0, 2, 0, 0,
		3,
	})
	// Unsorted variable keys, duplicate keys, and a tombstone in one batch.
	f.Add([]byte{
		1, 5, 12, 0, 0,
		1, 1, 9, 1, 0,
		1, 5, 6, 2, 0,
		2, 1,
		3,
	})
	// Sync before AllowReads, then load more before AllowReads to exercise the
	// reopened/existing-data-style merge path while reads are still disabled.
	f.Add([]byte{
		0, 0, 0, 0, 0,
		0, 0, 1, 0, 0,
		3, 0,
		4,
		1, 0, 20, 3, 0,
		2, 1,
		3,
	})
	// Large value path, including VLOG pointer records in bulk materialization.
	f.Add([]byte{
		1, 2, 90, 4, 1,
		1, 3, 72, 5, 2,
		3,
	})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 512 {
			data = data[:512]
		}

		fs := vfs.NewMem()
		dir := "bulkload_fuzz_" + cryRand15B()
		if err := fs.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		db, err := OpenFlexDB(dir, &Config{
			FS:                     fs,
			DisableBackgroundFlush: true,
		})
		if err != nil {
			t.Fatalf("OpenFlexDB: %v", err)
		}
		defer func() {
			if db != nil {
				db.Close()
			}
		}()

		want := make(map[string]bulkLoadFuzzWant)
		batch := db.NewBatch()
		defer batch.Close()

		var pending []bulkLoadFuzzOp
		setCalls := 0
		setBytesCalls := 0
		deleteCalls := 0
		commits := 0
		syncsBeforeAllowReads := 0
		maxBulkCount := 0
		defer func() {
			t.Logf("FuzzBulkLoadBeforeAllowReads len(data)=%d Set=%d SetBytes=%d Delete=%d Commit=%d SyncBeforeAllowReads=%d maxBulkCount=%d",
				len(data), setCalls, setBytesCalls, deleteCalls, commits, syncsBeforeAllowReads, maxBulkCount)
		}()

		commitPending := func(doFsync bool) {
			if len(pending) == 0 {
				return
			}
			iv, err := batch.Commit(doFsync)
			if err != nil {
				t.Fatalf("Batch.Commit(%v): %v", doFsync, err)
			}
			if iv.Begin == 0 || iv.Endx != iv.Begin+1 {
				t.Fatalf("Batch.Commit(%v) interval=%v, want one non-zero HLC", doFsync, iv)
			}
			for _, op := range pending {
				want[op.key] = bulkLoadFuzzWant{
					value:  append([]byte(nil), op.value...),
					vtyp:   op.vtyp,
					hlc:    iv.Begin,
					delete: op.delete,
				}
			}
			pending = pending[:0]
			commits++
			if db.allowReads.Load() {
				t.Fatal("bulk-load fuzz allowed reads before final AllowReads")
			}
			if got := db.mt.bt.Len(); got != 0 {
				t.Fatalf("pre-AllowReads fast batch used normal memtable: bt.Len()=%d", got)
			}
			if db.mt.bulk.count > maxBulkCount {
				maxBulkCount = db.mt.bulk.count
			}
		}

		for i, ops := 0, 0; i < len(data) && ops < 128; ops++ {
			op := data[i] % 5
			i++
			switch op {
			case 0: // SetBytes with value aliasing key.
				if i+3 > len(data) {
					continue
				}
				key := bulkLoadFuzzKey(data[i], data[i+1], true)
				vtyp := uint64(0)
				if data[i+2]&1 != 0 {
					vtyp = uint64(data[i+2])
				}
				i += 3
				keyBytes := []byte(key)
				if err := batch.SetBytes(keyBytes, keyBytes, vtyp); err != nil {
					t.Fatalf("Batch.SetBytes alias key=%q vtyp=%d: %v", key, vtyp, err)
				}
				pending = append(pending, bulkLoadFuzzOp{key: key, value: append([]byte(nil), keyBytes...), vtyp: vtyp})
				setBytesCalls++

			case 1: // Set with generated value, sometimes larger than the VLOG threshold.
				if i+4 > len(data) {
					continue
				}
				key := bulkLoadFuzzKey(data[i], data[i+1], false)
				valueLen := bulkLoadFuzzValueLen(data[i+2])
				seed := data[i+3]
				i += 4
				value := bulkLoadFuzzValue(valueLen, seed)
				vtyp := uint64(seed & 7)
				if err := batch.Set(key, value, vtyp); err != nil {
					t.Fatalf("Batch.Set key=%q len=%d vtyp=%d: %v", key, len(value), vtyp, err)
				}
				pending = append(pending, bulkLoadFuzzOp{key: key, value: append([]byte(nil), value...), vtyp: vtyp})
				setCalls++

			case 2: // Delete tombstone in the pre-AllowReads batch path.
				if i >= len(data) {
					continue
				}
				key := bulkLoadFuzzKey(data[i], data[i], false)
				i++
				batch.Delete(key)
				pending = append(pending, bulkLoadFuzzOp{key: key, delete: true})
				deleteCalls++

			case 3: // Commit, with fuzzed fsync choice.
				doFsync := false
				if i < len(data) {
					doFsync = data[i]&1 != 0
					i++
				}
				commitPending(doFsync)

			case 4: // Sync before AllowReads; this must not enable reads.
				commitPending(false)
				if len(want) == 0 {
					continue
				}
				if err := db.Sync(); err != nil {
					t.Fatalf("pre-AllowReads Sync: %v", err)
				}
				syncsBeforeAllowReads++
				if db.allowReads.Load() {
					t.Fatal("db.Sync enabled reads before AllowReads")
				}
				if got := db.mt.bt.Len(); got != 0 {
					t.Fatalf("pre-AllowReads Sync left normal memtable entries: bt.Len()=%d", got)
				}
			}
		}

		commitPending(false)
		if commits == 0 {
			return
		}
		if maxBulkCount == 0 && syncsBeforeAllowReads == 0 {
			t.Fatal("bulk-load fuzz did not observe any fast bulk entries before AllowReads")
		}
		db.AllowReads()
		verifyBulkLoadFuzzOracle(t, db, want)
		if err := db.Sync(); err != nil {
			t.Fatalf("post-AllowReads Sync: %v", err)
		}
		verifyBulkLoadFuzzOracle(t, db, want)
		db.Close()
		db = nil

		db, err = OpenFlexDB(dir, &Config{
			FS:                     fs,
			DisableBackgroundFlush: true,
		})
		if err != nil {
			t.Fatalf("reopen OpenFlexDB: %v", err)
		}
		db.AllowReads()
		verifyBulkLoadFuzzOracle(t, db, want)
	})
}

func FuzzBulkLoadReloadBeforeAllowReads(f *testing.F) {
	// Overwrite, delete, and insert into an existing DB before the second
	// handle has called AllowReads.
	f.Add([]byte{
		0, 1, 20, 1,
		2, 2,
		1, 3,
		3,
	})
	// Include Sync before AllowReads on the second handle.
	f.Add([]byte{
		0, 4, 12, 2,
		3, 0,
		4,
		2, 4,
		0, 5, 90, 3,
		3,
	})
	// Repeated operations on the same limited key set; final HLC must win.
	f.Add([]byte{
		0, 7, 8, 1,
		2, 7,
		0, 7, 16, 2,
		1, 7,
		3,
	})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 512 {
			data = data[:512]
		}

		fs := vfs.NewMem()
		dir := "bulkload_reload_fuzz_" + cryRand15B()
		if err := fs.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		cfg := &Config{
			FS:                     fs,
			DisableBackgroundFlush: true,
		}
		db, err := OpenFlexDB(dir, cfg)
		if err != nil {
			t.Fatalf("initial OpenFlexDB: %v", err)
		}
		defer func() {
			if db != nil {
				db.Close()
			}
		}()

		want := make(map[string]bulkLoadFuzzWant)
		loadBulkLoadFuzzInitialData(t, db, want)
		db.AllowReads()
		verifyBulkLoadFuzzOracle(t, db, want)
		if err := db.Sync(); err != nil {
			t.Fatalf("initial Sync: %v", err)
		}
		db.Close()
		db = nil

		db, err = OpenFlexDB(dir, cfg)
		if err != nil {
			t.Fatalf("second OpenFlexDB: %v", err)
		}
		if db.allowReads.Load() {
			t.Fatal("second OpenFlexDB unexpectedly allowed reads")
		}

		batch := db.NewBatch()
		defer batch.Close()
		var pending []bulkLoadFuzzOp
		setCalls := 0
		setBytesCalls := 0
		deleteCalls := 0
		commits := 0
		syncsBeforeAllowReads := 0
		maxBulkCount := 0
		defer func() {
			t.Logf("FuzzBulkLoadReloadBeforeAllowReads len(data)=%d Set=%d SetBytes=%d Delete=%d Commit=%d SyncBeforeAllowReads=%d maxBulkCount=%d",
				len(data), setCalls, setBytesCalls, deleteCalls, commits, syncsBeforeAllowReads, maxBulkCount)
		}()

		commitPending := func(doFsync bool) {
			if len(pending) == 0 {
				return
			}
			iv, err := batch.Commit(doFsync)
			if err != nil {
				t.Fatalf("second Batch.Commit(%v): %v", doFsync, err)
			}
			if iv.Begin == 0 || iv.Endx != iv.Begin+1 {
				t.Fatalf("second Batch.Commit(%v) interval=%v, want one non-zero HLC", doFsync, iv)
			}
			for _, op := range pending {
				want[op.key] = bulkLoadFuzzWant{
					value:  append([]byte(nil), op.value...),
					vtyp:   op.vtyp,
					hlc:    iv.Begin,
					delete: op.delete,
				}
			}
			pending = pending[:0]
			commits++
			if db.allowReads.Load() {
				t.Fatal("second handle allowed reads before final AllowReads")
			}
			if got := db.mt.bt.Len(); got != 0 {
				t.Fatalf("second pre-AllowReads batch used normal memtable: bt.Len()=%d", got)
			}
			if db.mt.bulk.count > maxBulkCount {
				maxBulkCount = db.mt.bulk.count
			}
		}

		for i, ops := 0, 0; i < len(data) && ops < 128; ops++ {
			op := data[i] % 5
			i++
			switch op {
			case 0:
				if i+3 > len(data) {
					continue
				}
				key := bulkLoadReloadFuzzKey(data[i])
				value := bulkLoadFuzzValue(bulkLoadFuzzValueLen(data[i+1]), data[i+2])
				vtyp := uint64(data[i+2] & 7)
				i += 3
				if err := batch.Set(key, value, vtyp); err != nil {
					t.Fatalf("second Batch.Set key=%q len=%d vtyp=%d: %v", key, len(value), vtyp, err)
				}
				pending = append(pending, bulkLoadFuzzOp{key: key, value: append([]byte(nil), value...), vtyp: vtyp})
				setCalls++

			case 1:
				if i >= len(data) {
					continue
				}
				key := bulkLoadReloadFuzzKey(data[i])
				i++
				keyBytes := []byte(key)
				if err := batch.SetBytes(keyBytes, keyBytes, 0); err != nil {
					t.Fatalf("second Batch.SetBytes alias key=%q: %v", key, err)
				}
				pending = append(pending, bulkLoadFuzzOp{key: key, value: append([]byte(nil), keyBytes...)})
				setBytesCalls++

			case 2:
				if i >= len(data) {
					continue
				}
				key := bulkLoadReloadFuzzKey(data[i])
				i++
				batch.Delete(key)
				pending = append(pending, bulkLoadFuzzOp{key: key, delete: true})
				deleteCalls++

			case 3:
				doFsync := false
				if i < len(data) {
					doFsync = data[i]&1 != 0
					i++
				}
				commitPending(doFsync)

			case 4:
				commitPending(false)
				if commits == 0 {
					continue
				}
				if err := db.Sync(); err != nil {
					t.Fatalf("second pre-AllowReads Sync: %v", err)
				}
				syncsBeforeAllowReads++
				if db.allowReads.Load() {
					t.Fatal("second db.Sync enabled reads before AllowReads")
				}
				if got := db.mt.bt.Len(); got != 0 {
					t.Fatalf("second pre-AllowReads Sync left normal memtable entries: bt.Len()=%d", got)
				}
			}
		}

		commitPending(false)
		if commits == 0 {
			return
		}
		if maxBulkCount == 0 && syncsBeforeAllowReads == 0 {
			t.Fatal("second bulk-load fuzz did not observe fast reload bulk entries before AllowReads")
		}

		db.AllowReads()
		verifyBulkLoadFuzzOracle(t, db, want)
		if err := db.Sync(); err != nil {
			t.Fatalf("second post-AllowReads Sync: %v", err)
		}
		verifyBulkLoadFuzzOracle(t, db, want)
		db.Close()
		db = nil

		db, err = OpenFlexDB(dir, cfg)
		if err != nil {
			t.Fatalf("final reopen OpenFlexDB: %v", err)
		}
		db.AllowReads()
		verifyBulkLoadFuzzOracle(t, db, want)
	})
}

func loadBulkLoadFuzzInitialData(t *testing.T, db *FlexDB, want map[string]bulkLoadFuzzWant) {
	t.Helper()
	batch := db.NewBatch()
	defer batch.Close()
	var ops []bulkLoadFuzzOp
	for i := 0; i < 16; i++ {
		key := bulkLoadReloadFuzzKey(byte(i))
		value := bulkLoadFuzzValue(12+(i%5)*7, byte(i))
		vtyp := uint64(i % 4)
		if err := batch.Set(key, value, vtyp); err != nil {
			t.Fatalf("initial Batch.Set key=%q: %v", key, err)
		}
		ops = append(ops, bulkLoadFuzzOp{key: key, value: append([]byte(nil), value...), vtyp: vtyp})
	}
	iv, err := batch.Commit(false)
	if err != nil {
		t.Fatalf("initial Batch.Commit: %v", err)
	}
	for _, op := range ops {
		want[op.key] = bulkLoadFuzzWant{
			value: append([]byte(nil), op.value...),
			vtyp:  op.vtyp,
			hlc:   iv.Begin,
		}
	}
	if err := db.Sync(); err != nil {
		t.Fatalf("initial pre-AllowReads Sync: %v", err)
	}
}

func bulkLoadReloadFuzzKey(raw byte) string {
	return fmt.Sprintf("reload-key-%02d", int(raw)%16)
}

func bulkLoadFuzzKey(a, b byte, fixed bool) string {
	if fixed {
		return fmt.Sprintf("k%03d", int(a)%64)
	}
	suffixLen := int(b%5) + 1
	suffix := bytes.Repeat([]byte{byte('a' + a%26)}, suffixLen)
	return fmt.Sprintf("tenant/%02d/%s/%03d", int(a)%17, suffix, int(b)%64)
}

func bulkLoadFuzzValueLen(raw byte) int {
	switch raw % 8 {
	case 0:
		return 0
	case 1:
		return 1
	case 2:
		return 8
	case 3:
		return 31
	case 4:
		return int(vlogInlineThreshold)
	default:
		return int(vlogInlineThreshold) + int(raw%48) + 1
	}
}

func bulkLoadFuzzValue(n int, seed byte) []byte {
	if n == 0 {
		return nil
	}
	out := make([]byte, n)
	for i := range out {
		out[i] = byte('A' + byte(i+int(seed))%26)
	}
	if n >= 8 {
		binary.LittleEndian.PutUint64(out[:8], uint64(seed)<<32|uint64(n))
	}
	return out
}

func verifyBulkLoadFuzzOracle(t *testing.T, db *FlexDB, want map[string]bulkLoadFuzzWant) {
	t.Helper()
	live := 0
	keys := make([]string, 0, len(want))
	for k := range want {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		rec := want[key]
		got, found, gotVtyp, gotHLC, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if rec.delete {
			if found {
				t.Fatalf("Get(%q) found value=%q vtyp=%d hlc=%d after tombstone hlc=%d", key, got, gotVtyp, gotHLC, rec.hlc)
			}
			continue
		}
		live++
		if !found {
			t.Fatalf("Get(%q) not found; want len=%d vtyp=%d hlc=%d", key, len(rec.value), rec.vtyp, rec.hlc)
		}
		if gotVtyp != rec.vtyp {
			t.Fatalf("Get(%q) vtyp=%d, want %d", key, gotVtyp, rec.vtyp)
		}
		if gotHLC != rec.hlc {
			t.Fatalf("Get(%q) HLC=%d, want %d", key, gotHLC, rec.hlc)
		}
		if !bytes.Equal(got, rec.value) {
			t.Fatalf("Get(%q) value len=%d, want len=%d", key, len(got), len(rec.value))
		}
	}
	if got := db.Len(); got != int64(live) {
		t.Fatalf("Len()=%d, want %d", got, live)
	}
	if errs := db.CheckIntegrity(); len(errs) > 0 {
		for _, err := range errs {
			t.Errorf("CheckIntegrity: %v", err)
		}
		t.Fatalf("CheckIntegrity found %d errors", len(errs))
	}
}
