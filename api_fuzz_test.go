package yogadb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"math"
	"math/rand"
	"os"
	"path"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/glycerine/vfs"
)

const (
	apiFuzzKeyCount     = 256
	apiFuzzMaxValueSize = 1 << 20
	apiFuzzMaxInput     = 4096
	apiFuzzMaxOps       = 180
)

type apiFuzzValue struct {
	value []byte
	vtyp  uint64
}

type apiFuzzOp struct {
	key    string
	value  []byte
	vtyp   uint64
	delete bool
}

type apiFuzzHarness struct {
	t      *testing.T
	seed   uint64
	rng    *rand.Rand
	zipf   *rand.Zipf
	baseFS *vfs.MemFS
	fs     *apiFuzzQuotaFS
	dir    string
	cfg    Config
	db     *FlexDB
	model  map[string]apiFuzzValue
}

func FuzzYogaDBAPI(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8})
	f.Add([]byte{0x42, 0x11, 0x99, 0x05, 0x31, 0x7f, 0x80, 0xff, 1, 1, 1, 2, 3, 5, 8, 13})
	f.Add([]byte{0xff, 0xee, 0xdd, 0xcc, 0xbb, 0xaa, 0x99, 0x88, 64, 65, 0, 255, 17, 29, 47, 91})
	f.Add([]byte{0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 18, 17, 16, 15, 14, 13, 12, 11})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 || len(data) > apiFuzzMaxInput {
			return
		}
		seed := apiFuzzSeed(data)
		memBudget := apiFuzzMemBudget()
		if memBudget > 0 {
			old := debug.SetMemoryLimit(memBudget)
			t.Cleanup(func() { debug.SetMemoryLimit(old) })
		}

		rng := rand.New(rand.NewSource(int64(seed)))
		zipf := rand.NewZipf(rng, 1.18, 1, apiFuzzMaxValueSize)
		baseFS := vfs.NewCrashableMem()
		fs := newAPIFuzzQuotaFS(baseFS, memBudget)
		dir := fmt.Sprintf("api_fuzz_%016x", seed)
		if err := fs.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}

		flushEnabled := data[0]&1 == 0
		flushInterval := time.Duration(1+int((seed>>8)%7)) * time.Millisecond
		cfg := Config{
			FS:                         fs,
			CacheMB:                    4,
			DisableBackgroundFlush:     !flushEnabled,
			BackgroundFlushInterval:    flushInterval,
			OmitFlexSpaceOpsRedoLog:    seed&0x20 != 0,
			PiggybackGC_on_SyncOrFlush: seed&0x40 != 0,
			GCGarbagePct:               0.20,
		}

		db, err := OpenFlexDB(dir, &cfg)
		if err != nil {
			apiFuzzFatalIfNotQuota(t, err, "OpenFlexDB")
			return
		}
		h := &apiFuzzHarness{
			t:      t,
			seed:   seed,
			rng:    rng,
			zipf:   zipf,
			baseFS: baseFS,
			fs:     fs,
			dir:    dir,
			cfg:    cfg,
			db:     db,
			model:  make(map[string]apiFuzzValue),
		}
		defer func() {
			if h.db != nil {
				h.db.Close()
			}
		}()

		h.preAllowReads(data)
		h.db.AllowReads()
		h.verifyModel("after AllowReads")

		opBytes := data
		if len(opBytes) > 8 {
			opBytes = opBytes[8:]
		}
		maxOps := len(opBytes) * 2
		if maxOps < 48 {
			maxOps = 48 + int(seed%32)
		}
		if maxOps > apiFuzzMaxOps {
			maxOps = apiFuzzMaxOps
		}

		for i := 0; i < maxOps; i++ {
			var b byte
			if len(opBytes) > 0 {
				b = opBytes[i%len(opBytes)]
			} else {
				b = byte(h.rng.Intn(256))
			}
			h.step(i, b)
		}

		if err := h.db.Sync(); err != nil {
			apiFuzzFatalIfNotQuota(t, err, "final Sync")
			return
		}
		h.verifyModel("final")
		if errs := h.db.CheckIntegrity(); len(errs) != 0 {
			t.Fatalf("final CheckIntegrity found %d errors: %v", len(errs), errs)
		}
	})
}

func (h *apiFuzzHarness) preAllowReads(data []byte) {
	h.expectBeforeAllowReadsPanics()

	batchCount := 1 + int(h.seed%4)
	for b := 0; b < batchCount; b++ {
		ops := h.randomOps(1+int((h.seed>>uint((b%8)*8))&7), true)
		h.commitBatch(fmt.Sprintf("pre-AllowReads batch %d", b), ops, h.rng.Intn(4) == 0, h.rng.Intn(3) == 0)
		if h.rng.Intn(3) == 0 {
			if err := h.db.Sync(); err != nil {
				apiFuzzFatalIfNotQuota(h.t, err, "pre-AllowReads Sync")
			}
		}
		h.expectBeforeAllowReadsPanics()
	}

	if len(data) > 3 && data[2]&1 != 0 {
		b := h.db.NewBatch()
		_ = b.Set(apiFuzzKey(0), []byte("discarded"), 0)
		b.Reset()
		b.Close()
	}
}

func (h *apiFuzzHarness) expectBeforeAllowReadsPanics() {
	checks := []func(){
		func() { _, _, _, _, _ = h.db.Get(apiFuzzKey(h.rng.Intn(apiFuzzKeyCount))) },
		func() { _, _ = h.db.GetKV(apiFuzzKey(h.rng.Intn(apiFuzzKeyCount))) },
		func() { _, _, _ = h.db.Find(Exact, apiFuzzKey(h.rng.Intn(apiFuzzKeyCount))) },
		func() { _ = h.db.Len() },
		func() { _, _ = h.db.LenBigSmall() },
		func() { _, _ = h.db.Put(apiFuzzKey(h.rng.Intn(apiFuzzKeyCount)), []byte("x"), 0) },
		func() { _ = h.db.Delete(apiFuzzKey(h.rng.Intn(apiFuzzKeyCount))) },
		func() { _, _, _ = h.db.DeleteRange(true, apiFuzzKey(0), apiFuzzKey(3), true, true) },
		func() { _, _ = h.db.Clear(false) },
		func() {
			_ = h.db.Merge(apiFuzzKey(0), func([]byte, bool, uint64) ([]byte, bool, bool, uint64) {
				return nil, false, false, 0
			})
		},
		func() { _ = h.db.View(func(*ReadOnlyTx) error { return nil }) },
		func() { _ = h.db.Update(func(*WriteTx) error { return nil }) },
		func() {
			tx, err := h.db.BeginUpdate()
			if err == nil {
				_ = tx.Rollback()
			}
		},
		func() {
			tx := h.db.BeginView()
			if tx != nil {
				tx.Close()
			}
		},
		func() { _, _ = h.db.VacuumVLOG() },
		func() { _, _ = h.db.VacuumKV() },
		func() { _ = h.db.CheckIntegrity() },
	}
	n := 1 + h.rng.Intn(4)
	for i := 0; i < n; i++ {
		fn := checks[h.rng.Intn(len(checks))]
		apiFuzzExpectAllowReadsPanic(h.t, fn)
	}
}

func (h *apiFuzzHarness) step(i int, b byte) {
	switch int(b) % 19 {
	case 0:
		key := apiFuzzKey(h.rng.Intn(apiFuzzKeyCount))
		value := h.randomValue()
		vtyp := h.randomVtyp()
		if _, err := h.db.Put(key, value, vtyp); err != nil {
			apiFuzzFatalIfNotQuota(h.t, err, "Put")
			return
		}
		h.model[key] = apiFuzzValue{value: apiFuzzCopy(value), vtyp: vtyp}
		h.maybeVerify("after Put")
	case 1:
		key := apiFuzzKey(h.rng.Intn(apiFuzzKeyCount))
		h.expectGet("db.Get", key)
		h.maybeVerify("after Get")
	case 2:
		key := apiFuzzKey(h.rng.Intn(apiFuzzKeyCount))
		if err := h.db.Delete(key); err != nil {
			apiFuzzFatalIfNotQuota(h.t, err, "Delete")
			return
		}
		delete(h.model, key)
		h.maybeVerify("after Delete")
	case 3:
		ops := h.randomOps(1+h.rng.Intn(12), false)
		h.commitBatch("post-AllowReads batch", ops, h.rng.Intn(4) == 0, h.rng.Intn(5) == 0)
		h.maybeVerify("after Batch")
	case 4:
		h.mergeOne()
	case 5:
		h.deleteRange()
	case 6:
		h.clear()
	case 7:
		h.updateCommit()
	case 8:
		h.updateRollback()
	case 9:
		h.beginUpdateManual()
	case 10:
		h.exerciseView()
	case 11:
		h.exerciseFind()
	case 12:
		h.exerciseIter()
	case 13:
		if err := h.db.Sync(); err != nil {
			apiFuzzFatalIfNotQuota(h.t, err, "Sync")
			return
		}
		h.maybeVerify("after Sync")
	case 14:
		h.reopen()
	case 15:
		h.vacuum()
	case 16:
		h.crashTransactionAtomicity()
	case 17:
		_ = h.db.SessionMetrics()
		_ = h.db.CumulativeMetrics()
		if h.rng.Intn(3) == 0 {
			if errs := h.db.CheckIntegrity(); len(errs) != 0 {
				h.t.Fatalf("CheckIntegrity found %d errors: %v", len(errs), errs)
			}
		}
		h.maybeVerify("after metrics/integrity")
	case 18:
		h.beginView()
	}
	_ = i
}

func (h *apiFuzzHarness) commitBatch(phase string, ops []apiFuzzOp, doFsync bool, metrics bool) {
	b := h.db.NewBatch()
	pending := apiFuzzCloneModel(h.model)
	for i, op := range ops {
		if op.delete {
			b.Delete(op.key)
			delete(pending, op.key)
			continue
		}
		var err error
		modelValue := op.value
		if i%2 == 0 {
			err = b.Set(op.key, op.value, op.vtyp)
		} else {
			keyBytes := []byte(op.key)
			value := op.value
			if h.rng.Intn(9) == 0 && op.vtyp == 0 && len(op.value) <= vlogInlineThreshold {
				value = keyBytes
				modelValue = keyBytes
			}
			err = b.SetBytes(keyBytes, value, op.vtyp)
		}
		if err != nil {
			apiFuzzFatalIfNotQuota(h.t, err, phase+" Set")
			b.Close()
			return
		}
		if i == len(ops)/2 && h.rng.Intn(23) == 0 {
			b.Reset()
			pending = apiFuzzCloneModel(h.model)
			continue
		}
		if !op.delete {
			pending[op.key] = apiFuzzValue{value: apiFuzzCopy(modelValue), vtyp: op.vtyp}
		}
	}

	var err error
	if metrics {
		_, _, err = b.CommitGetMetrics(doFsync)
	} else {
		_, err = b.Commit(doFsync)
	}
	b.Close()
	if err != nil {
		apiFuzzFatalIfNotQuota(h.t, err, phase+" Commit")
		return
	}
	h.model = pending
	if h.db.allowReads.Load() {
		h.maybeVerify(phase)
	}
}

func (h *apiFuzzHarness) mergeOne() {
	key := apiFuzzKey(h.rng.Intn(apiFuzzKeyCount))
	wantOld, wantExists := h.model[key]
	mode := h.rng.Intn(4)
	newValue := h.randomValue()
	newVtyp := h.randomVtyp()
	called := false
	err := h.db.Merge(key, func(oldVal []byte, exists bool, oldVtyp uint64) ([]byte, bool, bool, uint64) {
		called = true
		if exists != wantExists {
			h.t.Fatalf("Merge callback key=%q exists=%v want %v; model keys=%v",
				key, exists, wantExists, apiFuzzSortedKeys(h.model))
		}
		if exists && (!bytes.Equal(oldVal, wantOld.value) || oldVtyp != wantOld.vtyp) {
			h.t.Fatalf("Merge callback old value mismatch for %q: len=%d vtyp=%#x want len=%d vtyp=%#x",
				key, len(oldVal), oldVtyp, len(wantOld.value), wantOld.vtyp)
		}
		switch mode {
		case 0:
			return nil, false, false, 0
		case 1:
			return newValue, true, false, newVtyp
		case 2:
			return nil, false, true, 0
		default:
			return append(apiFuzzCopy(oldVal), byte(len(oldVal))), true, false, oldVtyp ^ uint64(len(oldVal)+1)
		}
	})
	if err != nil {
		apiFuzzFatalIfNotQuota(h.t, err, "Merge")
		return
	}
	if !called {
		h.t.Fatal("Merge callback was not called")
	}
	switch mode {
	case 1:
		h.model[key] = apiFuzzValue{value: apiFuzzCopy(newValue), vtyp: newVtyp}
	case 2:
		delete(h.model, key)
	case 3:
		old := wantOld.value
		oldVtyp := wantOld.vtyp
		h.model[key] = apiFuzzValue{value: append(apiFuzzCopy(old), byte(len(old))), vtyp: oldVtyp ^ uint64(len(old)+1)}
	}
	h.maybeVerify("after Merge")
}

func (h *apiFuzzHarness) deleteRange() {
	beg := apiFuzzRangeKey(h.rng.Intn(apiFuzzKeyCount + 2))
	end := apiFuzzRangeKey(h.rng.Intn(apiFuzzKeyCount + 2))
	if beg > end && h.rng.Intn(4) != 0 {
		beg, end = end, beg
	}
	begInclusive := h.rng.Intn(2) == 0
	endInclusive := h.rng.Intn(2) == 0
	includeLarge := h.rng.Intn(3) != 0
	before := apiFuzzCloneModel(h.model)
	_, _, err := h.db.DeleteRange(includeLarge, beg, end, begInclusive, endInclusive)
	if beg > end {
		if err == nil {
			h.t.Fatalf("DeleteRange(%q,%q) succeeded with beg > end", beg, end)
		}
		h.model = before
		return
	}
	if err != nil {
		apiFuzzFatalIfNotQuota(h.t, err, "DeleteRange")
		return
	}
	for key, rec := range h.model {
		if apiFuzzInDeleteRange(key, beg, end, begInclusive, endInclusive) && (includeLarge || len(rec.value) <= vlogInlineThreshold) {
			delete(h.model, key)
		}
	}
	h.maybeVerify("after DeleteRange")
}

func (h *apiFuzzHarness) clear() {
	includeLarge := h.rng.Intn(3) != 0
	if _, err := h.db.Clear(includeLarge); err != nil {
		apiFuzzFatalIfNotQuota(h.t, err, "Clear")
		return
	}
	if includeLarge {
		h.model = make(map[string]apiFuzzValue)
	} else {
		for key, rec := range h.model {
			if len(rec.value) <= vlogInlineThreshold {
				delete(h.model, key)
			}
		}
	}
	h.maybeVerify("after Clear")
}

func (h *apiFuzzHarness) updateCommit() {
	ops := h.randomOps(1+h.rng.Intn(8), false)
	pending := apiFuzzCloneModel(h.model)
	err := h.db.Update(func(tx *WriteTx) error {
		if h.applyTxOps(tx, pending, ops, false) {
			return nil
		}
		h.exerciseWriteTxReaders(tx, pending)
		return nil
	})
	if err != nil {
		apiFuzzFatalIfNotQuota(h.t, err, "Update commit")
		return
	}
	h.model = pending
	h.maybeVerify("after Update commit")
}

func (h *apiFuzzHarness) updateRollback() {
	ops := h.randomOps(1+h.rng.Intn(8), false)
	before := apiFuzzCloneModel(h.model)
	terminal := false
	terminalModel := before
	sentinel := errors.New("api fuzz rollback")
	err := h.db.Update(func(tx *WriteTx) error {
		pending := apiFuzzCloneModel(h.model)
		if h.applyTxOps(tx, pending, ops, false) {
			terminal = true
			terminalModel = apiFuzzCloneModel(pending)
			return sentinel
		}
		h.exerciseWriteTxReaders(tx, pending)
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		apiFuzzFatalIfNotQuota(h.t, err, "Update rollback")
		return
	}
	if terminal {
		h.model = terminalModel
	} else {
		h.model = before
	}
	h.verifyModel("after Update rollback")
}

func (h *apiFuzzHarness) beginUpdateManual() {
	tx, err := h.db.BeginUpdate()
	if err != nil {
		apiFuzzFatalIfNotQuota(h.t, err, "BeginUpdate")
		return
	}
	ops := h.randomOps(1+h.rng.Intn(8), false)
	pending := apiFuzzCloneModel(h.model)
	if h.applyTxOps(tx, pending, ops, false) {
		h.model = pending
		h.verifyModel("after terminal BeginUpdate")
		return
	}
	h.exerciseWriteTxReaders(tx, pending)
	if h.rng.Intn(2) == 0 {
		if err := tx.Commit(); err != nil {
			apiFuzzFatalIfNotQuota(h.t, err, "WriteTx.Commit")
			return
		}
		h.model = pending
		if _, err := tx.Put(apiFuzzKey(0), []byte("closed"), 0); !errors.Is(err, ErrWriteTxClosed) {
			h.t.Fatalf("Put after Commit err=%v, want ErrWriteTxClosed", err)
		}
	} else {
		if err := tx.Rollback(); err != nil {
			apiFuzzFatalIfNotQuota(h.t, err, "WriteTx.Rollback")
			return
		}
		if err := tx.Commit(); err != nil {
			h.t.Fatalf("Commit after Rollback err=%v, want nil", err)
		}
		if _, err := tx.Put(apiFuzzKey(0), []byte("closed"), 0); !errors.Is(err, ErrWriteTxClosed) {
			h.t.Fatalf("Put after Rollback err=%v, want ErrWriteTxClosed", err)
		}
	}
	h.maybeVerify("after BeginUpdate")
}

func (h *apiFuzzHarness) crashTransactionAtomicity() {
	if h.rng.Intn(2) == 0 {
		if err := h.db.Sync(); err != nil {
			apiFuzzFatalIfNotQuota(h.t, err, "pre-crash Sync")
			return
		}
	}

	ops := h.randomOps(2+h.rng.Intn(5), false)
	before := apiFuzzCloneModel(h.model)
	after := apiFuzzCloneModel(h.model)

	tx, err := h.db.BeginUpdate()
	if err != nil {
		apiFuzzFatalIfNotQuota(h.t, err, "BeginUpdate crash probe")
		return
	}
	h.applyTxOps(tx, after, ops, true)
	h.exerciseWriteTxReaders(tx, after)

	if err := h.db.mt.logSync(); err != nil {
		_ = tx.Rollback()
		apiFuzzFatalIfNotQuota(h.t, err, "sync incomplete transaction WAL")
		return
	}
	h.verifyCrashClone(before, "crash with incomplete transaction")
	if err := tx.Rollback(); err != nil {
		apiFuzzFatalIfNotQuota(h.t, err, "Rollback incomplete crash probe")
		return
	}
	h.verifyModel("after incomplete transaction rollback")

	tx, err = h.db.BeginUpdate()
	if err != nil {
		apiFuzzFatalIfNotQuota(h.t, err, "BeginUpdate committed crash probe")
		return
	}
	h.applyTxOps(tx, after, ops, true)
	if err := tx.Commit(); err != nil {
		apiFuzzFatalIfNotQuota(h.t, err, "Commit crash probe")
		return
	}
	h.model = after
	h.verifyCrashClone(after, "crash after committed transaction")
	h.verifyModel("after committed transaction crash probe")
}

func (h *apiFuzzHarness) applyTxOps(tx *WriteTx, model map[string]apiFuzzValue, ops []apiFuzzOp, noRangeOrClear bool) (terminal bool) {
	for i, op := range ops {
		if op.delete {
			if err := tx.Delete(op.key); err != nil {
				apiFuzzFatalIfNotQuota(h.t, err, "tx.Delete")
				return false
			}
			delete(model, op.key)
		} else {
			if _, err := tx.Put(op.key, op.value, op.vtyp); err != nil {
				apiFuzzFatalIfNotQuota(h.t, err, "tx.Put")
				return false
			}
			model[op.key] = apiFuzzValue{value: apiFuzzCopy(op.value), vtyp: op.vtyp}
		}
		if i == 0 {
			h.expectTxGet(tx, model, op.key)
		}
	}
	if noRangeOrClear {
		return false
	}
	switch h.rng.Intn(6) {
	case 0:
		key := apiFuzzKey(h.rng.Intn(apiFuzzKeyCount))
		value := h.randomValue()
		vtyp := h.randomVtyp()
		if err := tx.Merge(key, func([]byte, bool, uint64) ([]byte, bool, bool, uint64) {
			return value, true, false, vtyp
		}); err != nil {
			apiFuzzFatalIfNotQuota(h.t, err, "tx.Merge")
			return false
		}
		model[key] = apiFuzzValue{value: apiFuzzCopy(value), vtyp: vtyp}
	case 1:
		includeLarge := h.rng.Intn(2) == 0
		beg := apiFuzzKey(h.rng.Intn(apiFuzzKeyCount))
		end := apiFuzzKey(h.rng.Intn(apiFuzzKeyCount))
		if beg > end {
			beg, end = end, beg
		}
		_, allGone, err := tx.DeleteRange(includeLarge, beg, end, true, true)
		if err != nil {
			apiFuzzFatalIfNotQuota(h.t, err, "tx.DeleteRange")
			return false
		}
		for key, rec := range model {
			if key >= beg && key <= end && (includeLarge || len(rec.value) <= vlogInlineThreshold) {
				delete(model, key)
			}
		}
		if allGone {
			return true
		}
	case 2:
		if _, err := tx.Clear(false); err != nil {
			apiFuzzFatalIfNotQuota(h.t, err, "tx.Clear(false)")
			return false
		}
		for key, rec := range model {
			if len(rec.value) <= vlogInlineThreshold {
				delete(model, key)
			}
		}
	}
	return false
}

func (h *apiFuzzHarness) exerciseWriteTxReaders(tx *WriteTx, model map[string]apiFuzzValue) {
	wantKeys := apiFuzzSortedKeys(model)
	var gotKeys []string
	scan := tx.NewIter()
	defer scan.Close()
	for scan.SeekFirst(); scan.Valid(); scan.Next() {
		key, val, vtyp, _, found, err := scan.GetAnySize()
		if err != nil {
			h.t.Fatalf("tx iterator GetAnySize: %v", err)
		}
		if !found {
			h.t.Fatal("tx iterator returned found=false at valid position")
		}
		want, ok := model[key]
		if !ok {
			h.t.Fatalf("tx iterator visited unknown key %q", key)
		}
		if !bytes.Equal(val, want.value) || vtyp != want.vtyp {
			h.t.Fatalf("tx iterator key %q len=%d vtyp=%#x want len=%d vtyp=%#x",
				key, len(val), vtyp, len(want.value), want.vtyp)
		}
		gotKeys = append(gotKeys, strings.Clone(key))
	}
	if !slicesEqualString(gotKeys, wantKeys) {
		h.t.Fatalf("tx iterator keys=%v want %v", gotKeys, wantKeys)
	}
	if tx.Len() != int64(len(model)) {
		gotBig, gotSmall := tx.LenBigSmall()
		wantBig, wantSmall := apiFuzzBigSmall(model)
		h.t.Fatalf("tx.Len=%d want %d; LenBigSmall=(%d,%d) want (%d,%d); iterator keys=%v model keys=%v",
			tx.Len(), len(model), gotBig, gotSmall, wantBig, wantSmall, gotKeys, wantKeys)
	}
	big, small := apiFuzzBigSmall(model)
	gotBig, gotSmall := tx.LenBigSmall()
	if gotBig != big || gotSmall != small {
		h.t.Fatalf("tx.LenBigSmall=(%d,%d) want (%d,%d)", gotBig, gotSmall, big, small)
	}
	key := apiFuzzKey(h.rng.Intn(apiFuzzKeyCount))
	h.expectTxGet(tx, model, key)
	kvc, _, err := tx.Find(Exact|LAZY_SMALL, key)
	if err != nil {
		h.t.Fatalf("tx.Find(%q): %v", key, err)
	}
	h.checkKVC("tx.Find", kvc, model[key], modelHas(model, key), false)
	if kvc != nil {
		kvc.Close()
	}
	it := tx.NewIter()
	defer it.Close()
	it.Seek(key)
	if it.Valid() {
		_, _, _, _, _, err := it.GetAnySize()
		if err != nil {
			h.t.Fatalf("tx iterator GetAnySize: %v", err)
		}
	}
}

func (h *apiFuzzHarness) exerciseView() {
	err := h.db.View(func(ro *ReadOnlyTx) error {
		if ro.Len() != int64(len(h.model)) {
			h.t.Fatalf("ro.Len=%d want %d", ro.Len(), len(h.model))
		}
		big, small := apiFuzzBigSmall(h.model)
		gotBig, gotSmall := ro.LenBigSmall()
		if gotBig != big || gotSmall != small {
			h.t.Fatalf("ro.LenBigSmall=(%d,%d) want (%d,%d)", gotBig, gotSmall, big, small)
		}
		keys := apiFuzzSortedKeys(h.model)
		var asc []string
		ro.Ascend("", func(key string, value []byte, vtyp uint64, hlc HLC) bool {
			h.checkCallbackKV("ro.Ascend", key, value, vtyp)
			asc = append(asc, strings.Clone(key))
			return len(asc) < 17 || h.rng.Intn(2) == 0
		})
		for _, key := range asc {
			if _, ok := h.model[key]; !ok {
				h.t.Fatalf("ro.Ascend returned unknown key %q", key)
			}
		}
		var desc []string
		ro.Descend("", func(key string, value []byte, vtyp uint64, hlc HLC) bool {
			h.checkCallbackKV("ro.Descend", key, value, vtyp)
			desc = append(desc, strings.Clone(key))
			return len(desc) < 17 || h.rng.Intn(2) == 0
		})
		if len(keys) > 0 {
			pivot := keys[h.rng.Intn(len(keys))]
			ro.AscendRange(pivot, "", func(key string, value []byte, vtyp uint64, hlc HLC) bool {
				h.checkCallbackKV("ro.AscendRange", key, value, vtyp)
				return h.rng.Intn(5) != 0
			})
			ro.DescendRange(pivot, "", func(key string, value []byte, vtyp uint64, hlc HLC) bool {
				h.checkCallbackKV("ro.DescendRange", key, value, vtyp)
				return h.rng.Intn(5) != 0
			})
		}
		key := apiFuzzKey(h.rng.Intn(apiFuzzKeyCount))
		h.expectROGet(ro, key)
		kvc, exact, err := ro.Find(GTE|LAZY, key)
		if err != nil {
			h.t.Fatalf("ro.Find: %v", err)
		}
		h.checkFindResult("ro.Find", GTE, key, kvc, exact, false)
		if kvc != nil {
			kvc.Close()
		}
		kvc, _, err, it := ro.FindIt(GTE|LAZY_LARGE, key)
		if err != nil {
			h.t.Fatalf("ro.FindIt: %v", err)
		}
		if kvc != nil {
			_ = kvc.Fetch()
			kvc.Close()
		}
		if it != nil && it.Valid() {
			_, _, _, _, _, err := it.GetAnySize()
			if err != nil {
				h.t.Fatalf("ro.FindIt iterator GetAnySize: %v", err)
			}
		}
		if !h.cfg.DisableBackgroundFlush && h.rng.Intn(3) == 0 {
			time.Sleep(2 * h.cfg.BackgroundFlushInterval)
		}
		return nil
	})
	if err != nil {
		apiFuzzFatalIfNotQuota(h.t, err, "View")
		return
	}
	if !h.cfg.DisableBackgroundFlush && h.rng.Intn(2) == 0 {
		time.Sleep(2 * h.cfg.BackgroundFlushInterval)
	}
	h.maybeVerify("after View")
}

func (h *apiFuzzHarness) beginView() {
	ro := h.db.BeginView()
	defer ro.Close()
	key := apiFuzzKey(h.rng.Intn(apiFuzzKeyCount))
	h.expectROGet(ro, key)
	it := ro.NewIter()
	it.SeekFirst()
	if it.Valid() {
		_, _, _, _, _, err := it.GetAnySize()
		if err != nil {
			h.t.Fatalf("BeginView iterator GetAnySize: %v", err)
		}
	}
	it.Close()
}

func (h *apiFuzzHarness) exerciseFind() {
	key := apiFuzzKey(h.rng.Intn(apiFuzzKeyCount))
	mods := []SearchModifier{Exact, GTE, GT, LTE, LT}
	for _, base := range mods {
		smod := base
		switch h.rng.Intn(4) {
		case 0:
			smod |= LAZY
		case 1:
			smod |= LAZY_LARGE
		case 2:
			smod |= LAZY_SMALL
		case 3:
			smod |= SKIP_VALUES
		}
		kvc, exact, err := h.db.Find(smod, key)
		if err != nil {
			apiFuzzFatalIfNotQuota(h.t, err, "Find")
			return
		}
		h.checkFindResult("db.Find", base, key, kvc, exact, smod&SKIP_VALUES != 0)
		if kvc != nil {
			if smod&SKIP_VALUES == 0 {
				if err := kvc.Fetch(); err != nil {
					h.t.Fatalf("KVcloser.Fetch: %v", err)
				}
			}
			kvc.Close()
		}
	}
	kvc, err := h.db.GetKV(key)
	if err != nil {
		apiFuzzFatalIfNotQuota(h.t, err, "GetKV")
		return
	}
	h.checkKVC("GetKV", kvc, h.model[key], modelHas(h.model, key), false)
	if kvc != nil {
		if kvc.Large() {
			val, vtyp, _, err := h.db.FetchLarge(&kvc.KV)
			if err != nil {
				h.t.Fatalf("FetchLarge: %v", err)
			}
			want := h.model[key]
			if !bytes.Equal(val, want.value) || vtyp != want.vtyp {
				h.t.Fatalf("FetchLarge(%q) len=%d vtyp=%#x want len=%d vtyp=%#x",
					key, len(val), vtyp, len(want.value), want.vtyp)
			}
		}
		kvc.Close()
	}
	h.maybeVerify("after Find")
}

func (h *apiFuzzHarness) exerciseIter() {
	err := h.db.View(func(ro *ReadOnlyTx) error {
		keys := apiFuzzSortedKeys(h.model)
		it := ro.NewIter()
		defer it.Close()
		switch h.rng.Intn(4) {
		case 0:
			it.SeekFirst()
		case 1:
			it.SeekLast()
		case 2:
			it.Seek(apiFuzzKey(h.rng.Intn(apiFuzzKeyCount)))
		default:
			it.Seek("")
		}
		steps := 0
		for it.Valid() && steps < 32 {
			key, val, vtyp, _, found, err := it.GetAnySize()
			if err != nil {
				h.t.Fatalf("iterator GetAnySize: %v", err)
			}
			if !found {
				h.t.Fatal("valid iterator returned found=false")
			}
			h.checkCallbackKV("iterator", key, val, vtyp)
			kv := it.KV()
			if kv == nil || kv.Key != key {
				h.t.Fatalf("iterator KV key=%v want %q", kv, key)
			}
			inline := it.Vin()
			vel, empty, large := it.Vel()
			if it.Large() != (len(h.model[key].value) > vlogInlineThreshold) || large != it.Large() {
				h.t.Fatalf("iterator Large mismatch for %q", key)
			}
			if !large && !bytes.Equal(inline, h.model[key].value) {
				h.t.Fatalf("iterator Vin(%q) len=%d want %d", key, len(inline), len(h.model[key].value))
			}
			if empty != (!large && len(h.model[key].value) == 0) {
				h.t.Fatalf("iterator Vel empty=%v key=%q", empty, key)
			}
			if !large && !bytes.Equal(vel, h.model[key].value) {
				h.t.Fatalf("iterator Vel(%q) len=%d want %d", key, len(vel), len(h.model[key].value))
			}
			if large {
				got, gotVtyp, _, err := it.FetchV()
				if err != nil {
					h.t.Fatalf("iterator FetchV: %v", err)
				}
				want := h.model[key]
				if !bytes.Equal(got, want.value) || gotVtyp != want.vtyp {
					h.t.Fatalf("iterator FetchV(%q) len=%d vtyp=%#x want len=%d vtyp=%#x",
						key, len(got), gotVtyp, len(want.value), want.vtyp)
				}
			}
			if it.Vtyp() != h.model[key].vtyp {
				h.t.Fatalf("iterator Vtyp(%q)=%#x want %#x", key, it.Vtyp(), h.model[key].vtyp)
			}
			_ = it.Hlc()
			steps++
			if h.rng.Intn(2) == 0 {
				it.Next()
			} else {
				it.Prev()
			}
		}
		if len(keys) == 0 && it.Valid() {
			h.t.Fatal("empty model has valid iterator")
		}
		return nil
	})
	if err != nil {
		apiFuzzFatalIfNotQuota(h.t, err, "exerciseIter View")
	}
	h.maybeVerify("after iterator")
}

func (h *apiFuzzHarness) vacuum() {
	if h.rng.Intn(2) == 0 {
		if _, err := h.db.VacuumVLOG(); err != nil {
			apiFuzzFatalIfNotQuota(h.t, err, "VacuumVLOG")
			return
		}
	}
	if _, err := h.db.VacuumKV(); err != nil {
		apiFuzzFatalIfNotQuota(h.t, err, "VacuumKV")
		return
	}
	h.verifyModel("after vacuum")
}

func (h *apiFuzzHarness) reopen() {
	if h.rng.Intn(2) == 0 {
		if err := h.db.Sync(); err != nil {
			apiFuzzFatalIfNotQuota(h.t, err, "reopen Sync")
			return
		}
	}
	h.db.Close()
	h.db = nil
	db, err := OpenFlexDB(h.dir, &h.cfg)
	if err != nil {
		apiFuzzFatalIfNotQuota(h.t, err, "reopen OpenFlexDB")
		return
	}
	h.db = db
	h.db.AllowReads()
	h.verifyModel("after reopen")
}

func (h *apiFuzzHarness) verifyCrashClone(want map[string]apiFuzzValue, phase string) {
	crashedMem := h.baseFS.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 0})
	crashedFS := newAPIFuzzQuotaFS(crashedMem, h.fs.maxBytes)
	cfg := h.cfg
	cfg.FS = crashedFS
	db, err := OpenFlexDB(h.dir, &cfg)
	if err != nil {
		h.t.Fatalf("%s OpenFlexDB: %v", phase, err)
	}
	defer db.Close()
	db.AllowReads()
	apiFuzzVerifyModel(h.t, db, want, phase)
}

func (h *apiFuzzHarness) maybeVerify(phase string) {
	if h.rng.Intn(7) == 0 {
		h.verifyModel(phase)
	}
}

func (h *apiFuzzHarness) verifyModel(phase string) {
	apiFuzzVerifyModel(h.t, h.db, h.model, phase)
}

func apiFuzzVerifyModel(t *testing.T, db *FlexDB, model map[string]apiFuzzValue, phase string) {
	t.Helper()
	wantKeys := apiFuzzSortedKeys(model)
	var gotKeys []string
	if err := db.View(func(ro *ReadOnlyTx) error {
		it := ro.NewIter()
		defer it.Close()
		for it.SeekFirst(); it.Valid(); it.Next() {
			key, val, vtyp, _, found, err := it.GetAnySize()
			if err != nil {
				return err
			}
			if !found {
				t.Fatalf("%s iterator returned found=false at valid position", phase)
			}
			want, ok := model[key]
			if !ok {
				t.Fatalf("%s iterator visited unknown key %q", phase, key)
			}
			if !bytes.Equal(val, want.value) || vtyp != want.vtyp {
				t.Fatalf("%s iterator key %q len=%d vtyp=%#x want len=%d vtyp=%#x",
					phase, key, len(val), vtyp, len(want.value), want.vtyp)
			}
			gotKeys = append(gotKeys, strings.Clone(key))
		}
		return nil
	}); err != nil {
		t.Fatalf("%s View: %v", phase, err)
	}
	if !slicesEqualString(gotKeys, wantKeys) {
		t.Fatalf("%s iterator keys=%v want %v", phase, gotKeys, wantKeys)
	}
	for i := 0; i < apiFuzzKeyCount; i++ {
		key := apiFuzzKey(i)
		got, found, gotVtyp, _, err := db.Get(key)
		if err != nil {
			t.Fatalf("%s Get(%q): %v", phase, key, err)
		}
		want, ok := model[key]
		if found != ok {
			t.Fatalf("%s Get(%q) found=%v want %v; iterator keys=%v model keys=%v", phase, key, found, ok, gotKeys, wantKeys)
		}
		if ok && (!bytes.Equal(got, want.value) || gotVtyp != want.vtyp) {
			t.Fatalf("%s Get(%q) len=%d vtyp=%#x want len=%d vtyp=%#x",
				phase, key, len(got), gotVtyp, len(want.value), want.vtyp)
		}
	}
	if got := db.Len(); got != int64(len(model)) {
		t.Fatalf("%s Len=%d want %d; iterator keys=%v model keys=%v", phase, got, len(model), gotKeys, wantKeys)
	}
	wantBig, wantSmall := apiFuzzBigSmall(model)
	gotBig, gotSmall := db.LenBigSmall()
	if gotBig != wantBig || gotSmall != wantSmall {
		t.Fatalf("%s LenBigSmall=(%d,%d) want (%d,%d); iterator keys=%v model keys=%v",
			phase, gotBig, gotSmall, wantBig, wantSmall, gotKeys, wantKeys)
	}
}

func (h *apiFuzzHarness) expectGet(context, key string) {
	got, found, gotVtyp, _, err := h.db.Get(key)
	if err != nil {
		apiFuzzFatalIfNotQuota(h.t, err, context)
		return
	}
	want, ok := h.model[key]
	if found != ok {
		h.t.Fatalf("%s Get(%q) found=%v want %v", context, key, found, ok)
	}
	if ok && (!bytes.Equal(got, want.value) || gotVtyp != want.vtyp) {
		h.t.Fatalf("%s Get(%q) len=%d vtyp=%#x want len=%d vtyp=%#x",
			context, key, len(got), gotVtyp, len(want.value), want.vtyp)
	}
}

func (h *apiFuzzHarness) expectROGet(ro *ReadOnlyTx, key string) {
	got, found, gotVtyp, _, err := ro.Get(key)
	if err != nil {
		h.t.Fatalf("ro.Get(%q): %v", key, err)
	}
	want, ok := h.model[key]
	if found != ok || (ok && (!bytes.Equal(got, want.value) || gotVtyp != want.vtyp)) {
		h.t.Fatalf("ro.Get(%q) found=%v len=%d vtyp=%#x want found=%v len=%d vtyp=%#x",
			key, found, len(got), gotVtyp, ok, len(want.value), want.vtyp)
	}
}

func (h *apiFuzzHarness) expectTxGet(tx *WriteTx, model map[string]apiFuzzValue, key string) {
	got, found, gotVtyp, _, err := tx.Get(key)
	if err != nil {
		h.t.Fatalf("tx.Get(%q): %v", key, err)
	}
	want, ok := model[key]
	if found != ok || (ok && (!bytes.Equal(got, want.value) || gotVtyp != want.vtyp)) {
		h.t.Fatalf("tx.Get(%q) found=%v len=%d vtyp=%#x want found=%v len=%d vtyp=%#x",
			key, found, len(got), gotVtyp, ok, len(want.value), want.vtyp)
	}
}

func (h *apiFuzzHarness) checkCallbackKV(context, key string, value []byte, vtyp uint64) {
	want, ok := h.model[key]
	if !ok {
		h.t.Fatalf("%s returned unknown key %q", context, key)
	}
	if !bytes.Equal(value, want.value) || vtyp != want.vtyp {
		h.t.Fatalf("%s key %q len=%d vtyp=%#x want len=%d vtyp=%#x",
			context, key, len(value), vtyp, len(want.value), want.vtyp)
	}
}

func (h *apiFuzzHarness) checkFindResult(context string, smod SearchModifier, query string, kvc *KVcloser, exact bool, skipValues bool) {
	wantKey, ok := apiFuzzExpectedFindKey(h.model, smod, query)
	if !ok {
		if kvc != nil {
			h.t.Fatalf("%s(%v,%q) found %q, want nil", context, smod, query, kvc.Key)
		}
		if exact {
			h.t.Fatalf("%s(%v,%q) exact=true on miss", context, smod, query)
		}
		return
	}
	if kvc == nil {
		h.t.Fatalf("%s(%v,%q) nil, want %q", context, smod, query, wantKey)
	}
	want := h.model[wantKey]
	if kvc.Key != wantKey || exact != (wantKey == query) {
		h.t.Fatalf("%s(%v,%q) key=%q exact=%v want key=%q exact=%v",
			context, smod, query, kvc.Key, exact, wantKey, wantKey == query)
	}
	h.checkKVC(context, kvc, want, true, skipValues)
}

func (h *apiFuzzHarness) checkKVC(context string, kvc *KVcloser, want apiFuzzValue, wantFound bool, skipValues bool) {
	if !wantFound {
		if kvc != nil {
			h.t.Fatalf("%s kvc=%q want nil", context, kvc.Key)
		}
		return
	}
	if kvc == nil {
		h.t.Fatalf("%s kvc=nil want found", context)
	}
	if kvc.Vtyp != want.vtyp {
		h.t.Fatalf("%s Vtyp=%#x want %#x", context, kvc.Vtyp, want.vtyp)
	}
	if skipValues {
		if kvc.Value != nil {
			h.t.Fatalf("%s skip-values returned len=%d value", context, len(kvc.Value))
		}
		return
	}
	if kvc.Large() && kvc.Value == nil {
		if err := kvc.Fetch(); err != nil {
			h.t.Fatalf("%s Fetch: %v", context, err)
		}
	}
	if !bytes.Equal(kvc.Value, want.value) {
		h.t.Fatalf("%s value len=%d want %d", context, len(kvc.Value), len(want.value))
	}
}

func (h *apiFuzzHarness) randomOps(n int, allowDeletes bool) []apiFuzzOp {
	ops := make([]apiFuzzOp, 0, n)
	for i := 0; i < n; i++ {
		key := apiFuzzKey(h.rng.Intn(apiFuzzKeyCount))
		del := allowDeletes && h.rng.Intn(5) == 0
		ops = append(ops, apiFuzzOp{
			key:    key,
			value:  h.randomValue(),
			vtyp:   h.randomVtyp(),
			delete: del,
		})
	}
	return ops
}

func (h *apiFuzzHarness) randomValue() []byte {
	size := int(h.zipf.Uint64())
	switch h.rng.Intn(64) {
	case 0:
		size = 0
	case 1:
		size = 1
	case 2:
		size = vlogInlineThreshold
	case 3:
		size = vlogInlineThreshold + 1
	case 4:
		size = apiFuzzMaxValueSize
	}
	if size > apiFuzzMaxValueSize {
		size = apiFuzzMaxValueSize
	}
	return apiFuzzValueBytes(size, h.rng.Uint64()^h.seed)
}

func (h *apiFuzzHarness) randomVtyp() uint64 {
	switch h.rng.Intn(8) {
	case 0:
		return 0
	case 1:
		return 1
	case 2:
		return math.MaxUint64
	default:
		return h.rng.Uint64()
	}
}

func apiFuzzValueBytes(size int, seed uint64) []byte {
	if size == 0 {
		return nil
	}
	out := make([]byte, size)
	x := seed ^ uint64(size)*0x9e3779b97f4a7c15
	for i := range out {
		x ^= x << 7
		x ^= x >> 9
		x *= 0x2545f4914f6cdd1d
		out[i] = byte(x >> 56)
	}
	return out
}

func apiFuzzKey(i int) string {
	return fmt.Sprintf("k%03d", i&(apiFuzzKeyCount-1))
}

func apiFuzzRangeKey(i int) string {
	switch i {
	case apiFuzzKeyCount:
		return ""
	case apiFuzzKeyCount + 1:
		return "k999"
	default:
		return apiFuzzKey(i)
	}
}

func apiFuzzInDeleteRange(key, beg, end string, begInclusive, endInclusive bool) bool {
	if beg != "" {
		if begInclusive {
			if key < beg {
				return false
			}
		} else if key <= beg {
			return false
		}
	}
	if end != "" {
		if endInclusive {
			if key > end {
				return false
			}
		} else if key >= end {
			return false
		}
	}
	return true
}

func apiFuzzExpectedFindKey(model map[string]apiFuzzValue, smod SearchModifier, query string) (string, bool) {
	keys := apiFuzzSortedKeys(model)
	switch smod {
	case Exact:
		if _, ok := model[query]; ok {
			return query, true
		}
	case GTE:
		for _, key := range keys {
			if key >= query {
				return key, true
			}
		}
	case GT:
		for _, key := range keys {
			if key > query {
				return key, true
			}
		}
	case LTE:
		for i := len(keys) - 1; i >= 0; i-- {
			if keys[i] <= query || query == "" {
				return keys[i], true
			}
		}
	case LT:
		for i := len(keys) - 1; i >= 0; i-- {
			if query == "" || keys[i] < query {
				return keys[i], true
			}
		}
	}
	return "", false
}

func apiFuzzSortedKeys(model map[string]apiFuzzValue) []string {
	keys := make([]string, 0, len(model))
	for key := range model {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func apiFuzzBigSmall(model map[string]apiFuzzValue) (big, small int64) {
	for _, rec := range model {
		if len(rec.value) > vlogInlineThreshold {
			big++
		} else {
			small++
		}
	}
	return
}

func apiFuzzCloneModel(src map[string]apiFuzzValue) map[string]apiFuzzValue {
	dst := make(map[string]apiFuzzValue, len(src))
	for key, rec := range src {
		dst[key] = apiFuzzValue{value: apiFuzzCopy(rec.value), vtyp: rec.vtyp}
	}
	return dst
}

func apiFuzzCopy(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func modelHas(model map[string]apiFuzzValue, key string) bool {
	_, ok := model[key]
	return ok
}

func apiFuzzSeed(data []byte) uint64 {
	var h uint64 = 1469598103934665603
	for _, b := range data {
		h ^= uint64(b)
		h *= 1099511628211
	}
	h ^= uint64(len(data)) << 32
	if h == 0 {
		h = 1
	}
	return h
}

func apiFuzzMemBudget() int64 {
	data, err := os.ReadFile("/proc/meminfo")
	if err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "MemAvailable:") {
				fields := strings.Fields(line)
				if len(fields) >= 2 {
					var kb int64
					if _, err := fmt.Sscan(fields[1], &kb); err == nil && kb > 0 {
						return (kb << 10) / 2
					}
				}
			}
		}
	}
	return 512 << 20
}

func apiFuzzExpectAllowReadsPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("operation before AllowReads did not panic")
		}
		if got := fmt.Sprint(r); !strings.HasPrefix(got, "must call db.AllowReads() first") {
			t.Fatalf("panic = %q, want AllowReads panic", got)
		}
	}()
	fn()
}

func apiFuzzFatalIfNotQuota(t *testing.T, err error, context string) {
	t.Helper()
	if err == nil {
		return
	}
	if apiFuzzIsQuotaErr(err) {
		t.Skipf("%s reached fuzz MemFS quota: %v", context, err)
		return
	}
	t.Fatalf("%s: %v", context, err)
}

func apiFuzzIsQuotaErr(err error) bool {
	return errors.Is(err, syscall.ENOSPC) || strings.Contains(err.Error(), "api fuzz memfs quota")
}

func slicesEqualString(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type apiFuzzQuotaFS struct {
	inner    *vfs.MemFS
	maxBytes int64
	mu       sync.Mutex
	sizes    map[string]int64
	used     int64
}

func newAPIFuzzQuotaFS(inner *vfs.MemFS, maxBytes int64) *apiFuzzQuotaFS {
	if maxBytes <= 0 {
		maxBytes = 512 << 20
	}
	return &apiFuzzQuotaFS{
		inner:    inner,
		maxBytes: maxBytes,
		sizes:    make(map[string]int64),
	}
}

func (fs *apiFuzzQuotaFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	name = apiFuzzCleanPath(name)
	f, err := fs.inner.Create(name, category)
	if err != nil {
		return nil, err
	}
	fs.mu.Lock()
	fs.setSizeLocked(name, 0)
	fs.mu.Unlock()
	return &apiFuzzQuotaFile{File: f, fs: fs, name: name}, nil
}

func (fs *apiFuzzQuotaFS) Link(oldname, newname string) error {
	err := fs.inner.Link(oldname, newname)
	if err == nil {
		fs.refreshAll()
	}
	return err
}

func (fs *apiFuzzQuotaFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	return fs.inner.Open(name, opts...)
}

func (fs *apiFuzzQuotaFS) OpenReadWrite(name string, category vfs.DiskWriteCategory, opts ...vfs.OpenOption) (vfs.File, error) {
	name = apiFuzzCleanPath(name)
	f, err := fs.inner.OpenReadWrite(name, category, opts...)
	if err != nil {
		return nil, err
	}
	fs.refreshOne(name)
	return &apiFuzzQuotaFile{File: f, fs: fs, name: name}, nil
}

func (fs *apiFuzzQuotaFS) OpenDir(name string) (vfs.File, error) {
	return fs.inner.OpenDir(name)
}

func (fs *apiFuzzQuotaFS) Remove(name string) error {
	name = apiFuzzCleanPath(name)
	err := fs.inner.Remove(name)
	if err == nil {
		fs.mu.Lock()
		fs.setSizeLocked(name, 0)
		fs.mu.Unlock()
	}
	return err
}

func (fs *apiFuzzQuotaFS) RemoveAll(name string) error {
	err := fs.inner.RemoveAll(name)
	if err == nil {
		fs.refreshAll()
	}
	return err
}

func (fs *apiFuzzQuotaFS) Rename(oldname, newname string) error {
	err := fs.inner.Rename(oldname, newname)
	if err == nil {
		fs.refreshAll()
	}
	return err
}

func (fs *apiFuzzQuotaFS) ReuseForWrite(oldname, newname string, category vfs.DiskWriteCategory) (vfs.File, error) {
	newname = apiFuzzCleanPath(newname)
	f, err := fs.inner.ReuseForWrite(oldname, newname, category)
	if err != nil {
		return nil, err
	}
	fs.refreshAll()
	return &apiFuzzQuotaFile{File: f, fs: fs, name: newname}, nil
}

func (fs *apiFuzzQuotaFS) MkdirAll(dir string, perm os.FileMode) error {
	return fs.inner.MkdirAll(dir, perm)
}

func (fs *apiFuzzQuotaFS) Lock(name string) (io.Closer, error) {
	return fs.inner.Lock(name)
}

func (fs *apiFuzzQuotaFS) List(dir string) ([]string, error) {
	return fs.inner.List(dir)
}

func (fs *apiFuzzQuotaFS) Stat(name string) (vfs.FileInfo, error) {
	return fs.inner.Stat(name)
}

func (fs *apiFuzzQuotaFS) PathBase(p string) string {
	return fs.inner.PathBase(p)
}

func (fs *apiFuzzQuotaFS) PathJoin(elem ...string) string {
	return fs.inner.PathJoin(elem...)
}

func (fs *apiFuzzQuotaFS) PathDir(p string) string {
	return fs.inner.PathDir(p)
}

func (fs *apiFuzzQuotaFS) GetDiskUsage(string) (vfs.DiskUsage, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	used := uint64(fs.used)
	total := uint64(fs.maxBytes)
	avail := uint64(0)
	if total > used {
		avail = total - used
	}
	return vfs.DiskUsage{AvailBytes: avail, TotalBytes: total, UsedBytes: used}, nil
}

func (fs *apiFuzzQuotaFS) Unwrap() vfs.FS {
	return fs.inner
}

func (fs *apiFuzzQuotaFS) ReadDir(dirname string) ([]os.DirEntry, error) {
	return fs.inner.ReadDir(dirname)
}

func (fs *apiFuzzQuotaFS) IsReal() bool {
	return false
}

func (fs *apiFuzzQuotaFS) WalkDir(root string, fn iofs.WalkDirFunc) error {
	return fs.inner.WalkDir(root, fn)
}

func (fs *apiFuzzQuotaFS) MountReadOnlyRealDir(fromRealDir string, mountPointInsideDir string) error {
	return fs.inner.MountReadOnlyRealDir(fromRealDir, mountPointInsideDir)
}

func (fs *apiFuzzQuotaFS) reserve(name string, size int64) error {
	name = apiFuzzCleanPath(name)
	fs.mu.Lock()
	defer fs.mu.Unlock()
	old := fs.sizes[name]
	if size < 0 {
		size = 0
	}
	if size > old && fs.used+(size-old) > fs.maxBytes {
		return &os.PathError{Op: "api fuzz memfs quota", Path: name, Err: syscall.ENOSPC}
	}
	fs.setSizeLocked(name, size)
	return nil
}

func (fs *apiFuzzQuotaFS) setSizeLocked(name string, size int64) {
	old := fs.sizes[name]
	fs.used += size - old
	if size == 0 {
		delete(fs.sizes, name)
	} else {
		fs.sizes[name] = size
	}
}

func (fs *apiFuzzQuotaFS) refreshOne(name string) {
	name = apiFuzzCleanPath(name)
	fi, err := fs.inner.Stat(name)
	if err != nil || fi.IsDir() {
		return
	}
	fs.mu.Lock()
	fs.setSizeLocked(name, fi.Size())
	fs.mu.Unlock()
}

func (fs *apiFuzzQuotaFS) refreshAll() {
	sizes := make(map[string]int64)
	var used int64
	_ = fs.inner.WalkDir(".", func(p string, d os.DirEntry, err error) error {
		if err != nil || d == nil || d.IsDir() {
			return nil
		}
		fi, err := fs.inner.Stat(p)
		if err != nil {
			return nil
		}
		clean := apiFuzzCleanPath(p)
		sizes[clean] = fi.Size()
		used += fi.Size()
		return nil
	})
	fs.mu.Lock()
	fs.sizes = sizes
	fs.used = used
	fs.mu.Unlock()
}

type apiFuzzQuotaFile struct {
	vfs.File
	fs   *apiFuzzQuotaFS
	name string
}

func (f *apiFuzzQuotaFile) Write(p []byte) (int, error) {
	fi, err := f.File.Stat()
	if err != nil {
		return 0, err
	}
	if err := f.fs.reserve(f.name, fi.Size()+int64(len(p))); err != nil {
		return 0, err
	}
	n, err := f.File.Write(p)
	f.fs.refreshOne(f.name)
	return n, err
}

func (f *apiFuzzQuotaFile) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, os.ErrInvalid
	}
	fi, err := f.File.Stat()
	if err != nil {
		return 0, err
	}
	newSize := fi.Size()
	if end := off + int64(len(p)); end > newSize {
		newSize = end
	}
	if err := f.fs.reserve(f.name, newSize); err != nil {
		return 0, err
	}
	n, err := f.File.WriteAt(p, off)
	f.fs.refreshOne(f.name)
	return n, err
}

func (f *apiFuzzQuotaFile) Preallocate(offset, length int64) error {
	if offset < 0 || length < 0 {
		return os.ErrInvalid
	}
	if length > 0 {
		if err := f.fs.reserve(f.name, offset+length); err != nil {
			return err
		}
	}
	err := f.File.Preallocate(offset, length)
	f.fs.refreshOne(f.name)
	return err
}

func (f *apiFuzzQuotaFile) Truncate(size int64) error {
	if err := f.fs.reserve(f.name, size); err != nil {
		return err
	}
	err := f.File.Truncate(size)
	f.fs.refreshOne(f.name)
	return err
}

func apiFuzzCleanPath(name string) string {
	if name == "" {
		return "."
	}
	clean := path.Clean(strings.ReplaceAll(name, "\\", "/"))
	clean = strings.TrimPrefix(clean, "/")
	return clean
}

var _ vfs.FS = (*apiFuzzQuotaFS)(nil)
var _ vfs.File = (*apiFuzzQuotaFile)(nil)
