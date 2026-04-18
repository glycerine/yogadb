package ramflextree

import (
	"bytes"
	"fmt"
	"slices"
	"testing"
)

// ====================== Helpers ======================

func openTestDB(t *testing.T, cfg *Config) *FlexDB {
	t.Helper()
	if cfg == nil {
		cfg = &Config{}
	}
	db, err := OpenFlexDB("", cfg)
	if err != nil {
		t.Fatalf("OpenFlexDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func makeTestValue(size int) string {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte('A' + (i%26))
	}
	return string(b)
}

// generateBenchKeys creates 100000 unique keys using prng.
func generateBenchKeys() [][]byte {
	var seed [32]byte
	seed[0] = 99
	p := newPRNG(seed)
	n := 100000
	keys := make([][]byte, n)
	dup := make(map[string]bool)
	for i := 0; i < n; i++ {
		for {
			cid := p.NewCallID()
			if !dup[cid] {
				dup[cid] = true
				keys[i] = []byte(cid)
				break
			}
		}
	}
	return keys
}

func mustPut(t *testing.T, db *FlexDB, key, value string) {
	t.Helper()
	if err := db.Put(key, []byte(value)); err != nil {
		t.Fatalf("Put(%q, %q): %v", key, value, err)
	}
}

func mustDelete(t *testing.T, db *FlexDB, key string) {
	t.Helper()
	if err := db.Delete(key); err != nil {
		t.Fatalf("Delete(%q): %v", key, err)
	}
}

func mustGet(t *testing.T, db *FlexDB, key, wantValue string) {
	t.Helper()
	val, ok, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if !ok {
		t.Fatalf("Get(%q): not found (want %q)", key, wantValue)
	}
	if string(val) != wantValue {
		t.Fatalf("Get(%q) = %q, want %q", key, val, wantValue)
	}
}

func mustMiss(t *testing.T, db *FlexDB, key string) {
	t.Helper()
	val, ok, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if ok {
		t.Fatalf("Get(%q) = %q, want miss", key, val)
	}
}

// ====================== Tests ======================

// TestFlexDB_BasicMemtable tests Put/Get while data is still in memtable.
func TestFlexDB_BasicMemtable(t *testing.T) {
	db := openTestDB(t, nil)

	mustPut(t, db, "hello", "world")
	mustGet(t, db, "hello", "world")
	mustMiss(t, db, "missing")
}

// TestFlexDB_Update tests that a second Put replaces the first.
func TestFlexDB_Update(t *testing.T) {
	db := openTestDB(t, nil)

	mustPut(t, db, "key", "val1")
	mustGet(t, db, "key", "val1")

	mustPut(t, db, "key", "val2")
	mustGet(t, db, "key", "val2")
}

// TestFlexDB_Delete tests deletion via tombstone.
func TestFlexDB_Delete(t *testing.T) {
	db := openTestDB(t, nil)

	mustPut(t, db, "k1", "v1")
	mustPut(t, db, "k2", "v2")
	mustPut(t, db, "k3", "v3")

	mustDelete(t, db, "k2")

	mustGet(t, db, "k1", "v1")
	mustMiss(t, db, "k2")
	mustGet(t, db, "k3", "v3")
}

// TestFlexDB_SyncAndGet tests that data is readable after Sync() (FlexSpace).
func TestFlexDB_SyncAndGet(t *testing.T) {
	db := openTestDB(t, nil)

	mustPut(t, db, "key1", "val1")
	mustPut(t, db, "key2", "val2")
	db.Sync()

	mustGet(t, db, "key1", "val1")
	mustGet(t, db, "key2", "val2")
}

// TestFlexDB_ManyKeys inserts many keys (more than one FlexSpace interval),
// forcing interval splits and verifying correctness.
func TestFlexDB_ManyKeys(t *testing.T) {
	db := openTestDB(t, nil)

	const N = 200
	keys := make([]string, N)
	for i := 0; i < N; i++ {
		keys[i] = fmt.Sprintf("key%06d", i)
		mustPut(t, db, keys[i], fmt.Sprintf("val%06d", i))
	}
	db.Sync()

	for i, k := range keys {
		mustGet(t, db, k, fmt.Sprintf("val%06d", i))
	}
}

// TestFlexDB_UpdateAfterSync updates keys after they've been flushed to FlexSpace.
func TestFlexDB_UpdateAfterSync(t *testing.T) {
	db := openTestDB(t, nil)

	for i := 0; i < 50; i++ {
		mustPut(t, db, fmt.Sprintf("key%03d", i), fmt.Sprintf("original%03d", i))
	}
	db.Sync()

	// Update every other key
	for i := 0; i < 50; i += 2 {
		mustPut(t, db, fmt.Sprintf("key%03d", i), fmt.Sprintf("updated%03d", i))
	}
	db.Sync()

	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("key%03d", i)
		if i%2 == 0 {
			mustGet(t, db, k, fmt.Sprintf("updated%03d", i))
		} else {
			mustGet(t, db, k, fmt.Sprintf("original%03d", i))
		}
	}
}

// TestFlexDB_DeleteAfterSync tests delete of keys that are in FlexSpace.
func TestFlexDB_DeleteAfterSync(t *testing.T) {
	db := openTestDB(t, nil)

	for i := 0; i < 50; i++ {
		mustPut(t, db, fmt.Sprintf("k%03d", i), fmt.Sprintf("v%03d", i))
	}
	db.Sync()

	// Delete every third key
	for i := 0; i < 50; i += 3 {
		mustDelete(t, db, fmt.Sprintf("k%03d", i))
	}
	db.Sync()

	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("k%03d", i)
		if i%3 == 0 {
			mustMiss(t, db, k)
		} else {
			mustGet(t, db, k, fmt.Sprintf("v%03d", i))
		}
	}
}

// TestFlexDB_kv128RoundTrip tests kv128 encode/decode.
func TestFlexDB_kv128RoundTrip(t *testing.T) {
	cases := []KV{
		{Key: "hello", Value: []byte("world"), Hlc: 12345},
		{Key: "", Value: []byte(""), Hlc: 0},
		{Key: "a", Vptr: VPtr{Length: tombstoneVPtrLength}, Hlc: 999}, // tombstone
		{Key: string(make([]byte, 100)), Value: make([]byte, 200), Hlc: 0x7FFFFFFFFFFFFFFF},
		// VPtr case with HLC
		{Key: "big", Vptr: VPtr{Offset: 1024, Length: 256}, Hlc: 42},
	}
	for _, kv := range cases {
		buf := kv128Encode(nil, kv)
		if len(buf) != kv128EncodedSize(kv) {
			t.Fatalf("size mismatch: encoded %d, predicted %d", len(buf), kv128EncodedSize(kv))
		}
		got, n, ok := kv128Decode(buf)
		if !ok {
			t.Fatalf("kv128Decode failed for key=%q", kv.Key)
		}
		if n != len(buf) {
			t.Fatalf("consumed %d bytes, expected %d", n, len(buf))
		}
		if got.Key != kv.Key {
			t.Fatalf("key mismatch: got %q, want %q", got.Key, kv.Key)
		}
		if !bytes.Equal(got.Value, kv.Value) {
			t.Fatalf("value mismatch: got %q, want %q", got.Value, kv.Value)
		}
		if got.Hlc != kv.Hlc {
			t.Fatalf("HLC mismatch: got %v, want %v", got.Hlc, kv.Hlc)
		}
		if got.HasVPtr() != kv.HasVPtr() {
			t.Fatalf("HasVPtr mismatch: got %v, want %v", got.HasVPtr(), kv.HasVPtr())
		}
		if got.HasVPtr() && got.Vptr != kv.Vptr {
			t.Fatalf("Vptr mismatch: got %+v, want %+v", got.Vptr, kv.Vptr)
		}
		// Also verify kv128SizePrefix
		pfxSize, pfxOK := kv128SizePrefix(buf)
		if !pfxOK {
			t.Fatalf("kv128SizePrefix failed for key=%q", kv.Key)
		}
		if pfxSize != len(buf) {
			t.Fatalf("kv128SizePrefix = %d, want %d", pfxSize, len(buf))
		}
	}
}

// TestFlexDB_kv128CRC32C verifies CRC32C detection of corrupted records.
func TestFlexDB_kv128CRC32C(t *testing.T) {
	kv := KV{Key: "testkey", Value: []byte("testvalue"), Hlc: 42}
	buf := kv128Encode(nil, kv)

	// Verify clean decode works
	_, _, ok := kv128Decode(buf)
	if !ok {
		t.Fatal("clean kv128Decode failed")
	}

	// Flip a bit in the key - CRC should catch it
	corrupted := make([]byte, len(buf))
	copy(corrupted, buf)
	corrupted[3] ^= 0x01 // flip a bit in the key area
	_, _, ok = kv128Decode(corrupted)
	if ok {
		t.Fatal("kv128Decode should have failed on corrupted data")
	}

	// Flip a bit in the CRC itself
	corrupted2 := make([]byte, len(buf))
	copy(corrupted2, buf)
	corrupted2[len(corrupted2)-1] ^= 0x80 // flip high bit of CRC
	_, _, ok = kv128Decode(corrupted2)
	if ok {
		t.Fatal("kv128Decode should have failed on corrupted CRC")
	}

	// Tombstone path
	tomb := KV{Key: "delme", Hlc: 7}
	tbuf := kv128Encode(nil, tomb)
	_, _, ok = kv128Decode(tbuf)
	if !ok {
		t.Fatal("tombstone decode failed")
	}
	tbuf[2] ^= 0x01
	_, _, ok = kv128Decode(tbuf)
	if ok {
		t.Fatal("corrupted tombstone should have failed CRC")
	}
}

// TestFlexDB_LogEntryCRC32C verifies FLEXSPACE.REDO.LOG entry CRC32C detection.
func TestFlexDB_LogEntryCRC32C(t *testing.T) {
	buf := make([]byte, flexLogEntrySize)
	encodeLogEntry(buf, flexOpTreeInsert, 12345, 67890, 4096)

	// Clean decode
	_, _, _, _, ok := decodeLogEntry(buf)
	if !ok {
		t.Fatal("clean decodeLogEntry failed")
	}

	// Corrupt a data byte
	buf[5] ^= 0x01
	_, _, _, _, ok = decodeLogEntry(buf)
	if ok {
		t.Fatal("corrupted log entry should have failed CRC")
	}
}

// TestFlexDB_TagHelpers tests file tag encoding/decoding.
func TestFlexDB_TagHelpers(t *testing.T) {
	tag := flexdbTagGenerate(true, 7)
	if !flexdbTagIsAnchor(tag) {
		t.Fatal("expected isAnchor=true")
	}
	if flexdbTagUnsorted(tag) != 7 {
		t.Fatalf("unsorted = %d, want 7", flexdbTagUnsorted(tag))
	}

	tag2 := flexdbTagGenerate(false, 15)
	if flexdbTagIsAnchor(tag2) {
		t.Fatal("expected isAnchor=false")
	}
	if flexdbTagUnsorted(tag2) != 15 {
		t.Fatalf("unsorted = %d, want 15", flexdbTagUnsorted(tag2))
	}

	// Max unsorted (7 bits = 127)
	tag3 := flexdbTagGenerate(true, 127)
	if flexdbTagUnsorted(tag3) != 127 {
		t.Fatalf("unsorted = %d, want 127", flexdbTagUnsorted(tag3))
	}
}

// TestFlexDB_MultipleSync tests multiple Sync() calls in sequence.
func TestFlexDB_MultipleSync(t *testing.T) {
	db := openTestDB(t, nil)

	for round := 0; round < 5; round++ {
		for i := 0; i < 20; i++ {
			k := fmt.Sprintf("r%d_k%03d", round, i)
			mustPut(t, db, k, fmt.Sprintf("v%d_%03d", round, i))
		}
		db.Sync()
	}

	for round := 0; round < 5; round++ {
		for i := 0; i < 20; i++ {
			k := fmt.Sprintf("r%d_k%03d", round, i)
			mustGet(t, db, k, fmt.Sprintf("v%d_%03d", round, i))
		}
	}
}

// TestFlexDB_EmptyDB tests operations on a freshly opened empty database.
func TestFlexDB_EmptyDB(t *testing.T) {
	db := openTestDB(t, nil)

	mustMiss(t, db, "anything")

	db.View(func(roDB *ReadOnlyTx) error {
		it := roDB.NewIter()
		it.SeekFirst()
		if it.Valid() {
			t.Fatalf("iterator on empty DB should not be valid, got key %q", it.Key())
		}
		it.Close()
		return nil
	})
}

// TestFlexDB_OverwriteLoop tests repeatedly overwriting the same key.
func TestFlexDB_OverwriteLoop(t *testing.T) {
	db := openTestDB(t, nil)

	const K = 5
	const Rounds = 10
	keys := make([]string, K)
	for i := range keys {
		keys[i] = fmt.Sprintf("key%d", i)
	}

	for r := 0; r < Rounds; r++ {
		for i, k := range keys {
			mustPut(t, db, k, fmt.Sprintf("v%d_r%d", i, r))
		}
		if r%3 == 0 {
			db.Sync()
		}
	}
	db.Sync()

	for i, k := range keys {
		mustGet(t, db, k, fmt.Sprintf("v%d_r%d", i, Rounds-1))
	}
}

// TestFlexDB_LargeValues tests KVs near the maximum size.
func TestFlexDB_LargeValues(t *testing.T) {
	db := openTestDB(t, nil)

	// Max KV is flexdbMaxKVSize = 4096; key+value+overhead must fit.
	largeVal := make([]byte, 3000)
	for i := range largeVal {
		largeVal[i] = byte(i % 256)
	}

	mustPut(t, db, "big", string(largeVal))
	db.Sync()
	mustGet(t, db, "big", string(largeVal))
}

// ====================== Merge tests ======================

// TestFlexDB_MergeNewKey tests merge on a key that doesn't exist yet.
func TestFlexDB_MergeNewKey(t *testing.T) {
	db := openTestDB(t, nil)

	err := db.Merge("counter", func(old []byte, exists bool) (newValue []byte, doWrite bool, doDelete bool) {
		if exists {
			t.Fatal("expected key to not exist")
		}
		newValue = []byte("1")
		doWrite = true
		return
	})
	if err != nil {
		t.Fatal(err)
	}
	mustGet(t, db, "counter", "1")
}

// TestFlexDB_MergeExistingMemtable tests merge on a key in the active memtable.
func TestFlexDB_MergeExistingMemtable(t *testing.T) {
	db := openTestDB(t, nil)

	mustPut(t, db, "counter", "5")

	err := db.Merge("counter", func(old []byte, exists bool) (newValue []byte, doWrite bool, doDelete bool) {
		if !exists {
			t.Fatal("expected key to exist")
		}
		if string(old) != "5" {
			t.Fatalf("expected old=5, got %q", old)
		}
		newValue = []byte("6")
		doWrite = true
		return
	})
	if err != nil {
		t.Fatal(err)
	}
	mustGet(t, db, "counter", "6")
}

// TestFlexDB_MergeExistingFlexSpace tests merge on a key flushed to FlexSpace.
func TestFlexDB_MergeExistingFlexSpace(t *testing.T) {
	db := openTestDB(t, nil)

	mustPut(t, db, "counter", "10")
	db.Sync()

	err := db.Merge("counter", func(old []byte, exists bool) (newValue []byte, doWrite bool, doDelete bool) {
		if !exists {
			t.Fatal("expected key to exist in FlexSpace")
		}
		if string(old) != "10" {
			t.Fatalf("expected old=10, got %q", old)
		}
		newValue = []byte("11")
		doWrite = true
		return
	})
	if err != nil {
		t.Fatal(err)
	}
	mustGet(t, db, "counter", "11")
}

// TestFlexDB_MergeNoWrite tests that merge can be a no-op.
func TestFlexDB_MergeNoWrite(t *testing.T) {
	db := openTestDB(t, nil)

	mustPut(t, db, "key", "val")

	err := db.Merge("key", func(old []byte, exists bool) (newValue []byte, doWrite bool, doDelete bool) {
		return // no-op: all zero values
	})
	if err != nil {
		t.Fatal(err)
	}
	mustGet(t, db, "key", "val") // unchanged
}

// TestFlexDB_MergeDelete tests that merge can delete a key.
func TestFlexDB_MergeDelete(t *testing.T) {
	db := openTestDB(t, nil)

	mustPut(t, db, "key", "val")

	err := db.Merge("key", func(old []byte, exists bool) (newValue []byte, doWrite bool, doDelete bool) {
		doDelete = true
		return
	})
	if err != nil {
		t.Fatal(err)
	}
	mustMiss(t, db, "key")
}

// TestFlexDB_MergeWriteAndDeleteError tests that returning both doWrite=true
// and doDelete=true from a Merge callback produces an error.
func TestFlexDB_MergeWriteAndDeleteError(t *testing.T) {
	db := openTestDB(t, nil)

	mustPut(t, db, "key", "val")

	err := db.Merge("key", func(old []byte, exists bool) (newValue []byte, doWrite bool, doDelete bool) {
		newValue = []byte("conflict")
		doWrite = true
		doDelete = true
		return
	})
	if err == nil {
		t.Fatal("expected error when both doWrite and doDelete are true")
	}
	// Key should be unchanged.
	mustGet(t, db, "key", "val")
}

// TestFlexDB_MergeIncrement tests a counter-increment pattern with merge.
func TestFlexDB_MergeIncrement(t *testing.T) {
	db := openTestDB(t, nil)

	increment := func(old []byte, exists bool) (newValue []byte, doWrite bool, doDelete bool) {
		n := 0
		if exists {
			n = int(old[0])
		}
		n++
		newValue = []byte{byte(n)}
		doWrite = true
		return
	}

	for i := 0; i < 10; i++ {
		err := db.Merge("ctr", increment)
		if err != nil {
			t.Fatal(err)
		}
	}

	val, ok, err := db.Get("ctr")
	panicOn(err)
	if !ok || val[0] != 10 {
		t.Fatalf("expected counter=10, got %v ok=%v", val, ok)
	}
}

// TestFlexDB_MergeAfterDelete tests merge after key has been deleted.
func TestFlexDB_MergeAfterDelete(t *testing.T) {
	db := openTestDB(t, nil)

	mustPut(t, db, "key", "original")
	mustDelete(t, db, "key")

	err := db.Merge("key", func(old []byte, exists bool) (newValue []byte, doWrite bool, doDelete bool) {
		if exists {
			t.Fatal("expected key to not exist after delete")
		}
		newValue = []byte("revived")
		doWrite = true
		return
	})
	if err != nil {
		t.Fatal(err)
	}
	mustGet(t, db, "key", "revived")
}

// TestFlexDB_MergeManyKeys tests merge across many keys with some in FlexSpace.
func TestFlexDB_MergeManyKeys(t *testing.T) {
	db := openTestDB(t, nil)

	// Put 100 keys and flush to FlexSpace.
	for i := 0; i < 100; i++ {
		mustPut(t, db, fmt.Sprintf("key%03d", i), fmt.Sprintf("%d", i))
	}
	db.Sync()

	// Merge all keys: append suffix.
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("key%03d", i)
		err := db.Merge(k, func(old []byte, exists bool) (newValue []byte, doWrite bool, doDelete bool) {
			if !exists {
				t.Fatalf("key %s should exist", k)
			}
			newValue = append(old, []byte("+m")...)
			doWrite = true
			return
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Verify.
	for i := 0; i < 100; i++ {
		mustGet(t, db, fmt.Sprintf("key%03d", i), fmt.Sprintf("%d+m", i))
	}
}

// ====================== Ascend / Descend tests ======================

// populateDB inserts keys aaa..eee with values v:aaa..v:eee and optionally syncs.
func populateDB(t *testing.T, db *FlexDB, doSync bool) {
	t.Helper()
	for _, k := range []string{"aaa", "bbb", "ccc", "ddd", "eee"} {
		mustPut(t, db, k, "v:"+k)
	}
	if doSync {
		db.Sync()
	}
}

// collectAscend is a helper that collects all keys from db.Ascend.
func collectAscend(db *FlexDB, pivot string) []string {
	var keys []string
	db.View(func(roDB *ReadOnlyTx) error {
		roDB.Ascend(pivot, func(key string, value []byte) bool {
			keys = append(keys, key)
			return true
		})
		return nil
	})
	return keys
}

// collectDescend is a helper that collects all keys from db.Descend.
func collectDescend(db *FlexDB, pivot string) []string {
	var keys []string
	db.View(func(roDB *ReadOnlyTx) error {
		roDB.Descend(pivot, func(key string, value []byte) bool {
			keys = append(keys, key)
			return true
		})
		return nil
	})
	return keys
}

func expectKeys(t *testing.T, label string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %d keys %v, want %d keys %v", label, len(got), got, len(want), want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("%s: key[%d] = %q, want %q (full: %v)", label, i, got[i], want[i], got)
		}
	}
}

// TestFlexDB_AscendAll tests Ascend with nil pivot (all keys ascending).
func TestFlexDB_AscendAll(t *testing.T) {
	db := openTestDB(t, nil)
	populateDB(t, db, false)

	keys := collectAscend(db, "")
	expectKeys(t, "Ascend(nil)", keys, []string{"aaa", "bbb", "ccc", "ddd", "eee"})
}

// TestFlexDB_AscendPivot tests Ascend with a pivot key.
func TestFlexDB_AscendPivot(t *testing.T) {
	db := openTestDB(t, nil)
	populateDB(t, db, false)

	// Exact match pivot
	keys := collectAscend(db, "ccc")
	expectKeys(t, "Ascend(ccc)", keys, []string{"ccc", "ddd", "eee"})

	// Pivot between keys
	keys = collectAscend(db, "bbc")
	expectKeys(t, "Ascend(bbc)", keys, []string{"ccc", "ddd", "eee"})

	// Pivot past all keys
	keys = collectAscend(db, "zzz")
	expectKeys(t, "Ascend(zzz)", keys, nil)
}

// TestFlexDB_AscendAfterSync tests Ascend with data in FlexSpace.
func TestFlexDB_AscendAfterSync(t *testing.T) {
	db := openTestDB(t, nil)
	populateDB(t, db, true) // sync to FlexSpace

	keys := collectAscend(db, "bbb")
	expectKeys(t, "Ascend(bbb) after sync", keys, []string{"bbb", "ccc", "ddd", "eee"})
}

// TestFlexDB_AscendEarlyStop tests that returning false stops Ascend.
func TestFlexDB_AscendEarlyStop(t *testing.T) {
	db := openTestDB(t, nil)
	populateDB(t, db, false)

	var keys []string
	db.View(func(roDB *ReadOnlyTx) error {
		roDB.Ascend("", func(key string, value []byte) bool {
			keys = append(keys, key)
			return len(keys) < 3 // stop after 3
		})
		return nil
	})
	expectKeys(t, "Ascend stop at 3", keys, []string{"aaa", "bbb", "ccc"})
}

// TestFlexDB_DescendAll tests Descend with nil pivot (all keys descending).
func TestFlexDB_DescendAll(t *testing.T) {
	db := openTestDB(t, nil)
	populateDB(t, db, false)

	keys := collectDescend(db, "")
	expectKeys(t, "Descend(nil)", keys, []string{"eee", "ddd", "ccc", "bbb", "aaa"})
}

func TestFlexDB_DescendAll_big(t *testing.T) {
	db := openTestDB(t, nil)

	keys := generateBenchKeys()

	// Insert all keys
	batch := db.NewBatch()
	for i, k := range keys {
		batch.Set(string(k), k)
		if (i+1)%10000 == 0 {
			batch.Commit(false)
			batch = db.NewBatch()
		}
	}
	batch.Commit(false)

	slices.SortFunc(keys, bytes.Compare)
	slices.Reverse(keys) // compare to Descending

	db.View(func(roDB *ReadOnlyTx) error {
		it := roDB.NewIter()
		it.SeekLast()
		count := 0
		for it.Valid() {
			if it.Key() != string(keys[count]) {
				panicf("Descending at count = %v, want '%v'; got '%v'", count, string(keys[count]), it.Key())
			}
			count++
			it.Prev()
		}
		it.Close()
		return nil
	})
}

// TestFlexDB_DescendPivot tests Descend with a pivot key.
func TestFlexDB_DescendPivot(t *testing.T) {
	db := openTestDB(t, nil)
	populateDB(t, db, false)

	// Exact match pivot
	keys := collectDescend(db, "ccc")
	expectKeys(t, "Descend(ccc)", keys, []string{"ccc", "bbb", "aaa"})

	// Pivot between keys
	keys = collectDescend(db, "ccx")
	expectKeys(t, "Descend(ccx)", keys, []string{"ccc", "bbb", "aaa"})

	// Pivot before all keys
	keys = collectDescend(db, "aaa")
	expectKeys(t, "Descend(aaa)", keys, []string{"aaa"})

	keys = collectDescend(db, "a")
	expectKeys(t, "Descend(a)", keys, nil)
}

// TestFlexDB_DescendAfterSync tests Descend with data in FlexSpace.
func TestFlexDB_DescendAfterSync(t *testing.T) {
	db := openTestDB(t, nil)
	populateDB(t, db, true)

	keys := collectDescend(db, "ddd")
	expectKeys(t, "Descend(ddd) after sync", keys, []string{"ddd", "ccc", "bbb", "aaa"})
}

// TestFlexDB_DescendEarlyStop tests that returning false stops Descend.
func TestFlexDB_DescendEarlyStop(t *testing.T) {
	db := openTestDB(t, nil)
	populateDB(t, db, false)

	var keys []string
	db.View(func(roDB *ReadOnlyTx) error {
		roDB.Descend("", func(key string, value []byte) bool {
			keys = append(keys, key)
			return len(keys) < 2
		})
		return nil
	})
	expectKeys(t, "Descend stop at 2", keys, []string{"eee", "ddd"})
}

// ====================== CheckIntegrity tests ======================

// TestFlexDB_CheckIntegrity_Clean verifies CheckIntegrity passes on a
// freshly populated database (no vacuum, no corruption).
func TestFlexDB_CheckIntegrity_Clean(t *testing.T) {
	db := openTestDB(t, nil)

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("ikey%04d", i)
		mustPut(t, db, key, makeTestValue(30))
	}
	db.Sync()
	mustCheckIntegrity(t, db)

	// Overwrite some keys and delete others.
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("ikey%04d", i)
		mustPut(t, db, key, makeTestValue(35))
	}
	for i := 50; i < 70; i++ {
		key := fmt.Sprintf("ikey%04d", i)
		mustDelete(t, db, key)
	}
	db.Sync()
	mustCheckIntegrity(t, db)
}

// ====================== DeleteRange Tests ======================

func TestDeleteRange_Basic(t *testing.T) {
	db := openTestDB(t, nil)

	// Insert keys a..z
	for c := byte('a'); c <= 'z'; c++ {
		mustPut(t, db, string([]byte{c}), fmt.Sprintf("val_%c", c))
	}

	// Delete range [c, f] - both inclusive
	n, _, err := db.DeleteRange(true, "c", "f", true, true)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatalf("DeleteRange returned n=%d, want 4", n)
	}

	// Verify c-f are gone
	for c := byte('c'); c <= 'f'; c++ {
		mustMiss(t, db, string([]byte{c}))
	}
	// Verify a-b and g-z remain
	for c := byte('a'); c <= 'b'; c++ {
		mustGet(t, db, string([]byte{c}), fmt.Sprintf("val_%c", c))
	}
	for c := byte('g'); c <= 'z'; c++ {
		mustGet(t, db, string([]byte{c}), fmt.Sprintf("val_%c", c))
	}
}

func TestDeleteRange_EmptyRange(t *testing.T) {
	db := openTestDB(t, nil)

	mustPut(t, db, "a", "1")
	mustPut(t, db, "z", "2")

	// Delete range with no keys in it
	n, _, err := db.DeleteRange(true, "m", "n", true, true)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("DeleteRange returned n=%d, want 0", n)
	}

	mustGet(t, db, "a", "1")
	mustGet(t, db, "z", "2")
}

func TestDeleteRange_SingleKey(t *testing.T) {
	db := openTestDB(t, nil)

	mustPut(t, db, "x", "val")

	n, allGone, err := db.DeleteRange(true, "x", "x", true, true)
	if err != nil {
		t.Fatal(err)
	}
	// Only key in DB -> allGone fast path.
	if !allGone {
		t.Fatal("expected allGone=true")
	}
	if n != 0 {
		t.Fatalf("DeleteRange returned n=%d, want 0 (allGone)", n)
	}
	mustMiss(t, db, "x")
}

func TestDeleteRange_Idempotent(t *testing.T) {
	db := openTestDB(t, nil)

	for c := byte('a'); c <= 'e'; c++ {
		mustPut(t, db, string([]byte{c}), "v")
	}

	n1, _, err := db.DeleteRange(true, "b", "d", true, true)
	if err != nil {
		t.Fatal(err)
	}
	if n1 != 3 {
		t.Fatalf("first DeleteRange returned n=%d, want 3", n1)
	}

	// Second call on same range should find 0 non-tombstone keys
	n2, _, err := db.DeleteRange(true, "b", "d", true, true)
	if err != nil {
		t.Fatal(err)
	}
	if n2 != 0 {
		t.Fatalf("second DeleteRange returned n=%d, want 0", n2)
	}
}

func TestDeleteRange_FullRange(t *testing.T) {
	db := openTestDB(t, nil)

	for i := 0; i < 100; i++ {
		mustPut(t, db, fmt.Sprintf("key%03d", i), fmt.Sprintf("val%03d", i))
	}

	n, allGone, err := db.DeleteRange(true, "key000", "key999", true, true)
	if err != nil {
		t.Fatal(err)
	}
	// All keys in range -> allGone fast path.
	if !allGone {
		t.Fatal("expected allGone=true")
	}
	if n != 0 {
		t.Fatalf("DeleteRange returned n=%d, want 0 (allGone)", n)
	}

	for i := 0; i < 100; i++ {
		mustMiss(t, db, fmt.Sprintf("key%03d", i))
	}
}

func TestDeleteRange_InvalidRange(t *testing.T) {
	db := openTestDB(t, nil)

	_, _, err := db.DeleteRange(true, "z", "a", true, true)
	if err == nil {
		t.Fatal("expected error for begKey > endKey")
	}
}

func TestDeleteRange_LargeFlushToFlexSpace(t *testing.T) {
	db := openTestDB(t, nil)

	// Insert 10K keys, flush to FlexSpace, then delete middle range
	const total = 10000
	for i := 0; i < total; i++ {
		mustPut(t, db, fmt.Sprintf("k%05d", i), fmt.Sprintf("v%05d", i))
	}
	// Force flush to FlexSpace
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}

	// Delete middle 5K [k02500, k07499] both inclusive
	n, _, err := db.DeleteRange(true, "k02500", "k07499", true, true)
	if err != nil {
		t.Fatal(err)
	}
	if n != 5000 {
		t.Fatalf("DeleteRange returned n=%d, want 5000", n)
	}

	// Verify boundaries
	mustGet(t, db, "k02499", "v02499")
	mustMiss(t, db, "k02500")
	mustMiss(t, db, "k07499")
	mustGet(t, db, "k07500", "v07500")

	// Spot check some deleted keys
	for i := 2500; i < 7500; i += 500 {
		mustMiss(t, db, fmt.Sprintf("k%05d", i))
	}
	// Spot check surviving keys
	for i := 0; i < 2500; i += 500 {
		mustGet(t, db, fmt.Sprintf("k%05d", i), fmt.Sprintf("v%05d", i))
	}
	for i := 7500; i < total; i += 500 {
		mustGet(t, db, fmt.Sprintf("k%05d", i), fmt.Sprintf("v%05d", i))
	}
}

// TestDeleteRange_Bounds tests all four bound combinations: [,], [,), (,], (,)
func TestDeleteRange_Bounds(t *testing.T) {
	setup := func(t *testing.T) *FlexDB {
		t.Helper()
		db := openTestDB(t, nil)
		for c := byte('a'); c <= 'e'; c++ {
			mustPut(t, db, string([]byte{c}), string([]byte{c}))
		}
		return db
	}

	t.Run("both_inclusive", func(t *testing.T) {
		db := setup(t)
		n, _, err := db.DeleteRange(true, "b", "d", true, true)
		if err != nil {
			t.Fatal(err)
		}
		if n != 3 {
			t.Fatalf("n=%d, want 3", n)
		}
		mustGet(t, db, "a", "a")
		mustMiss(t, db, "b")
		mustMiss(t, db, "c")
		mustMiss(t, db, "d")
		mustGet(t, db, "e", "e")
	})

	t.Run("beg_inclusive_end_exclusive", func(t *testing.T) {
		db := setup(t)
		// [b, d) => deletes b, c
		n, _, err := db.DeleteRange(true, "b", "d", true, false)
		if err != nil {
			t.Fatal(err)
		}
		if n != 2 {
			t.Fatalf("n=%d, want 2", n)
		}
		mustGet(t, db, "a", "a")
		mustMiss(t, db, "b")
		mustMiss(t, db, "c")
		mustGet(t, db, "d", "d")
		mustGet(t, db, "e", "e")
	})

	t.Run("beg_exclusive_end_inclusive", func(t *testing.T) {
		db := setup(t)
		// (b, d] => deletes c, d
		n, _, err := db.DeleteRange(true, "b", "d", false, true)
		if err != nil {
			t.Fatal(err)
		}
		if n != 2 {
			t.Fatalf("n=%d, want 2", n)
		}
		mustGet(t, db, "a", "a")
		mustGet(t, db, "b", "b")
		mustMiss(t, db, "c")
		mustMiss(t, db, "d")
		mustGet(t, db, "e", "e")
	})

	t.Run("both_exclusive", func(t *testing.T) {
		db := setup(t)
		// (b, d) => deletes c only
		n, _, err := db.DeleteRange(true, "b", "d", false, false)
		if err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("n=%d, want 1", n)
		}
		mustGet(t, db, "a", "a")
		mustGet(t, db, "b", "b")
		mustMiss(t, db, "c")
		mustGet(t, db, "d", "d")
		mustGet(t, db, "e", "e")
	})

	t.Run("equal_keys_both_inclusive", func(t *testing.T) {
		db := setup(t)
		// [c, c] => deletes c
		n, _, err := db.DeleteRange(true, "c", "c", true, true)
		if err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("n=%d, want 1", n)
		}
		mustGet(t, db, "b", "b")
		mustMiss(t, db, "c")
		mustGet(t, db, "d", "d")
	})

	t.Run("equal_keys_beg_exclusive", func(t *testing.T) {
		db := setup(t)
		// (c, c] => empty range
		n, _, err := db.DeleteRange(true, "c", "c", false, true)
		if err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Fatalf("n=%d, want 0", n)
		}
		mustGet(t, db, "c", "c")
	})

	t.Run("equal_keys_end_exclusive", func(t *testing.T) {
		db := setup(t)
		// [c, c) => empty range
		n, _, err := db.DeleteRange(true, "c", "c", true, false)
		if err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Fatalf("n=%d, want 0", n)
		}
		mustGet(t, db, "c", "c")
	})

	t.Run("equal_keys_both_exclusive", func(t *testing.T) {
		db := setup(t)
		// (c, c) => empty range
		n, _, err := db.DeleteRange(true, "c", "c", false, false)
		if err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Fatalf("n=%d, want 0", n)
		}
		mustGet(t, db, "c", "c")
	})
}

// TestDeleteRange_BoundsFlexSpace tests bound combos after flushing to FlexSpace.
func TestDeleteRange_BoundsFlexSpace(t *testing.T) {
	setup := func(t *testing.T) *FlexDB {
		t.Helper()
		db := openTestDB(t, nil)
		for c := byte('a'); c <= 'e'; c++ {
			mustPut(t, db, string([]byte{c}), string([]byte{c}))
		}
		if err := db.Sync(); err != nil {
			t.Fatal(err)
		}
		return db
	}

	t.Run("both_inclusive", func(t *testing.T) {
		db := setup(t)
		n, _, err := db.DeleteRange(true, "b", "d", true, true)
		if err != nil {
			t.Fatal(err)
		}
		if n != 3 {
			t.Fatalf("n=%d, want 3", n)
		}
		mustGet(t, db, "a", "a")
		mustMiss(t, db, "b")
		mustMiss(t, db, "c")
		mustMiss(t, db, "d")
		mustGet(t, db, "e", "e")
	})

	t.Run("beg_inclusive_end_exclusive", func(t *testing.T) {
		db := setup(t)
		n, _, err := db.DeleteRange(true, "b", "d", true, false)
		if err != nil {
			t.Fatal(err)
		}
		if n != 2 {
			t.Fatalf("n=%d, want 2", n)
		}
		mustGet(t, db, "a", "a")
		mustMiss(t, db, "b")
		mustMiss(t, db, "c")
		mustGet(t, db, "d", "d")
		mustGet(t, db, "e", "e")
	})

	t.Run("beg_exclusive_end_inclusive", func(t *testing.T) {
		db := setup(t)
		n, _, err := db.DeleteRange(true, "b", "d", false, true)
		if err != nil {
			t.Fatal(err)
		}
		if n != 2 {
			t.Fatalf("n=%d, want 2", n)
		}
		mustGet(t, db, "a", "a")
		mustGet(t, db, "b", "b")
		mustMiss(t, db, "c")
		mustMiss(t, db, "d")
		mustGet(t, db, "e", "e")
	})

	t.Run("both_exclusive", func(t *testing.T) {
		db := setup(t)
		n, _, err := db.DeleteRange(true, "b", "d", false, false)
		if err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("n=%d, want 1", n)
		}
		mustGet(t, db, "a", "a")
		mustGet(t, db, "b", "b")
		mustMiss(t, db, "c")
		mustGet(t, db, "d", "d")
		mustGet(t, db, "e", "e")
	})
}

// TestDeleteRange_AllGone tests the fast "delete all" path and that
// the DB is fully usable afterward.
func TestDeleteRange_AllGone(t *testing.T) {

	t.Run("memtable_only", func(t *testing.T) {
		db := openTestDB(t, nil)
		for c := byte('a'); c <= 'z'; c++ {
			mustPut(t, db, string([]byte{c}), string([]byte{c}))
		}
		_, allGone, err := db.DeleteRange(true, "a", "z", true, true)
		if err != nil {
			t.Fatal(err)
		}
		if !allGone {
			t.Fatal("expected allGone=true")
		}
		// DB should be empty and usable.
		for c := byte('a'); c <= 'z'; c++ {
			mustMiss(t, db, string([]byte{c}))
		}
		// Can write new data.
		mustPut(t, db, "new", "data")
		mustGet(t, db, "new", "data")
	})

	t.Run("after_flush", func(t *testing.T) {
		db := openTestDB(t, nil)
		for i := 0; i < 500; i++ {
			mustPut(t, db, fmt.Sprintf("k%04d", i), fmt.Sprintf("v%04d", i))
		}
		if err := db.Sync(); err != nil {
			t.Fatal(err)
		}
		_, allGone, err := db.DeleteRange(true, "k0000", "k9999", true, true)
		if err != nil {
			t.Fatal(err)
		}
		if !allGone {
			t.Fatal("expected allGone=true")
		}
		for i := 0; i < 500; i++ {
			mustMiss(t, db, fmt.Sprintf("k%04d", i))
		}
		mustPut(t, db, "after", "wipe")
		mustGet(t, db, "after", "wipe")
	})

	t.Run("superset_range", func(t *testing.T) {
		// Range much larger than actual keys - still triggers allGone.
		db := openTestDB(t, nil)
		mustPut(t, db, "m", "1")
		mustPut(t, db, "n", "2")
		_, allGone, err := db.DeleteRange(true, "a", "z", true, true)
		if err != nil {
			t.Fatal(err)
		}
		if !allGone {
			t.Fatal("expected allGone=true")
		}
		mustMiss(t, db, "m")
		mustMiss(t, db, "n")
	})

	t.Run("not_allGone_partial", func(t *testing.T) {
		// Partial delete should NOT trigger allGone.
		db := openTestDB(t, nil)
		mustPut(t, db, "a", "1")
		mustPut(t, db, "b", "2")
		mustPut(t, db, "c", "3")
		n, allGone, err := db.DeleteRange(true, "b", "b", true, true)
		if err != nil {
			t.Fatal(err)
		}
		if allGone {
			t.Fatal("expected allGone=false for partial delete")
		}
		if n != 1 {
			t.Fatalf("n=%d, want 1", n)
		}
		mustGet(t, db, "a", "1")
		mustMiss(t, db, "b")
		mustGet(t, db, "c", "3")
	})

	t.Run("empty_db", func(t *testing.T) {
		// Deleting range from empty DB should be allGone (no-op reinit).
		db := openTestDB(t, nil)
		_, allGone, err := db.DeleteRange(true, "a", "z", true, true)
		if err != nil {
			t.Fatal(err)
		}
		if !allGone {
			t.Fatal("expected allGone=true for empty DB")
		}
	})
}

// ====================== DeleteRange includeLarge Tests ======================

func TestDeleteRange_SkipLargeValues(t *testing.T) {
	bigVal := makeTestValue(200)
	smallVal := "small"

	t.Run("memtable_only", func(t *testing.T) {
		db := openTestDB(t, nil)
		mustPut(t, db, "a", smallVal)
		mustPut(t, db, "b", bigVal)
		mustPut(t, db, "c", smallVal)
		mustPut(t, db, "d", bigVal)
		mustPut(t, db, "e", smallVal)

		// Delete range [a, e], skip large values.
		n, allGone, err := db.DeleteRange(false, "a", "e", true, true)
		if err != nil {
			t.Fatal(err)
		}
		if allGone {
			t.Fatal("expected allGone=false when skipping large values")
		}
		if n != 3 {
			t.Fatalf("n=%d, want 3 (only small-value keys)", n)
		}
		// Small keys deleted.
		mustMiss(t, db, "a")
		mustMiss(t, db, "c")
		mustMiss(t, db, "e")
		// Large keys survive.
		val, ok, err := db.Get("b")
		panicOn(err)
		if !ok {
			t.Fatal("large key 'b' should survive")
		}
		if string(val) != bigVal {
			t.Fatalf("large key 'b' wrong value: got %d bytes", len(val))
		}
		val, ok, err = db.Get("d")
		panicOn(err)
		if !ok {
			t.Fatal("large key 'd' should survive")
		}
		if string(val) != bigVal {
			t.Fatalf("large key 'd' wrong value: got %d bytes", len(val))
		}
	})

	t.Run("after_flush", func(t *testing.T) {
		db := openTestDB(t, nil)
		mustPut(t, db, "k1", smallVal)
		mustPut(t, db, "k2", bigVal)
		mustPut(t, db, "k3", smallVal)
		if err := db.Sync(); err != nil {
			t.Fatal(err)
		}

		n, _, err := db.DeleteRange(false, "k1", "k3", true, true)
		if err != nil {
			t.Fatal(err)
		}
		if n != 2 {
			t.Fatalf("n=%d, want 2", n)
		}
		mustMiss(t, db, "k1")
		mustMiss(t, db, "k3")
		val, ok, err := db.Get("k2")
		panicOn(err)
		if !ok {
			t.Fatal("large key 'k2' should survive")
		}
		if string(val) != bigVal {
			t.Fatalf("large key 'k2' wrong value")
		}
	})

	t.Run("includeLarge_true_deletes_all", func(t *testing.T) {
		db := openTestDB(t, nil)
		mustPut(t, db, "x", bigVal)
		mustPut(t, db, "y", smallVal)

		_, allGone, err := db.DeleteRange(true, "x", "y", true, true)
		if err != nil {
			t.Fatal(err)
		}
		if !allGone {
			t.Fatal("expected allGone=true")
		}
		mustMiss(t, db, "x")
		mustMiss(t, db, "y")
	})

	t.Run("all_large_no_deletes", func(t *testing.T) {
		db := openTestDB(t, nil)
		mustPut(t, db, "a", bigVal)
		mustPut(t, db, "b", bigVal)

		n, allGone, err := db.DeleteRange(false, "a", "b", true, true)
		if err != nil {
			t.Fatal(err)
		}
		if allGone {
			t.Fatal("expected allGone=false")
		}
		if n != 0 {
			t.Fatalf("n=%d, want 0 (all keys are large)", n)
		}
		// Both keys survive.
		_, ok, err := db.Get("a")
		panicOn(err)
		if !ok {
			t.Fatal("key 'a' should survive")
		}
		_, ok, err = db.Get("b")
		panicOn(err)
		if !ok {
			t.Fatal("key 'b' should survive")
		}
	})
}

// ====================== Clear Tests ======================

func TestClear(t *testing.T) {

	t.Run("clear_all", func(t *testing.T) {
		db := openTestDB(t, nil)
		for i := 0; i < 50; i++ {
			mustPut(t, db, fmt.Sprintf("k%03d", i), fmt.Sprintf("v%03d", i))
		}
		allGone, err := db.Clear(true)
		if err != nil {
			t.Fatal(err)
		}
		if !allGone {
			t.Fatal("expected allGone=true")
		}
		for i := 0; i < 50; i++ {
			mustMiss(t, db, fmt.Sprintf("k%03d", i))
		}
		// DB is usable after clear.
		mustPut(t, db, "new", "data")
		mustGet(t, db, "new", "data")
	})

	t.Run("clear_all_after_flush", func(t *testing.T) {
		db := openTestDB(t, nil)
		for i := 0; i < 50; i++ {
			mustPut(t, db, fmt.Sprintf("k%03d", i), fmt.Sprintf("v%03d", i))
		}
		if err := db.Sync(); err != nil {
			t.Fatal(err)
		}
		allGone, err := db.Clear(true)
		if err != nil {
			t.Fatal(err)
		}
		if !allGone {
			t.Fatal("expected allGone=true")
		}
		for i := 0; i < 50; i++ {
			mustMiss(t, db, fmt.Sprintf("k%03d", i))
		}
	})

	t.Run("clear_small_only", func(t *testing.T) {
		bigVal := makeTestValue(200)
		db := openTestDB(t, nil)
		mustPut(t, db, "a", "small1")
		mustPut(t, db, "b", bigVal)
		mustPut(t, db, "c", "small2")
		mustPut(t, db, "d", bigVal)

		allGone, err := db.Clear(false)
		if err != nil {
			t.Fatal(err)
		}
		if allGone {
			t.Fatal("expected allGone=false when skipping large values")
		}
		mustMiss(t, db, "a")
		mustMiss(t, db, "c")
		val, ok, err := db.Get("b")
		panicOn(err)
		if !ok {
			t.Fatal("large key 'b' should survive")
		}
		if string(val) != bigVal {
			t.Fatalf("wrong value for 'b'")
		}
		val, ok, err = db.Get("d")
		panicOn(err)
		if !ok {
			t.Fatal("large key 'd' should survive")
		}
		if string(val) != bigVal {
			t.Fatalf("wrong value for 'd'")
		}
	})

	t.Run("clear_small_after_flush", func(t *testing.T) {
		bigVal := makeTestValue(200)
		db := openTestDB(t, nil)
		mustPut(t, db, "x", "tiny")
		mustPut(t, db, "y", bigVal)
		if err := db.Sync(); err != nil {
			t.Fatal(err)
		}
		allGone, err := db.Clear(false)
		if err != nil {
			t.Fatal(err)
		}
		if allGone {
			t.Fatal("expected allGone=false")
		}
		mustMiss(t, db, "x")
		val, ok, err := db.Get("y")
		panicOn(err)
		if !ok {
			t.Fatal("large key 'y' should survive")
		}
		if string(val) != bigVal {
			t.Fatalf("wrong value for 'y'")
		}
	})

	t.Run("clear_empty_db", func(t *testing.T) {
		db := openTestDB(t, nil)
		allGone, err := db.Clear(true)
		if err != nil {
			t.Fatal(err)
		}
		if !allGone {
			t.Fatal("expected allGone=true for empty DB")
		}
	})
}

func TestLen(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		db := openTestDB(t, nil)
		if n := db.Len(); n != 0 {
			t.Fatalf("empty DB Len() = %d, want 0", n)
		}
		big, small := db.LenBigSmall()
		if big != 0 || small != 0 {
			t.Fatalf("empty DB LenBigSmall() = (%d,%d), want (0,0)", big, small)
		}
	})

	t.Run("put_small_keys", func(t *testing.T) {
		db := openTestDB(t, nil)
		N := 50
		for i := 0; i < N; i++ {
			db.Put(fmt.Sprintf("key%04d", i), []byte("val"))
		}
		if n := db.Len(); n != int64(N) {
			t.Fatalf("Len() = %d, want %d", n, N)
		}
		big, small := db.LenBigSmall()
		if big != 0 || small != int64(N) {
			t.Fatalf("LenBigSmall() = (%d,%d), want (0,%d)", big, small, N)
		}
	})

	t.Run("put_large_values", func(t *testing.T) {
		db := openTestDB(t, nil)
		largeVal := make([]byte, 128) // > vlogInlineThreshold (64)
		for i := range largeVal {
			largeVal[i] = byte(i)
		}
		N := 10
		for i := 0; i < N; i++ {
			db.Put(fmt.Sprintf("big%04d", i), largeVal)
		}
		if n := db.Len(); n != int64(N) {
			t.Fatalf("Len() = %d, want %d", n, N)
		}
		big, small := db.LenBigSmall()
		if big != int64(N) || small != 0 {
			t.Fatalf("LenBigSmall() = (%d,%d), want (%d,0)", big, small, N)
		}
	})

	t.Run("overwrite_small_to_big", func(t *testing.T) {
		db := openTestDB(t, nil)
		key := "mykey"
		db.Put(key, []byte("small"))
		if big, small := db.LenBigSmall(); big != 0 || small != 1 {
			t.Fatalf("after small put: (%d,%d), want (0,1)", big, small)
		}
		largeVal := make([]byte, 128)
		db.Put(key, largeVal)
		if n := db.Len(); n != 1 {
			t.Fatalf("Len() = %d, want 1", n)
		}
		big, small := db.LenBigSmall()
		if big != 1 || small != 0 {
			t.Fatalf("after big overwrite: (%d,%d), want (1,0)", big, small)
		}
	})

	t.Run("overwrite_big_to_small", func(t *testing.T) {
		db := openTestDB(t, nil)
		key := "mykey"
		largeVal := make([]byte, 128)
		db.Put(key, largeVal)
		if big, small := db.LenBigSmall(); big != 1 || small != 0 {
			t.Fatalf("after big put: (%d,%d), want (1,0)", big, small)
		}
		db.Put(key, []byte("small"))
		if n := db.Len(); n != 1 {
			t.Fatalf("Len() = %d, want 1", n)
		}
		big, small := db.LenBigSmall()
		if big != 0 || small != 1 {
			t.Fatalf("after small overwrite: (%d,%d), want (0,1)", big, small)
		}
	})

	t.Run("delete_key", func(t *testing.T) {
		db := openTestDB(t, nil)
		db.Put("a", []byte("1"))
		db.Put("b", []byte("2"))
		db.Put("c", []byte("3"))
		if n := db.Len(); n != 3 {
			t.Fatalf("Len() = %d, want 3", n)
		}
		db.Delete("b")
		if n := db.Len(); n != 2 {
			t.Fatalf("after Del: Len() = %d, want 2", n)
		}
	})

	t.Run("delete_nonexistent", func(t *testing.T) {
		db := openTestDB(t, nil)
		db.Put("a", []byte("1"))
		db.Delete("nonexistent")
		if n := db.Len(); n != 1 {
			t.Fatalf("Len() = %d, want 1", n)
		}
	})

	t.Run("tombstone_overwrites_tombstone", func(t *testing.T) {
		db := openTestDB(t, nil)
		db.Put("a", []byte("1"))
		db.Delete("a")
		if n := db.Len(); n != 0 {
			t.Fatalf("after first Del: Len() = %d, want 0", n)
		}
		db.Delete("a") // delete again
		if n := db.Len(); n != 0 {
			t.Fatalf("after second Del: Len() = %d, want 0", n)
		}
	})

	t.Run("reinsert_after_delete", func(t *testing.T) {
		db := openTestDB(t, nil)
		db.Put("a", []byte("1"))
		db.Delete("a")
		if n := db.Len(); n != 0 {
			t.Fatalf("after Del: Len() = %d, want 0", n)
		}
		db.Put("a", []byte("2"))
		if n := db.Len(); n != 1 {
			t.Fatalf("after re-Put: Len() = %d, want 1", n)
		}
	})

	t.Run("clear", func(t *testing.T) {
		db := openTestDB(t, nil)
		for i := 0; i < 20; i++ {
			db.Put(fmt.Sprintf("k%02d", i), []byte("v"))
		}
		if n := db.Len(); n != 20 {
			t.Fatalf("Len() = %d, want 20", n)
		}
		db.Clear(false)
		if n := db.Len(); n != 0 {
			t.Fatalf("after Clear: Len() = %d, want 0", n)
		}
		big, small := db.LenBigSmall()
		if big != 0 || small != 0 {
			t.Fatalf("after Clear: LenBigSmall() = (%d,%d), want (0,0)", big, small)
		}
	})

	t.Run("after_flush", func(t *testing.T) {
		db := openTestDB(t, nil)
		N := 100
		for i := 0; i < N; i++ {
			db.Put(fmt.Sprintf("key%04d", i), []byte("val"))
		}
		db.Sync()
		if n := db.Len(); n != int64(N) {
			t.Fatalf("after Sync: Len() = %d, want %d", n, N)
		}
		// Add more after flush
		for i := N; i < N+20; i++ {
			db.Put(fmt.Sprintf("key%04d", i), []byte("val"))
		}
		if n := db.Len(); n != int64(N+20) {
			t.Fatalf("after more puts: Len() = %d, want %d", n, N+20)
		}
	})

	t.Run("mixed_memtable_and_flexspace", func(t *testing.T) {
		db := openTestDB(t, nil)
		// Put keys, flush to FlexSpace
		for i := 0; i < 50; i++ {
			db.Put(fmt.Sprintf("key%04d", i), []byte("val"))
		}
		db.Sync()
		// Overwrite some from memtable (shadow FlexSpace)
		for i := 0; i < 10; i++ {
			db.Put(fmt.Sprintf("key%04d", i), []byte("newval"))
		}
		// Delete some
		for i := 10; i < 15; i++ {
			db.Delete(fmt.Sprintf("key%04d", i))
		}
		// Add new keys in memtable
		for i := 50; i < 60; i++ {
			db.Put(fmt.Sprintf("key%04d", i), []byte("val"))
		}
		// Expected: 50 original - 5 deleted + 10 new = 55
		if n := db.Len(); n != 55 {
			t.Fatalf("mixed: Len() = %d, want 55", n)
		}
	})

	t.Run("batch", func(t *testing.T) {
		db := openTestDB(t, nil)
		b := db.NewBatch()
		for i := 0; i < 25; i++ {
			b.Set(fmt.Sprintf("bk%04d", i), []byte("bv"))
		}
		b.Commit(false)
		if n := db.Len(); n != 25 {
			t.Fatalf("after batch: Len() = %d, want 25", n)
		}

		// Batch with overwrites
		b2 := db.NewBatch()
		for i := 0; i < 10; i++ {
			b.Set(fmt.Sprintf("bk%04d", i), []byte("updated"))
		}
		b2.Commit(false)
		if n := db.Len(); n != 25 {
			t.Fatalf("after overwrite batch: Len() = %d, want 25", n)
		}
	})

	t.Run("delete_range", func(t *testing.T) {
		db := openTestDB(t, nil)
		for i := 0; i < 20; i++ {
			db.Put(fmt.Sprintf("key%04d", i), []byte("val"))
		}
		if n := db.Len(); n != 20 {
			t.Fatalf("Len() = %d, want 20", n)
		}
		// Delete range [key0005, key0014] - should delete 10 keys (0005..0014)
		db.DeleteRange(false, "key0005", "key0014", true, true)
		want := int64(10) // 20 - 10 = 10
		if n := db.Len(); n != want {
			t.Fatalf("after DeleteRange: Len() = %d, want %d", n, want)
		}
	})
}

func TestFlexDB_NilValuePreservation(t *testing.T) {
	db := openTestDB(t, nil)

	// Put with nil value - should store a live key, NOT delete
	err := db.Put("setkey", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Get should return (nil, true, nil) - found, with nil value
	val, found, err := db.Get("setkey")
	panicOn(err)
	if !found {
		t.Fatal("nil-value key should be found")
	}
	if val != nil {
		t.Fatalf("expected nil value, got %v", val)
	}

	// Len should count it
	if db.Len() != 1 {
		t.Fatalf("expected Len=1, got %d", db.Len())
	}

	// Now actually delete it
	err = db.Delete("setkey")
	if err != nil {
		t.Fatal(err)
	}

	// Should be gone
	_, found, err = db.Get("setkey")
	panicOn(err)
	if found {
		t.Fatal("deleted key should not be found")
	}
	if db.Len() != 0 {
		t.Fatalf("expected Len=0, got %d", db.Len())
	}

	// Put nil again, Sync, and read back from FlexSpace
	err = db.Put("setkey2", nil)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Sync()
	if err != nil {
		t.Fatal(err)
	}

	val, found, err = db.Get("setkey2")
	panicOn(err)
	if !found {
		t.Fatal("nil-value key should survive Sync")
	}
	if val != nil {
		t.Fatalf("expected nil value after Sync, got %v", val)
	}

	// Verify empty-value []byte{} also works and is distinct conceptually
	err = db.Put("emptykey", []byte{})
	if err != nil {
		t.Fatal(err)
	}
	val, found, err = db.Get("emptykey")
	panicOn(err)
	if !found {
		t.Fatal("empty-value key should be found")
	}
	// Both nil and empty return len=0, but both are live
	if db.Len() != 2 {
		t.Fatalf("expected Len=2, got %d", db.Len())
	}
}
