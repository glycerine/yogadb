package yogadb

import (
	"bytes"
	"fmt"
	"slices"
	"testing"

	"github.com/glycerine/vfs"
)

// ====================== Helpers ======================

func openTestDB(t *testing.T, cfg *Config) (*FlexDB, vfs.FS) {
	t.Helper()

	// get the real filesystem, or a mock.
	fs, dir := newTestFS(t)
	if cfg == nil {
		cfg = &Config{}
	}
	cfg.FS = fs
	//vv("openTestDB has fs = '%#v'", fs)

	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		//vv("err = '%v'", err)
		t.Fatalf("OpenFlexDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	return db, fs
}

func openTestDBreportSize(t *testing.T, cfg *Config) *FlexDB {
	t.Helper()
	fs, dir := newTestFS(t)
	if cfg == nil {
		cfg = &Config{}
	}
	cfg.FS = fs
	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatalf("OpenFlexDB: %v", err)
	}
	t.Cleanup(func() {
		alwaysPrintf("%v end of test, YogaDB DirSize='%v'", t.Name(), MustDirSize(fs, dir))
		db.Close()
	})
	return db
}

func openTestDBAt(fs vfs.FS, t *testing.T, dir string, cfg *Config) *FlexDB {
	t.Helper()
	if cfg == nil {
		cfg = &Config{}
	}
	cfg.FS = fs
	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatalf("OpenFlexDB: %v", err)
	}
	return db
}

func mustPut(t *testing.T, db *FlexDB, key, value string) {
	t.Helper()
	if err := db.Put([]byte(key), []byte(value)); err != nil {
		t.Fatalf("Put(%q, %q): %v", key, value, err)
	}
}

func mustDelete(t *testing.T, db *FlexDB, key string) {
	t.Helper()
	if err := db.Delete([]byte(key)); err != nil {
		t.Fatalf("Delete(%q): %v", key, err)
	}
}

func mustGet(t *testing.T, db *FlexDB, key, wantValue string) {
	t.Helper()
	val, ok := db.Get([]byte(key))
	if !ok {
		t.Fatalf("Get(%q): not found (want %q)", key, wantValue)
	}
	if string(val) != wantValue {
		t.Fatalf("Get(%q) = %q, want %q", key, val, wantValue)
	}
}

func mustMiss(t *testing.T, db *FlexDB, key string) {
	t.Helper()
	val, ok := db.Get([]byte(key))
	if ok {
		t.Fatalf("Get(%q) = %q, want miss", key, val)
	}
}

// ====================== Tests ======================

// TestFlexDB_BasicMemtable tests Put/Get while data is still in memtable.
func TestFlexDB_BasicMemtable(t *testing.T) {
	db, _ := openTestDB(t, nil)

	mustPut(t, db, "hello", "world")
	mustGet(t, db, "hello", "world")
	mustMiss(t, db, "missing")
}

// TestFlexDB_Update tests that a second Put replaces the first.
func TestFlexDB_Update(t *testing.T) {
	db, _ := openTestDB(t, nil)

	mustPut(t, db, "key", "val1")
	mustGet(t, db, "key", "val1")

	mustPut(t, db, "key", "val2")
	mustGet(t, db, "key", "val2")
}

// TestFlexDB_Delete tests deletion via tombstone.
func TestFlexDB_Delete(t *testing.T) {
	db, _ := openTestDB(t, nil)

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
	db, _ := openTestDB(t, nil)

	mustPut(t, db, "key1", "val1")
	mustPut(t, db, "key2", "val2")
	db.Sync()

	mustGet(t, db, "key1", "val1")
	mustGet(t, db, "key2", "val2")
}

// TestFlexDB_Persistence tests that data survives Close() + reopen.
func TestFlexDB_Persistence(t *testing.T) {

	fs, dir := newTestFS(t)

	db := openTestDBAt(fs, t, dir, nil)
	mustPut(t, db, "key1", "val1")
	mustPut(t, db, "key2", "val2")
	mustPut(t, db, "key3", "val3")
	db.Sync()
	db.Close()

	db2 := openTestDBAt(fs, t, dir, nil)
	defer db2.Close()
	mustGet(t, db2, "key1", "val1")
	mustGet(t, db2, "key2", "val2")
	mustGet(t, db2, "key3", "val3")
}

// TestFlexDB_PersistenceWAL tests that WAL data survives crash (data in WAL, not FlexSpace).
func TestFlexDB_PersistenceWAL(t *testing.T) {
	fs, dir := newTestFS(t)

	db := openTestDBAt(fs, t, dir, nil)
	mustPut(t, db, "a", "1")
	mustPut(t, db, "b", "2")
	// Flush WAL but don't Sync() (don't flush to FlexSpace).
	// WAL is written on every Put, so data is in WAL.
	db.memtables[db.activeMT].logFlush()
	// Close without Sync - but Close() itself flushes active memtable
	db.Close()

	db2 := openTestDBAt(fs, t, dir, nil)
	defer db2.Close()
	mustGet(t, db2, "a", "1")
	mustGet(t, db2, "b", "2")
}

// TestFlexDB_DeletePersistence tests that deleted keys don't reappear after reopen.
func TestFlexDB_DeletePersistence(t *testing.T) {
	fs, dir := newTestFS(t)

	db := openTestDBAt(fs, t, dir, nil)
	mustPut(t, db, "k1", "v1")
	mustPut(t, db, "k2", "v2")
	db.Sync()

	mustDelete(t, db, "k1")
	db.Sync()
	db.Close()

	db2 := openTestDBAt(fs, t, dir, nil)
	defer db2.Close()
	mustMiss(t, db2, "k1")
	mustGet(t, db2, "k2", "v2")
}

// TestFlexDB_ManyKeys inserts many keys (more than one FlexSpace interval),
// forcing interval splits and verifying correctness.
func TestFlexDB_ManyKeys(t *testing.T) {
	db, _ := openTestDB(t, nil)

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

// TestFlexDB_ManyKeysPersistence inserts many keys, closes and reopens.
func TestFlexDB_ManyKeysPersistence(t *testing.T) {
	fs, dir := newTestFS(t)

	const N = 200
	keys := make([]string, N)

	db := openTestDBAt(fs, t, dir, nil)
	for i := 0; i < N; i++ {
		keys[i] = fmt.Sprintf("key%06d", i)
		mustPut(t, db, keys[i], fmt.Sprintf("val%06d", i))
	}
	db.Sync()
	db.Close()

	db2 := openTestDBAt(fs, t, dir, nil)
	defer db2.Close()
	for i, k := range keys {
		mustGet(t, db2, k, fmt.Sprintf("val%06d", i))
	}
}

// TestFlexDB_UpdateAfterSync updates keys after they've been flushed to FlexSpace.
func TestFlexDB_UpdateAfterSync(t *testing.T) {
	db, _ := openTestDB(t, nil)

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
	db, _ := openTestDB(t, nil)

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
		{Key: []byte("hello"), Value: []byte("world"), Hlc: 12345},
		{Key: []byte(""), Value: []byte(""), Hlc: 0},
		{Key: []byte("a"), Value: nil, Hlc: 999}, // tombstone
		{Key: make([]byte, 100), Value: make([]byte, 200), Hlc: 0x7FFFFFFFFFFFFFFF},
		// VPtr case with HLC
		{Key: []byte("big"), Vptr: VPtr{Offset: 1024, Length: 256}, HasVPtr: true, Hlc: 42},
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
		if !bytes.Equal(got.Key, kv.Key) {
			t.Fatalf("key mismatch: got %q, want %q", got.Key, kv.Key)
		}
		if !bytes.Equal(got.Value, kv.Value) {
			t.Fatalf("value mismatch: got %q, want %q", got.Value, kv.Value)
		}
		if got.Hlc != kv.Hlc {
			t.Fatalf("HLC mismatch: got %v, want %v", got.Hlc, kv.Hlc)
		}
		if got.HasVPtr != kv.HasVPtr {
			t.Fatalf("HasVPtr mismatch: got %v, want %v", got.HasVPtr, kv.HasVPtr)
		}
		if got.HasVPtr && got.Vptr != kv.Vptr {
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
	kv := KV{Key: []byte("testkey"), Value: []byte("testvalue"), Hlc: 42}
	buf := kv128Encode(nil, kv)

	// Verify clean decode works
	_, _, ok := kv128Decode(buf)
	if !ok {
		t.Fatal("clean kv128Decode failed")
	}

	// Flip a bit in the key — CRC should catch it
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
	tomb := KV{Key: []byte("delme"), Hlc: 7}
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

// TestFlexDB_FullRoundtrip does a comprehensive write/sync/close/reopen/verify cycle.
func TestFlexDB_FullRoundtrip(t *testing.T) {
	fs, dir := newTestFS(t)

	// Phase 1: write
	db := openTestDBAt(fs, t, dir, nil)
	const N = 100
	for i := 0; i < N; i++ {
		mustPut(t, db, fmt.Sprintf("key%05d", i), fmt.Sprintf("val%05d", i))
	}
	db.Sync()

	// Phase 2: update half
	for i := 0; i < N; i += 2 {
		mustPut(t, db, fmt.Sprintf("key%05d", i), fmt.Sprintf("new%05d", i))
	}
	db.Sync()

	// Phase 3: delete some
	for i := 0; i < N; i += 5 {
		mustDelete(t, db, fmt.Sprintf("key%05d", i))
	}
	db.Sync()
	db.Close()

	// Phase 4: reopen and verify
	db2 := openTestDBAt(fs, t, dir, nil)
	defer db2.Close()

	for i := 0; i < N; i++ {
		k := fmt.Sprintf("key%05d", i)
		if i%5 == 0 {
			mustMiss(t, db2, k)
		} else if i%2 == 0 {
			mustGet(t, db2, k, fmt.Sprintf("new%05d", i))
		} else {
			mustGet(t, db2, k, fmt.Sprintf("val%05d", i))
		}
	}
}

// TestFlexDB_MultipleSync tests multiple Sync() calls in sequence.
func TestFlexDB_MultipleSync(t *testing.T) {
	db, _ := openTestDB(t, nil)

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
	db, _ := openTestDB(t, nil)

	mustMiss(t, db, "anything")

	it := db.NewIter()
	it.SeekToFirst()
	if it.Valid() {
		t.Fatalf("iterator on empty DB should not be valid, got key %q", it.Key())
	}
	it.Close()
}

// TestFlexDB_OverwriteLoop tests repeatedly overwriting the same key.
func TestFlexDB_OverwriteLoop(t *testing.T) {
	db, _ := openTestDB(t, nil)

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
	db, _ := openTestDB(t, nil)

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
	db, _ := openTestDB(t, nil)

	err := db.Merge([]byte("counter"), func(old []byte, exists bool) ([]byte, bool) {
		if exists {
			t.Fatal("expected key to not exist")
		}
		return []byte("1"), true
	})
	if err != nil {
		t.Fatal(err)
	}
	mustGet(t, db, "counter", "1")
}

// TestFlexDB_MergeExistingMemtable tests merge on a key in the active memtable.
func TestFlexDB_MergeExistingMemtable(t *testing.T) {
	db, _ := openTestDB(t, nil)

	mustPut(t, db, "counter", "5")

	err := db.Merge([]byte("counter"), func(old []byte, exists bool) ([]byte, bool) {
		if !exists {
			t.Fatal("expected key to exist")
		}
		if string(old) != "5" {
			t.Fatalf("expected old=5, got %q", old)
		}
		return []byte("6"), true
	})
	if err != nil {
		t.Fatal(err)
	}
	mustGet(t, db, "counter", "6")
}

// TestFlexDB_MergeExistingFlexSpace tests merge on a key flushed to FlexSpace.
func TestFlexDB_MergeExistingFlexSpace(t *testing.T) {
	db, _ := openTestDB(t, nil)

	mustPut(t, db, "counter", "10")
	db.Sync()

	err := db.Merge([]byte("counter"), func(old []byte, exists bool) ([]byte, bool) {
		if !exists {
			t.Fatal("expected key to exist in FlexSpace")
		}
		if string(old) != "10" {
			t.Fatalf("expected old=10, got %q", old)
		}
		return []byte("11"), true
	})
	if err != nil {
		t.Fatal(err)
	}
	mustGet(t, db, "counter", "11")
}

// TestFlexDB_MergeNoWrite tests that merge can be a no-op.
func TestFlexDB_MergeNoWrite(t *testing.T) {
	db, _ := openTestDB(t, nil)

	mustPut(t, db, "key", "val")

	err := db.Merge([]byte("key"), func(old []byte, exists bool) ([]byte, bool) {
		return nil, false // no-op
	})
	if err != nil {
		t.Fatal(err)
	}
	mustGet(t, db, "key", "val") // unchanged
}

// TestFlexDB_MergeDelete tests that merge can delete a key.
func TestFlexDB_MergeDelete(t *testing.T) {
	db, _ := openTestDB(t, nil)

	mustPut(t, db, "key", "val")

	err := db.Merge([]byte("key"), func(old []byte, exists bool) ([]byte, bool) {
		return nil, true // delete via tombstone
	})
	if err != nil {
		t.Fatal(err)
	}
	mustMiss(t, db, "key")
}

// TestFlexDB_MergeIncrement tests a counter-increment pattern with merge.
func TestFlexDB_MergeIncrement(t *testing.T) {
	db, _ := openTestDB(t, nil)

	increment := func(old []byte, exists bool) ([]byte, bool) {
		n := 0
		if exists {
			n = int(old[0])
		}
		n++
		return []byte{byte(n)}, true
	}

	for i := 0; i < 10; i++ {
		err := db.Merge([]byte("ctr"), increment)
		if err != nil {
			t.Fatal(err)
		}
	}

	val, ok := db.Get([]byte("ctr"))
	if !ok || val[0] != 10 {
		t.Fatalf("expected counter=10, got %v ok=%v", val, ok)
	}
}

// TestFlexDB_MergeAfterDelete tests merge after key has been deleted.
func TestFlexDB_MergeAfterDelete(t *testing.T) {
	db, _ := openTestDB(t, nil)

	mustPut(t, db, "key", "original")
	mustDelete(t, db, "key")

	err := db.Merge([]byte("key"), func(old []byte, exists bool) ([]byte, bool) {
		if exists {
			t.Fatal("expected key to not exist after delete")
		}
		return []byte("revived"), true
	})
	if err != nil {
		t.Fatal(err)
	}
	mustGet(t, db, "key", "revived")
}

// TestFlexDB_MergePersistence tests merge results survive close/reopen.
func TestFlexDB_MergePersistence(t *testing.T) {
	fs, dir := newTestFS(t)

	db := openTestDBAt(fs, t, dir, nil)
	mustPut(t, db, "k1", "v1")
	db.Sync()

	err := db.Merge([]byte("k1"), func(old []byte, exists bool) ([]byte, bool) {
		return []byte(string(old) + "+merged"), true
	})
	if err != nil {
		t.Fatal(err)
	}
	db.Sync()
	db.Close()

	db2 := openTestDBAt(fs, t, dir, nil)
	defer db2.Close()
	mustGet(t, db2, "k1", "v1+merged")
}

// TestFlexDB_MergeManyKeys tests merge across many keys with some in FlexSpace.
func TestFlexDB_MergeManyKeys(t *testing.T) {
	db, _ := openTestDB(t, nil)

	// Put 100 keys and flush to FlexSpace.
	for i := 0; i < 100; i++ {
		mustPut(t, db, fmt.Sprintf("key%03d", i), fmt.Sprintf("%d", i))
	}
	db.Sync()

	// Merge all keys: append suffix.
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("key%03d", i)
		err := db.Merge([]byte(k), func(old []byte, exists bool) ([]byte, bool) {
			if !exists {
				t.Fatalf("key %s should exist", k)
			}
			return append(old, []byte("+m")...), true
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
func collectAscend(db *FlexDB, pivot []byte) []string {
	var keys []string
	db.Ascend(pivot, func(key, value []byte) bool {
		keys = append(keys, string(key))
		return true
	})
	return keys
}

// collectDescend is a helper that collects all keys from db.Descend.
func collectDescend(db *FlexDB, pivot []byte) []string {
	var keys []string
	db.Descend(pivot, func(key, value []byte) bool {
		keys = append(keys, string(key))
		return true
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
	db, _ := openTestDB(t, nil)
	populateDB(t, db, false)

	keys := collectAscend(db, nil)
	expectKeys(t, "Ascend(nil)", keys, []string{"aaa", "bbb", "ccc", "ddd", "eee"})
}

// TestFlexDB_AscendPivot tests Ascend with a pivot key.
func TestFlexDB_AscendPivot(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateDB(t, db, false)

	// Exact match pivot
	keys := collectAscend(db, []byte("ccc"))
	expectKeys(t, "Ascend(ccc)", keys, []string{"ccc", "ddd", "eee"})

	// Pivot between keys
	keys = collectAscend(db, []byte("bbc"))
	expectKeys(t, "Ascend(bbc)", keys, []string{"ccc", "ddd", "eee"})

	// Pivot past all keys
	keys = collectAscend(db, []byte("zzz"))
	expectKeys(t, "Ascend(zzz)", keys, nil)
}

// TestFlexDB_AscendAfterSync tests Ascend with data in FlexSpace.
func TestFlexDB_AscendAfterSync(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateDB(t, db, true) // sync to FlexSpace

	keys := collectAscend(db, []byte("bbb"))
	expectKeys(t, "Ascend(bbb) after sync", keys, []string{"bbb", "ccc", "ddd", "eee"})
}

// TestFlexDB_AscendEarlyStop tests that returning false stops Ascend.
func TestFlexDB_AscendEarlyStop(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateDB(t, db, false)

	var keys []string
	db.Ascend(nil, func(key, value []byte) bool {
		keys = append(keys, string(key))
		return len(keys) < 3 // stop after 3
	})
	expectKeys(t, "Ascend stop at 3", keys, []string{"aaa", "bbb", "ccc"})
}

// TestFlexDB_DescendAll tests Descend with nil pivot (all keys descending).
func TestFlexDB_DescendAll(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateDB(t, db, false)

	keys := collectDescend(db, nil)
	expectKeys(t, "Descend(nil)", keys, []string{"eee", "ddd", "ccc", "bbb", "aaa"})
}

func TestFlexDB_DescendAll_big(t *testing.T) {
	db, _ := openTestDB(t, nil)

	keys := generateBenchKeys()

	// Insert all keys
	batch := db.NewBatch()
	for i, k := range keys {
		batch.Set(k, k)
		if (i+1)%10000 == 0 {
			batch.Commit(false)
			batch = db.NewBatch()
		}
	}
	batch.Commit(false)
	// omit! to test the rest. of the iteration using memtables too
	// db.Sync()

	slices.SortFunc(keys, bytes.Compare)
	slices.Reverse(keys) // compare to Descending

	it := db.NewIter()
	it.SeekToLast()
	count := 0
	for it.Valid() {
		cmp := bytes.Compare(it.Key(), keys[count])
		if cmp != 0 {
			panicf("Descending at count = %v, want '%v'; got '%v'", count, string(keys[count]), string(it.Key()))
		}
		count++
		it.Prev()
	}
	it.Close()
}

// TestFlexDB_DescendPivot tests Descend with a pivot key.
func TestFlexDB_DescendPivot(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateDB(t, db, false)

	// Exact match pivot
	keys := collectDescend(db, []byte("ccc"))
	expectKeys(t, "Descend(ccc)", keys, []string{"ccc", "bbb", "aaa"})

	// Pivot between keys
	keys = collectDescend(db, []byte("ccx"))
	expectKeys(t, "Descend(ccx)", keys, []string{"ccc", "bbb", "aaa"})

	// Pivot before all keys
	keys = collectDescend(db, []byte("aaa"))
	expectKeys(t, "Descend(aaa)", keys, []string{"aaa"})

	keys = collectDescend(db, []byte("a"))
	expectKeys(t, "Descend(a)", keys, nil)
}

// TestFlexDB_DescendAfterSync tests Descend with data in FlexSpace.
func TestFlexDB_DescendAfterSync(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateDB(t, db, true)

	keys := collectDescend(db, []byte("ddd"))
	expectKeys(t, "Descend(ddd) after sync", keys, []string{"ddd", "ccc", "bbb", "aaa"})
}

// TestFlexDB_DescendEarlyStop tests that returning false stops Descend.
func TestFlexDB_DescendEarlyStop(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateDB(t, db, false)

	var keys []string
	db.Descend(nil, func(key, value []byte) bool {
		keys = append(keys, string(key))
		return len(keys) < 2
	})
	expectKeys(t, "Descend stop at 2", keys, []string{"eee", "ddd"})
}

// ====================== CheckIntegrity tests ======================

// TestFlexDB_CheckIntegrity_Clean verifies CheckIntegrity passes on a
// freshly populated database (no vacuum, no corruption).
func TestFlexDB_CheckIntegrity_Clean(t *testing.T) {
	fs, dir := newTestFS(t)
	db := openTestDBAt(fs, t, dir, nil)

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

	db.Close()

	// Verify after reopen.
	db2 := openTestDBAt(fs, t, dir, nil)
	defer db2.Close()
	mustCheckIntegrity(t, db2)

	// Verify data correctness.
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("ikey%04d", i)
		mustGet(t, db2, key, makeTestValue(35))
	}
	for i := 50; i < 70; i++ {
		key := fmt.Sprintf("ikey%04d", i)
		mustMiss(t, db2, key)
	}
	for i := 70; i < 100; i++ {
		key := fmt.Sprintf("ikey%04d", i)
		mustGet(t, db2, key, makeTestValue(30))
	}
}

// TestFlexDB_CheckIntegrity_AfterVacuumAndReopen exercises the integrity
// checker through a full cycle: populate → vacuum → reopen → verify.
func TestFlexDB_CheckIntegrity_AfterVacuumAndReopen(t *testing.T) {
	fs, dir := newTestFS(t)
	db := openTestDBAt(fs, t, dir, nil)

	// Phase 1: Initial population.
	numKeys := 300
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("vk%04d", i)
		mustPut(t, db, key, makeTestValue(40))
	}
	db.Sync()
	mustCheckIntegrity(t, db)

	// Phase 2: Close, reopen, overwrite half, delete a quarter.
	db.Close()
	db = openTestDBAt(fs, t, dir, nil)
	mustCheckIntegrity(t, db)

	for i := 0; i < numKeys/2; i++ {
		key := fmt.Sprintf("vk%04d", i)
		mustPut(t, db, key, makeTestValue(45))
	}
	for i := numKeys / 2; i < 3*numKeys/4; i++ {
		key := fmt.Sprintf("vk%04d", i)
		mustDelete(t, db, key)
	}
	db.Sync()
	mustCheckIntegrity(t, db)

	// Phase 3: Vacuum.
	stats, err := db.VacuumKV()
	if err != nil {
		t.Fatalf("VacuumKV: %v", err)
	}
	t.Logf("VacuumKV stats: %s", stats)
	mustCheckIntegrity(t, db)

	// Phase 4: Close, reopen, verify.
	db.Close()
	db2 := openTestDBAt(fs, t, dir, nil)
	defer db2.Close()
	mustCheckIntegrity(t, db2)

	// Verify data correctness.
	for i := 0; i < numKeys/2; i++ {
		key := fmt.Sprintf("vk%04d", i)
		mustGet(t, db2, key, makeTestValue(45))
	}
	for i := numKeys / 2; i < 3*numKeys/4; i++ {
		key := fmt.Sprintf("vk%04d", i)
		mustMiss(t, db2, key)
	}
	for i := 3 * numKeys / 4; i < numKeys; i++ {
		key := fmt.Sprintf("vk%04d", i)
		mustGet(t, db2, key, makeTestValue(40))
	}
}

// TestFlexDB_WA_Counters_are_persisted verifies that totalLogicalBytesWrit and
// TotalPhysicalBytesWrit survive across database restarts and monotonically
// increase as more data is written.
func TestFlexDB_WA_Counters_are_persisted(t *testing.T) {
	fs, dir := newTestFS(t)

	// Session 1: write some data, verify counters > 0, close.
	db := openTestDBAt(fs, t, dir, nil)
	for i := 0; i < 50; i++ {
		mustPut(t, db, fmt.Sprintf("key%04d", i), fmt.Sprintf("value%04d", i))
	}
	db.Sync()

	logical1 := db.totalLogicalBytesWrit()
	physical1 := db.totalPhysicalBytesWrit()

	if logical1 <= 0 {
		t.Fatalf("session 1: totalLogicalBytesWrit = %d, want > 0", logical1)
	}
	if physical1 <= 0 {
		t.Fatalf("session 1: totalPhysicalBytesWrit = %d, want > 0", physical1)
	}
	if physical1 < logical1 {
		t.Fatalf("session 1: physical (%d) < logical (%d), impossible", physical1, logical1)
	}

	m1 := db.SessionMetrics()
	vv("m1 = %v", m1)
	if m1.totalLogicalBytesWrit != logical1 {
		t.Fatalf("session 1 metrics: totalLogicalBytesWrit = %d, want %d", m1.totalLogicalBytesWrit, logical1)
	}
	if m1.totalPhysicalBytesWrit != physical1 {
		t.Fatalf("session 1 metrics: totalPhysicalBytesWrit = %d, want %d", m1.totalPhysicalBytesWrit, physical1)
	}
	if m1.CumulativeWriteAmp <= 0 {
		t.Fatalf("session 1 metrics: CumulativeWriteAmp = %f, want > 0", m1.CumulativeWriteAmp)
	}

	closeMetrics1 := db.Close()
	if closeMetrics1 == nil {
		t.Fatal("session 1: Close() returned nil Metrics")
	}
	if closeMetrics1.totalLogicalBytesWrit < logical1 {
		t.Fatalf("session 1 Close() metrics: totalLogicalBytesWrit = %d, want >= %d", closeMetrics1.totalLogicalBytesWrit, logical1)
	}
	if closeMetrics1.totalPhysicalBytesWrit < physical1 {
		t.Fatalf("session 1 Close() metrics: totalPhysicalBytesWrit = %d, want >= %d", closeMetrics1.totalPhysicalBytesWrit, physical1)
	}
	if closeMetrics1.CumulativeWriteAmp <= 0 {
		t.Fatalf("session 1 Close() metrics: CumulativeWriteAmp = %f, want > 0", closeMetrics1.CumulativeWriteAmp)
	}
	vv("closeMetrics1 = %v", closeMetrics1)

	// Session 2: reopen, verify counters persisted, write more, verify they increase.
	db2 := openTestDBAt(fs, t, dir, nil)

	logical2a := db2.totalLogicalBytesWrit()
	physical2a := db2.totalPhysicalBytesWrit()

	if logical2a != logical1 {
		t.Fatalf("session 2 after reopen: totalLogicalBytesWrit = %d, want %d (from session 1)", logical2a, logical1)
	}
	// Physical bytes may be slightly larger than session 1's snapshot because
	// Close() writes CoW pages and META records after the last persistCounters().
	if physical2a < physical1 {
		t.Fatalf("session 2 after reopen: totalPhysicalBytesWrit = %d, want >= %d (from session 1)", physical2a, physical1)
	}

	// Write more data in session 2.
	for i := 50; i < 100; i++ {
		mustPut(t, db2, fmt.Sprintf("key%04d", i), fmt.Sprintf("value%04d", i))
	}
	db2.Sync()

	logical2b := db2.totalLogicalBytesWrit()
	physical2b := db2.totalPhysicalBytesWrit()

	if logical2b <= logical1 {
		t.Fatalf("session 2 after more writes: totalLogicalBytesWrit = %d, want > %d", logical2b, logical1)
	}
	if physical2b <= physical1 {
		t.Fatalf("session 2 after more writes: totalPhysicalBytesWrit = %d, want > %d", physical2b, physical1)
	}

	closeMetrics2 := db2.Close()
	if closeMetrics2 == nil {
		t.Fatal("session 2: Close() returned nil Metrics")
	}
	// Close() metrics must reflect at least what we observed mid-session.
	if closeMetrics2.totalLogicalBytesWrit < logical2b {
		t.Fatalf("session 2 Close() metrics: totalLogicalBytesWrit = %d, want >= %d", closeMetrics2.totalLogicalBytesWrit, logical2b)
	}
	if closeMetrics2.totalPhysicalBytesWrit < physical2b {
		t.Fatalf("session 2 Close() metrics: totalPhysicalBytesWrit = %d, want >= %d", closeMetrics2.totalPhysicalBytesWrit, physical2b)
	}
	vv("closeMetrics2 = %v", closeMetrics2)

	// Session 3: reopen and verify the cumulative counters survived session 2.
	db3 := openTestDBAt(fs, t, dir, nil)
	defer db3.Close()

	logical3 := db3.totalLogicalBytesWrit()
	physical3 := db3.totalPhysicalBytesWrit()

	if logical3 != logical2b {
		t.Fatalf("session 3: totalLogicalBytesWrit = %d, want %d (from session 2)", logical3, logical2b)
	}
	if physical3 < physical2b {
		t.Fatalf("session 3: totalPhysicalBytesWrit = %d, want >= %d (from session 2)", physical3, physical2b)
	}

	// Verify CumulativeMetrics also reports correctly.
	m3 := db3.CumulativeMetrics()
	if m3.totalLogicalBytesWrit != logical3 {
		t.Fatalf("session 3 cumulative metrics: totalLogicalBytesWrit = %d, want %d", m3.totalLogicalBytesWrit, logical3)
	}
	if m3.CumulativeWriteAmp <= 0 {
		t.Fatalf("session 3 cumulative metrics: CumulativeWriteAmp = %f, want > 0", m3.CumulativeWriteAmp)
	}
}

// TestFlexDB_HLC_UpdatedOnReload verifies that reloading the same dataset
// updates HLC timestamps in FlexSpace (not just in the memtable).
// Regression test for the bug where Overwrite bytes weren't counted in
// KV128BytesWritten, making it look like no data was written on reload.
func TestFlexDB_HLC_UpdatedOnReload(t *testing.T) {
	fs, dir := newTestFS(t)
	cfg := &Config{FS: fs}
	keys := []string{"alpha", "bravo", "charlie", "delta", "echo"}

	// Session 1: write initial data.
	db1, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if err := db1.Put([]byte(k), []byte("val-"+k)); err != nil {
			t.Fatal(err)
		}
	}
	m1 := db1.Close()
	if m1.KV128BytesWritten == 0 {
		t.Fatal("session 1: expected KV128BytesWritten > 0")
	}

	// Session 2: reopen, read back HLCs from passthrough (session 1 values).
	db2, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	hlcs1 := make(map[string]HLC)
	for _, k := range keys {
		kv, ok := db2.getPassthroughKV([]byte(k))
		if !ok {
			t.Fatalf("session 2: key %q not found in passthrough", k)
		}
		hlcs1[k] = kv.Hlc
		if kv.Hlc == 0 {
			t.Fatalf("session 2: key %q has zero HLC", k)
		}
	}

	// Re-put the same keys (new HLCs will be assigned).
	for _, k := range keys {
		if err := db2.Put([]byte(k), []byte("val-"+k)); err != nil {
			t.Fatal(err)
		}
	}
	m2 := db2.Close()
	if m2.KV128BytesWritten == 0 {
		t.Fatal("session 2: expected KV128BytesWritten > 0 (dirty pages overwritten)")
	}

	// Session 3: reopen and verify HLCs have been updated.
	db3, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		kv, ok := db3.getPassthroughKV([]byte(k))
		if !ok {
			t.Fatalf("session 3: key %q not found in passthrough", k)
		}
		if kv.Hlc <= hlcs1[k] {
			t.Fatalf("session 3: key %q HLC not updated: got %v, session1 had %v", k, kv.Hlc, hlcs1[k])
		}
	}
	db3.Close()
}

// ====================== DeleteRange Tests ======================

func TestDeleteRange_Basic(t *testing.T) {
	db, _ := openTestDB(t, nil)

	// Insert keys a..z
	for c := byte('a'); c <= 'z'; c++ {
		mustPut(t, db, string([]byte{c}), fmt.Sprintf("val_%c", c))
	}

	// Delete range [c, f] — both inclusive
	n, _, err := db.DeleteRange(true, []byte("c"), []byte("f"), true, true)
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
	db, _ := openTestDB(t, nil)

	mustPut(t, db, "a", "1")
	mustPut(t, db, "z", "2")

	// Delete range with no keys in it
	n, _, err := db.DeleteRange(true, []byte("m"), []byte("n"), true, true)
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
	db, _ := openTestDB(t, nil)

	mustPut(t, db, "x", "val")

	n, allGone, err := db.DeleteRange(true, []byte("x"), []byte("x"), true, true)
	if err != nil {
		t.Fatal(err)
	}
	// Only key in DB → allGone fast path.
	if !allGone {
		t.Fatal("expected allGone=true")
	}
	if n != 0 {
		t.Fatalf("DeleteRange returned n=%d, want 0 (allGone)", n)
	}
	mustMiss(t, db, "x")
}

func TestDeleteRange_Idempotent(t *testing.T) {
	db, _ := openTestDB(t, nil)

	for c := byte('a'); c <= 'e'; c++ {
		mustPut(t, db, string([]byte{c}), "v")
	}

	n1, _, err := db.DeleteRange(true, []byte("b"), []byte("d"), true, true)
	if err != nil {
		t.Fatal(err)
	}
	if n1 != 3 {
		t.Fatalf("first DeleteRange returned n=%d, want 3", n1)
	}

	// Second call on same range should find 0 non-tombstone keys
	n2, _, err := db.DeleteRange(true, []byte("b"), []byte("d"), true, true)
	if err != nil {
		t.Fatal(err)
	}
	if n2 != 0 {
		t.Fatalf("second DeleteRange returned n=%d, want 0", n2)
	}
}

func TestDeleteRange_FullRange(t *testing.T) {
	db, _ := openTestDB(t, nil)

	for i := 0; i < 100; i++ {
		mustPut(t, db, fmt.Sprintf("key%03d", i), fmt.Sprintf("val%03d", i))
	}

	n, allGone, err := db.DeleteRange(true, []byte("key000"), []byte("key999"), true, true)
	if err != nil {
		t.Fatal(err)
	}
	// All keys in range → allGone fast path.
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
	db, _ := openTestDB(t, nil)

	_, _, err := db.DeleteRange(true, []byte("z"), []byte("a"), true, true)
	if err == nil {
		t.Fatal("expected error for begKey > endKey")
	}
}

func TestDeleteRange_Persistence(t *testing.T) {
	fs, dir := newTestFS(t)
	cfg := &Config{FS: fs}

	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}

	for c := byte('a'); c <= 'j'; c++ {
		if err := db.Put([]byte{c}, []byte{c}); err != nil {
			t.Fatal(err)
		}
	}

	n, _, err := db.DeleteRange(true, []byte("c"), []byte("g"), true, true)
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("DeleteRange returned n=%d, want 5", n)
	}

	db.Close()

	// Reopen and verify
	db2 := openTestDBAt(fs, t, dir, cfg)
	defer db2.Close()

	for c := byte('a'); c <= 'b'; c++ {
		mustGet(t, db2, string([]byte{c}), string([]byte{c}))
	}
	for c := byte('c'); c <= 'g'; c++ {
		mustMiss(t, db2, string([]byte{c}))
	}
	for c := byte('h'); c <= 'j'; c++ {
		mustGet(t, db2, string([]byte{c}), string([]byte{c}))
	}
}

func TestDeleteRange_LargeFlushToFlexSpace(t *testing.T) {
	db, _ := openTestDB(t, nil)

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
	n, _, err := db.DeleteRange(true, []byte("k02500"), []byte("k07499"), true, true)
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
	// keys: a, b, c, d, e
	// We'll test DeleteRange("b", "d", ...) with all four combos.

	setup := func(t *testing.T) *FlexDB {
		t.Helper()
		db, _ := openTestDB(t, nil)
		for c := byte('a'); c <= 'e'; c++ {
			mustPut(t, db, string([]byte{c}), string([]byte{c}))
		}
		return db
	}

	t.Run("both_inclusive", func(t *testing.T) {
		db := setup(t)
		n, _, err := db.DeleteRange(true, []byte("b"), []byte("d"), true, true)
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
		n, _, err := db.DeleteRange(true, []byte("b"), []byte("d"), true, false)
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
		n, _, err := db.DeleteRange(true, []byte("b"), []byte("d"), false, true)
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
		n, _, err := db.DeleteRange(true, []byte("b"), []byte("d"), false, false)
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
		n, _, err := db.DeleteRange(true, []byte("c"), []byte("c"), true, true)
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
		n, _, err := db.DeleteRange(true, []byte("c"), []byte("c"), false, true)
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
		n, _, err := db.DeleteRange(true, []byte("c"), []byte("c"), true, false)
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
		n, _, err := db.DeleteRange(true, []byte("c"), []byte("c"), false, false)
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
		db, _ := openTestDB(t, nil)
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
		n, _, err := db.DeleteRange(true, []byte("b"), []byte("d"), true, true)
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
		n, _, err := db.DeleteRange(true, []byte("b"), []byte("d"), true, false)
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
		n, _, err := db.DeleteRange(true, []byte("b"), []byte("d"), false, true)
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
		n, _, err := db.DeleteRange(true, []byte("b"), []byte("d"), false, false)
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
		db, _ := openTestDB(t, nil)
		for c := byte('a'); c <= 'z'; c++ {
			mustPut(t, db, string([]byte{c}), string([]byte{c}))
		}
		_, allGone, err := db.DeleteRange(true, []byte("a"), []byte("z"), true, true)
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
		db, _ := openTestDB(t, nil)
		for i := 0; i < 500; i++ {
			mustPut(t, db, fmt.Sprintf("k%04d", i), fmt.Sprintf("v%04d", i))
		}
		if err := db.Sync(); err != nil {
			t.Fatal(err)
		}
		_, allGone, err := db.DeleteRange(true, []byte("k0000"), []byte("k9999"), true, true)
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
		// Range much larger than actual keys — still triggers allGone.
		db, _ := openTestDB(t, nil)
		mustPut(t, db, "m", "1")
		mustPut(t, db, "n", "2")
		_, allGone, err := db.DeleteRange(true, []byte("a"), []byte("z"), true, true)
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
		db, _ := openTestDB(t, nil)
		mustPut(t, db, "a", "1")
		mustPut(t, db, "b", "2")
		mustPut(t, db, "c", "3")
		n, allGone, err := db.DeleteRange(true, []byte("b"), []byte("b"), true, true)
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
		db, _ := openTestDB(t, nil)
		_, allGone, err := db.DeleteRange(true, []byte("a"), []byte("z"), true, true)
		if err != nil {
			t.Fatal(err)
		}
		if !allGone {
			t.Fatal("expected allGone=true for empty DB")
		}
	})

	t.Run("persistence_after_allGone", func(t *testing.T) {
		fs, dir := newTestFS(t)
		cfg := &Config{FS: fs}

		db, err := OpenFlexDB(dir, cfg)
		if err != nil {
			t.Fatal(err)
		}
		for c := byte('a'); c <= 'e'; c++ {
			if err := db.Put([]byte{c}, []byte{c}); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.Sync(); err != nil {
			t.Fatal(err)
		}
		_, allGone, err := db.DeleteRange(true, []byte("a"), []byte("e"), true, true)
		if err != nil {
			t.Fatal(err)
		}
		if !allGone {
			t.Fatal("expected allGone=true")
		}
		// Write new data post-wipe.
		if err := db.Put([]byte("fresh"), []byte("start")); err != nil {
			t.Fatal(err)
		}
		db.Close()

		// Reopen and verify.
		db2 := openTestDBAt(fs, t, dir, cfg)
		defer db2.Close()
		for c := byte('a'); c <= 'e'; c++ {
			mustMiss(t, db2, string([]byte{c}))
		}
		mustGet(t, db2, "fresh", "start")
	})
}

// ====================== DeleteRange includeLarge Tests ======================

func TestDeleteRange_SkipLargeValues(t *testing.T) {
	// Values > vlogInlineThreshold (64 bytes) go to VLOG → HasVPtr=true.
	bigVal := makeTestValue(200)
	smallVal := "small"

	t.Run("memtable_only", func(t *testing.T) {
		db, _ := openTestDB(t, nil)
		mustPut(t, db, "a", smallVal)
		mustPut(t, db, "b", bigVal)
		mustPut(t, db, "c", smallVal)
		mustPut(t, db, "d", bigVal)
		mustPut(t, db, "e", smallVal)

		// Delete range [a, e], skip large values.
		n, allGone, err := db.DeleteRange(false, []byte("a"), []byte("e"), true, true)
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
		val, ok := db.Get([]byte("b"))
		if !ok {
			t.Fatal("large key 'b' should survive")
		}
		if string(val) != bigVal {
			t.Fatalf("large key 'b' wrong value: got %d bytes", len(val))
		}
		val, ok = db.Get([]byte("d"))
		if !ok {
			t.Fatal("large key 'd' should survive")
		}
		if string(val) != bigVal {
			t.Fatalf("large key 'd' wrong value: got %d bytes", len(val))
		}
	})

	t.Run("after_flush", func(t *testing.T) {
		db, _ := openTestDB(t, nil)
		mustPut(t, db, "k1", smallVal)
		mustPut(t, db, "k2", bigVal)
		mustPut(t, db, "k3", smallVal)
		if err := db.Sync(); err != nil {
			t.Fatal(err)
		}

		n, _, err := db.DeleteRange(false, []byte("k1"), []byte("k3"), true, true)
		if err != nil {
			t.Fatal(err)
		}
		if n != 2 {
			t.Fatalf("n=%d, want 2", n)
		}
		mustMiss(t, db, "k1")
		mustMiss(t, db, "k3")
		val, ok := db.Get([]byte("k2"))
		if !ok {
			t.Fatal("large key 'k2' should survive")
		}
		if string(val) != bigVal {
			t.Fatalf("large key 'k2' wrong value")
		}
	})

	t.Run("includeLarge_true_deletes_all", func(t *testing.T) {
		db, _ := openTestDB(t, nil)
		mustPut(t, db, "x", bigVal)
		mustPut(t, db, "y", smallVal)

		_, allGone, err := db.DeleteRange(true, []byte("x"), []byte("y"), true, true)
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
		db, _ := openTestDB(t, nil)
		mustPut(t, db, "a", bigVal)
		mustPut(t, db, "b", bigVal)

		n, allGone, err := db.DeleteRange(false, []byte("a"), []byte("b"), true, true)
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
		_, ok := db.Get([]byte("a"))
		if !ok {
			t.Fatal("key 'a' should survive")
		}
		_, ok = db.Get([]byte("b"))
		if !ok {
			t.Fatal("key 'b' should survive")
		}
	})
}

// ====================== Clear Tests ======================

func TestClear(t *testing.T) {

	t.Run("clear_all", func(t *testing.T) {
		db, _ := openTestDB(t, nil)
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
		db, _ := openTestDB(t, nil)
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
		db, _ := openTestDB(t, nil)
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
		val, ok := db.Get([]byte("b"))
		if !ok {
			t.Fatal("large key 'b' should survive")
		}
		if string(val) != bigVal {
			t.Fatalf("wrong value for 'b'")
		}
		val, ok = db.Get([]byte("d"))
		if !ok {
			t.Fatal("large key 'd' should survive")
		}
		if string(val) != bigVal {
			t.Fatalf("wrong value for 'd'")
		}
	})

	t.Run("clear_small_after_flush", func(t *testing.T) {
		bigVal := makeTestValue(200)
		db, _ := openTestDB(t, nil)
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
		val, ok := db.Get([]byte("y"))
		if !ok {
			t.Fatal("large key 'y' should survive")
		}
		if string(val) != bigVal {
			t.Fatalf("wrong value for 'y'")
		}
	})

	t.Run("clear_empty_db", func(t *testing.T) {
		db, _ := openTestDB(t, nil)
		allGone, err := db.Clear(true)
		if err != nil {
			t.Fatal(err)
		}
		if !allGone {
			t.Fatal("expected allGone=true for empty DB")
		}
	})

	t.Run("clear_persistence", func(t *testing.T) {
		fs, dir := newTestFS(t)
		cfg := &Config{FS: fs}

		db, err := OpenFlexDB(dir, cfg)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 20; i++ {
			if err := db.Put([]byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%02d", i))); err != nil {
				t.Fatal(err)
			}
		}
		allGone, err := db.Clear(true)
		if err != nil {
			t.Fatal(err)
		}
		if !allGone {
			t.Fatal("expected allGone=true")
		}
		if err := db.Put([]byte("post"), []byte("clear")); err != nil {
			t.Fatal(err)
		}
		db.Close()

		db2 := openTestDBAt(fs, t, dir, cfg)
		defer db2.Close()
		for i := 0; i < 20; i++ {
			mustMiss(t, db2, fmt.Sprintf("k%02d", i))
		}
		mustGet(t, db2, "post", "clear")
	})
}

func TestLen(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		db, _ := openTestDB(t, nil)
		if n := db.Len(); n != 0 {
			t.Fatalf("empty DB Len() = %d, want 0", n)
		}
		big, small := db.LenBigSmall()
		if big != 0 || small != 0 {
			t.Fatalf("empty DB LenBigSmall() = (%d,%d), want (0,0)", big, small)
		}
	})

	t.Run("put_small_keys", func(t *testing.T) {
		db, _ := openTestDB(t, nil)
		N := 50
		for i := 0; i < N; i++ {
			db.Put([]byte(fmt.Sprintf("key%04d", i)), []byte("val"))
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
		db, _ := openTestDB(t, nil)
		largeVal := make([]byte, 128) // > vlogInlineThreshold (64)
		for i := range largeVal {
			largeVal[i] = byte(i)
		}
		N := 10
		for i := 0; i < N; i++ {
			db.Put([]byte(fmt.Sprintf("big%04d", i)), largeVal)
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
		db, _ := openTestDB(t, nil)
		key := []byte("mykey")
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
		db, _ := openTestDB(t, nil)
		key := []byte("mykey")
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
		db, _ := openTestDB(t, nil)
		db.Put([]byte("a"), []byte("1"))
		db.Put([]byte("b"), []byte("2"))
		db.Put([]byte("c"), []byte("3"))
		if n := db.Len(); n != 3 {
			t.Fatalf("Len() = %d, want 3", n)
		}
		db.Put([]byte("b"), nil)
		if n := db.Len(); n != 2 {
			t.Fatalf("after Del: Len() = %d, want 2", n)
		}
	})

	t.Run("delete_nonexistent", func(t *testing.T) {
		db, _ := openTestDB(t, nil)
		db.Put([]byte("a"), []byte("1"))
		db.Put([]byte("nonexistent"), nil)
		if n := db.Len(); n != 1 {
			t.Fatalf("Len() = %d, want 1", n)
		}
	})

	t.Run("tombstone_overwrites_tombstone", func(t *testing.T) {
		db, _ := openTestDB(t, nil)
		db.Put([]byte("a"), []byte("1"))
		db.Put([]byte("a"), nil)
		if n := db.Len(); n != 0 {
			t.Fatalf("after first Del: Len() = %d, want 0", n)
		}
		db.Put([]byte("a"), nil) // delete again
		if n := db.Len(); n != 0 {
			t.Fatalf("after second Del: Len() = %d, want 0", n)
		}
	})

	t.Run("reinsert_after_delete", func(t *testing.T) {
		db, _ := openTestDB(t, nil)
		db.Put([]byte("a"), []byte("1"))
		db.Put([]byte("a"), nil)
		if n := db.Len(); n != 0 {
			t.Fatalf("after Del: Len() = %d, want 0", n)
		}
		db.Put([]byte("a"), []byte("2"))
		if n := db.Len(); n != 1 {
			t.Fatalf("after re-Put: Len() = %d, want 1", n)
		}
	})

	t.Run("clear", func(t *testing.T) {
		db, _ := openTestDB(t, nil)
		for i := 0; i < 20; i++ {
			db.Put([]byte(fmt.Sprintf("k%02d", i)), []byte("v"))
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

	t.Run("persistence", func(t *testing.T) {
		fs, dir := newTestFS(t)
		cfg := &Config{FS: fs}
		db, err := OpenFlexDB(dir, cfg)
		if err != nil {
			t.Fatal(err)
		}
		N := 30
		largeVal := make([]byte, 128)
		for i := 0; i < N; i++ {
			if i%3 == 0 {
				db.Put([]byte(fmt.Sprintf("key%04d", i)), largeVal)
			} else {
				db.Put([]byte(fmt.Sprintf("key%04d", i)), []byte("small"))
			}
		}
		// Delete a few
		db.Put([]byte("key0003"), nil)
		db.Put([]byte("key0006"), nil)

		wantLen := db.Len()
		wantBig, wantSmall := db.LenBigSmall()
		db.Close()

		db2, err := OpenFlexDB(dir, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer db2.Close()
		if n := db2.Len(); n != wantLen {
			t.Fatalf("after reopen: Len() = %d, want %d", n, wantLen)
		}
		big, small := db2.LenBigSmall()
		if big != wantBig || small != wantSmall {
			t.Fatalf("after reopen: LenBigSmall() = (%d,%d), want (%d,%d)", big, small, wantBig, wantSmall)
		}
	})

	t.Run("after_flush", func(t *testing.T) {
		db, _ := openTestDB(t, nil)
		N := 100
		for i := 0; i < N; i++ {
			db.Put([]byte(fmt.Sprintf("key%04d", i)), []byte("val"))
		}
		db.Sync()
		if n := db.Len(); n != int64(N) {
			t.Fatalf("after Sync: Len() = %d, want %d", n, N)
		}
		// Add more after flush
		for i := N; i < N+20; i++ {
			db.Put([]byte(fmt.Sprintf("key%04d", i)), []byte("val"))
		}
		if n := db.Len(); n != int64(N+20) {
			t.Fatalf("after more puts: Len() = %d, want %d", n, N+20)
		}
	})

	t.Run("mixed_memtable_and_flexspace", func(t *testing.T) {
		db, _ := openTestDB(t, nil)
		// Put keys, flush to FlexSpace
		for i := 0; i < 50; i++ {
			db.Put([]byte(fmt.Sprintf("key%04d", i)), []byte("val"))
		}
		db.Sync()
		// Overwrite some from memtable (shadow FlexSpace)
		for i := 0; i < 10; i++ {
			db.Put([]byte(fmt.Sprintf("key%04d", i)), []byte("newval"))
		}
		// Delete some
		for i := 10; i < 15; i++ {
			db.Put([]byte(fmt.Sprintf("key%04d", i)), nil)
		}
		// Add new keys in memtable
		for i := 50; i < 60; i++ {
			db.Put([]byte(fmt.Sprintf("key%04d", i)), []byte("val"))
		}
		// Expected: 50 original - 5 deleted + 10 new = 55
		if n := db.Len(); n != 55 {
			t.Fatalf("mixed: Len() = %d, want 55", n)
		}
	})

	t.Run("batch", func(t *testing.T) {
		db, _ := openTestDB(t, nil)
		b := db.NewBatch()
		for i := 0; i < 25; i++ {
			b.Set([]byte(fmt.Sprintf("bk%04d", i)), []byte("bv"))
		}
		b.Commit(false)
		if n := db.Len(); n != 25 {
			t.Fatalf("after batch: Len() = %d, want 25", n)
		}

		// Batch with overwrites
		b2 := db.NewBatch()
		for i := 0; i < 10; i++ {
			b2.Set([]byte(fmt.Sprintf("bk%04d", i)), []byte("updated"))
		}
		b2.Commit(false)
		if n := db.Len(); n != 25 {
			t.Fatalf("after overwrite batch: Len() = %d, want 25", n)
		}
	})

	t.Run("delete_range", func(t *testing.T) {
		db, _ := openTestDB(t, nil)
		for i := 0; i < 20; i++ {
			db.Put([]byte(fmt.Sprintf("key%04d", i)), []byte("val"))
		}
		if n := db.Len(); n != 20 {
			t.Fatalf("Len() = %d, want 20", n)
		}
		// Delete range [key0005, key0014] — should delete 10 keys (0005..0014)
		db.DeleteRange(false, []byte("key0005"), []byte("key0014"), true, true)
		want := int64(10) // 20 - 10 = 10
		if n := db.Len(); n != want {
			t.Fatalf("after DeleteRange: Len() = %d, want %d", n, want)
		}
	})
}
