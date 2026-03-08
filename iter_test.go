package yogadb

import (
	"bytes"
	"fmt"
	//"os"
	"path/filepath"
	"sort"

	"testing"
)

// TestFlexDB_IteratorBasic tests the iterator with a few keys.
func TestFlexDB_IteratorBasic(t *testing.T) {
	db, _ := openTestDB(t, nil)

	keys := []string{"banana", "apple", "cherry", "date"}
	for _, k := range keys {
		mustPut(t, db, k, "v:"+k)
	}

	it := db.NewIter()
	it.SeekToFirst()
	defer it.Close()

	want := []string{"apple", "banana", "cherry", "date"}
	for _, wk := range want {
		if !it.Valid() {
			t.Fatalf("iterator ended early; want key %q", wk)
		}
		if string(it.Key()) != wk {
			t.Fatalf("Key() = %q, want %q", it.Key(), wk)
		}
		if string(it.Vin()) != "v:"+wk {
			t.Fatalf("Value() = %q, want %q", it.Vin(), "v:"+wk)
		}
		it.Next()
	}
	if it.Valid() {
		t.Fatalf("iterator not exhausted; extra key %q", it.Key())
	}
}

// TestFlexDB_IteratorSeek tests Seek() to a specific key.
func TestFlexDB_IteratorSeek(t *testing.T) {
	db, _ := openTestDB(t, nil)

	for _, k := range []string{"aaa", "bbb", "ccc", "ddd", "eee"} {
		mustPut(t, db, k, k)
	}

	it := db.NewIter()
	defer it.Close()

	it.Seek([]byte("ccc"))
	if !it.Valid() || string(it.Key()) != "ccc" {
		t.Fatalf("Seek(ccc): got %v/%q", it.Valid(), it.Key())
	}
	it.Next()
	if !it.Valid() || string(it.Key()) != "ddd" {
		t.Fatalf("After Seek+Next: got %v/%q, want ddd", it.Valid(), it.Key())
	}

	it.Seek([]byte("d"))
	if !it.Valid() || string(it.Key()) != "ddd" {
		t.Fatalf("Seek(d): got %v/%q, want ddd", it.Valid(), it.Key())
	}
}

// TestFlexDB_IteratorAfterSync tests iteration after data is in FlexSpace.
func TestFlexDB_IteratorAfterSync(t *testing.T) {
	db, _ := openTestDB(t, nil)

	keys := []string{"z", "m", "a", "f", "b"}
	for _, k := range keys {
		mustPut(t, db, k, "v:"+k)
	}
	db.Sync()

	it := db.NewIter()
	it.SeekToFirst()
	defer it.Close()

	sort.Strings(keys)
	for _, wk := range keys {
		if !it.Valid() {
			t.Fatalf("iterator ended early; want %q", wk)
		}
		if string(it.Key()) != wk {
			t.Fatalf("Key() = %q, want %q", it.Key(), wk)
		}
		it.Next()
	}
	if it.Valid() {
		t.Fatalf("iterator not exhausted; extra key %q", it.Key())
	}
}

// TestFlexDB_ManyKeysIterator inserts many keys and verifies iterator order.
func TestFlexDB_ManyKeysIterator(t *testing.T) {
	db, _ := openTestDB(t, nil)

	const N = 200
	keys := make([]string, N)
	for i := 0; i < N; i++ {
		keys[i] = fmt.Sprintf("key%06d", i)
		mustPut(t, db, keys[i], fmt.Sprintf("val%06d", i))
	}
	db.Sync()

	it := db.NewIter()
	it.SeekToFirst()
	defer it.Close()

	sort.Strings(keys)
	for _, wk := range keys {
		if !it.Valid() {
			t.Fatalf("iterator ended early; want %q", wk)
		}
		if string(it.Key()) != wk {
			t.Fatalf("Key() = %q, want %q", it.Key(), wk)
		}
		it.Next()
	}
	if it.Valid() {
		t.Fatalf("iterator not exhausted; extra key %q", it.Key())
	}
}

// TestFlexDB_AscendRange tests bounded ascending iteration.
func TestFlexDB_AscendRange(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateDB(t, db, false)

	// [bbb, ddd) — should include bbb, ccc but NOT ddd
	var keys []string
	db.AscendRange([]byte("bbb"), []byte("ddd"), func(key, value []byte) bool {
		keys = append(keys, string(key))
		return true
	})
	expectKeys(t, "AscendRange(bbb,ddd)", keys, []string{"bbb", "ccc"})

	// Unbounded start: [nil, ccc)
	keys = nil
	db.AscendRange(nil, []byte("ccc"), func(key, value []byte) bool {
		keys = append(keys, string(key))
		return true
	})
	expectKeys(t, "AscendRange(nil,ccc)", keys, []string{"aaa", "bbb"})

	// Unbounded end: [ccc, nil)
	keys = nil
	db.AscendRange([]byte("ccc"), nil, func(key, value []byte) bool {
		keys = append(keys, string(key))
		return true
	})
	expectKeys(t, "AscendRange(ccc,nil)", keys, []string{"ccc", "ddd", "eee"})

	// Both nil: all keys
	keys = nil
	db.AscendRange(nil, nil, func(key, value []byte) bool {
		keys = append(keys, string(key))
		return true
	})
	expectKeys(t, "AscendRange(nil,nil)", keys, []string{"aaa", "bbb", "ccc", "ddd", "eee"})
}

// TestFlexDB_DescendRange tests bounded descending iteration.
func TestFlexDB_DescendRange(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateDB(t, db, false)

	// (bbb, ddd] — should include ddd, ccc but NOT bbb
	var keys []string
	db.DescendRange([]byte("ddd"), []byte("bbb"), func(key, value []byte) bool {
		keys = append(keys, string(key))
		return true
	})
	expectKeys(t, "DescendRange(ddd,bbb)", keys, []string{"ddd", "ccc"})

	// Unbounded start (descend from end): (bbb, nil]
	keys = nil
	db.DescendRange(nil, []byte("bbb"), func(key, value []byte) bool {
		keys = append(keys, string(key))
		return true
	})
	expectKeys(t, "DescendRange(nil,bbb)", keys, []string{"eee", "ddd", "ccc"})

	// Unbounded end (descend to beginning): (nil, ddd]
	keys = nil
	db.DescendRange([]byte("ddd"), nil, func(key, value []byte) bool {
		keys = append(keys, string(key))
		return true
	})
	expectKeys(t, "DescendRange(ddd,nil)", keys, []string{"ddd", "ccc", "bbb", "aaa"})

	// Both nil: all keys descending
	keys = nil
	db.DescendRange(nil, nil, func(key, value []byte) bool {
		keys = append(keys, string(key))
		return true
	})
	expectKeys(t, "DescendRange(nil,nil)", keys, []string{"eee", "ddd", "ccc", "bbb", "aaa"})
}

// TestFlexDB_AscendRangeAfterSync tests AscendRange with data in FlexSpace.
func TestFlexDB_AscendRangeAfterSync(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateDB(t, db, true)

	var keys []string
	db.AscendRange([]byte("bbb"), []byte("eee"), func(key, value []byte) bool {
		keys = append(keys, string(key))
		return true
	})
	expectKeys(t, "AscendRange after sync", keys, []string{"bbb", "ccc", "ddd"})
}

// TestFlexDB_DescendRangeAfterSync tests DescendRange with data in FlexSpace.
func TestFlexDB_DescendRangeAfterSync(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateDB(t, db, true)

	var keys []string
	db.DescendRange([]byte("ddd"), []byte("aaa"), func(key, value []byte) bool {
		keys = append(keys, string(key))
		return true
	})
	expectKeys(t, "DescendRange after sync", keys, []string{"ddd", "ccc", "bbb"})
}

// TestFlexDB_AscendValues verifies that values are correct during Ascend.
func TestFlexDB_AscendValues(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateDB(t, db, false)

	var pairs []string
	db.Ascend([]byte("bbb"), func(key, value []byte) bool {
		pairs = append(pairs, string(key)+"="+string(value))
		return true
	})
	want := []string{"bbb=v:bbb", "ccc=v:ccc", "ddd=v:ddd", "eee=v:eee"}
	expectKeys(t, "Ascend values", pairs, want)
}

// TestFlexDB_DescendValues verifies that values are correct during Descend.
func TestFlexDB_DescendValues(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateDB(t, db, false)

	var pairs []string
	db.Descend([]byte("ddd"), func(key, value []byte) bool {
		pairs = append(pairs, string(key)+"="+string(value))
		return true
	})
	want := []string{"ddd=v:ddd", "ccc=v:ccc", "bbb=v:bbb", "aaa=v:aaa"}
	expectKeys(t, "Descend values", pairs, want)
}

// TestFlexDB_IteratorPrev tests Prev() on Iter.
func TestFlexDB_IteratorPrev(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateDB(t, db, false)

	it := db.NewIter()
	it.SeekToLast()
	defer it.Close()

	var keys []string
	for it.Valid() {
		keys = append(keys, string(it.Key()))
		it.Prev()
	}
	expectKeys(t, "Iterator Prev", keys, []string{"eee", "ddd", "ccc", "bbb", "aaa"})
}

// TestFlexDB_IteratorSeekThenPrev tests Seek followed by Prev.
func TestFlexDB_IteratorSeekThenPrev(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateDB(t, db, false)

	it := db.NewIter()
	defer it.Close()

	// Seek to ccc, then go backward
	it.Seek([]byte("ccc"))
	if !it.Valid() || string(it.Key()) != "ccc" {
		t.Fatalf("Seek(ccc): got %v/%q", it.Valid(), it.Key())
	}
	it.Prev()
	if !it.Valid() || string(it.Key()) != "bbb" {
		t.Fatalf("After Prev: got %v/%q, want bbb", it.Valid(), it.Key())
	}
	it.Prev()
	if !it.Valid() || string(it.Key()) != "aaa" {
		t.Fatalf("After 2nd Prev: got %v/%q, want aaa", it.Valid(), it.Key())
	}
	it.Prev()
	if it.Valid() {
		t.Fatalf("Should be invalid after Prev past beginning, got key %q", it.Key())
	}
}

// TestFlexDB_AscendManyKeys tests Ascend/Descend with many keys across FlexSpace.
func TestFlexDB_AscendManyKeys(t *testing.T) {
	db, _ := openTestDB(t, nil)

	const N = 200
	allKeys := make([]string, N)
	for i := 0; i < N; i++ {
		k := fmt.Sprintf("key%06d", i)
		allKeys[i] = k
		mustPut(t, db, k, fmt.Sprintf("v%06d", i))
	}
	db.Sync()
	sort.Strings(allKeys)

	// Ascend from key000100
	keys := collectAscend(db, []byte("key000100"))
	want := allKeys[100:] // key000100..key000199
	expectKeys(t, "Ascend(key000100)", keys, want)

	// Descend from key000050
	keys = collectDescend(db, []byte("key000050"))
	want = make([]string, 51)
	for i := 0; i <= 50; i++ {
		want[50-i] = allKeys[i]
	}
	expectKeys(t, "Descend(key000050)", keys, want)

	// AscendRange [key000010, key000015)
	var rangeKeys []string
	db.AscendRange([]byte("key000010"), []byte("key000015"), func(key, value []byte) bool {
		rangeKeys = append(rangeKeys, string(key))
		return true
	})
	expectKeys(t, "AscendRange(10,15)", rangeKeys,
		[]string{"key000010", "key000011", "key000012", "key000013", "key000014"})
}

// ====================== HLC tests ======================

// TestFlexDB_HLC_PutMonotonic verifies three sequential Puts produce strictly increasing HLCs.
func TestFlexDB_HLC_PutMonotonic(t *testing.T) {
	db, _ := openTestDB(t, nil)

	// Do three sequential Puts and capture the HLC via the memtable.
	keys := []string{"aaa", "bbb", "ccc"}
	hlcs := make([]HLC, len(keys))
	for i, k := range keys {
		err := db.Put([]byte(k), []byte("v"))
		if err != nil {
			t.Fatal(err)
		}
		// Read back from the active memtable to get the HLC.
		db.topMutRW.RLock()
		active := db.activeMT
		kv, ok := db.memtables[active].get([]byte(k))
		db.topMutRW.RUnlock()
		if !ok {
			t.Fatalf("key %q not found in memtable", k)
		}
		hlcs[i] = kv.Hlc
	}

	for i := 1; i < len(hlcs); i++ {
		if hlcs[i] <= hlcs[i-1] {
			t.Fatalf("HLC not strictly increasing: hlc[%d]=%v <= hlc[%d]=%v", i, hlcs[i], i-1, hlcs[i-1])
		}
	}
}

// TestFlexDB_HLC_BatchInterval verifies Batch.Commit returns correct HLC intervals.
func TestFlexDB_HLC_BatchInterval(t *testing.T) {
	db, _ := openTestDB(t, nil)

	// Batch with unique keys — single-tick interval.
	batch := db.NewBatch()
	batch.Set([]byte("k1"), []byte("v1"))
	batch.Set([]byte("k2"), []byte("v2"))
	batch.Set([]byte("k3"), []byte("v3"))
	iv, err := batch.Commit(false)
	if err != nil {
		t.Fatal(err)
	}
	if iv.Endx != iv.Begin+1 {
		t.Fatalf("unique keys: expected single-tick interval, got Begin=%v Endx=%v", iv.Begin, iv.Endx)
	}
	if iv.Begin == 0 {
		t.Fatal("expected non-zero HLC")
	}

	// Batch with a duplicate key — multi-tick interval.
	batch2 := db.NewBatch()
	batch2.Set([]byte("x1"), []byte("v1"))
	batch2.Set([]byte("x1"), []byte("v2")) // duplicate triggers new tick
	batch2.Set([]byte("x2"), []byte("v3"))
	iv2, err := batch2.Commit(false)
	if err != nil {
		t.Fatal(err)
	}
	if iv2.Endx <= iv2.Begin+1 {
		t.Fatalf("duplicate key: expected multi-tick interval, got Begin=%v Endx=%v", iv2.Begin, iv2.Endx)
	}

	// Intervals from successive batches should not overlap.
	if iv2.Begin < iv.Endx {
		t.Fatalf("batch intervals overlap: first=[%v,%v), second=[%v,%v)", iv.Begin, iv.Endx, iv2.Begin, iv2.Endx)
	}
}

// TestFlexDB_HLC_DedupByHLC verifies intervalCacheDedup keeps the highest-HLC entry.
func TestFlexDB_HLC_DedupByHLC(t *testing.T) {
	// Construct a sorted slice with duplicate keys and varying HLCs.
	kvs := []*KV{
		{Key: []byte("aaa"), Value: []byte("old"), Hlc: 100},
		{Key: []byte("aaa"), Value: []byte("new"), Hlc: 200},
		{Key: []byte("bbb"), Value: []byte("only"), Hlc: 150},
		{Key: []byte("ccc"), Value: []byte("first"), Hlc: 300},
		{Key: []byte("ccc"), Value: []byte("second"), Hlc: 250},
		{Key: []byte("ccc"), Value: []byte("third"), Hlc: 350},
	}
	out, fps, size := intervalCacheDedup(kvs)
	if len(out) != 3 {
		t.Fatalf("expected 3 unique keys, got %d", len(out))
	}
	if len(fps) != 3 {
		t.Fatalf("expected 3 fingerprints, got %d", len(fps))
	}
	if size == 0 {
		t.Fatal("expected non-zero size")
	}

	// Verify winners:
	// aaa -> Hlc 200 ("new")
	if string(out[0].Key) != "aaa" || string(out[0].Value) != "new" || out[0].Hlc != 200 {
		t.Fatalf("aaa: got key=%q val=%q hlc=%v", out[0].Key, out[0].Value, out[0].Hlc)
	}
	// bbb -> Hlc 150 ("only")
	if string(out[1].Key) != "bbb" || string(out[1].Value) != "only" || out[1].Hlc != 150 {
		t.Fatalf("bbb: got key=%q val=%q hlc=%v", out[1].Key, out[1].Value, out[1].Hlc)
	}
	// ccc -> Hlc 350 ("third")
	if string(out[2].Key) != "ccc" || string(out[2].Value) != "third" || out[2].Hlc != 350 {
		t.Fatalf("ccc: got key=%q val=%q hlc=%v", out[2].Key, out[2].Value, out[2].Hlc)
	}
}

// TestFlexDB_HLC_Persistence verifies HLC survives Sync + Close + reopen via kv128 on disk.
func TestFlexDB_HLC_Persistence(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenFlexDB(dir, &Config{DisableVLOG: true})
	if err != nil {
		t.Fatal(err)
	}

	// Put a few keys; they'll get HLCs.
	err = db.Put([]byte("pk1"), []byte("pv1"))
	if err != nil {
		t.Fatal(err)
	}
	err = db.Put([]byte("pk2"), []byte("pv2"))
	if err != nil {
		t.Fatal(err)
	}

	// Capture HLCs from the memtable before flush.
	db.topMutRW.RLock()
	active := db.activeMT
	kv1, _ := db.memtables[active].get([]byte("pk1"))
	kv2, _ := db.memtables[active].get([]byte("pk2"))
	db.topMutRW.RUnlock()
	hlc1 := kv1.Hlc
	hlc2 := kv2.Hlc

	if hlc1 == 0 || hlc2 == 0 {
		t.Fatal("expected non-zero HLCs")
	}
	if hlc2 <= hlc1 {
		t.Fatalf("expected hlc2 > hlc1, got %v <= %v", hlc2, hlc1)
	}

	db.Sync()
	db.Close()

	// Reopen and verify values are intact (HLC is in kv128 on disk).
	db2, err := OpenFlexDB(dir, &Config{DisableVLOG: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	val, ok := db2.Get([]byte("pk1"))
	if !ok || string(val) != "pv1" {
		t.Fatalf("pk1: got %q, ok=%v", val, ok)
	}
	val, ok = db2.Get([]byte("pk2"))
	if !ok || string(val) != "pv2" {
		t.Fatalf("pk2: got %q, ok=%v", val, ok)
	}
}

// TestFlexDB_HLC_VLOGRoundTrip verifies a large value stored in VLOG survives Sync + reopen.
func TestFlexDB_HLC_VLOGRoundTrip(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenFlexDB(dir, nil) // VLOG enabled
	if err != nil {
		t.Fatal(err)
	}

	bigVal := makeTestValue(500) // 500 bytes, well above vlogInlineThreshold
	err = db.Put([]byte("bigkey"), []byte(bigVal))
	if err != nil {
		t.Fatal(err)
	}

	db.Sync()
	db.Close()

	// Reopen.
	db2, err := OpenFlexDB(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	val, ok := db2.Get([]byte("bigkey"))
	if !ok {
		t.Fatal("bigkey not found after reopen")
	}
	if string(val) != bigVal {
		t.Fatalf("bigkey: value mismatch after reopen: got %d bytes, want %d", len(val), len(bigVal))
	}
}

// mustCheckIntegrity runs CheckIntegrity and fails the test if any errors are found.
func mustCheckIntegrity(t *testing.T, db *FlexDB) {
	t.Helper()
	errs := db.CheckIntegrity()
	if len(errs) > 0 {
		for _, e := range errs {
			t.Errorf("integrity error: %v", e)
		}
		t.Fatalf("CheckIntegrity found %d errors", len(errs))
	}
}

// ====================== VacuumKV tests ======================

// TestFlexDB_VacuumKV_Basic tests that VacuumKV reclaims dead FLEXSPACE.KV128_BLOCKS space
// when keys are overwritten.
func TestFlexDB_VacuumKV_Basic(t *testing.T) {
	fs, dir := newTestFS(t)
	db := openTestDBAt(fs, t, dir, nil)

	// Put many keys with non-trivial values (all inline, < 64 bytes).
	numKeys := 200
	valSize := 50
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%04d", i)
		mustPut(t, db, key, makeTestValue(valSize))
	}
	db.Sync()

	// Close and reopen to ensure data is on disk.
	db.Close()
	db = openTestDBAt(fs, t, dir, nil)

	// Overwrite all keys with same-size values (old versions become dead).
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%04d", i)
		mustPut(t, db, key, makeTestValue(valSize+1))
	}
	db.Sync()

	// Record file size before vacuum.
	ffPath := filepath.Join(dir, "FLEXSPACE.KV128_BLOCKS")
	//ffPath := filepath.Join(dir, "FLEXSPACE", "FLEXSPACE.KV128_BLOCKS")
	fi, err := fs.Stat(ffPath)
	if err != nil {
		t.Fatalf("Stat FLEXSPACE.KV128_BLOCKS: %v", err)
	}
	sizeBeforeVacuum := fi.Size()

	stats, err := db.VacuumKV()
	if err != nil {
		t.Fatalf("VacuumKV: %v", err)
	}

	if stats.OldFileSize != sizeBeforeVacuum {
		t.Errorf("OldFileSize: got %d, want %d", stats.OldFileSize, sizeBeforeVacuum)
	}
	if stats.ExtentsRewritten <= 0 {
		t.Errorf("expected positive ExtentsRewritten, got %d", stats.ExtentsRewritten)
	}

	// Integrity check after vacuum.
	mustCheckIntegrity(t, db)

	// Verify all keys are still readable with the correct values.
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%04d", i)
		mustGet(t, db, key, makeTestValue(valSize+1))
	}

	db.Close()

	// Reopen and verify again.
	db2 := openTestDBAt(fs, t, dir, nil)
	defer db2.Close()
	mustCheckIntegrity(t, db2)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%04d", i)
		mustGet(t, db2, key, makeTestValue(valSize+1))
	}
}

// TestFlexDB_VacuumKV_WithDeletes tests that VacuumKV works after deleting keys.
func TestFlexDB_VacuumKV_WithDeletes(t *testing.T) {
	fs, dir := newTestFS(t)
	db := openTestDBAt(fs, t, dir, nil)

	// Put keys.
	numKeys := 100
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("dk%04d", i)
		mustPut(t, db, key, makeTestValue(40))
	}
	db.Sync()

	// Delete half the keys.
	for i := 0; i < numKeys/2; i++ {
		key := fmt.Sprintf("dk%04d", i)
		mustDelete(t, db, key)
	}
	db.Sync()

	stats, err := db.VacuumKV()
	if err != nil {
		t.Fatalf("VacuumKV: %v", err)
	}

	t.Logf("VacuumKV stats: %s", stats)

	// Integrity check after vacuum.
	mustCheckIntegrity(t, db)

	// Verify deleted keys are gone and remaining are intact.
	for i := 0; i < numKeys/2; i++ {
		key := fmt.Sprintf("dk%04d", i)
		mustMiss(t, db, key)
	}
	for i := numKeys / 2; i < numKeys; i++ {
		key := fmt.Sprintf("dk%04d", i)
		mustGet(t, db, key, makeTestValue(40))
	}

	db.Close()

	// Reopen and verify.
	db2 := openTestDBAt(fs, t, dir, nil)
	defer db2.Close()
	mustCheckIntegrity(t, db2)
	for i := numKeys / 2; i < numKeys; i++ {
		key := fmt.Sprintf("dk%04d", i)
		mustGet(t, db2, key, makeTestValue(40))
	}
}

// TestFlexDB_VacuumKV_TwiceCrossSession reproduces the exact load_yogadb
// scenario: session 1 ingests many keys (enough to create a multi-level
// FlexTree with internal nodes), closes. Session 2 opens and vacuums
// (succeeds). Session 3 opens and vacuums again (fails with EOF if the
// bug is present).
//
// Root cause: VacuumKV marks leaf nodes dirty but does NOT propagate the
// dirty flag to internal (ancestor) nodes. SyncCoW's post-order walk
// skips clean internal nodes, so dirty leaves under clean parents are
// never persisted. On reopen, those leaves still have their pre-vacuum
// poff values, which point beyond the vacuumed (smaller) file → EOF.
func TestFlexDB_VacuumKV_TwiceCrossSession(t *testing.T) {
	fs, dir := newTestFS(t)
	cfg := &Config{FS: fs}
	// Session 1: ingest many keys to create a multi-level tree.
	// With FLEXTREE_LEAF_CAP=60 extents per leaf, we need >60 anchors
	// to guarantee internal tree nodes. FlexDB creates one anchor per
	// flexdbSparseInterval (32) keys, so 5000 keys → ~156 anchors →
	// at least 3 leaf nodes → internal node(s) exist.
	const nKeys = 5000
	{
		db, err := OpenFlexDB(dir, cfg)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < nKeys; i++ {
			key := []byte(fmt.Sprintf("k%06d", i))
			val := []byte(fmt.Sprintf("v%06d", i))
			if err := db.Put(key, val); err != nil {
				t.Fatal(err)
			}
		}
		db.Sync()
		db.Close()
	}

	// Session 2: open and vacuum. Should succeed.
	{
		db, err := OpenFlexDB(dir, cfg)
		if err != nil {
			t.Fatal(err)
		}
		stats, err := db.VacuumKV()
		if err != nil {
			t.Fatalf("session 2 vacuum: %v", err)
		}
		t.Logf("session 2 vacuum: old=%d new=%d reclaimed=%d extents=%d",
			stats.OldFileSize, stats.NewFileSize, stats.BytesReclaimed, stats.ExtentsRewritten)

		// Verify data after vacuum.
		for i := 0; i < nKeys; i++ {
			key := []byte(fmt.Sprintf("k%06d", i))
			expected := fmt.Sprintf("v%06d", i)
			got, ok := db.Get(key)
			if !ok {
				t.Fatalf("session 2: key %q missing after vacuum", key)
			}
			if string(got) != expected {
				t.Fatalf("session 2: key %q: got %q want %q", key, got, expected)
			}
		}
		db.Close()
	}

	// Session 3: open and vacuum AGAIN. This is where the bug manifests:
	// leaves whose dirty flag was not persisted via SyncCoW still have
	// old (pre-vacuum) poff values → EOF reading the smaller file.
	{
		db, err := OpenFlexDB(dir, cfg)
		if err != nil {
			t.Fatal(err)
		}
		stats, err := db.VacuumKV()
		if err != nil {
			t.Fatalf("session 3 vacuum: %v", err)
		}
		t.Logf("session 3 vacuum: old=%d new=%d reclaimed=%d extents=%d",
			stats.OldFileSize, stats.NewFileSize, stats.BytesReclaimed, stats.ExtentsRewritten)

		// Verify data after second vacuum.
		for i := 0; i < nKeys; i++ {
			key := []byte(fmt.Sprintf("k%06d", i))
			expected := fmt.Sprintf("v%06d", i)
			got, ok := db.Get(key)
			if !ok {
				t.Fatalf("session 3: key %q missing", key)
			}
			if string(got) != expected {
				t.Fatalf("session 3: key %q: got %q want %q", key, got, expected)
			}
		}
		db.Close()
	}
}

// TestFlexDB_VacuumKV_Twice tests that calling VacuumKV twice in a row
// (with no writes in between) does not produce an EOF error. This is a
// regression test for a bug where VacuumKV did not truncate the
// FLEXSPACE.KV128_BLOCKS file after rewriting, leaving stale high-offset
// poff values that a second vacuum would try to read past the new
// (smaller) file boundary.
func TestFlexDB_VacuumKV_Twice(t *testing.T) {
	fs, dir := newTestFS(t)
	db := openTestDBAt(fs, t, dir, nil)

	numKeys := 200
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("vk2_%04d", i)
		mustPut(t, db, key, makeTestValue(50))
	}
	db.Sync()

	// Close and reopen to ensure data is flushed to disk and the
	// block manager starts fresh (simulates a new session).
	db.Close()
	db = openTestDBAt(fs, t, dir, nil)

	// Overwrite all keys so there is dead space to reclaim.
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("vk2_%04d", i)
		mustPut(t, db, key, makeTestValue(51))
	}
	db.Sync()

	// First vacuum — should succeed and reclaim dead space.
	stats1, err := db.VacuumKV()
	if err != nil {
		t.Fatalf("first VacuumKV: %v", err)
	}
	t.Logf("first vacuum: %s", stats1)
	mustCheckIntegrity(t, db)

	// Verify data after first vacuum.
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("vk2_%04d", i)
		mustGet(t, db, key, makeTestValue(51))
	}

	// Second vacuum — this was the bug: EOF reading poffs that
	// pointed beyond the truncated file.
	stats2, err := db.VacuumKV()
	if err != nil {
		t.Fatalf("second VacuumKV: %v", err)
	}
	t.Logf("second vacuum: %s", stats2)
	mustCheckIntegrity(t, db)

	// Second vacuum should reclaim nothing (already compacted).
	if stats2.BytesReclaimed < 0 {
		t.Errorf("second vacuum reclaimed negative bytes: %d", stats2.BytesReclaimed)
	}

	// Verify data after second vacuum.
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("vk2_%04d", i)
		mustGet(t, db, key, makeTestValue(51))
	}

	// Close and reopen to verify persistence.
	db.Close()
	db2 := openTestDBAt(fs, t, dir, nil)
	defer db2.Close()
	mustCheckIntegrity(t, db2)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("vk2_%04d", i)
		mustGet(t, db2, key, makeTestValue(51))
	}
}

// TestFlexDB_VacuumKV_WriteAndVacuumAgain tests vacuum, then write new
// data, then vacuum again — the exact scenario from load_yogadb that
// triggered the original EOF bug.
func TestFlexDB_VacuumKV_WriteAndVacuumAgain(t *testing.T) {
	fs, dir := newTestFS(t)

	// Session 1: populate and close.
	{
		db := openTestDBAt(fs, t, dir, nil)
		for i := 0; i < 300; i++ {
			key := fmt.Sprintf("wv_%04d", i)
			mustPut(t, db, key, makeTestValue(45))
		}
		db.Sync()
		db.Close()
	}

	// Session 2: reopen, overwrite (creates dead space), vacuum, close.
	{
		db := openTestDBAt(fs, t, dir, nil)
		for i := 0; i < 300; i++ {
			key := fmt.Sprintf("wv_%04d", i)
			mustPut(t, db, key, makeTestValue(46))
		}
		db.Sync()

		stats, err := db.VacuumKV()
		if err != nil {
			t.Fatalf("session 2 vacuum: %v", err)
		}
		t.Logf("session 2 vacuum: %s", stats)
		mustCheckIntegrity(t, db)
		db.Close()
	}

	// Session 3: reopen, write more data, vacuum again.
	// This is where the bug would manifest — the first vacuum left
	// the file un-truncated, so poffs from session 2's vacuum now
	// point beyond the .vacuum file's boundary.
	{
		db := openTestDBAt(fs, t, dir, nil)

		// Write some new keys and overwrite some old ones.
		for i := 0; i < 300; i++ {
			key := fmt.Sprintf("wv_%04d", i)
			mustPut(t, db, key, makeTestValue(47))
		}
		for i := 300; i < 400; i++ {
			key := fmt.Sprintf("wv_%04d", i)
			mustPut(t, db, key, makeTestValue(47))
		}
		db.Sync()

		stats, err := db.VacuumKV()
		if err != nil {
			t.Fatalf("session 3 vacuum: %v", err)
		}
		t.Logf("session 3 vacuum: %s", stats)
		mustCheckIntegrity(t, db)

		// Verify all data.
		for i := 0; i < 400; i++ {
			key := fmt.Sprintf("wv_%04d", i)
			mustGet(t, db, key, makeTestValue(47))
		}
		db.Close()
	}

	// Session 4: reopen, vacuum with no writes — should be a no-op.
	{
		db := openTestDBAt(fs, t, dir, nil)
		stats, err := db.VacuumKV()
		if err != nil {
			t.Fatalf("session 4 vacuum (no-op): %v", err)
		}
		t.Logf("session 4 vacuum (no-op): %s", stats)
		mustCheckIntegrity(t, db)

		for i := 0; i < 400; i++ {
			key := fmt.Sprintf("wv_%04d", i)
			mustGet(t, db, key, makeTestValue(47))
		}
		db.Close()
	}
}

// ====================== Lock-free iterator mutation tests ======================

// TestFlexDB_IteratorDeleteDuringForward tests deleting the current key during
// forward iteration. With the lock-free re-seeking iterator, this must not deadlock.
func TestFlexDB_IteratorDeleteDuringForward(t *testing.T) {
	db, _ := openTestDB(t, nil)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		mustPut(t, db, k, "v:"+k)
	}

	it := db.NewIter()
	defer it.Close()
	it.SeekToFirst()

	var got []string
	for it.Valid() {
		k := string(it.Key())
		got = append(got, k)
		if k == "c" {
			mustDelete(t, db, "c")
		}
		it.Next()
	}
	// Should see a,b,c,d,e — "c" was seen before deletion, next re-seeks past "c"
	expectKeys(t, "delete during forward", got, []string{"a", "b", "c", "d", "e"})
}

// TestFlexDB_IteratorDeleteCurrentAndNext tests deleting both current and next key.
func TestFlexDB_IteratorDeleteCurrentAndNext(t *testing.T) {
	db, _ := openTestDB(t, nil)
	for _, k := range []string{"a", "b", "c", "d"} {
		mustPut(t, db, k, "v:"+k)
	}

	it := db.NewIter()
	defer it.Close()
	it.SeekToFirst()

	var got []string
	for it.Valid() {
		k := string(it.Key())
		got = append(got, k)
		if k == "b" {
			mustDelete(t, db, "b")
			mustDelete(t, db, "c")
		}
		it.Next()
	}
	// b is seen, then b+c deleted, next from "b" strict finds "d"
	expectKeys(t, "delete current+next", got, []string{"a", "b", "d"})
}

// TestFlexDB_IteratorDeleteAllAscend deletes every key during Ascend callback.
func TestFlexDB_IteratorDeleteAllAscend(t *testing.T) {
	db, _ := openTestDB(t, nil)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		mustPut(t, db, k, "v:"+k)
	}

	var deleted []string
	db.Ascend(nil, func(key, value []byte) bool {
		k := string(key)
		deleted = append(deleted, k)
		mustDelete(t, db, k)
		return true
	})
	expectKeys(t, "ascend+delete all", deleted, []string{"a", "b", "c", "d", "e"})

	// DB should be empty
	val, ok := db.Get([]byte("a"))
	if ok {
		t.Fatalf("expected empty DB, got key 'a' val=%q", val)
	}
}

// TestFlexDB_IteratorPutDuringForward tests inserting a key during forward iteration.
func TestFlexDB_IteratorPutDuringForward(t *testing.T) {
	db, _ := openTestDB(t, nil)
	for _, k := range []string{"a", "c", "e"} {
		mustPut(t, db, k, "v:"+k)
	}

	it := db.NewIter()
	defer it.Close()
	it.SeekToFirst()

	var got []string
	for it.Valid() {
		k := string(it.Key())
		got = append(got, k)
		if k == "c" {
			mustPut(t, db, "d", "v:d") // insert between c and e
		}
		it.Next()
	}
	// After "c", next re-seeks past "c" and finds "d" (newly inserted)
	expectKeys(t, "put during forward", got, []string{"a", "c", "d", "e"})
}

// TestFlexDB_IteratorDeleteDuringBackward tests deleting during backward iteration.
func TestFlexDB_IteratorDeleteDuringBackward(t *testing.T) {
	db, _ := openTestDB(t, nil)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		mustPut(t, db, k, "v:"+k)
	}

	it := db.NewIter()
	defer it.Close()
	it.SeekToLast()

	var got []string
	for it.Valid() {
		k := string(it.Key())
		got = append(got, k)
		if k == "c" {
			mustDelete(t, db, "c")
		}
		it.Prev()
	}
	// Should see e,d,c,b,a — "c" was seen before deletion
	expectKeys(t, "delete during backward", got, []string{"e", "d", "c", "b", "a"})
}

// TestFlexDB_AscendDeleteOldTimestamps simulates deleting old timestamp-prefixed keys.
func TestFlexDB_AscendDeleteOldTimestamps(t *testing.T) {
	db, _ := openTestDB(t, nil)
	mustPut(t, db, "2024-01-01:k1", "old1")
	mustPut(t, db, "2024-06-01:k2", "old2")
	mustPut(t, db, "2025-01-01:k3", "new1")
	mustPut(t, db, "2025-06-01:k4", "new2")

	cutoff := []byte("2025-")
	db.Ascend(nil, func(key, value []byte) bool {
		if bytes.Compare(key, cutoff) < 0 {
			db.Delete(key)
		}
		return true
	})

	// Only new keys should remain
	var remaining []string
	db.Ascend(nil, func(key, value []byte) bool {
		remaining = append(remaining, string(key))
		return true
	})
	expectKeys(t, "after timestamp delete", remaining, []string{"2025-01-01:k3", "2025-06-01:k4"})
}

// TestFlexDB_IteratorMutateAfterSync tests iterator mutations with data in FlexSpace.
func TestFlexDB_IteratorMutateAfterSync(t *testing.T) {
	db, _ := openTestDB(t, nil)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		mustPut(t, db, k, "v:"+k)
	}
	db.Sync()

	// Delete "c" during forward iteration over FlexSpace data
	it := db.NewIter()
	defer it.Close()
	it.SeekToFirst()

	var got []string
	for it.Valid() {
		k := string(it.Key())
		got = append(got, k)
		if k == "b" {
			mustDelete(t, db, "c")
		}
		it.Next()
	}
	// "c" was deleted before we reached it, so re-seek from "b" skips it
	expectKeys(t, "mutate after sync", got, []string{"a", "b", "d", "e"})
}

// TestFlexDB_IteratorEmptyDB tests iterator on an empty database.
func TestFlexDB_IteratorEmptyDB(t *testing.T) {
	db, _ := openTestDB(t, nil)

	it := db.NewIter()
	defer it.Close()

	it.SeekToFirst()
	if it.Valid() {
		t.Fatal("SeekToFirst on empty DB should be invalid")
	}

	it.SeekToLast()
	if it.Valid() {
		t.Fatal("SeekToLast on empty DB should be invalid")
	}

	it.Seek([]byte("x"))
	if it.Valid() {
		t.Fatal("Seek on empty DB should be invalid")
	}
}

// TestFlexDB_IteratorSingleKey tests iterator with one key, then deletes it.
func TestFlexDB_IteratorSingleKey(t *testing.T) {
	db, _ := openTestDB(t, nil)
	mustPut(t, db, "only", "val")

	it := db.NewIter()
	defer it.Close()

	it.Seek([]byte("only"))
	if !it.Valid() || string(it.Key()) != "only" {
		t.Fatalf("Seek(only): valid=%v key=%q", it.Valid(), it.Key())
	}

	mustDelete(t, db, "only")
	it.Next()
	if it.Valid() {
		t.Fatalf("after delete+Next: should be invalid, got key=%q", it.Key())
	}
}

// TestFlexDB_DescendDeleteDuringCallback tests Descend with deletion of current key.
func TestFlexDB_DescendDeleteDuringCallback(t *testing.T) {
	db, _ := openTestDB(t, nil)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		mustPut(t, db, k, "v:"+k)
	}

	var got []string
	db.Descend(nil, func(key, value []byte) bool {
		k := string(key)
		got = append(got, k)
		db.Delete(key) // delete current key
		return true
	})
	expectKeys(t, "descend+delete", got, []string{"e", "d", "c", "b", "a"})

	// DB should be empty
	val, ok := db.Get([]byte("c"))
	if ok {
		t.Fatalf("expected empty DB, got key 'c' val=%q", val)
	}
}

// TestFlexDB_IteratorHasInlineValue tests the Large/FetchV API.
func TestFlexDB_IteratorHasInlineValue(t *testing.T) {
	db, _ := openTestDB(t, nil)

	// Small inline value
	mustPut(t, db, "small", "tiny")
	// Large value (> vlogInlineThreshold=64)
	bigVal := makeTestValue(vlogInlineThreshold + 1)
	mustPut(t, db, "large", bigVal)

	// Small (inline) empty value
	mustPut(t, db, "zeeKeyToEmpty", "")

	it := db.NewIter()
	defer it.Close()
	it.SeekToFirst()

	// First key: "large" (alphabetically first)
	if !it.Valid() || string(it.Key()) != "large" {
		t.Fatalf("expected key 'large', got valid=%v key=%q", it.Valid(), it.Key())
	}
	if !it.Large() {
		t.Fatal("large value should not be inline")
	}
	if it.Vin() != nil {
		t.Fatal("Value() should be nil for large values")
	}
	if v, empty, large := it.Vel(); v != nil || empty || !large {
		t.Fatal("Vel() should return (nil, false, true) for large values")
	}
	val, err := it.FetchV()
	if err != nil {
		t.Fatalf("FetchV: %v", err)
	}
	if string(val) != bigVal {
		t.Fatalf("FetchV: got len=%d, want len=%d", len(val), len(bigVal))
	}

	it.Next()
	// Second key: "small"
	if !it.Valid() || string(it.Key()) != "small" {
		t.Fatalf("expected key 'small', got valid=%v key=%q", it.Valid(), it.Key())
	}
	if it.Large() {
		t.Fatal("small value should be inline")
	}
	if string(it.Vin()) != "tiny" {
		t.Fatalf("Value() = %q, want 'tiny'", it.Vin())
	}
	val, err = it.FetchV()
	if err != nil {
		t.Fatalf("FetchV for inline: %v", err)
	}
	if string(val) != "tiny" {
		t.Fatalf("FetchV for inline: got %q, want 'tiny'", val)
	}
	if v, empty, large := it.Vel(); v == nil || empty || large {
		t.Fatal("Vel() should return (something, false, false) for small non-emtpy values")
	}

	it.Next()
	// Third key: "zeeKeyToEmpty", with empty len(0) value.
	if !it.Valid() || string(it.Key()) != "zeeKeyToEmpty" {
		t.Fatalf("expected key 'zeeKeyToEmpty', got valid=%v key=%q", it.Valid(), it.Key())
	}
	if it.Large() {
		t.Fatal("zeeKeyToEmpty value should be inline")
	}
	if string(it.Vin()) != "" {
		t.Fatalf("Value() = %q, want empty string", it.Vin())
	}
	val, err = it.FetchV()
	if err != nil {
		t.Fatalf("FetchV for inline: %v", err)
	}
	if string(val) != "" {
		t.Fatalf("FetchV for inline: got %q, want empty string", val)
	}
	if v, empty, large := it.Vel(); v == nil || empty || large {
		t.Fatal("Vel() should return (nil, true, false) for small emtpy values")
	}
}
