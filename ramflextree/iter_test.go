package ramflextree

import (
	"bytes"
	"fmt"
	"sort"

	"testing"
)

// TestFlexDB_IteratorBasic tests the iterator with a few keys.
func TestFlexDB_IteratorBasic(t *testing.T) {
	db := openTestDB(t, nil)

	keys := []string{"banana", "apple", "cherry", "date"}
	for i, k := range keys {
		mustPutVtyp(t, db, k, "v:"+k, uint64(i))
	}

	db.View(func(roDB *ReadOnlyTx) error {
		it := roDB.NewIter()
		it.SeekFirst()
		defer it.Close()

		want := []string{"apple", "banana", "cherry", "date"}
		wantVtyp := []uint64{1, 0, 2, 3}
		for k, wk := range want {
			if !it.Valid() {
				t.Fatalf("iterator ended early; want key %q", wk)
			}
			if it.Key() != wk {
				t.Fatalf("Key() = %q, want %q", it.Key(), wk)
			}
			expectedVtyp := wantVtyp[k]
			gotVtyp := it.Vtyp()
			if gotVtyp != expectedVtyp {
				t.Fatalf("got Vtyp() = %v, wanted %v", gotVtyp, expectedVtyp)
			}
			if string(it.Vin()) != "v:"+wk {
				t.Fatalf("Value() = %q, want %q", it.Vin(), "v:"+wk)
			}
			it.Next()
		}
		if it.Valid() {
			t.Fatalf("iterator not exhausted; extra key %q", it.Key())
		}
		return nil
	})
}

// TestFlexDB_IteratorSeek tests Seek() to a specific key.
func TestFlexDB_IteratorSeek(t *testing.T) {
	db := openTestDB(t, nil)

	for _, k := range []string{"aaa", "bbb", "ccc", "ddd", "eee"} {
		mustPut(t, db, k, k)
	}

	db.View(func(roDB *ReadOnlyTx) error {
		it := roDB.NewIter()
		defer it.Close()

		it.Seek("ccc")
		if !it.Valid() || it.Key() != "ccc" {
			t.Fatalf("Seek(ccc): got %v/%q", it.Valid(), it.Key())
		}
		it.Next()
		if !it.Valid() || it.Key() != "ddd" {
			t.Fatalf("After Seek+Next: got %v/%q, want ddd", it.Valid(), it.Key())
		}

		it.Seek("d")
		if !it.Valid() || it.Key() != "ddd" {
			t.Fatalf("Seek(d): got %v/%q, want ddd", it.Valid(), it.Key())
		}
		return nil
	})
}

// TestFlexDB_IteratorAfterSync tests iteration after data is in FlexSpace.
func TestFlexDB_IteratorAfterSync(t *testing.T) {
	db := openTestDB(t, nil)

	keys := []string{"z", "m", "a", "f", "b"}
	for _, k := range keys {
		mustPut(t, db, k, "v:"+k)
	}
	db.Sync()

	db.View(func(roDB *ReadOnlyTx) error {
		it := roDB.NewIter()
		it.SeekFirst()
		defer it.Close()

		sort.Strings(keys)
		for _, wk := range keys {
			if !it.Valid() {
				t.Fatalf("iterator ended early; want %q", wk)
			}
			if it.Key() != wk {
				t.Fatalf("Key() = %q, want %q", it.Key(), wk)
			}
			it.Next()
		}
		if it.Valid() {
			t.Fatalf("iterator not exhausted; extra key %q", it.Key())
		}
		return nil
	})
}

func TestFlexDB_IteratorDirectionChangeAfterReversePrefetch(t *testing.T) {
	db := openTestDB(t, &Config{DisableBackgroundFlush: true})

	for _, k := range []string{"a", "b", "c", "d"} {
		mustPut(t, db, k, "v:"+k)
	}
	if err := db.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	if err := db.View(func(roDB *ReadOnlyTx) error {
		it := roDB.NewIter()
		defer it.Close()

		it.SeekLast()
		if !it.Valid() || it.Key() != "d" {
			t.Fatalf("SeekLast: got valid=%v key=%q, want d", it.Valid(), it.Key())
		}

		it.Prev()
		if !it.Valid() || it.Key() != "c" {
			t.Fatalf("Prev: got valid=%v key=%q, want c", it.Valid(), it.Key())
		}

		it.Next()
		if !it.Valid() || it.Key() != "d" {
			t.Fatalf("Next after reverse prefetch: got valid=%v key=%q, want d", it.Valid(), it.Key())
		}
		return nil
	}); err != nil {
		t.Fatalf("View: %v", err)
	}
}

func TestFlexDB_IteratorGetAnySizeDoesNotPoisonInlineCache(t *testing.T) {
	db := openTestDB(t, &Config{DisableBackgroundFlush: true})

	records := []dbVtypFuzzRecord{
		{
			key:   "inline/a",
			value: bytes.Repeat([]byte{'a'}, 64),
			vtyp:  0x1111,
		},
		{
			key:   "inline/b",
			value: bytes.Repeat([]byte{'b'}, 64),
			vtyp:  0x2222,
		},
	}
	for _, rec := range records {
		if err := db.Put(rec.key, rec.value, rec.vtyp); err != nil {
			t.Fatalf("Put(%q): %v", rec.key, err)
		}
	}
	if err := db.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	if err := db.View(func(roDB *ReadOnlyTx) error {
		it := roDB.NewIter()
		defer it.Close()
		for it.SeekFirst(); it.Valid(); it.Next() {
			if _, _, _, _, err := it.GetAnySize(); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("View: %v", err)
	}

	for _, rec := range records {
		got, found, gotVtyp, err := db.Get(rec.key)
		if err != nil {
			t.Fatalf("Get(%q): %v", rec.key, err)
		}
		if !found {
			t.Fatalf("Get(%q): not found", rec.key)
		}
		if !bytes.Equal(got, rec.value) {
			t.Fatalf("Get(%q) after iteration = %q, want %q", rec.key, got, rec.value)
		}
		if gotVtyp != rec.vtyp {
			t.Fatalf("Get(%q) vtyp=%#x, want %#x", rec.key, gotVtyp, rec.vtyp)
		}
	}
}

// TestFlexDB_ManyKeysIterator inserts many keys and verifies iterator order.
func TestFlexDB_ManyKeysIterator(t *testing.T) {
	db := openTestDB(t, nil)

	const N = 200
	keys := make([]string, N)
	for i := 0; i < N; i++ {
		keys[i] = fmt.Sprintf("key%06d", i)
		mustPut(t, db, keys[i], fmt.Sprintf("val%06d", i))
	}
	db.Sync()

	db.View(func(roDB *ReadOnlyTx) error {
		it := roDB.NewIter()
		it.SeekFirst()
		defer it.Close()

		sort.Strings(keys)
		for _, wk := range keys {
			if !it.Valid() {
				t.Fatalf("iterator ended early; want %q", wk)
			}
			if it.Key() != wk {
				t.Fatalf("Key() = %q, want %q", it.Key(), wk)
			}
			it.Next()
		}
		if it.Valid() {
			t.Fatalf("iterator not exhausted; extra key %q", it.Key())
		}
		return nil
	})
}

// TestFlexDB_AscendRange tests bounded ascending iteration.
func TestFlexDB_AscendRange(t *testing.T) {
	db := openTestDB(t, nil)
	populateDB(t, db, false)

	db.View(func(roDB *ReadOnlyTx) error {
		// [bbb, ddd) - should include bbb, ccc but NOT ddd
		var keys []string
		roDB.AscendRange("bbb", "ddd", func(key string, value []byte) bool {
			keys = append(keys, key)
			return true
		})
		expectKeys(t, "AscendRange(bbb,ddd)", keys, []string{"bbb", "ccc"})

		// Unbounded start: ["", ccc)
		keys = nil
		roDB.AscendRange("", "ccc", func(key string, value []byte) bool {
			keys = append(keys, key)
			return true
		})
		expectKeys(t, "AscendRange(,ccc)", keys, []string{"aaa", "bbb"})

		// Unbounded end: [ccc, "")
		keys = nil
		roDB.AscendRange("ccc", "", func(key string, value []byte) bool {
			keys = append(keys, key)
			return true
		})
		expectKeys(t, "AscendRange(ccc,)", keys, []string{"ccc", "ddd", "eee"})

		// Both empty: all keys
		keys = nil
		roDB.AscendRange("", "", func(key string, value []byte) bool {
			keys = append(keys, key)
			return true
		})
		expectKeys(t, "AscendRange(,)", keys, []string{"aaa", "bbb", "ccc", "ddd", "eee"})
		return nil
	})
}

// TestFlexDB_DescendRange tests bounded descending iteration.
func TestFlexDB_DescendRange(t *testing.T) {
	db := openTestDB(t, nil)
	populateDB(t, db, false)

	db.View(func(roDB *ReadOnlyTx) error {
		// (bbb, ddd] - should include ddd, ccc but NOT bbb
		var keys []string
		roDB.DescendRange("ddd", "bbb", func(key string, value []byte) bool {
			keys = append(keys, key)
			return true
		})
		expectKeys(t, "DescendRange(ddd,bbb)", keys, []string{"ddd", "ccc"})

		// Unbounded start (descend from end): (bbb, ""]
		keys = nil
		roDB.DescendRange("", "bbb", func(key string, value []byte) bool {
			keys = append(keys, key)
			return true
		})
		expectKeys(t, "DescendRange(,bbb)", keys, []string{"eee", "ddd", "ccc"})

		// Unbounded end (descend to beginning): ("", ddd]
		keys = nil
		roDB.DescendRange("ddd", "", func(key string, value []byte) bool {
			keys = append(keys, key)
			return true
		})
		expectKeys(t, "DescendRange(ddd,)", keys, []string{"ddd", "ccc", "bbb", "aaa"})

		// Both empty: all keys descending
		keys = nil
		roDB.DescendRange("", "", func(key string, value []byte) bool {
			keys = append(keys, key)
			return true
		})
		expectKeys(t, "DescendRange(,)", keys, []string{"eee", "ddd", "ccc", "bbb", "aaa"})
		return nil
	})
}

// TestFlexDB_AscendRangeAfterSync tests AscendRange with data in FlexSpace.
func TestFlexDB_AscendRangeAfterSync(t *testing.T) {
	db := openTestDB(t, nil)
	populateDB(t, db, true)

	db.View(func(roDB *ReadOnlyTx) error {
		var keys []string
		roDB.AscendRange("bbb", "eee", func(key string, value []byte) bool {
			keys = append(keys, key)
			return true
		})
		expectKeys(t, "AscendRange after sync", keys, []string{"bbb", "ccc", "ddd"})
		return nil
	})
}

// TestFlexDB_DescendRangeAfterSync tests DescendRange with data in FlexSpace.
func TestFlexDB_DescendRangeAfterSync(t *testing.T) {
	db := openTestDB(t, nil)
	populateDB(t, db, true)

	db.View(func(roDB *ReadOnlyTx) error {
		var keys []string
		roDB.DescendRange("ddd", "aaa", func(key string, value []byte) bool {
			keys = append(keys, key)
			return true
		})
		expectKeys(t, "DescendRange after sync", keys, []string{"ddd", "ccc", "bbb"})
		return nil
	})
}

// TestFlexDB_AscendValues verifies that values are correct during Ascend.
func TestFlexDB_AscendValues(t *testing.T) {
	db := openTestDB(t, nil)
	populateDB(t, db, false)

	db.View(func(roDB *ReadOnlyTx) error {
		var pairs []string
		roDB.Ascend("bbb", func(key string, value []byte) bool {
			pairs = append(pairs, key+"="+string(value))
			return true
		})
		want := []string{"bbb=v:bbb", "ccc=v:ccc", "ddd=v:ddd", "eee=v:eee"}
		expectKeys(t, "Ascend values", pairs, want)
		return nil
	})
}

// TestFlexDB_DescendValues verifies that values are correct during Descend.
func TestFlexDB_DescendValues(t *testing.T) {
	db := openTestDB(t, nil)
	populateDB(t, db, false)

	db.View(func(roDB *ReadOnlyTx) error {
		var pairs []string
		roDB.Descend("ddd", func(key string, value []byte) bool {
			pairs = append(pairs, key+"="+string(value))
			return true
		})
		want := []string{"ddd=v:ddd", "ccc=v:ccc", "bbb=v:bbb", "aaa=v:aaa"}
		expectKeys(t, "Descend values", pairs, want)
		return nil
	})
}

// TestFlexDB_IteratorPrev tests Prev() on Iter.
func TestFlexDB_IteratorPrev(t *testing.T) {
	db := openTestDB(t, nil)
	populateDB(t, db, false)

	db.View(func(roDB *ReadOnlyTx) error {
		it := roDB.NewIter()
		it.SeekLast()
		defer it.Close()

		var keys []string
		for it.Valid() {
			keys = append(keys, it.Key())
			it.Prev()
		}
		expectKeys(t, "Iterator Prev", keys, []string{"eee", "ddd", "ccc", "bbb", "aaa"})
		return nil
	})
}

// TestFlexDB_IteratorSeekThenPrev tests Seek followed by Prev.
func TestFlexDB_IteratorSeekThenPrev(t *testing.T) {
	db := openTestDB(t, nil)
	populateDB(t, db, false)

	db.View(func(roDB *ReadOnlyTx) error {
		it := roDB.NewIter()
		defer it.Close()

		// Seek to ccc, then go backward
		it.Seek("ccc")
		if !it.Valid() || it.Key() != "ccc" {
			t.Fatalf("Seek(ccc): got %v/%q", it.Valid(), it.Key())
		}
		it.Prev()
		if !it.Valid() || it.Key() != "bbb" {
			t.Fatalf("After Prev: got %v/%q, want bbb", it.Valid(), it.Key())
		}
		it.Prev()
		if !it.Valid() || it.Key() != "aaa" {
			t.Fatalf("After 2nd Prev: got %v/%q, want aaa", it.Valid(), it.Key())
		}
		it.Prev()
		if it.Valid() {
			t.Fatalf("Should be invalid after Prev past beginning, got key %q", it.Key())
		}
		return nil
	})
}

// TestFlexDB_AscendManyKeys tests Ascend/Descend with many keys across FlexSpace.
func TestFlexDB_AscendManyKeys(t *testing.T) {
	db := openTestDB(t, nil)

	const N = 200
	allKeys := make([]string, N)
	for i := 0; i < N; i++ {
		k := fmt.Sprintf("key%06d", i)
		allKeys[i] = k
		mustPut(t, db, k, fmt.Sprintf("v%06d", i))
	}
	db.Sync()
	sort.Strings(allKeys)

	db.View(func(roDB *ReadOnlyTx) error {
		// Ascend from key000100
		var keys []string
		roDB.Ascend("key000100", func(key string, value []byte) bool {
			keys = append(keys, key)
			return true
		})
		want := allKeys[100:] // key000100..key000199
		expectKeys(t, "Ascend(key000100)", keys, want)

		// Descend from key000050
		var dkeys []string
		roDB.Descend("key000050", func(key string, value []byte) bool {
			dkeys = append(dkeys, string(key))
			return true
		})
		want = make([]string, 51)
		for i := 0; i <= 50; i++ {
			want[50-i] = allKeys[i]
		}
		expectKeys(t, "Descend(key000050)", dkeys, want)

		// AscendRange [key000010, key000015)
		var rangeKeys []string
		roDB.AscendRange("key000010", "key000015", func(key string, value []byte) bool {
			rangeKeys = append(rangeKeys, string(key))
			return true
		})
		expectKeys(t, "AscendRange(10,15)", rangeKeys,
			[]string{"key000010", "key000011", "key000012", "key000013", "key000014"})
		return nil
	})
}

// ====================== HLC tests ======================

// TestFlexDB_HLC_PutMonotonic verifies three sequential Puts produce strictly increasing HLCs.
func TestFlexDB_HLC_PutMonotonic(t *testing.T) {
	db := openTestDB(t, nil)

	// Do three sequential Puts and capture the HLC via the memtable.
	keys := []string{"aaa", "bbb", "ccc"}
	hlcs := make([]HLC, len(keys))
	for i, k := range keys {
		err := db.Put(k, []byte("v"), 0)
		if err != nil {
			t.Fatal(err)
		}
		// Read back from the active memtable to get the HLC.
		db.topMutRW.RLock()
		kv, ok := db.mt.get(k)
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
	db := openTestDB(t, nil)

	// Batch with unique keys - single-tick interval.
	batch := db.NewBatch()
	batch.Set("k1", []byte("v1"), 0)
	batch.Set("k2", []byte("v2"), 0)
	batch.Set("k3", []byte("v3"), 0)
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

	// Batch with a duplicate key - multi-tick interval.
	batch2 := db.NewBatch()
	batch2.Set("x1", []byte("v1"), 0)
	batch2.Set("x1", []byte("v2"), 0) // duplicate triggers new tick
	batch2.Set("x2", []byte("v3"), 0)
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
	kvs := []KV{
		{Key: "aaa", Value: []byte("old"), Hlc: 100},
		{Key: "aaa", Value: []byte("new"), Hlc: 200},
		{Key: "bbb", Value: []byte("only"), Hlc: 150},
		{Key: "ccc", Value: []byte("first"), Hlc: 300},
		{Key: "ccc", Value: []byte("second"), Hlc: 250},
		{Key: "ccc", Value: []byte("third"), Hlc: 350},
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
	if string(out[0].Key) != "aaa" || string(out[0].Value) != "new" || out[0].Hlc != 200 {
		t.Fatalf("aaa: got key=%q val=%q hlc=%v", out[0].Key, out[0].Value, out[0].Hlc)
	}
	if string(out[1].Key) != "bbb" || string(out[1].Value) != "only" || out[1].Hlc != 150 {
		t.Fatalf("bbb: got key=%q val=%q hlc=%v", out[1].Key, out[1].Value, out[1].Hlc)
	}
	if string(out[2].Key) != "ccc" || string(out[2].Value) != "third" || out[2].Hlc != 350 {
		t.Fatalf("ccc: got key=%q val=%q hlc=%v", out[2].Key, out[2].Value, out[2].Hlc)
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

// ====================== Iterator mutation tests ======================

// TestFlexDB_IteratorDeleteDuringForward tests deleting the current key during
// forward iteration via rwDB.Delete.
func TestFlexDB_IteratorDeleteDuringForward(t *testing.T) {
	db := openTestDB(t, nil)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		mustPut(t, db, k, "v:"+k)
	}

	db.Update(func(rwDB *WriteTx) error {
		it := rwDB.NewIter()
		defer it.Close()
		it.SeekFirst()

		var got []string
		for it.Valid() {
			k := it.Key()
			got = append(got, k)
			if k == "c" {
				if err := rwDB.Delete("c"); err != nil {
					t.Fatal(err)
				}
			}
			it.Next()
		}
		expectKeys(t, "delete during forward", got, []string{"a", "b", "c", "d", "e"})
		return nil
	})
}

// TestFlexDB_IteratorDeleteCurrentAndNext tests deleting both current and next key.
func TestFlexDB_IteratorDeleteCurrentAndNext(t *testing.T) {
	db := openTestDB(t, nil)
	for _, k := range []string{"a", "b", "c", "d"} {
		mustPut(t, db, k, "v:"+k)
	}

	db.Update(func(rwDB *WriteTx) error {
		it := rwDB.NewIter()
		defer it.Close()
		it.SeekFirst()

		var got []string
		for it.Valid() {
			k := it.Key()
			got = append(got, k)
			if k == "b" {
				if err := rwDB.Delete("b"); err != nil {
					t.Fatal(err)
				}
				if err := rwDB.Delete("c"); err != nil {
					t.Fatal(err)
				}
			}
			it.Next()
		}
		expectKeys(t, "delete current+next", got, []string{"a", "b", "d"})
		return nil
	})
}

// TestFlexDB_IteratorDeleteAllForward deletes every key during forward iteration.
func TestFlexDB_IteratorDeleteAllForward(t *testing.T) {
	db := openTestDB(t, nil)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		mustPut(t, db, k, "v:"+k)
	}

	db.Update(func(rwDB *WriteTx) error {
		it := rwDB.NewIter()
		it.SeekFirst()

		var deleted []string
		for it.Valid() {
			k := it.Key()
			deleted = append(deleted, k)
			if err := rwDB.Delete(k); err != nil {
				it.Close()
				t.Fatal(err)
			}
			it.Next()
		}
		it.Close()
		expectKeys(t, "delete all forward", deleted, []string{"a", "b", "c", "d", "e"})
		return nil
	})

	// DB should be empty
	val, ok, _, gerr := db.Get("a")
	panicOn(gerr)
	if ok {
		t.Fatalf("expected empty DB, got key 'a' val=%q", val)
	}
}

// TestFlexDB_IteratorPutDuringForward tests inserting a key during forward iteration.
func TestFlexDB_IteratorPutDuringForward(t *testing.T) {
	db := openTestDB(t, nil)
	for _, k := range []string{"a", "c", "e"} {
		mustPut(t, db, k, "v:"+k)
	}

	db.Update(func(rwDB *WriteTx) error {
		it := rwDB.NewIter()
		defer it.Close()
		it.SeekFirst()

		var got []string
		for it.Valid() {
			k := it.Key()
			got = append(got, k)
			if k == "c" {
				if err := rwDB.Put("d", []byte("v:d"), 0); err != nil {
					t.Fatal(err)
				}
			}
			it.Next()
		}
		expectKeys(t, "put during forward", got, []string{"a", "c", "d", "e"})
		return nil
	})
}

// TestFlexDB_IteratorDeleteDuringBackward tests deleting during backward iteration.
func TestFlexDB_IteratorDeleteDuringBackward(t *testing.T) {
	db := openTestDB(t, nil)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		mustPut(t, db, k, "v:"+k)
	}

	db.Update(func(rwDB *WriteTx) error {
		it := rwDB.NewIter()
		defer it.Close()
		it.SeekLast()

		var got []string
		for it.Valid() {
			k := it.Key()
			got = append(got, k)
			if k == "c" {
				if err := rwDB.Delete("c"); err != nil {
					t.Fatal(err)
				}
			}
			it.Prev()
		}
		expectKeys(t, "delete during backward", got, []string{"e", "d", "c", "b", "a"})
		return nil
	})
}

// TestFlexDB_IteratorDeleteOldTimestamps simulates deleting old timestamp-prefixed keys.
func TestFlexDB_IteratorDeleteOldTimestamps(t *testing.T) {
	db := openTestDB(t, nil)
	mustPut(t, db, "2024-01-01:k1", "old1")
	mustPut(t, db, "2024-06-01:k2", "old2")
	mustPut(t, db, "2025-01-01:k3", "new1")
	mustPut(t, db, "2025-06-01:k4", "new2")

	cutoff := "2025-"
	db.Update(func(rwDB *WriteTx) error {
		it := rwDB.NewIter()
		it.SeekFirst()
		for it.Valid() {
			if it.Key() < cutoff {
				if err := rwDB.Delete(it.Key()); err != nil {
					it.Close()
					return err
				}
			}
			it.Next()
		}
		it.Close()
		return nil
	})

	// Only new keys should remain
	db.View(func(roDB *ReadOnlyTx) error {
		var remaining []string
		roDB.Ascend("", func(key string, value []byte) bool {
			remaining = append(remaining, key)
			return true
		})
		expectKeys(t, "after timestamp delete", remaining, []string{"2025-01-01:k3", "2025-06-01:k4"})
		return nil
	})
}

// TestFlexDB_IteratorMutateAfterSync tests iterator mutations with data in FlexSpace.
func TestFlexDB_IteratorMutateAfterSync(t *testing.T) {
	db := openTestDB(t, nil)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		mustPut(t, db, k, "v:"+k)
	}
	db.Sync()

	// Delete "c" during forward iteration over FlexSpace data
	db.Update(func(rwDB *WriteTx) error {
		it := rwDB.NewIter()
		defer it.Close()
		it.SeekFirst()

		var got []string
		for it.Valid() {
			k := it.Key()
			got = append(got, k)
			if k == "b" {
				if err := rwDB.Delete("c"); err != nil {
					t.Fatal(err)
				}
			}
			it.Next()
		}
		expectKeys(t, "mutate after sync", got, []string{"a", "b", "d", "e"})
		return nil
	})
}

// TestFlexDB_IteratorEmptyDB tests iterator on an empty database.
func TestFlexDB_IteratorEmptyDB(t *testing.T) {
	db := openTestDB(t, nil)

	db.View(func(roDB *ReadOnlyTx) error {
		it := roDB.NewIter()
		defer it.Close()

		it.SeekFirst()
		if it.Valid() {
			t.Fatal("SeekFirst on empty DB should be invalid")
		}

		it.SeekLast()
		if it.Valid() {
			t.Fatal("SeekLast on empty DB should be invalid")
		}

		it.Seek("x")
		if it.Valid() {
			t.Fatal("Seek on empty DB should be invalid")
		}
		return nil
	})
}

// TestFlexDB_IteratorSingleKey tests iterator with one key, then deletes it.
func TestFlexDB_IteratorSingleKey(t *testing.T) {
	db := openTestDB(t, nil)
	mustPut(t, db, "only", "val")

	db.Update(func(rwDB *WriteTx) error {
		it := rwDB.NewIter()
		defer it.Close()

		it.Seek("only")
		if !it.Valid() || it.Key() != "only" {
			t.Fatalf("Seek(only): valid=%v key=%q", it.Valid(), it.Key())
		}

		if err := rwDB.Delete("only"); err != nil {
			t.Fatal(err)
		}
		it.Next()
		if it.Valid() {
			t.Fatalf("after delete+Next: should be invalid, got key=%q", it.Key())
		}
		return nil
	})
}

// TestFlexDB_IteratorDeleteAllBackward tests deleting every key during backward iteration.
func TestFlexDB_IteratorDeleteAllBackward(t *testing.T) {
	db := openTestDB(t, nil)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		mustPut(t, db, k, "v:"+k)
	}

	db.Update(func(rwDB *WriteTx) error {
		it := rwDB.NewIter()
		it.SeekLast()

		var got []string
		for it.Valid() {
			k := it.Key()
			got = append(got, k)
			if err := rwDB.Delete(k); err != nil {
				it.Close()
				t.Fatal(err)
			}
			it.Prev()
		}
		it.Close()
		expectKeys(t, "delete all backward", got, []string{"e", "d", "c", "b", "a"})
		return nil
	})

	// DB should be empty
	val, ok, _, gerr := db.Get("c")
	panicOn(gerr)
	if ok {
		t.Fatalf("expected empty DB, got key 'c' val=%q", val)
	}
}

// TestFlexDB_IteratorHasInlineValue tests the Large/FetchV API for inline values.
func TestFlexDB_IteratorHasInlineValue(t *testing.T) {
	db := openTestDB(t, nil)

	// Small inline value
	mustPut(t, db, "small", "tiny")
	// Large value (> 64 bytes)
	bigVal := makeTestValue(65)
	mustPut(t, db, "large", bigVal)

	// Small (inline) empty value
	mustPut(t, db, "zeeKeyToEmpty", "")

	db.View(func(roDB *ReadOnlyTx) error {
		it := roDB.NewIter()
		defer it.Close()
		it.SeekFirst()

		// First key: "large" (alphabetically first)
		if !it.Valid() || it.Key() != "large" {
			t.Fatalf("expected key 'large', got valid=%v key=%q", it.Valid(), it.Key())
		}
		if it.Large() {
			t.Fatal("large value should still be inline in RAM-only mode")
		}

		it.Next()
		// Second key: "small"
		if !it.Valid() || it.Key() != "small" {
			t.Fatalf("expected key 'small', got valid=%v key=%q", it.Valid(), it.Key())
		}
		if it.Large() {
			t.Fatal("small value should be inline")
		}
		if string(it.Vin()) != "tiny" {
			t.Fatalf("Value() = %q, want 'tiny'", it.Vin())
		}

		it.Next()
		// Third key: "zeeKeyToEmpty", with empty len(0) value.
		if !it.Valid() || it.Key() != "zeeKeyToEmpty" {
			t.Fatalf("expected key 'zeeKeyToEmpty', got valid=%v key=%q", it.Valid(), it.Key())
		}
		if it.Large() {
			t.Fatal("zeeKeyToEmpty value should be inline")
		}
		if string(it.Vin()) != "" {
			t.Fatalf("Value() = %q, want empty string", it.Vin())
		}
		return nil
	})
}

// ====================== Iter.KV() tests ======================

// TestIterKV_ViewBasic tests KV() returns correct fields during a View scan.
func TestIterKV_ViewBasic(t *testing.T) {
	db := openTestDB(t, nil)
	populateDB(t, db, false) // aaa..eee

	db.View(func(roDB *ReadOnlyTx) error {
		it := roDB.NewIter()
		defer it.Close()
		it.SeekFirst()

		var keys []string
		for it.Valid() {
			kv := it.KV()
			if kv == nil {
				t.Fatal("KV() returned nil on valid iterator")
			}
			keys = append(keys, string(kv.Key))
			wantVal := "v:" + string(kv.Key)
			if string(kv.Value) != wantVal {
				t.Fatalf("KV().Value = %q, want %q", kv.Value, wantVal)
			}
			if kv.Hlc == 0 {
				t.Fatalf("KV().Hlc is zero for key %q", kv.Key)
			}
			it.Next()
		}
		expectKeys(t, "KV() View scan", keys, []string{"aaa", "bbb", "ccc", "ddd", "eee"})
		return nil
	})
}

// TestIterKV_UpdateMutate tests KV() during an Update with mutations.
func TestIterKV_UpdateMutate(t *testing.T) {
	db := openTestDB(t, nil)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		mustPut(t, db, k, "v:"+k)
	}

	var got []string
	db.Update(func(rwDB *WriteTx) error {
		it := rwDB.NewIter()
		defer it.Close()
		it.SeekFirst()

		for it.Valid() {
			kv := it.KV()
			if kv == nil {
				t.Fatal("KV() nil")
			}
			got = append(got, string(kv.Key))
			// Delete "c" when we see it
			if string(kv.Key) == "c" {
				if err := rwDB.Delete("c"); err != nil {
					t.Fatal(err)
				}
			}
			it.Next()
		}
		return nil
	})
	expectKeys(t, "KV() Update scan", got, []string{"a", "b", "c", "d", "e"})
	mustMiss(t, db, "c")
}

// TestIterKV_NilOnInvalid tests KV() returns nil when iterator is invalid.
func TestIterKV_NilOnInvalid(t *testing.T) {
	db := openTestDB(t, nil)

	db.View(func(roDB *ReadOnlyTx) error {
		it := roDB.NewIter()
		defer it.Close()

		// Before any seek
		if kv := it.KV(); kv != nil {
			t.Fatalf("KV() should be nil before seek, got key=%q", kv.Key)
		}

		// After seek on empty DB
		it.SeekFirst()
		if kv := it.KV(); kv != nil {
			t.Fatalf("KV() should be nil on empty DB, got key=%q", kv.Key)
		}
		return nil
	})
}

// TestIterKV_AfterSync tests KV() with data flushed to FlexSpace.
func TestIterKV_AfterSync(t *testing.T) {
	db := openTestDB(t, nil)
	populateDB(t, db, true) // flush to FlexSpace

	db.View(func(roDB *ReadOnlyTx) error {
		it := roDB.NewIter()
		defer it.Close()
		it.SeekFirst()

		var keys []string
		for it.Valid() {
			kv := it.KV()
			keys = append(keys, string(kv.Key))
			if string(kv.Value) != "v:"+string(kv.Key) {
				t.Fatalf("after sync: key=%q value=%q", kv.Key, kv.Value)
			}
			it.Next()
		}
		expectKeys(t, "KV() after sync", keys, []string{"aaa", "bbb", "ccc", "ddd", "eee"})
		return nil
	})
}
