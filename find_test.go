package yogadb

import (
	"bytes"
	"fmt"
	"testing"
)

// TestFind_Exact tests Find with Exact modifier.
func TestFind_Exact(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateFindTestDB(t, db)

	// Exact match: existing key.
	kvc, exact, err := db.Find(Exact, "key003")
	panicOn(err)
	if kvc == nil || !exact {
		t.Fatalf("Exact key003: kvc=%v exact=%v, want non-nil true", kvc, exact)
	}
	defer kvc.Close()
	if kvc.Large() {
		t.Fatal("unexpected large value")
	}
	if string(kvc.Key) != "key003" {
		t.Fatalf("got key %q, want key003", kvc.Key)
	}
	if string(kvc.Value) != "val003" {
		t.Fatalf("got value %q, want val003", kvc.Value)
	}

	// Exact match: missing key.
	kvc2, exact2, err2 := db.Find(Exact, "key999")
	if err2 != nil {
		t.Fatal(err2)
	}
	if kvc2 != nil || exact2 {
		t.Fatalf("Exact key999: kvc=%v exact=%v, want nil false", kvc2, exact2)
	}
}

// TestFind_GTE tests Find with GTE modifier.
func TestFind_GTE(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateFindTestDB(t, db)

	// GTE on existing key -> exact match.
	kvc, exact, err := db.Find(GTE, "key003")
	panicOn(err)
	if kvc == nil || !exact {
		t.Fatalf("GTE key003: kvc=%v exact=%v, want non-nil true", kvc, exact)
	}
	if string(kvc.Key) != "key003" || string(kvc.Value) != "val003" {
		t.Fatalf("got key=%q value=%q, want key003/val003", kvc.Key, kvc.Value)
	}
	kvc.Close()

	// GTE on gap key -> next key.
	kvc2, exact2, err := db.Find(GTE, "key002a")
	panicOn(err)
	if kvc2 == nil {
		t.Fatal("GTE key002a: not found")
	}
	if exact2 {
		t.Fatal("GTE key002a: should not be exact")
	}
	if string(kvc2.Key) != "key003" {
		t.Fatalf("GTE key002a: got key %q, want key003", kvc2.Key)
	}
	if string(kvc2.Value) != "val003" {
		t.Fatalf("got value %q, want val003", kvc2.Value)
	}
	kvc2.Close()

	// GTE past end -> not found.
	kvc3, _, err := db.Find(GTE, "key999")
	panicOn(err)
	if kvc3 != nil {
		t.Fatal("expected nil KVcloser")
	}

	// GTE nil -> first key.
	kvc4, _, err := db.Find(GTE, "")
	panicOn(err)
	if kvc4 == nil {
		t.Fatal("GTE nil: not found")
	}
	if string(kvc4.Key) != "key001" {
		t.Fatalf("GTE nil: got key %q, want key001", kvc4.Key)
	}
	kvc4.Close()
}

// TestFind_GT tests Find with GT modifier.
func TestFind_GT(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateFindTestDB(t, db)

	// GT on existing key -> next key.
	kvc, exact, err := db.Find(GT, "key003")
	panicOn(err)
	if kvc == nil {
		t.Fatal("GT key003: not found")
	}
	if exact {
		t.Fatal("GT key003: should not be exact (key003 itself is skipped)")
	}
	if string(kvc.Key) != "key004" {
		t.Fatalf("GT key003: got key %q, want key004", kvc.Key)
	}
	if string(kvc.Value) != "val004" {
		t.Fatalf("got value %q, want val004", kvc.Value)
	}
	kvc.Close()

	// GT on gap key -> next key (same as GTE on gap).
	kvc2, _, err := db.Find(GT, "key002a")
	panicOn(err)
	if kvc2 == nil {
		t.Fatal("GT key002a: not found")
	}
	if string(kvc2.Key) != "key003" {
		t.Fatalf("GT key002a: got key %q, want key003", kvc2.Key)
	}
	kvc2.Close()

	// GT on last key -> not found.
	kvc3, _, err := db.Find(GT, "key010")
	panicOn(err)
	if kvc3 != nil {
		t.Fatal("GT key010: should not be found")
	}

	// GT nil -> first key.
	kvc4, _, err := db.Find(GT, "")
	panicOn(err)
	if kvc4 == nil {
		t.Fatal("GT nil: not found")
	}
	if string(kvc4.Key) != "key001" {
		t.Fatalf("GT nil: got key %q, want key001", kvc4.Key)
	}
	kvc4.Close()
}

// TestFind_LTE tests Find with LTE modifier.
func TestFind_LTE(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateFindTestDB(t, db)

	// LTE on existing key -> exact match.
	kvc, exact, err := db.Find(LTE, "key003")
	panicOn(err)
	if kvc == nil || !exact {
		t.Fatalf("LTE key003: kvc=%v exact=%v, want non-nil true", kvc, exact)
	}
	if string(kvc.Key) != "key003" || string(kvc.Value) != "val003" {
		t.Fatalf("got key=%q value=%q, want key003/val003", kvc.Key, kvc.Value)
	}
	kvc.Close()

	// LTE on gap key -> previous key.
	kvc2, exact2, err := db.Find(LTE, "key003a")
	panicOn(err)
	if kvc2 == nil {
		t.Fatal("LTE key003a: not found")
	}
	if exact2 {
		t.Fatal("LTE key003a: should not be exact")
	}
	if string(kvc2.Key) != "key003" {
		t.Fatalf("LTE key003a: got key %q, want key003", kvc2.Key)
	}
	kvc2.Close()

	// LTE before first -> not found.
	kvc3, _, err := db.Find(LTE, "key000")
	panicOn(err)
	if kvc3 != nil {
		t.Fatal("LTE key000: should not be found")
	}

	// LTE nil -> last key.
	kvc4, _, err := db.Find(LTE, "")
	panicOn(err)
	if kvc4 == nil {
		t.Fatal("LTE nil: not found")
	}
	if string(kvc4.Key) != "key010" {
		t.Fatalf("LTE nil: got key %q, want key010", kvc4.Key)
	}
	kvc4.Close()
}

// TestFind_LT tests Find with LT modifier.
func TestFind_LT(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateFindTestDB(t, db)

	// LT on existing key -> previous key.
	kvc, exact, err := db.Find(LT, "key003")
	panicOn(err)
	if kvc == nil {
		t.Fatal("LT key003: not found")
	}
	if exact {
		t.Fatal("LT key003: should not be exact")
	}
	if string(kvc.Key) != "key002" {
		t.Fatalf("LT key003: got key %q, want key002", kvc.Key)
	}
	if string(kvc.Value) != "val002" {
		t.Fatalf("got value %q, want val002", kvc.Value)
	}
	kvc.Close()

	// LT on gap key -> previous key.
	kvc2, _, err := db.Find(LT, "key003a")
	panicOn(err)
	if kvc2 == nil {
		t.Fatal("LT key003a: not found")
	}
	if string(kvc2.Key) != "key003" {
		t.Fatalf("LT key003a: got key %q, want key003", kvc2.Key)
	}
	kvc2.Close()

	// LT on first key -> not found.
	kvc3, _, err := db.Find(LT, "key001")
	panicOn(err)
	if kvc3 != nil {
		t.Fatal("LT key001: should not be found")
	}

	// LT nil -> last key.
	kvc4, _, err := db.Find(LT, "")
	panicOn(err)
	if kvc4 == nil {
		t.Fatal("LT nil: not found")
	}
	if string(kvc4.Key) != "key010" {
		t.Fatalf("LT nil: got key %q, want key010", kvc4.Key)
	}
	kvc4.Close()
}

// TestFindIt_IteratorContinuation verifies that the returned iterator
// can be used to scan beyond the found key.
func TestFindIt_IteratorContinuation(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateFindTestDB(t, db)

	err := db.View(func(roDB *ReadOnlyTx) error {
		// FindIt GTE key005, then iterate forward.
		kvc, _, err, it := roDB.FindIt(GTE, "key005")
		panicOn(err)
		if kvc == nil {
			it.Close()
			t.Fatal("GTE key005: not found")
		}
		if string(kvc.Key) != "key005" {
			it.Close()
			t.Fatalf("got key %q, want key005", kvc.Key)
		}
		kvc.Close()
		var keys []string
		for it.Valid() {
			keys = append(keys, it.Key())
			it.Next()
		}
		it.Close() // must close before opening next iterator
		expected := []string{"key005", "key006", "key007", "key008", "key009", "key010"}
		if len(keys) != len(expected) {
			t.Fatalf("got %d keys, want %d: %v", len(keys), len(expected), keys)
		}
		for i, k := range keys {
			if k != expected[i] {
				t.Fatalf("key[%d] = %q, want %q", i, k, expected[i])
			}
		}

		// FindIt LTE key005, then iterate backward.
		kvc2, _, err2, it2 := roDB.FindIt(LTE, "key005")
		defer it2.Close()
		panicOn(err2)
		if kvc2 == nil {
			t.Fatal("LTE key005: not found")
		}
		if string(kvc2.Key) != "key005" {
			t.Fatalf("got key %q, want key005", kvc2.Key)
		}
		kvc2.Close()
		var keys2 []string
		for it2.Valid() {
			keys2 = append(keys2, string(it2.Key()))
			it2.Prev()
		}
		expected2 := []string{"key005", "key004", "key003", "key002", "key001"}
		if len(keys2) != len(expected2) {
			t.Fatalf("got %d keys, want %d: %v", len(keys2), len(expected2), keys2)
		}
		for i, k := range keys2 {
			if k != expected2[i] {
				t.Fatalf("key[%d] = %q, want %q", i, k, expected2[i])
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestFind_EmptyDB tests Find on an empty database.
func TestFind_EmptyDB(t *testing.T) {
	db, _ := openTestDB(t, nil)

	for _, smod := range []SearchModifier{Exact, GTE, GT, LTE, LT} {
		kvc, _, err := db.Find(smod, "anything")
		if err != nil {
			t.Errorf("smod=%d on empty DB: unexpected error: %v", smod, err)
		}
		if kvc != nil {
			t.Errorf("smod=%d on empty DB: expected nil KVcloser", smod)
		}
	}
}

// TestFind_AfterSync verifies Find works after data is flushed to FlexSpace.
func TestFind_AfterSync(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateFindTestDB(t, db)
	db.Sync()

	// GTE on gap.
	kvc, exact, err := db.Find(GTE, "key004a")
	panicOn(err)
	if kvc == nil || exact {
		t.Fatalf("GTE key004a after sync: kvc=%v exact=%v", kvc, exact)
	}
	if string(kvc.Key) != "key005" {
		t.Fatalf("got key %q, want key005", kvc.Key)
	}
	kvc.Close()

	// LT on existing.
	kvc2, exact2, err := db.Find(LT, "key005")
	panicOn(err)
	if kvc2 == nil || exact2 {
		t.Fatalf("LT key005 after sync: kvc=%v exact=%v", kvc2, exact2)
	}
	if string(kvc2.Key) != "key004" {
		t.Fatalf("got key %q, want key004", kvc2.Key)
	}
	kvc2.Close()
}

// TestFind_KVOwnership verifies that the returned KV from Find is safe
// to retain by mutating the DB after Find returns.
func TestFind_KVOwnership(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateFindTestDB(t, db)
	db.Sync() // flush to FlexSpace

	kvc, _, err := db.Find(GTE, "key005")
	panicOn(err)
	if kvc == nil {
		t.Fatal("not found")
	}
	origKey := string(kvc.Key)
	origVal := string(kvc.Value)

	// Overwrite the key and sync to potentially evict cache.
	mustPut(t, db, "key005", "CHANGED")
	db.Sync()

	// The original KV should still hold the old values (owned copy).
	if string(kvc.Key) != origKey {
		t.Fatalf("key mutated: got %q, want %q", kvc.Key, origKey)
	}
	if string(kvc.Value) != origVal {
		t.Fatalf("value mutated: got %q, want %q", kvc.Value, origVal)
	}
	kvc.Close()
}

// TestFind_HLCPopulated verifies that the returned KV has a non-zero HLC.
func TestFind_HLCPopulated(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateFindTestDB(t, db)

	kvc, _, err := db.Find(Exact, "key005")
	panicOn(err)
	if kvc == nil {
		t.Fatal("not found")
	}
	if kvc.Hlc == 0 {
		t.Fatal("expected non-zero HLC on found KV")
	}
	kvc.Close()
}

// TestFetchLarge_InlineValue verifies FetchLarge works for inline values.
func TestFetchLarge_InlineValue(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateFindTestDB(t, db)

	kvc, _, err := db.Find(Exact, "key003")
	panicOn(err)
	if kvc == nil {
		t.Fatal("not found")
	}
	if kvc.Large() {
		t.Fatal("expected inline value, got large")
	}
	// FetchLarge on an inline value should return the value.
	val, err := db.FetchLarge(&kvc.KV)
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "val003" {
		t.Fatalf("got %q, want val003", val)
	}
	kvc.Close()
}

// TestFind_LazyLarge tests the LAZY_LARGE flag: large values are not
// auto-fetched, and must be retrieved via kvc.Fetch().
func TestFind_LazyLarge(t *testing.T) {
	db, _ := openTestDB(t, nil)

	bigVal := bytes.Repeat([]byte("x"), 200)
	panicOn(db.Put("bigkey", bigVal))
	db.Sync()

	// Without LAZY_LARGE: value auto-fetched
	kvc, _, err := db.Find(Exact, "bigkey")
	panicOn(err)
	if kvc == nil {
		t.Fatal("not found")
	}
	if !bytes.Equal(kvc.Value, bigVal) {
		t.Fatal("value mismatch on auto-fetch")
	}
	kvc.Close()

	// With LAZY_LARGE: value NOT fetched until Fetch()
	kvc2, _, err := db.Find(Exact|LAZY_LARGE, "bigkey")
	panicOn(err)
	if kvc2 == nil {
		t.Fatal("not found")
	}
	if kvc2.Value != nil {
		t.Fatal("expected nil Value with LAZY_LARGE before Fetch")
	}
	if !kvc2.Large() {
		t.Fatal("expected Large() == true")
	}
	err = kvc2.Fetch()
	panicOn(err)
	if !bytes.Equal(kvc2.Value, bigVal) {
		t.Fatal("value mismatch after Fetch")
	}
	kvc2.Close()
}

// TestFind_LazySmall tests the LAZY_SMALL flag: zero-copy inline values
// via cache pinning. Values alias cache memory and require Close().
func TestFind_LazySmall(t *testing.T) {
	db, _ := openTestDB(t, nil)

	// Populate and flush to FlexSpace so values are in cache.
	for i := 1; i <= 10; i++ {
		k := fmt.Sprintf("key%03d", i)
		v := fmt.Sprintf("val%03d", i)
		panicOn(db.Put(k, []byte(v)))
	}
	db.Sync()

	// LAZY_SMALL: zero-copy inline value
	kvc, exact, err := db.Find(Exact|LAZY_SMALL, "key005")
	panicOn(err)
	if kvc == nil || !exact {
		t.Fatal("not found")
	}
	if string(kvc.Value) != "val005" {
		t.Fatalf("got %q, want val005", kvc.Value)
	}
	// Value aliases cache memory - verify it's valid before Close
	valRef := kvc.Value
	if len(valRef) != 6 {
		t.Fatal("unexpected length")
	}
	kvc.Close()
	// After Close, kvc.Value is nil
	if kvc.Value != nil {
		t.Fatal("Value should be nil after Close")
	}

	// LAZY_SMALL with GTE: non-exact match
	kvc2, exact2, err := db.Find(GTE|LAZY_SMALL, "key004a")
	panicOn(err)
	if kvc2 == nil {
		t.Fatal("not found")
	}
	if exact2 {
		t.Fatal("should not be exact")
	}
	if kvc2.Key != "key005" {
		t.Fatalf("got %q, want key005", kvc2.Key)
	}
	if string(kvc2.Value) != "val005" {
		t.Fatalf("got %q, want val005", kvc2.Value)
	}
	kvc2.Close()

	// LAZY_SMALL|LAZY_LARGE combined with a large value
	bigVal := bytes.Repeat([]byte("B"), 200)
	panicOn(db.Put("large001", bigVal))
	db.Sync()

	// equivalent to LAZY, but just to be explicit:
	//kvc3, _, err := db.Find(Exact|LAZY_SMALL|LAZY_LARGE, "large001")
	kvc3, _, err := db.Find(Exact|LAZY, "large001")
	panicOn(err)
	if kvc3 == nil {
		t.Fatal("not found")
	}
	if !kvc3.Large() {
		t.Fatal("expected Large")
	}
	if kvc3.Value != nil {
		t.Fatal("expected nil Value before Fetch")
	}
	err = kvc3.Fetch()
	panicOn(err)
	if !bytes.Equal(kvc3.Value, bigVal) {
		t.Fatal("value mismatch after Fetch")
	}
	kvc3.Close()
}

// TestFind_SkipValues verifies SKIP_VALUES returns keys with nil Values
// for both inline and VLOG values, and that iterator Vin/Vel/FetchV return nil.
func TestFind_SkipValues(t *testing.T) {
	db, _ := openTestDB(t, nil)

	// Populate with small (inline) and large (VLOG) values.
	for i := 1; i <= 10; i++ {
		k := fmt.Sprintf("key%03d", i)
		v := fmt.Sprintf("val%03d", i)
		panicOn(db.Put(k, []byte(v)))
	}
	bigVal := bytes.Repeat([]byte("X"), 200) // > vlogInlineThreshold
	panicOn(db.Put("large001", bigVal))
	db.Sync()

	// Find with SKIP_VALUES: inline value
	kvc, exact, err := db.Find(Exact|SKIP_VALUES, "key005")
	panicOn(err)
	if kvc == nil || !exact {
		t.Fatal("not found")
	}
	if kvc.Key != "key005" {
		t.Fatalf("got key %q, want key005", kvc.Key)
	}
	if kvc.Value != nil {
		t.Fatalf("SKIP_VALUES: expected nil Value, got %q", kvc.Value)
	}
	kvc.Close()

	// Find with SKIP_VALUES: large value
	kvc2, exact2, err := db.Find(Exact|SKIP_VALUES, "large001")
	panicOn(err)
	if kvc2 == nil || !exact2 {
		t.Fatal("not found")
	}
	if kvc2.Key != "large001" {
		t.Fatalf("got key %q, want large001", kvc2.Key)
	}
	if kvc2.Value != nil {
		t.Fatalf("SKIP_VALUES: expected nil Value for large key, got len=%d", len(kvc2.Value))
	}
	kvc2.Close()

	// FindIt + iterator with SKIP_VALUES: keys-only scan
	err = db.View(func(ro *ReadOnlyTx) error {
		kvc3, _, ferr, it := ro.FindIt(GTE|SKIP_VALUES, "key001")
		if ferr != nil {
			return ferr
		}
		if kvc3 == nil {
			t.Fatal("FindIt: not found")
		}
		if kvc3.Value != nil {
			t.Fatalf("FindIt SKIP_VALUES: expected nil Value, got %q", kvc3.Value)
		}
		kvc3.Close()

		// Iterate: all Vin/Vel/FetchV should return nil
		count := 0
		for it.Valid() {
			if it.Key() == "" {
				t.Fatal("empty key")
			}
			if it.Vin() != nil {
				t.Fatalf("Vin should be nil with SKIP_VALUES, key=%s", it.Key())
			}
			val, empty, large := it.Vel()
			if val != nil {
				t.Fatalf("Vel should return nil val with SKIP_VALUES, key=%s", it.Key())
			}
			_ = empty
			_ = large
			fv, fe := it.FetchV()
			if fv != nil || fe != nil {
				t.Fatalf("FetchV should be nil with SKIP_VALUES, key=%s", it.Key())
			}
			count++
			it.Next()
		}
		// 10 inline keys + 1 large key = 11, minus 1 for initial FindIt result
		if count < 10 {
			t.Fatalf("expected at least 10 keys, got %d", count)
		}
		return nil
	})
	panicOn(err)
}

// TestLockedIter_PutGetDelete verifies that rwDB.Put, rwDB.Get, and rwDB.Delete
// work correctly through an Update transaction, with read-your-writes visibility.
func TestLockedIter_PutGetDelete(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateFindTestDB(t, db)

	err := db.Update(func(rwDB *WriteTx) error {
		// Get existing key.
		val, ok, gerr := rwDB.Get("key005")
		panicOn(gerr)
		if !ok {
			t.Fatal("Get key005: not found")
		}
		if string(val) != "val005" {
			t.Fatalf("Get key005: got %q, want val005", val)
		}

		// Put a new key.
		if err := rwDB.Put("key005a", []byte("inserted")); err != nil {
			t.Fatal(err)
		}

		// Read-your-writes: Get the just-inserted key.
		val2, ok2, gerr2 := rwDB.Get("key005a")
		panicOn(gerr2)
		if !ok2 {
			t.Fatal("Get key005a: not found after Put")
		}
		if string(val2) != "inserted" {
			t.Fatalf("Get key005a: got %q, want inserted", val2)
		}

		// Delete a key.
		if err := rwDB.Delete("key003"); err != nil {
			t.Fatal(err)
		}

		// Read-your-writes: deleted key should be gone.
		_, ok3, gerr3 := rwDB.Get("key003")
		panicOn(gerr3)
		if ok3 {
			t.Fatal("Get key003: should not be found after Delete")
		}

		// Scan forward from key004 - should see key005a but not key003.
		it := rwDB.NewIter()
		defer it.Close()
		it.Seek("key004")
		var keys []string
		for it.Valid() {
			keys = append(keys, it.Key())
			it.Next()
		}
		for _, k := range keys {
			if k == "key003" {
				t.Fatal("key003 should not appear in scan after Delete")
			}
		}
		found005a := false
		for _, k := range keys {
			if k == "key005a" {
				found005a = true
			}
		}
		if !found005a {
			t.Fatalf("key005a should appear in scan after Put; got keys: %v", keys)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestLockedIter_Sync verifies that rwDB.Sync flushes the memtable.
func TestLockedIter_Sync(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateFindTestDB(t, db)

	err := db.Update(func(rwDB *WriteTx) error {
		// Sync should not error.
		if err := rwDB.Sync(); err != nil {
			t.Fatal(err)
		}

		// After sync, data should still be retrievable.
		val, ok, gerr := rwDB.Get("key007")
		panicOn(gerr)
		if !ok {
			t.Fatal("Get key007 after Sync: not found")
		}
		if string(val) != "val007" {
			t.Fatalf("Get key007 after Sync: got %q, want val007", val)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// populateFindTestDB inserts 10 keys: key001..key010 with values val001..val010.
func populateFindTestDB(t *testing.T, db *FlexDB) {
	t.Helper()
	for i := 1; i <= 10; i++ {
		k := fmt.Sprintf("key%03d", i)
		v := fmt.Sprintf("val%03d", i)
		mustPut(t, db, k, v)
	}
}
