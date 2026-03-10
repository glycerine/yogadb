package yogadb

import (
	"fmt"
	"testing"
)

// TestFind_Exact tests Find with Exact modifier.
func TestFind_Exact(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateFindTestDB(t, db)

	// Exact match: existing key.
	kv, found, exact := db.Find(Exact, "key003")
	if !found || !exact {
		t.Fatalf("Exact key003: found=%v exact=%v, want true true", found, exact)
	}
	if kv.Large() {
		t.Fatal("unexpected large value")
	}
	if string(kv.Key) != "key003" {
		t.Fatalf("got key %q, want key003", kv.Key)
	}
	if string(kv.Value) != "val003" {
		t.Fatalf("got value %q, want val003", kv.Value)
	}

	// Exact match: missing key.
	kv2, found2, exact2 := db.Find(Exact, "key999")
	if found2 || exact2 {
		t.Fatalf("Exact key999: found=%v exact=%v, want false false", found2, exact2)
	}
	if kv2 != nil {
		t.Fatalf("expected nil KV, got %v", kv2)
	}
}

// TestFind_GTE tests Find with GTE modifier.
func TestFind_GTE(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateFindTestDB(t, db)

	// GTE on existing key → exact match.
	kv, found, exact := db.Find(GTE, "key003")
	if !found || !exact {
		t.Fatalf("GTE key003: found=%v exact=%v, want true true", found, exact)
	}
	if string(kv.Key) != "key003" || string(kv.Value) != "val003" {
		t.Fatalf("got key=%q value=%q, want key003/val003", kv.Key, kv.Value)
	}

	// GTE on gap key → next key.
	kv2, found2, exact2 := db.Find(GTE, "key002a")
	if !found2 {
		t.Fatal("GTE key002a: not found")
	}
	if exact2 {
		t.Fatal("GTE key002a: should not be exact")
	}
	if string(kv2.Key) != "key003" {
		t.Fatalf("GTE key002a: got key %q, want key003", kv2.Key)
	}
	if string(kv2.Value) != "val003" {
		t.Fatalf("got value %q, want val003", kv2.Value)
	}

	// GTE past end → not found.
	kv3, found3, _ := db.Find(GTE, "key999")
	if found3 {
		t.Fatal("GTE key999: should not be found")
	}
	if kv3 != nil {
		t.Fatal("expected nil KV")
	}

	// GTE nil → first key.
	kv4, found4, _ := db.Find(GTE, "")
	if !found4 {
		t.Fatal("GTE nil: not found")
	}
	if string(kv4.Key) != "key001" {
		t.Fatalf("GTE nil: got key %q, want key001", kv4.Key)
	}
}

// TestFind_GT tests Find with GT modifier.
func TestFind_GT(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateFindTestDB(t, db)

	// GT on existing key → next key.
	kv, found, exact := db.Find(GT, "key003")
	if !found {
		t.Fatal("GT key003: not found")
	}
	if exact {
		t.Fatal("GT key003: should not be exact (key003 itself is skipped)")
	}
	if string(kv.Key) != "key004" {
		t.Fatalf("GT key003: got key %q, want key004", kv.Key)
	}
	if string(kv.Value) != "val004" {
		t.Fatalf("got value %q, want val004", kv.Value)
	}

	// GT on gap key → next key (same as GTE on gap).
	kv2, found2, _ := db.Find(GT, "key002a")
	if !found2 {
		t.Fatal("GT key002a: not found")
	}
	if string(kv2.Key) != "key003" {
		t.Fatalf("GT key002a: got key %q, want key003", kv2.Key)
	}

	// GT on last key → not found.
	_, found3, _ := db.Find(GT, "key010")
	if found3 {
		t.Fatal("GT key010: should not be found")
	}

	// GT nil → first key.
	kv4, found4, _ := db.Find(GT, "")
	if !found4 {
		t.Fatal("GT nil: not found")
	}
	if string(kv4.Key) != "key001" {
		t.Fatalf("GT nil: got key %q, want key001", kv4.Key)
	}
}

// TestFind_LTE tests Find with LTE modifier.
func TestFind_LTE(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateFindTestDB(t, db)

	// LTE on existing key → exact match.
	kv, found, exact := db.Find(LTE, "key003")
	if !found || !exact {
		t.Fatalf("LTE key003: found=%v exact=%v, want true true", found, exact)
	}
	if string(kv.Key) != "key003" || string(kv.Value) != "val003" {
		t.Fatalf("got key=%q value=%q, want key003/val003", kv.Key, kv.Value)
	}

	// LTE on gap key → previous key.
	kv2, found2, exact2 := db.Find(LTE, "key003a")
	if !found2 {
		t.Fatal("LTE key003a: not found")
	}
	if exact2 {
		t.Fatal("LTE key003a: should not be exact")
	}
	if string(kv2.Key) != "key003" {
		t.Fatalf("LTE key003a: got key %q, want key003", kv2.Key)
	}

	// LTE before first → not found.
	_, found3, _ := db.Find(LTE, "key000")
	if found3 {
		t.Fatal("LTE key000: should not be found")
	}

	// LTE nil → last key.
	kv4, found4, _ := db.Find(LTE, "")
	if !found4 {
		t.Fatal("LTE nil: not found")
	}
	if string(kv4.Key) != "key010" {
		t.Fatalf("LTE nil: got key %q, want key010", kv4.Key)
	}
}

// TestFind_LT tests Find with LT modifier.
func TestFind_LT(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateFindTestDB(t, db)

	// LT on existing key → previous key.
	kv, found, exact := db.Find(LT, "key003")
	if !found {
		t.Fatal("LT key003: not found")
	}
	if exact {
		t.Fatal("LT key003: should not be exact")
	}
	if string(kv.Key) != "key002" {
		t.Fatalf("LT key003: got key %q, want key002", kv.Key)
	}
	if string(kv.Value) != "val002" {
		t.Fatalf("got value %q, want val002", kv.Value)
	}

	// LT on gap key → previous key.
	kv2, found2, _ := db.Find(LT, "key003a")
	if !found2 {
		t.Fatal("LT key003a: not found")
	}
	if string(kv2.Key) != "key003" {
		t.Fatalf("LT key003a: got key %q, want key003", kv2.Key)
	}

	// LT on first key → not found.
	_, found3, _ := db.Find(LT, "key001")
	if found3 {
		t.Fatal("LT key001: should not be found")
	}

	// LT nil → last key.
	kv4, found4, _ := db.Find(LT, "")
	if !found4 {
		t.Fatal("LT nil: not found")
	}
	if string(kv4.Key) != "key010" {
		t.Fatalf("LT nil: got key %q, want key010", kv4.Key)
	}
}

// TestFindIt_IteratorContinuation verifies that the returned iterator
// can be used to scan beyond the found key.
func TestFindIt_IteratorContinuation(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateFindTestDB(t, db)

	err := db.View(func(roDB ReadOnlyDB) error {
		// FindIt GTE key005, then iterate forward.
		kv, found, _, it := roDB.FindIt(GTE, "key005")
		if !found {
			it.Close()
			t.Fatal("GTE key005: not found")
		}
		if string(kv.Key) != "key005" {
			it.Close()
			t.Fatalf("got key %q, want key005", kv.Key)
		}
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
		kv2, found2, _, it2 := roDB.FindIt(LTE, "key005")
		defer it2.Close()
		if !found2 {
			t.Fatal("LTE key005: not found")
		}
		if string(kv2.Key) != "key005" {
			t.Fatalf("got key %q, want key005", kv2.Key)
		}
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
		kv, found, _ := db.Find(smod, "anything")
		if found {
			t.Errorf("smod=%d on empty DB: should not find anything", smod)
		}
		if kv != nil {
			t.Errorf("smod=%d on empty DB: expected nil KV", smod)
		}
	}
}

// TestFind_AfterSync verifies Find works after data is flushed to FlexSpace.
func TestFind_AfterSync(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateFindTestDB(t, db)
	db.Sync()

	// GTE on gap.
	kv, found, exact := db.Find(GTE, "key004a")
	if !found || exact {
		t.Fatalf("GTE key004a after sync: found=%v exact=%v", found, exact)
	}
	if string(kv.Key) != "key005" {
		t.Fatalf("got key %q, want key005", kv.Key)
	}

	// LT on existing.
	kv2, found2, exact2 := db.Find(LT, "key005")
	if !found2 || exact2 {
		t.Fatalf("LT key005 after sync: found=%v exact=%v", found2, exact2)
	}
	if string(kv2.Key) != "key004" {
		t.Fatalf("got key %q, want key004", kv2.Key)
	}
}

// TestFind_KVOwnership verifies that the returned KV from Find is safe
// to retain by mutating the DB after Find returns.
func TestFind_KVOwnership(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateFindTestDB(t, db)
	db.Sync() // flush to FlexSpace

	kv, found, _ := db.Find(GTE, "key005")
	if !found {
		t.Fatal("not found")
	}
	origKey := string(kv.Key)
	origVal := string(kv.Value)

	// Overwrite the key and sync to potentially evict cache.
	mustPut(t, db, "key005", "CHANGED")
	db.Sync()

	// The original KV should still hold the old values (owned copy).
	if string(kv.Key) != origKey {
		t.Fatalf("key mutated: got %q, want %q", kv.Key, origKey)
	}
	if string(kv.Value) != origVal {
		t.Fatalf("value mutated: got %q, want %q", kv.Value, origVal)
	}
}

// TestFind_HLCPopulated verifies that the returned KV has a non-zero HLC.
func TestFind_HLCPopulated(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateFindTestDB(t, db)

	kv, found, _ := db.Find(Exact, "key005")
	if !found {
		t.Fatal("not found")
	}
	if kv.Hlc == 0 {
		t.Fatal("expected non-zero HLC on found KV")
	}
}

// TestFetchLarge_InlineValue verifies FetchLarge works for inline values.
func TestFetchLarge_InlineValue(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateFindTestDB(t, db)

	kv, found, _ := db.Find(Exact, "key003")
	if !found {
		t.Fatal("not found")
	}
	if kv.Large() {
		t.Fatal("expected inline value, got large")
	}
	// FetchLarge on an inline value should return the value.
	val, err := db.FetchLarge(kv)
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "val003" {
		t.Fatalf("got %q, want val003", val)
	}
}

// TestLockedIter_PutGetDelete verifies that rwDB.Put, rwDB.Get, and rwDB.Delete
// work correctly through an Update transaction, with read-your-writes visibility.
func TestLockedIter_PutGetDelete(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateFindTestDB(t, db)

	err := db.Update(func(rwDB WritableDB) error {
		// Get existing key.
		val, ok := rwDB.Get("key005")
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
		val2, ok2 := rwDB.Get("key005a")
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
		_, ok3 := rwDB.Get("key003")
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

	err := db.Update(func(rwDB WritableDB) error {
		// Sync should not error.
		if err := rwDB.Sync(); err != nil {
			t.Fatal(err)
		}

		// After sync, data should still be retrievable.
		val, ok := rwDB.Get("key007")
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
