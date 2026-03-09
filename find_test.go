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
	val, found, exact, large, it := db.Find(Exact, []byte("key003"), false)
	defer it.Close()
	if !found || !exact {
		t.Fatalf("Exact key003: found=%v exact=%v, want true true", found, exact)
	}
	if large {
		t.Fatal("unexpected large value")
	}
	if string(val) != "val003" {
		t.Fatalf("got %q, want %q", val, "val003")
	}

	// Exact match: missing key.
	val2, found2, exact2, _, it2 := db.Find(Exact, []byte("key999"), false)
	defer it2.Close()
	if found2 || exact2 {
		t.Fatalf("Exact key999: found=%v exact=%v, want false false", found2, exact2)
	}
	if val2 != nil {
		t.Fatalf("expected nil value, got %q", val2)
	}
}

// TestFind_GTE tests Find with GTE modifier.
func TestFind_GTE(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateFindTestDB(t, db)

	// GTE on existing key → exact match.
	val, found, exact, _, it := db.Find(GTE, []byte("key003"), false)
	defer it.Close()
	if !found || !exact {
		t.Fatalf("GTE key003: found=%v exact=%v, want true true", found, exact)
	}
	if string(val) != "val003" {
		t.Fatalf("got %q, want %q", val, "val003")
	}

	// GTE on gap key → next key.
	val2, found2, exact2, _, it2 := db.Find(GTE, []byte("key002a"), false)
	defer it2.Close()
	if !found2 {
		t.Fatal("GTE key002a: not found")
	}
	if exact2 {
		t.Fatal("GTE key002a: should not be exact")
	}
	if string(it2.Key()) != "key003" {
		t.Fatalf("GTE key002a: got key %q, want key003", it2.Key())
	}
	if string(val2) != "val003" {
		t.Fatalf("got %q, want %q", val2, "val003")
	}

	// GTE past end → not found.
	_, found3, _, _, it3 := db.Find(GTE, []byte("key999"), false)
	defer it3.Close()
	if found3 {
		t.Fatal("GTE key999: should not be found")
	}

	// GTE nil → first key.
	_, found4, _, _, it4 := db.Find(GTE, nil, false)
	defer it4.Close()
	if !found4 {
		t.Fatal("GTE nil: not found")
	}
	if string(it4.Key()) != "key001" {
		t.Fatalf("GTE nil: got key %q, want key001", it4.Key())
	}
}

// TestFind_GT tests Find with GT modifier.
func TestFind_GT(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateFindTestDB(t, db)

	// GT on existing key → next key.
	val, found, exact, _, it := db.Find(GT, []byte("key003"), false)
	defer it.Close()
	if !found {
		t.Fatal("GT key003: not found")
	}
	if exact {
		t.Fatal("GT key003: should not be exact (key003 itself is skipped)")
	}
	if string(it.Key()) != "key004" {
		t.Fatalf("GT key003: got key %q, want key004", it.Key())
	}
	if string(val) != "val004" {
		t.Fatalf("got %q, want %q", val, "val004")
	}

	// GT on gap key → next key (same as GTE on gap).
	_, found2, _, _, it2 := db.Find(GT, []byte("key002a"), false)
	defer it2.Close()
	if !found2 {
		t.Fatal("GT key002a: not found")
	}
	if string(it2.Key()) != "key003" {
		t.Fatalf("GT key002a: got key %q, want key003", it2.Key())
	}

	// GT on last key → not found.
	_, found3, _, _, it3 := db.Find(GT, []byte("key010"), false)
	defer it3.Close()
	if found3 {
		t.Fatal("GT key010: should not be found")
	}

	// GT nil → first key.
	_, found4, _, _, it4 := db.Find(GT, nil, false)
	defer it4.Close()
	if !found4 {
		t.Fatal("GT nil: not found")
	}
	if string(it4.Key()) != "key001" {
		t.Fatalf("GT nil: got key %q, want key001", it4.Key())
	}
}

// TestFind_LTE tests Find with LTE modifier.
func TestFind_LTE(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateFindTestDB(t, db)

	// LTE on existing key → exact match.
	val, found, exact, _, it := db.Find(LTE, []byte("key003"), false)
	defer it.Close()
	if !found || !exact {
		t.Fatalf("LTE key003: found=%v exact=%v, want true true", found, exact)
	}
	if string(val) != "val003" {
		t.Fatalf("got %q, want %q", val, "val003")
	}

	// LTE on gap key → previous key.
	_, found2, exact2, _, it2 := db.Find(LTE, []byte("key003a"), false)
	defer it2.Close()
	if !found2 {
		t.Fatal("LTE key003a: not found")
	}
	if exact2 {
		t.Fatal("LTE key003a: should not be exact")
	}
	if string(it2.Key()) != "key003" {
		t.Fatalf("LTE key003a: got key %q, want key003", it2.Key())
	}

	// LTE before first → not found.
	_, found3, _, _, it3 := db.Find(LTE, []byte("key000"), false)
	defer it3.Close()
	if found3 {
		t.Fatal("LTE key000: should not be found")
	}

	// LTE nil → last key.
	_, found4, _, _, it4 := db.Find(LTE, nil, false)
	defer it4.Close()
	if !found4 {
		t.Fatal("LTE nil: not found")
	}
	if string(it4.Key()) != "key010" {
		t.Fatalf("LTE nil: got key %q, want key010", it4.Key())
	}
}

// TestFind_LT tests Find with LT modifier.
func TestFind_LT(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateFindTestDB(t, db)

	// LT on existing key → previous key.
	val, found, exact, _, it := db.Find(LT, []byte("key003"), false)
	defer it.Close()
	if !found {
		t.Fatal("LT key003: not found")
	}
	if exact {
		t.Fatal("LT key003: should not be exact")
	}
	if string(it.Key()) != "key002" {
		t.Fatalf("LT key003: got key %q, want key002", it.Key())
	}
	if string(val) != "val002" {
		t.Fatalf("got %q, want %q", val, "val002")
	}

	// LT on gap key → previous key.
	_, found2, _, _, it2 := db.Find(LT, []byte("key003a"), false)
	defer it2.Close()
	if !found2 {
		t.Fatal("LT key003a: not found")
	}
	if string(it2.Key()) != "key003" {
		t.Fatalf("LT key003a: got key %q, want key003", it2.Key())
	}

	// LT on first key → not found.
	_, found3, _, _, it3 := db.Find(LT, []byte("key001"), false)
	defer it3.Close()
	if found3 {
		t.Fatal("LT key001: should not be found")
	}

	// LT nil → last key.
	_, found4, _, _, it4 := db.Find(LT, nil, false)
	defer it4.Close()
	if !found4 {
		t.Fatal("LT nil: not found")
	}
	if string(it4.Key()) != "key010" {
		t.Fatalf("LT nil: got key %q, want key010", it4.Key())
	}
}

// TestFind_IteratorContinuation verifies that the returned iterator
// can be used to scan beyond the found key.
func TestFind_IteratorContinuation(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateFindTestDB(t, db)

	// Find GTE key005, then iterate forward.
	_, found, _, _, it := db.Find(GTE, []byte("key005"), false)
	defer it.Close()
	if !found {
		t.Fatal("GTE key005: not found")
	}
	var keys []string
	for it.Valid() {
		keys = append(keys, string(it.Key()))
		it.Next()
	}
	expected := []string{"key005", "key006", "key007", "key008", "key009", "key010"}
	if len(keys) != len(expected) {
		t.Fatalf("got %d keys, want %d: %v", len(keys), len(expected), keys)
	}
	for i, k := range keys {
		if k != expected[i] {
			t.Fatalf("key[%d] = %q, want %q", i, k, expected[i])
		}
	}

	// Find LTE key005, then iterate backward.
	_, found2, _, _, it2 := db.Find(LTE, []byte("key005"), false)
	defer it2.Close()
	if !found2 {
		t.Fatal("LTE key005: not found")
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
}

// TestFind_EmptyDB tests Find on an empty database.
func TestFind_EmptyDB(t *testing.T) {
	db, _ := openTestDB(t, nil)

	for _, smod := range []SearchModifier{Exact, GTE, GT, LTE, LT} {
		_, found, _, _, it := db.Find(smod, []byte("anything"), false)
		it.Close()
		if found {
			t.Errorf("smod=%d on empty DB: should not find anything", smod)
		}
	}
}

// TestFind_AfterSync verifies Find works after data is flushed to FlexSpace.
func TestFind_AfterSync(t *testing.T) {
	db, _ := openTestDB(t, nil)
	populateFindTestDB(t, db)
	db.Sync()

	// GTE on gap.
	_, found, exact, _, it := db.Find(GTE, []byte("key004a"), false)
	defer it.Close()
	if !found || exact {
		t.Fatalf("GTE key004a after sync: found=%v exact=%v", found, exact)
	}
	if string(it.Key()) != "key005" {
		t.Fatalf("got %q, want key005", it.Key())
	}

	// LT on existing.
	_, found2, exact2, _, it2 := db.Find(LT, []byte("key005"), false)
	defer it2.Close()
	if !found2 || exact2 {
		t.Fatalf("LT key005 after sync: found=%v exact=%v", found2, exact2)
	}
	if string(it2.Key()) != "key004" {
		t.Fatalf("got %q, want key004", it2.Key())
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
