package ramflextree

import (
	"fmt"
	"testing"
)

// ====================== Batch tests ======================

// TestFlexDB_BatchBasic verifies Batch.Commit writes are visible via Get.
func TestFlexDB_BatchBasic(t *testing.T) {
	db := openTestDB(t, nil)

	batch := db.NewBatch()
	batch.Set("k1", []byte("v1"))
	batch.Set("k2", []byte("v2"))
	batch.Set("k3", []byte("v3"))
	if _, err := batch.Commit(false); err != nil {
		t.Fatal(err)
	}
	batch.Close()

	mustGet(t, db, "k1", "v1")
	mustGet(t, db, "k2", "v2")
	mustGet(t, db, "k3", "v3")
	mustMiss(t, db, "k4")
}

// TestFlexDB_BatchOverwrite verifies last write wins within a batch.
func TestFlexDB_BatchOverwrite(t *testing.T) {
	db := openTestDB(t, nil)

	mustPut(t, db, "k1", "original")

	batch := db.NewBatch()
	batch.Set("k1", []byte("updated"))
	batch.Set("k1", []byte("final"))
	if _, err := batch.Commit(false); err != nil {
		t.Fatal(err)
	}
	batch.Close()

	mustGet(t, db, "k1", "final")
}

// TestFlexDB_BatchEmpty verifies an empty batch commit is a no-op.
func TestFlexDB_BatchEmpty(t *testing.T) {
	db := openTestDB(t, nil)
	mustPut(t, db, "k1", "v1")

	batch := db.NewBatch()
	if _, err := batch.Commit(false); err != nil {
		t.Fatal(err)
	}
	batch.Close()

	mustGet(t, db, "k1", "v1")
}

// verifies a large batch (10k keys).
func Test630_FlexDB_BatchMany(t *testing.T) {
	db := openTestDB(t, nil)

	const n = 10000
	batch := db.NewBatch()
	nWrit := 0
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%06d", i)
		val := []byte(fmt.Sprintf("val%06d", i))
		batch.Set(key, val)
		nWrit += len(key) + len(val)
	}
	if _, err := batch.Commit(false); err != nil {
		t.Fatal(err)
	}
	batch.Close()

	db.Sync()

	for i := 0; i < n; i++ {
		mustGet(t, db, fmt.Sprintf("key%06d", i), fmt.Sprintf("val%06d", i))
	}
}
