package yogadb

import (
	"fmt"
	"path/filepath"

	"testing"
)

// ====================== VLOG Value Separation Tests ======================

// makeTestValue creates a deterministic value of the given size.
func makeTestValue(size int) string {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte('A' + (i % 26))
	}
	return string(b)
}

// TestFlexDB_VLOG_SmallInlineLargeVLOG verifies that small values stay inline
// and large values go to the VLOG file.
func TestFlexDB_VLOG_SmallInlineLargeVLOG(t *testing.T) {
	fs, dir := newTestFS(t)
	db := openTestDBAt(fs, t, dir, nil)

	smallVal := makeTestValue(32)  // <= vlogInlineThreshold (64)
	largeVal := makeTestValue(128) // above threshold

	mustPut(t, db, "small", smallVal)
	mustPut(t, db, "large", largeVal)
	mustGet(t, db, "small", smallVal)
	mustGet(t, db, "large", largeVal)

	// VLOG file should exist since we wrote a large value.
	vlogPath := filepath.Join(dir, "LARGE.VLOG")
	if !fileExists(fs, vlogPath) {
		t.Fatal("LARGE.VLOG file should exist after writing a large value")
	}
	db.Close()
}

// TestFlexDB_VLOG_PutGetMemtable tests Get of a large value before Sync (memtable path).
func TestFlexDB_VLOG_PutGetMemtable(t *testing.T) {
	db, _ := openTestDB(t, nil)
	val := makeTestValue(200)
	mustPut(t, db, "k", val)
	mustGet(t, db, "k", val)
}

// TestFlexDB_VLOG_PutSyncGet tests that a large value survives Sync (FlexSpace VPtr round-trip).
func TestFlexDB_VLOG_PutSyncGet(t *testing.T) {
	db, _ := openTestDB(t, nil)
	val := makeTestValue(256)
	mustPut(t, db, "key1", val)
	db.Sync()
	mustGet(t, db, "key1", val)
}

// TestFlexDB_VLOG_Persistence tests that a large value survives Close + reopen.
func TestFlexDB_VLOG_Persistence(t *testing.T) {
	fs, dir := newTestFS(t)
	val := makeTestValue(300)

	db := openTestDBAt(fs, t, dir, nil)
	mustPut(t, db, "persist_key", val)
	db.Sync()
	db.Close()

	db2 := openTestDBAt(fs, t, dir, nil)
	defer db2.Close()
	mustGet(t, db2, "persist_key", val)
}

// TestFlexDB_VLOG_WALRecovery tests that a large VPtr in the WAL survives recovery.
func TestFlexDB_VLOG_WALRecovery(t *testing.T) {
	fs, dir := newTestFS(t)
	val := makeTestValue(500)

	db := openTestDBAt(fs, t, dir, nil)
	mustPut(t, db, "wal_key", val)
	// Flush WAL but don't Sync to FlexSpace.
	db.memtables[db.activeMT].logFlush()
	db.Close()

	db2 := openTestDBAt(fs, t, dir, nil)
	defer db2.Close()
	mustGet(t, db2, "wal_key", val)
}

// TestFlexDB_VLOG_MixedSizes tests values of various sizes around the threshold.
func TestFlexDB_VLOG_MixedSizes(t *testing.T) {
	db, _ := openTestDB(t, nil)
	sizes := []int{32, 63, 64, 128, 3000}
	for _, sz := range sizes {
		key := fmt.Sprintf("k_%d", sz)
		val := makeTestValue(sz)
		mustPut(t, db, key, val)
	}
	db.Sync()
	for _, sz := range sizes {
		key := fmt.Sprintf("k_%d", sz)
		val := makeTestValue(sz)
		mustGet(t, db, key, val)
	}
}

// TestFlexDB_VLOG_Overwrite tests overwriting a large value returns the new value.
func TestFlexDB_VLOG_Overwrite(t *testing.T) {
	db, _ := openTestDB(t, nil)
	val1 := makeTestValue(128)
	val2 := makeTestValue(256)
	mustPut(t, db, "ow", val1)
	db.Sync()
	mustPut(t, db, "ow", val2)
	db.Sync()
	mustGet(t, db, "ow", val2)
}

// TestFlexDB_VLOG_Delete tests that deleting a large-value key works after Sync.
func TestFlexDB_VLOG_Delete(t *testing.T) {
	db, _ := openTestDB(t, nil)
	val := makeTestValue(128)
	mustPut(t, db, "del", val)
	db.Sync()
	mustGet(t, db, "del", val)
	mustDelete(t, db, "del")
	db.Sync()
	mustMiss(t, db, "del")
}

// TestFlexDB_VLOG_Batch tests Batch with mixed small/large values.
func TestFlexDB_VLOG_Batch(t *testing.T) {
	db, _ := openTestDB(t, nil)
	b := db.NewBatch()
	b.Set([]byte("small"), []byte("tiny"))
	b.Set([]byte("big"), []byte(makeTestValue(200)))
	if _, err := b.Commit(false); err != nil {
		t.Fatal(err)
	}
	mustGet(t, db, "small", "tiny")
	mustGet(t, db, "big", makeTestValue(200))
	db.Sync()
	mustGet(t, db, "small", "tiny")
	mustGet(t, db, "big", makeTestValue(200))
}

// TestFlexDB_VLOG_Ascend tests Ascend/Descend with large values after Sync.
// This exercises the loadInterval VPtr copy fix.
func TestFlexDB_VLOG_Ascend(t *testing.T) {
	db, _ := openTestDB(t, nil)
	keys := []string{"aaa", "bbb", "ccc"}
	vals := make(map[string]string)
	for _, k := range keys {
		v := makeTestValue(128 + len(k))
		vals[k] = v
		mustPut(t, db, k, v)
	}
	db.Sync()

	// Ascend and verify values
	var gotKeys []string
	db.View(func(roDB ReadOnlyDB) error {
		roDB.Ascend(nil, func(key, value []byte) bool {
			k := string(key)
			gotKeys = append(gotKeys, k)
			want := vals[k]
			if string(value) != want {
				t.Errorf("Ascend: key=%q got len=%d, want len=%d", k, len(value), len(want))
			}
			return true
		})
		return nil
	})
	if len(gotKeys) != 3 {
		t.Fatalf("Ascend got %d keys, want 3", len(gotKeys))
	}

	// Descend
	var descKeys []string
	db.View(func(roDB ReadOnlyDB) error {
		roDB.Descend(nil, func(key, value []byte) bool {
			k := string(key)
			descKeys = append(descKeys, k)
			want := vals[k]
			if string(value) != want {
				t.Errorf("Descend: key=%q got len=%d, want len=%d", k, len(value), len(want))
			}
			return true
		})
		return nil
	})
	if len(descKeys) != 3 {
		t.Fatalf("Descend got %d keys, want 3", len(descKeys))
	}
}

// TestFlexDB_VLOG_Merge tests Merge on a key with a large value.
func TestFlexDB_VLOG_Merge(t *testing.T) {
	db, _ := openTestDB(t, nil)
	val := makeTestValue(128)
	mustPut(t, db, "mkey", val)
	db.Sync()

	err := db.Merge([]byte("mkey"), func(old []byte, exists bool) ([]byte, bool) {
		if !exists {
			t.Fatal("Merge: key should exist")
		}
		if string(old) != val {
			t.Fatalf("Merge: old value mismatch (len %d vs %d)", len(old), len(val))
		}
		return []byte("merged_result"), true
	})
	if err != nil {
		t.Fatal(err)
	}
	mustGet(t, db, "mkey", "merged_result")
}

// TestFlexDB_VLOG_DisableVLOG tests that with DisableVLOG, large values stay inline.
func TestFlexDB_VLOG_DisableVLOG(t *testing.T) {
	fs, dir := newTestFS(t)
	cfg := &Config{DisableVLOG: true, FS: fs}
	db := openTestDBAt(fs, t, dir, cfg)

	val := makeTestValue(128)
	mustPut(t, db, "k", val)
	mustGet(t, db, "k", val)

	vlogPath := filepath.Join(dir, "LARGE.VLOG")
	if fileExists(fs, vlogPath) {
		t.Fatal("VLOG file should NOT exist when DisableVLOG is true")
	}
	db.Close()
}

// TestFlexDB_VLOG_ThresholdBoundary tests values at the exact threshold boundary.
func TestFlexDB_VLOG_ThresholdBoundary(t *testing.T) {
	fs, dir := newTestFS(t)
	db := openTestDBAt(fs, t, dir, nil)

	below := makeTestValue(63)       // inline
	atThreshold := makeTestValue(64) // VLOG

	mustPut(t, db, "below", below)
	mustPut(t, db, "at", atThreshold)
	db.Sync()
	mustGet(t, db, "below", below)
	mustGet(t, db, "at", atThreshold)
	db.Close()

	db2 := openTestDBAt(fs, t, dir, nil)
	defer db2.Close()
	mustGet(t, db2, "below", below)
	mustGet(t, db2, "at", atThreshold)
}

// TestFlexDB_VLOG_ManyKeysPersistence inserts 100 large-value keys, syncs, closes, reopens, verifies all.
func TestFlexDB_VLOG_ManyKeysPersistence(t *testing.T) {
	fs, dir := newTestFS(t)
	db := openTestDBAt(fs, t, dir, nil)

	const n = 100
	vals := make(map[string]string, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%03d", i)
		val := makeTestValue(128 + i)
		vals[key] = val
		mustPut(t, db, key, val)
	}
	db.Sync()
	db.Close()

	db2 := openTestDBAt(fs, t, dir, nil)
	defer db2.Close()
	for key, want := range vals {
		mustGet(t, db2, key, want)
	}
}

// TestFlexDB_VLOG_VacuumBasic tests VacuumVLOG reclaims dead space.
func TestFlexDB_VLOG_VacuumBasic(t *testing.T) {
	fs, dir := newTestFS(t)
	db := openTestDBAt(fs, t, dir, nil)

	// Put 10 large values.
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("vk%02d", i)
		mustPut(t, db, key, makeTestValue(200+i))
	}
	db.Sync()

	// Overwrite 5 of them (old values become dead).
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("vk%02d", i)
		mustPut(t, db, key, makeTestValue(300+i))
	}
	db.Sync()

	// Delete 2 more (those values become dead too).
	mustDelete(t, db, "vk05")
	mustDelete(t, db, "vk06")
	db.Sync()

	sizeBeforeVacuum := db.vlog.size()

	stats, err := db.VacuumVLOG()
	if err != nil {
		t.Fatalf("VacuumVLOG: %v", err)
	}

	if stats.OldVLOGSize != sizeBeforeVacuum {
		t.Errorf("OldVLOGSize: got %d, want %d", stats.OldVLOGSize, sizeBeforeVacuum)
	}
	if stats.BytesReclaimed <= 0 {
		t.Errorf("expected positive BytesReclaimed, got %d", stats.BytesReclaimed)
	}
	if stats.NewVLOGSize >= stats.OldVLOGSize {
		t.Errorf("NewVLOGSize %d should be less than OldVLOGSize %d", stats.NewVLOGSize, stats.OldVLOGSize)
	}

	// Verify all live values are correct.
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("vk%02d", i)
		mustGet(t, db, key, makeTestValue(300+i))
	}
	mustMiss(t, db, "vk05")
	mustMiss(t, db, "vk06")
	for i := 7; i < 10; i++ {
		key := fmt.Sprintf("vk%02d", i)
		mustGet(t, db, key, makeTestValue(200+i))
	}

	db.Close()

	// Reopen and verify again.
	db2 := openTestDBAt(fs, t, dir, nil)
	defer db2.Close()
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("vk%02d", i)
		mustGet(t, db2, key, makeTestValue(300+i))
	}
	mustMiss(t, db2, "vk05")
	mustMiss(t, db2, "vk06")
	for i := 7; i < 10; i++ {
		key := fmt.Sprintf("vk%02d", i)
		mustGet(t, db2, key, makeTestValue(200+i))
	}
}
