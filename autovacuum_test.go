package yogadb

import (
	"fmt"
	"testing"
	"time"
)

func TestManualVacuumWithAutoVacuumOffDropsAllDeletedData(t *testing.T) {
	db, _ := openTestDB(t, &Config{
		AutoVacuumPct:          0,
		DisableBackgroundFlush: true,
	})

	const n = 300
	for i := 0; i < n; i++ {
		mustPut(t, db, fmt.Sprintf("drop-tomb-%04d", i), "small-inline-value")
	}
	if err := db.Sync(); err != nil {
		t.Fatalf("initial Sync: %v", err)
	}
	for i := 0; i < n; i++ {
		mustDelete(t, db, fmt.Sprintf("drop-tomb-%04d", i))
	}
	if err := db.Sync(); err != nil {
		t.Fatalf("delete Sync: %v", err)
	}

	before := db.SessionMetrics()
	stats, err := db.VacuumKV()
	if err != nil {
		t.Fatalf("VacuumKV: %v", err)
	}
	after := db.SessionMetrics()
	t.Logf("before VacuumKV: liveKeys=%d kvLiveBytes=%d kvFootprint=%d", before.LiveKeyCount, before.KVBlocksTotalLiveBytes, before.KVBlocksOnDiskFootprintBytes)
	t.Logf("VacuumKV stats: %v", stats)
	t.Logf("after VacuumKV: liveKeys=%d kvLiveBytes=%d kvFootprint=%d", after.LiveKeyCount, after.KVBlocksTotalLiveBytes, after.KVBlocksOnDiskFootprintBytes)

	if after.LiveKeyCount != 0 {
		t.Fatalf("LiveKeyCount after deleting all keys = %d, want 0", after.LiveKeyCount)
	}
	if after.KVBlocksOnDiskFootprintBytes != 0 {
		t.Fatalf("VacuumKV preserved tombstone-only storage: KV footprint=%d, want 0", after.KVBlocksOnDiskFootprintBytes)
	}
	if stats.BytesReclaimed <= 0 {
		t.Fatalf("VacuumKV reclaimed %d bytes, want > 0", stats.BytesReclaimed)
	}
}

func TestAutoVacuumRunsAfterDeleteThreshold(t *testing.T) {
	db, _ := openTestDB(t, &Config{
		AutoVacuumPct:            0.05,
		AutoVacuumDeletedAboveKB: 1,
		DisableBackgroundFlush:   true,
	})

	const n = 400
	for i := 0; i < n; i++ {
		mustPut(t, db, fmt.Sprintf("auto-vac-%04d", i), "small-inline-value")
	}
	if err := db.Sync(); err != nil {
		t.Fatalf("initial Sync: %v", err)
	}
	if err := db.Update(func(tx *WriteTx) error {
		for i := 0; i < n; i++ {
			if err := tx.Delete(fmt.Sprintf("auto-vac-%04d", i)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("delete Update: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		m := db.SessionMetrics()
		if m.AutoVacuumRuns > 0 {
			if m.AutoVacuumLastErr != "" {
				t.Fatalf("AutoVacuumLastErr = %q", m.AutoVacuumLastErr)
			}
			if m.KVBlocksOnDiskFootprintBytes != 0 {
				t.Fatalf("AutoVacuum ran but KV footprint=%d, want 0", m.KVBlocksOnDiskFootprintBytes)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for AutoVacuum; metrics=%v", m)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestAutoVacuumDefaultDeletedThresholdSuppressesSmallDeletes(t *testing.T) {
	db, _ := openTestDB(t, &Config{
		AutoVacuumPct:          0.01,
		DisableBackgroundFlush: true,
	})

	const n = 200
	for i := 0; i < n; i++ {
		mustPut(t, db, fmt.Sprintf("auto-vac-small-%04d", i), "small-inline-value")
	}
	if err := db.Sync(); err != nil {
		t.Fatalf("initial Sync: %v", err)
	}
	if err := db.Update(func(tx *WriteTx) error {
		for i := 0; i < n; i++ {
			if err := tx.Delete(fmt.Sprintf("auto-vac-small-%04d", i)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("delete Update: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	m := db.SessionMetrics()
	if m.AutoVacuumRuns != 0 {
		t.Fatalf("AutoVacuumRuns=%d, want 0 for deletes below default 100MB threshold", m.AutoVacuumRuns)
	}
	if m.AutoVacuumDeletedBytes == 0 {
		t.Fatalf("AutoVacuumDeletedBytes=0, want tracked deleted bytes below threshold")
	}
}

func TestAutoVacuumReclaimsDeletedLargeValuesAndTombstones(t *testing.T) {
	db, _ := openTestDB(t, &Config{
		AutoVacuumPct:            0.05,
		AutoVacuumDeletedAboveKB: 1,
		DisableBackgroundFlush:   true,
	})

	const n = 80
	large := makeTestValue(512)
	for i := 0; i < n; i++ {
		mustPut(t, db, fmt.Sprintf("auto-vlog-%04d", i), large)
	}
	if err := db.Sync(); err != nil {
		t.Fatalf("initial Sync: %v", err)
	}
	beforeDelete := db.SessionMetrics()
	if beforeDelete.VlogOnDiskFootprintBytes == 0 {
		t.Fatal("expected non-empty VLOG before delete")
	}

	if err := db.Update(func(tx *WriteTx) error {
		for i := 0; i < n; i++ {
			if err := tx.Delete(fmt.Sprintf("auto-vlog-%04d", i)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("delete Update: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		m := db.SessionMetrics()
		if m.AutoVacuumRuns > 0 {
			if m.AutoVacuumLastErr != "" {
				t.Fatalf("AutoVacuumLastErr = %q", m.AutoVacuumLastErr)
			}
			if m.KVBlocksOnDiskFootprintBytes != 0 {
				t.Fatalf("AutoVacuum preserved tombstone KV storage: KV footprint=%d, want 0", m.KVBlocksOnDiskFootprintBytes)
			}
			if m.VlogOnDiskFootprintBytes != 0 {
				t.Fatalf("AutoVacuum preserved deleted VLOG storage: VLOG footprint=%d, want 0", m.VlogOnDiskFootprintBytes)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for AutoVacuum; metrics=%v", m)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
