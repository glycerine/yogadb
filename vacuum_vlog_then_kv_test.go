package yogadb

import (
	"bytes"
	"fmt"
	"testing"
)

// TestVacuumVLOG_ThenVacuumKV verifies that VacuumVLOG rewrites intervals
// containing VPtrs in slotted page format, and then VacuumKV can
// successfully compact the result. Both vacuums use slotted page format
// exclusively for KV.SLOT_BLOCKS.
func TestVacuumVLOG_ThenVacuumKV(t *testing.T) {
	fs, dir := newTestFS(t)

	cfg := &Config{
		OmitMemWalFsync: true,
	}
	db := openTestDBAt(fs, t, dir, cfg)

	// Batch-load keys with values large enough to go into VLOG.
	// The VLOG threshold is 64 bytes, so use the key itself as
	// the value (keys are ~10 bytes, but we pad the value to >64).
	nKeys := 5000
	batch := db.NewBatch()
	for i := 0; i < nKeys; i++ {
		key := fmt.Sprintf("loadkey_%06d", i)
		// Value > 64 bytes to force VLOG usage for some entries.
		val := key + "_" + makeTestValue(80)
		err := batch.Set(key, []byte(val), 0)
		if err != nil {
			t.Fatalf("batch.Set: %v", err)
		}
		if (i+1)%1000 == 0 {
			if _, err := batch.Commit(false); err != nil {
				t.Fatalf("batch.Commit: %v", err)
			}
		}
	}
	if _, err := batch.Commit(false); err != nil {
		t.Fatalf("batch.Commit final: %v", err)
	}
	batch.Close()
	db.Sync()
	db.Close()

	// Reopen (recovery path).
	db2 := openTestDBAt(fs, t, dir, cfg)

	// VacuumVLOG rewrites intervals with updated VPtrs in slotted page format.
	stats, err := db2.VacuumVLOG()
	if err != nil {
		t.Fatalf("VacuumVLOG: %v", err)
	}
	t.Logf("VacuumVLOG: %v", stats)

	// VacuumKV compacts KV.SLOT_BLOCKS. This calls rebuildAnchorsFromTags
	// which reads the first key from slotted page extents.
	stats2, err := db2.VacuumKV()
	if err != nil {
		t.Fatalf("VacuumKV: %v", err)
	}
	t.Logf("VacuumKV: %v", stats2)

	// Verify data survived both vacuums.
	for i := 0; i < nKeys; i++ {
		key := fmt.Sprintf("loadkey_%06d", i)
		wantVal := key + "_" + makeTestValue(80)
		mustGet(t, db2, key, wantVal)
	}

	mustCheckIntegrity(t, db2)
	db2.Close()
}

func TestVacuumPreservesVtypMetadata(t *testing.T) {
	fs, dir := newTestFS(t)
	cfg := &Config{DisableBackgroundFlush: true}
	db := openTestDBAt(fs, t, dir, cfg)
	t.Cleanup(func() {
		if db != nil {
			db.Close()
		}
	})

	records := []dbVtypFuzzRecord{
		{
			key:   "put/inline64",
			value: bytes.Repeat([]byte{'p'}, 64),
			vtyp:  0x1111222233334444,
		},
		{
			key:   "put/large65",
			value: bytes.Repeat([]byte{'q'}, 65),
			vtyp:  0x2222333344445555,
		},
		{
			key:      "batch/inline64",
			value:    bytes.Repeat([]byte{'b'}, 64),
			vtyp:     0x3333444455556666,
			viaBatch: true,
		},
		{
			key:      "batch/large65",
			value:    bytes.Repeat([]byte{'c'}, 65),
			vtyp:     0x4444555566667777,
			viaBatch: true,
		},
		{
			key:      "batch/large65-zero-vtyp",
			value:    bytes.Repeat([]byte{'z'}, 65),
			viaBatch: true,
		},
	}

	for _, rec := range records {
		if rec.viaBatch {
			continue
		}
		if err := db.Put(rec.key, rec.value, rec.vtyp); err != nil {
			t.Fatalf("Put(%q): %v", rec.key, err)
		}
	}
	batch := db.NewBatch()
	for _, rec := range records {
		if !rec.viaBatch {
			continue
		}
		if err := batch.Set(rec.key, rec.value, rec.vtyp); err != nil {
			t.Fatalf("Batch.Set(%q): %v", rec.key, err)
		}
	}
	if _, err := batch.Commit(false); err != nil {
		t.Fatalf("initial Batch.Commit: %v", err)
	}
	if err := db.Sync(); err != nil {
		t.Fatalf("initial Sync: %v", err)
	}

	records[1].value = bytes.Repeat([]byte{'Q'}, 65)
	records[1].vtyp = 0x5555666677778888
	if err := db.Put(records[1].key, records[1].value, records[1].vtyp); err != nil {
		t.Fatalf("overwrite Put(%q): %v", records[1].key, err)
	}
	records[3].value = bytes.Repeat([]byte{'C'}, 65)
	records[3].vtyp = 0x6666777788889999
	batch = db.NewBatch()
	if err := batch.Set(records[3].key, records[3].value, records[3].vtyp); err != nil {
		t.Fatalf("overwrite Batch.Set(%q): %v", records[3].key, err)
	}
	if _, err := batch.Commit(false); err != nil {
		t.Fatalf("overwrite Batch.Commit: %v", err)
	}
	if err := db.Sync(); err != nil {
		t.Fatalf("overwrite Sync: %v", err)
	}

	assertDBVtypFuzzRecords(t, db, records, "before vacuum")

	if _, err := db.VacuumVLOG(); err != nil {
		t.Fatalf("VacuumVLOG: %v", err)
	}
	assertDBVtypFuzzRecords(t, db, records, "after VacuumVLOG")

	if _, err := db.VacuumKV(); err != nil {
		t.Fatalf("VacuumKV: %v", err)
	}
	assertDBVtypFuzzRecords(t, db, records, "after VacuumKV")
	mustCheckIntegrity(t, db)

	db.Close()
	db = nil
	db = openTestDBAt(fs, t, dir, cfg)
	assertDBVtypFuzzRecords(t, db, records, "after reopen")
	mustCheckIntegrity(t, db)
}
