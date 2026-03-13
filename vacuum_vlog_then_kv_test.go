package yogadb

import (
	"fmt"
	"testing"
)

// TestVacuumVLOG_ThenVacuumKV reproduces a crash where VacuumVLOG rewrites
// intervals containing VPtrs from slotted page format to kv128 format
// (with kv128ExtentMagic prefix), and then VacuumKV's rebuildAnchorsFromTags
// fails to read the first key from those kv128 extents.
//
// Reproducer: load_yogadb loads ~434K keys with values large enough for VLOG,
// then yvac runs VacuumVLOG followed by VacuumKV. The VacuumKV panics in
// flexdbReadKVFromHandler because FlexSpaceHandler.Read is non-advancing
// and the kv128 magic is re-read instead of the actual kv128 data.
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
		err := batch.Set(key, []byte(val))
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

	// VacuumVLOG rewrites intervals with VPtrs from slotted page
	// to kv128 format (with kv128ExtentMagic prefix).
	stats, err := db2.VacuumVLOG()
	if err != nil {
		t.Fatalf("VacuumVLOG: %v", err)
	}
	t.Logf("VacuumVLOG: %v", stats)

	// VacuumKV compacts KV128_BLOCKS. This calls rebuildAnchorsFromTags
	// which must correctly read the first key from kv128 extents.
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
