package yogadb

import (
	"bufio"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func loadVacuumBloatKeys(t *testing.T) []string {
	t.Helper()

	assetsDir := filepath.Join("assets")
	entries, err := os.ReadDir(assetsDir)
	if err != nil {
		t.Fatalf("reading assets dir: %v", err)
	}

	var keys []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		f, err := os.Open(filepath.Join(assetsDir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := scanner.Text()
			if line != "" {
				keys = append(keys, line)
			}
		}
		f.Close()
		if err := scanner.Err(); err != nil {
			t.Fatal(err)
		}
	}
	return keys
}

func forceTinyIntervalCache(db *FlexDB) {
	db.cache.cap = 1
	for i := range db.cache.partitions {
		db.cache.partitions[i].cap = 1
	}
}

func TestVacuumKVOverwriteTinyCachePreservesAnchorTags(t *testing.T) {
	fs, dir := newTestFS(t)
	cfg := &Config{
		FS:                     fs,
		OmitMemWalFsync:        true,
		DisableBackgroundFlush: true,
	}
	keys := loadVacuumBloatKeys(t)
	t.Logf("loaded %d keys from assets/", len(keys))

	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	forceTinyIntervalCache(db)

	batch := db.NewBatch()
	for i, k := range keys {
		if err := batch.Set(k, []byte(k), 0); err != nil {
			t.Fatal(err)
		}
		if (i+1)%1000 == 0 {
			if _, err := batch.Commit(false); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := batch.Commit(false); err != nil {
		t.Fatal(err)
	}
	batch.Close()
	if err := db.Sync(); err != nil {
		t.Fatalf("initial Sync: %v", err)
	}
	db.Close()

	db, err = OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	forceTinyIntervalCache(db)
	if _, err := db.VacuumVLOG(); err != nil {
		t.Fatalf("VacuumVLOG: %v", err)
	}
	mustCheckIntegrity(t, db)
	if _, err := db.VacuumKV(); err != nil {
		t.Fatalf("VacuumKV: %v", err)
	}
	mustCheckIntegrity(t, db)

	batch = db.NewBatch()
	for i, k := range keys {
		if err := batch.Set(k, []byte(k), 0); err != nil {
			t.Fatalf("rewrite Set(%q): %v", k, err)
		}
		if (i+1)%1000 == 0 {
			if _, err := batch.Commit(false); err != nil {
				t.Fatalf("rewrite Commit: %v", err)
			}
		}
	}
	if _, err := batch.Commit(false); err != nil {
		t.Fatalf("final rewrite Commit: %v", err)
	}
	batch.Close()
	if err := db.Sync(); err != nil {
		t.Fatalf("rewrite Sync: %v", err)
	}
	mustCheckIntegrity(t, db)
	db.Close()
}

// TestVacuumThenOverwrite_DiskSizeBounded reproduces a space amplification
// regression: after VacuumVLOG + VacuumKV, reloading the same keys causes
// KV.SLOT_BLOCKS to grow by ~4 MB per load instead of staying constant.
//
// The pattern is: load keys (some with VLOG-sized values) -> close -> reopen ->
// VacuumVLOG -> VacuumKV -> reload same keys N times -> check disk size.
//
// Uses real-world keys from assets/*.txt (the same data as ~/all).
func TestVacuumThenOverwrite_DiskSizeBounded(t *testing.T) {
	fs, dir := newTestFS(t)
	cfg := &Config{
		FS:                         fs,
		OmitMemWalFsync:            true,
		PiggybackGC_on_SyncOrFlush: true,
	}

	keys := loadVacuumBloatKeys(t)
	t.Logf("loaded %d keys from assets/", len(keys))

	// Step 1: Initial load with value = key (like the reproducer).
	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}

	batch := db.NewBatch()
	for i, k := range keys {
		if err := batch.Set(k, []byte(k), 0); err != nil {
			t.Fatal(err)
		}
		if (i+1)%1000 == 0 {
			if _, err := batch.Commit(false); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := batch.Commit(false); err != nil {
		t.Fatal(err)
	}
	batch.Close()
	db.Sync()
	db.AllowReads()
	db.Close()

	// Step 2: Reopen, VacuumVLOG, VacuumKV.
	db, err = OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	db.AllowReads()

	vstats, err := db.VacuumVLOG()
	if err != nil {
		t.Fatalf("VacuumVLOG: %v", err)
	}
	t.Logf("VacuumVLOG: %v", vstats)

	mustCheckIntegrity(t, db)

	kstats, err := db.VacuumKV()
	if err != nil {
		t.Fatalf("VacuumKV: %v", err)
	}
	t.Logf("VacuumKV: %v", kstats)

	mustCheckIntegrity(t, db)
	t.Logf("integrity OK after VacuumVLOG+VacuumKV")

	// Step 3: Reload same keys across separate sessions (close+reopen).
	// Round 0 establishes the post-vacuum baseline.
	var sizeAfterRound0 int64
	for r := 0; r <= 4; r++ {
		batch := db.NewBatch()
		for i, k := range keys {
			if err := batch.Set(k, []byte(k), 0); err != nil {
				t.Fatalf("round %d Set(%q): %v", r, k, err)
			}
			if (i+1)%1000 == 0 {
				if _, err := batch.Commit(false); err != nil {
					t.Fatalf("round %d Commit: %v", r, err)
				}
			}
		}
		if _, err := batch.Commit(false); err != nil {
			t.Fatalf("round %d final Commit: %v", r, err)
		}
		batch.Close()
		db.Sync()
		db.AllowReads()

		m := db.SessionMetrics()
		uc := atomic.LoadInt64(&db.ff.updateCount)
		ug := atomic.LoadInt64(&db.ff.updateGarbageBytes)
		ic := atomic.LoadInt64(&db.ff.insertCount)
		ib := atomic.LoadInt64(&db.ff.insertBytes)
		atomic.StoreInt64(&db.ff.updateCount, 0)
		atomic.StoreInt64(&db.ff.updateGarbageBytes, 0)
		atomic.StoreInt64(&db.ff.insertCount, 0)
		atomic.StoreInt64(&db.ff.insertBytes, 0)
		sz := mustDirSize(fs, dir)
		if r == 0 {
			t.Logf("Round 0 (baseline): %d bytes, live=%d, free=%d, blocks=%d, ffUpdates=%d, ffGarbage=%d, ffInserts=%d, ffInsertBytes=%d",
				sz, m.KVBlocksTotalLiveBytes, m.TotalFreeBytesInBlocks, m.BlocksInUse, uc, ug, ic, ib)
		} else {
			t.Logf("Round %d: %d bytes (%.2fx), live=%d, free=%d, blocks=%d, ffUpdates=%d, ffGarbage=%d, ffInserts=%d, ffInsertBytes=%d",
				r, sz, float64(sz)/float64(sizeAfterRound0),
				m.KVBlocksTotalLiveBytes, m.TotalFreeBytesInBlocks, m.BlocksInUse, uc, ug, ic, ib)
		}

		errs := db.CheckIntegrity()
		if len(errs) > 0 {
			for _, e := range errs {
				t.Logf("round %d integrity error: %v", r, e)
			}
			t.Fatalf("round %d: CheckIntegrity found %d errors", r, len(errs))
		}

		// Close and reopen between rounds (like the reproducer).
		db.Close()
		db, err = OpenFlexDB(dir, cfg)
		if err != nil {
			t.Fatalf("round %d reopen: %v", r, err)
		}
		db.AllowReads()

		if r == 0 {
			sizeAfterRound0 = sz
		}
	}

	sizeAfterRounds := mustDirSize(fs, dir)

	// After the baseline round, 4 more rounds of the SAME data should NOT
	// grow the disk significantly. Allow 1.5x for redo log growth.
	maxAcceptable := int64(float64(sizeAfterRound0) * 1.5)
	if sizeAfterRounds > maxAcceptable {
		t.Errorf("disk grew too much on redundant writes after vacuum: "+
			"baseline=%d, after 4 rounds=%d (%.2fx); max=%.0f (1.5x)",
			sizeAfterRound0, sizeAfterRounds,
			float64(sizeAfterRounds)/float64(sizeAfterRound0),
			float64(maxAcceptable))
	}

	// Verify a sample of keys for correctness.
	for i := 0; i < len(keys); i += 100 {
		mustGet(t, db, keys[i], keys[i])
	}
	mustCheckIntegrity(t, db)
	db.Close()
}
