package yogadb

import "testing"

func TestReloadSameDataWriteAmplificationBounded(t *testing.T) {
	fs, dir := newTestFS(t)
	cfg := &Config{
		FS:                     fs,
		OmitMemWalFsync:        true,
		DisableBackgroundFlush: true,
	}

	keys := loadAssetsKeys(t)
	if len(keys) == 0 {
		t.Fatal("assets key set is empty")
	}
	logicalBytes := reloadLogicalBytes(keys)
	t.Logf("loaded %d input keys from assets/; logical payload per load=%d bytes", len(keys), logicalBytes)

	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	reloadSameData(t, db, keys)
	if err := db.Sync(); err != nil {
		t.Fatalf("first load Sync: %v", err)
	}
	firstLoad := db.SessionMetrics()
	t.Logf("first load metrics: writeAmp=%.3f kv128=%d memwal=%d redo=%d logical=%d total=%d",
		firstLoad.WriteAmp, firstLoad.KV128BytesWritten, firstLoad.MemWALBytesWritten,
		firstLoad.REDOLogBytesWritten, firstLoad.LogicalBytesWritten, firstLoad.TotalBytesWritten)

	vacstat, err := db.VacuumKV()
	if err != nil {
		t.Fatalf("first load VacuumKV: %v", err)
	}
	t.Logf("first load VacuumKV: %v", vacstat)
	mustCheckIntegrity(t, db)
	db.Close()

	db, err = OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	reloadSameData(t, db, keys)
	if err := db.Sync(); err != nil {
		t.Fatalf("duplicate reload Sync: %v", err)
	}
	duplicateLoad := db.SessionMetrics()
	t.Logf("duplicate reload metrics: writeAmp=%.3f kv128=%d memwal=%d redo=%d logical=%d total=%d",
		duplicateLoad.WriteAmp, duplicateLoad.KV128BytesWritten, duplicateLoad.MemWALBytesWritten,
		duplicateLoad.REDOLogBytesWritten, duplicateLoad.LogicalBytesWritten, duplicateLoad.TotalBytesWritten)
	mustCheckIntegrity(t, db)

	for i := 0; i < len(keys); i += 1000 {
		mustGet(t, db, keys[i], keys[i])
	}
	db.Close()

	const maxDuplicateReloadWriteAmp = 8.0
	if duplicateLoad.WriteAmp > maxDuplicateReloadWriteAmp {
		t.Fatalf("duplicate reload write amplification too high: got %.3fx, want <= %.1fx; "+
			"kv128=%d memwal=%d redo=%d logical=%d total=%d",
			duplicateLoad.WriteAmp, maxDuplicateReloadWriteAmp,
			duplicateLoad.KV128BytesWritten, duplicateLoad.MemWALBytesWritten,
			duplicateLoad.REDOLogBytesWritten, duplicateLoad.LogicalBytesWritten,
			duplicateLoad.TotalBytesWritten)
	}
}

func reloadSameData(t *testing.T, db *FlexDB, keys []string) {
	t.Helper()

	const maxBatchCount = 1000
	batch := db.NewBatch()
	defer batch.Close()
	currentBatchCount := 0
	for _, k := range keys {
		if err := batch.Set(k, []byte(k), 0); err != nil {
			t.Fatalf("Set(%q): %v", k, err)
		}
		currentBatchCount++
		if currentBatchCount >= maxBatchCount {
			if _, err := batch.Commit(false); err != nil {
				t.Fatalf("Commit: %v", err)
			}
			currentBatchCount = 0
		}
	}
	if currentBatchCount > 0 {
		if _, err := batch.Commit(false); err != nil {
			t.Fatalf("final Commit: %v", err)
		}
	}
}

func reloadLogicalBytes(keys []string) int64 {
	var n int64
	for _, k := range keys {
		n += int64(len(k) * 2)
	}
	return n
}
