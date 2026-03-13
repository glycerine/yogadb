package yogadb

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestLoadBloat_SpaceAmplification measures the disk footprint after
// loading all assets/* files with value=key (same pattern as load_yogadb).
// This is the primary test for tracking space amplification at different
// SLOTTED_PAGE_KB settings.
//
// At SLOTTED_PAGE_KB=4:  ~48 MB total (~2x raw data)
// At SLOTTED_PAGE_KB=64: ~120 MB total (~5x raw data) - excessive
func TestLoadBloat_SpaceAmplification(t *testing.T) {
	fs, dir := newTestFS(t)
	cfg := &Config{
		FS:              fs,
		OmitMemWalFsync: true,
		//PiggybackGC_on_SyncOrFlush: true,
	}

	// Load keys from assets/*.txt (value = key, like load_yogadb).
	keys := loadAssetsKeys(t)
	t.Logf("loaded %d keys from assets/", len(keys))

	// Compute raw data size (key + value where value=key).
	var rawBytes int64
	for _, k := range keys {
		rawBytes += int64(len(k)) * 2 // key + value (value=key)
	}
	t.Logf("raw data size (key+value): %d bytes (%.2f MB)", rawBytes, float64(rawBytes)/(1<<20))
	t.Logf("SLOTTED_PAGE_KB = %d", SLOTTED_PAGE_KB)

	// Initial load with value = key.
	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}

	batch := db.NewBatch()
	for i, k := range keys {
		if err := batch.Set(k, []byte(k)); err != nil {
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

	m := db.SessionMetrics()
	sz := mustDirSize(fs, dir)
	amp := float64(sz) / float64(rawBytes)

	t.Logf("total disk size after load: %d bytes (%.2f MB)", sz, float64(sz)/(1<<20))
	t.Logf("space amplification: %.2fx raw data", amp)
	t.Logf("metrics: live=%d, free=%d, blocks=%d",
		m.KVBlocksTotalLiveBytes, m.TotalFreeBytesInBlocks, m.BlocksInUse)

	// Report per-file breakdown.
	listFiles, _ := fs.List(dir)
	for _, name := range listFiles {
		fpath := filepath.Join(dir, name)
		info, err := fs.Stat(fpath)
		if err != nil {
			continue
		}
		if info.Size() > 0 {
			t.Logf("  %-30s %10d bytes (%.2f MB)", name, info.Size(), float64(info.Size())/(1<<20))
		}
	}

	// Verify key count (assets have 61 duplicates, so unique < total).
	uniqueKeys := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		uniqueKeys[k] = struct{}{}
	}
	db.View(func(ro *ReadOnlyTx) error {
		count := 0
		ro.Ascend("", func(key string, value []byte) bool {
			count++
			return true
		})
		t.Logf("key count in DB: %d (unique input: %d, total input: %d)", count, len(uniqueKeys), len(keys))
		if count != len(uniqueKeys) {
			t.Errorf("key count mismatch: got %d, want %d unique", count, len(uniqueKeys))
		}
		return nil
	})

	for i := 0; i < len(keys); i += 500 {
		mustGet(t, db, keys[i], keys[i])
	}

	db.Close()

	// Space amplification threshold. At SLOTTED_PAGE_KB=4 we see ~2x.
	// At SLOTTED_PAGE_KB=64 we see ~5x which is too high.
	// For now, warn rather than fail so we can track the trend.
	if amp > 3.0 {
		t.Logf("WARNING: space amplification %.2fx exceeds 3x target (SLOTTED_PAGE_KB=%d)", amp, SLOTTED_PAGE_KB)
	}
	if amp > 6.0 {
		t.Errorf("space amplification %.2fx exceeds 6x limit (SLOTTED_PAGE_KB=%d)", amp, SLOTTED_PAGE_KB)
	}
}

// loadAssetsKeys reads all lines from assets/*.txt files and returns them.
func loadAssetsKeys(t *testing.T) []string {
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

// helper so the import isn't unused
var _ = fmt.Sprintf
