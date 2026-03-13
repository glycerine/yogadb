package yogadb

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestGC_DebugDiskGrowth(t *testing.T) {
	fs, dir := newTestFS(t)
	cfg := &Config{FS: fs}

	debugTruncate = true
	defer func() { debugTruncate = false }()
	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const nKeys = 500
	keys := make([]string, nKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("key%06d", i)
	}

	for r := 0; r <= 9; r++ {
		for i, k := range keys {
			val := fmt.Sprintf("val-round%d-%06d-padding-data-here", r, i)
			if err := db.Put(k, []byte(val)); err != nil {
				t.Fatal(err)
			}
		}
		db.Sync()

		// List files and sizes
		entries, _ := fs.ReadDir(dir)
		total := int64(0)
		for _, e := range entries {
			info, _ := e.Info()
			if info != nil {
				total += info.Size()
				if r == 1 || r == 9 {
					t.Logf("  Round %d: %-30s %d", r, e.Name(), info.Size())
				}
			}
		}
		// Also check subdirs
		filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return nil
			}
			return nil
		})
		ffSize := db.ff.Size()
		var totalBlkUsage int64
		for i := range db.ff.bm.blkusage {
			totalBlkUsage += int64(db.ff.bm.blkusage[i])
		}
		kv128Info, _ := fs.Stat(filepath.Join(dir, "FLEXSPACE.KV.SLOT_BLOCKS"))
		kv128Size := int64(0)
		if kv128Info != nil {
			kv128Size = kv128Info.Size()
		}
		t.Logf("Round %d: dir=%d kv128=%d ff.Size=%d blkUsage=%d freeBlocks=%d blkid=%d blkoff=%d",
			r, total, kv128Size, ffSize, totalBlkUsage, db.ff.bm.freeBlocks, db.ff.bm.blkid, db.ff.bm.blkoff)
	}
}
