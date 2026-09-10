package yogadb

import (
	"fmt"
	"strconv"
	"testing"
)

const mergeBenchKeyCount = 100_000

func mergeBenchKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = "tenant/" + strconv.Itoa(i%97) + "/account/" + strconv.Itoa(i) + "/event/" + strconv.Itoa((i*7919)%1_000_003)
	}
	return keys
}

func benchmarkLoadSetOnly(b testing.TB, db *FlexDB, keys []string, prefix string, every int) {
	b.Helper()
	const maxBatchCount = 1000
	batch := db.NewBatch()
	defer batch.Close()
	currentBatchCount := 0
	for i, k := range keys {
		if every > 1 && i%every != 0 {
			continue
		}
		val := prefix + k
		if err := batch.Set(k, []byte(val), 0); err != nil {
			b.Fatalf("Set(%q): %v", k, err)
		}
		currentBatchCount++
		if currentBatchCount >= maxBatchCount {
			if _, err := batch.Commit(false); err != nil {
				b.Fatalf("Commit: %v", err)
			}
			currentBatchCount = 0
		}
	}
	if currentBatchCount > 0 {
		if _, err := batch.Commit(false); err != nil {
			b.Fatalf("final Commit: %v", err)
		}
	}
}

func BenchmarkReloadExistingDBSecondBulkLoad(b *testing.B) {
	keys := mergeBenchKeys(mergeBenchKeyCount)
	cfg := &Config{
		OmitMemWalFsync:        true,
		DisableBackgroundFlush: true,
	}

	for _, tc := range []struct {
		name  string
		every int
	}{
		{name: "duplicate_all", every: 1},
		{name: "sparse_patch_1pct", every: 100},
	} {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				dir := b.TempDir() + "/" + fmt.Sprintf("db-%06d", i)
				db, err := OpenFlexDB(dir, cfg)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkLoadSetOnly(b, db, keys, "first:", 1)
				if err := db.Sync(); err != nil {
					b.Fatalf("first Sync: %v", err)
				}
				db.AllowReads()
				db.Close()

				db, err = OpenFlexDB(dir, cfg)
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				benchmarkLoadSetOnly(b, db, keys, "second:", tc.every)
				db.AllowReads()
				if err := db.Sync(); err != nil {
					b.Fatalf("second Sync: %v", err)
				}
				b.StopTimer()
				db.Close()
			}
		})
	}
}
