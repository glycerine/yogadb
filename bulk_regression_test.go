package yogadb

import (
	"bytes"
	"slices"
	"testing"
)

func TestPostBulkDisjointWritesSyncDoesNotCorruptIntervals(t *testing.T) {
	const nkeys = 20_000

	db, err := OpenFlexDB(t.TempDir(), &Config{
		CacheMB:                1,
		DisableBackgroundFlush: true,
	})
	if err != nil {
		t.Fatalf("OpenFlexDB: %v", err)
	}
	defer db.Close()

	keys := generateBenchKeysNseed(nkeys, 0)
	vals := afterBulkValues(keys)
	loadAfterBulkKeys(t, db, keys, vals, 0)
	if err := db.Sync(); err != nil {
		t.Fatalf("initial Sync: %v", err)
	}
	db.AllowReads()

	keys2 := generateBenchKeysNseed(nkeys, 1)
	vals2 := afterBulkValues(keys2)
	loadAfterBulkKeys(t, db, keys2, vals2, uint64(len(keys)+1))
	if err := db.Sync(); err != nil {
		t.Fatalf("post-bulk Sync: %v", err)
	}

	allkeys := append([][]byte{}, keys...)
	allkeys = append(allkeys, keys2...)
	slices.SortFunc(allkeys, bytes.Compare)
	if got, want := db.Len(), int64(len(allkeys)); got != want {
		t.Fatalf("Len() = %d, want %d", got, want)
	}

	db.View(func(roDB *ReadOnlyTx) error {
		it := roDB.NewIter()
		defer it.Close()
		it.SeekFirst()
		for j := 0; j < len(allkeys); j++ {
			if !it.Valid() {
				t.Fatalf("iterator ended at j=%d, want %d keys", j, len(allkeys))
			}
			if got, want := it.Key(), string(allkeys[j]); got != want {
				t.Fatalf("at j=%d, expected key %q, got %q", j, want, got)
			}
			it.Next()
		}
		if it.Valid() {
			t.Fatalf("iterator has extra key %q after %d keys", it.Key(), len(allkeys))
		}
		return nil
	})
	mustCheckIntegrity(t, db)
}

func afterBulkValues(keys [][]byte) [][]byte {
	vals := make([][]byte, len(keys))
	for i := range keys {
		vals[i] = make([]byte, 100)
		n := 0
		for range 5 {
			n += copy(vals[i][n:], keys[i])
		}
	}
	return vals
}

func loadAfterBulkKeys(t *testing.T, db *FlexDB, keys [][]byte, vals [][]byte, vtypBase uint64) {
	t.Helper()
	batch := db.NewBatch()
	defer batch.Close()
	for i, k := range keys {
		if err := batch.SetBytes(k, vals[i], vtypBase+uint64(i)); err != nil {
			t.Fatalf("SetBytes(%q): %v", string(k), err)
		}
		if (i+1)%10000 == 0 {
			if _, err := batch.Commit(false); err != nil {
				t.Fatalf("Commit at %d: %v", i+1, err)
			}
			batch.Reset()
		}
	}
	if _, err := batch.Commit(false); err != nil {
		t.Fatalf("final Commit: %v", err)
	}
}
