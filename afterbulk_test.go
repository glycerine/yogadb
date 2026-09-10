package yogadb

import (
	"bytes"
	"slices"
	"testing"
	"time"
)

func Test_Writes_Occuring_After_Bulk_Load_YogaDB(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{}
	db, err := OpenFlexDB(dir, cfg)
	panicOn(err)
	defer db.Close()

	keys := generateBenchKeys()
	vals := make([][]byte, len(keys))
	for i := range keys {
		vals[i] = make([]byte, 100)
		n := 0
		for range 5 {
			n += copy(vals[i][:n], keys[i])
		}
	}

	// Insert first batch of keys, ignoring insert time.
	batch := db.NewBatch()
	for i, k := range keys {
		batch.SetBytes(k, vals[i], 0)
		if (i+1)%10000 == 0 {
			batch.Commit(false)
			batch = db.NewBatch()
		}
	}
	batch.Commit(false)
	db.Sync()

	db.AllowReads()

	// what we care about is this second fresh batch.
	// this is extremely unlikely to have any collisions with the first batch.
	keys = generateBenchKeys()
	vals = make([][]byte, len(keys))
	for i := range keys {
		vals[i] = make([]byte, 100)
		n := 0
		for range 5 {
			n += copy(vals[i][:n], keys[i])
		}
	}

	t0 := time.Now()
	batch = db.NewBatch()
	for i, k := range keys {
		batch.SetBytes(k, vals[i], uint64(i))
		if (i+1)%10000 == 0 {
			batch.Commit(false)
			batch = db.NewBatch()
		}
	}
	_, metrics, err := batch.CommitGetMetrics(true)
	insertElapsed := int64(time.Since(t0)) // nanosec elapsed
	panicOn(err)
	rate := float64(len(keys)) * float64(insertElapsed) / 1e9

	vv("after bulkload terminated with AllowReads: yogadb insert %v writes/sec\n%s\n", rate, metrics)

	slices.SortFunc(keys, bytes.Compare)

	db.View(func(roDB *ReadOnlyTx) error {
		it := roDB.NewIter()
		it.SeekFirst()
		j := 0
		for it.Valid() {
			got := it.Key()
			if string(keys[j]) != got {
				t.Fatalf("at j=%v, expected key '%v', got '%v'", j, keys[j], got)
			}
			gotv, vtyp, _, err := it.FetchV()
			panicOn(err)
			if 0 != bytes.Compare(gotv, vals[vtyp]) {
				t.Fatalf("at j=%v, expected value '%v', got '%v'", j, string(vals[vtyp]), gotv)
			}
			j++
			it.Next()
		}
		it.Close()
		if j == len(keys) {
			vv("good: verified all %v keys", j)
		} else {
			vv("only verified %v out of %v", j, len(keys))
		}
		return nil
	})
}
