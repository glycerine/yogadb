package yogadb

import (
	"bytes"
	"slices"
	"testing"
	"time"
)

func generateBenchKeysNseed(n int, seed0 byte) [][]byte {
	var seed [32]byte
	seed[0] = seed0
	prng := newPRNG(seed)
	keys := make([][]byte, 0, n)
	dup := make(map[string]bool, n)
	for len(keys) < n {
		cid := prng.NewCallID()
		if dup[cid] {
			continue
		}
		dup[cid] = true
		keys = append(keys, []byte(cid))
	}
	return keys
}

func Test_Writes_Occuring_After_Bulk_Load_YogaDB(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{}
	db, err := OpenFlexDB(dir, cfg)
	panicOn(err)
	defer db.Close()

	keys := generateBenchKeysNseed(1_000_000, 0)
	vals := make([][]byte, len(keys))
	for i := range keys {
		vals[i] = make([]byte, 100)
		n := 0
		for range 5 {
			n += copy(vals[i][n:], keys[i])
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
	keys2 := generateBenchKeysNseed(1_000_000, 1)

	vals2 := make([][]byte, len(keys2))
	for i := range keys2 {
		vals2[i] = make([]byte, 100)
		n := 0
		for range 5 {
			n += copy(vals2[i][n:], keys2[i])
		}
	}

	t0 := time.Now()
	expectedVals := make(map[string][]byte, len(keys)+len(keys2))
	expectedVtyps := make(map[string]uint64, len(keys)+len(keys2))
	for i, k := range keys {
		expectedVals[string(k)] = vals[i]
		expectedVtyps[string(k)] = 0
	}
	batch = db.NewBatch()
	for i, k := range keys2 {
		vtyp := uint64(len(keys) + i + 1)
		batch.SetBytes(k, vals2[i], vtyp)
		expectedVals[string(k)] = vals2[i]
		expectedVtyps[string(k)] = vtyp
		if (i+1)%10000 == 0 {
			batch.Commit(false)
			batch = db.NewBatch()
		}
	}
	_, metrics, err := batch.CommitGetMetrics(true)
	insertElapsed := time.Since(t0)
	panicOn(err)
	rate := float64(len(keys2)) / insertElapsed.Seconds()

	vv("after bulkload terminated with AllowReads: yogadb insert %v writes/sec\n%s\n", rate, metrics)

	allkeys := append([][]byte{}, keys...)
	allkeys = append(allkeys, keys2...)
	slices.SortFunc(allkeys, bytes.Compare)
	for i := range allkeys {
		if i == 0 {
			continue
		}
		if 0 == bytes.Compare(allkeys[i], allkeys[i-1]) {
			t.Fatalf("ugh. duplicated keys. we wanted batch 2 to be disjoint from batch 1.")
		}
	}
	vv("good: all %v keys were distinct. len(vals) = %v; len(vals2) = %v", len(allkeys), len(vals), len(vals2))

	db.View(func(roDB *ReadOnlyTx) error {
		it := roDB.NewIter()
		it.SeekFirst()
		j := 0
		for it.Valid() {
			got := it.Key()
			gotv, vtyp, _, err := it.FetchV()
			panicOn(err)

			if string(allkeys[j]) != string(got) {
				t.Fatalf("at j=%v, expected key '%v', got '%v'", j, string(allkeys[j]), string(got))
			}
			wantv, ok := expectedVals[string(got)]
			if !ok {
				t.Fatalf("at j=%v, unexpected key '%v'", j, string(got))
			}
			if 0 != bytes.Compare(gotv, wantv) {
				t.Fatalf("at j=%v, expected value '%v', got '%v'", j, string(wantv), gotv)
			}
			if wantVtyp := expectedVtyps[string(got)]; vtyp != wantVtyp {
				t.Fatalf("at j=%v, key '%v' got vtyp %v, want %v", j, string(got), vtyp, wantVtyp)
			}
			j++
			it.Next()
		}
		it.Close()
		if j == len(allkeys) {
			vv("good: verified all %v keys", j)
		} else {
			vv("only verified %v out of %v", j, len(allkeys))
		}
		return nil
	})
}
