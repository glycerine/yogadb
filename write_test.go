package yogadb

import (
	//"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

// close the gap on write performance by comparison to pebble which starts 70% faster, maybe?

func Test202_write_performance_yoga_versus_pebble(t *testing.T) {

	totalOps := 100_000

	batchSize := 800

	dir := t.TempDir()
	cfg := &Config{
		// we are slower without the RedoLog batching and amortizing.
		//OmitFlexSpaceOpsRedoLog: true,
	}
	yoga, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatalf("OpenFlexDB: %v", err)
	}
	//defer db.Close()

	//var logicalBytes int64

	var seed [32]byte
	prng := newPRNG(seed)
	// Pre-generate all keys to avoid measuring key generation.

	keys := make([][]byte, totalOps)
	dup := make(map[string]bool)
	for i := range keys {
		// only write unique keys for now.
		var cid string
		for {
			cid = prng.NewCallID()
			if dup[cid] {
				continue
			}
			dup[cid] = true
			break
		}
		keys[i] = []byte(cid)
		//logicalBytes += int64(len(cid) * 2)
	}

	{
		t0 := time.Now()
		ki := 0
		batch := yoga.NewBatch()
		for i := 0; i < totalOps; i++ {

			if i%batchSize == 0 && i > 0 {
				_, _ = batch.Commit(false)
				//vv("yoga commited batch at i = %v", i)
			}
			j := ki % len(keys)
			batch.Set(string(keys[j]), keys[j])
			ki++
		}
		//batch.Commit()
		_, m, _ := batch.CommitGetMetrics(false)
		//yoga.Sync()

		elap := time.Since(t0)
		//m := yoga.Close()
		yogaRate := float64(int64(elap)) / float64(totalOps)

		vv("yoga rate = %v  nsec/op;  elap = %v   ; writeAmp = %v", yogaRate, elap, m.CumulativeWriteAmp)
	}
	yoga.Close()

	//////////////// pebble, on same keys

	dir2 := t.TempDir()

	pebbl, err := pebble.Open(dir2, &pebble.Options{})
	if err != nil {
		t.Fatalf("pebble.Open: %v", err)
	}
	//defer pebbl.Close()

	//var logicalBytes int64

	{
		t1 := time.Now()
		ki := 0
		batch := pebbl.NewBatch()
		for i := 0; i < totalOps; i++ {

			if i%batchSize == 0 && i > 0 {
				//batch.Commit(pebble.Sync)
				batch.Commit(pebble.NoSync)
				batch = pebbl.NewBatch()
				//vv("pebble commited batch at i = %v", i)
			}
			j := ki % len(keys)
			batch.Set(keys[j], keys[j], pebble.NoSync)
			ki++
		}
		batch.Commit(pebble.Sync)
		//batch.Commit(pebble.NoSync)
		//pebbl.Flush()

		elap1 := time.Since(t1)
		pebbleRate := float64(int64(elap1)) / float64(totalOps)

		vv("pebble rate = %v  nsec/op; total elap1 = %v", pebbleRate, elap1)
	}
	pebbl.Close()

	//pebbl.Flush()
	//pebbl.Sync() // flush memtable to disk for accurate size measurement
	//m := pebbl.SessionMetrics()

	// metrics := pebbl.Metrics()
	// levelMetrics := metrics.Total()

	// diskBytes := MustDirSize(dir)
	// wa := float64(diskBytes) / float64(logicalBytes)

	// rateName := "our_GET_ns/op"
	// if write == 1 {
	// 	rateName = "our_PUT_ns/op"
	// }

}

/*
go test -v -run=202
=== RUN   Test202_write_performance_yoga_versus_pebble

write_test.go:69 [goID 19] 2026-03-04 23:03:35.684197000 +0000 UTC yoga rate = 1230.27818  nsec/op;  elap = 123.027818ms
2026/03/04 23:03:35 Found 0 WALs

write_test.go:109 [goID 19] 2026-03-04 23:03:36.547717000 +0000 UTC pebble rate = 6161.21972  nsec/op; total elap1 = 616.121972ms
*/
