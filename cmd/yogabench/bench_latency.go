package main

// bench_latency.go — Latency CDF benchmark (equivalent to C's dbcdf.c / Table 4).
//
// Measures per-operation latency and outputs a CDF histogram.
// Tests both Set and Get latency with Zipfian distribution.

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	yogadb "github.com/glycerine/yogadb"
)

const latencyBuckets = 10000 // microsecond buckets [0, 10000)

func runLatencyBench(args []string) {
	cf, _ := parseCommonFlags(args)

	op := stringFlagFromArgs(args, "-op", "set")

	p := cf.Profile
	maxKey := uint64(cf.Count - 1)

	printHeader(fmt.Sprintf("LATENCY-%s", op), cf)

	db, err := openDB(cf.Dir, cf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open db: %v\n", err)
		os.Exit(1)
	}

	switch op {
	case "set":
		runLatencySet(db, cf, p, maxKey)
	case "get":
		// Fill first, then measure get latency
		fmt.Println("--- Fill ---")
		parallelFill(db, cf.Count, cf.Threads, p.KeyLen, p.ValLen)
		fmt.Println("--- Get Latency ---")
		runLatencyGet(db, cf, p, maxKey)
	default:
		fmt.Fprintf(os.Stderr, "unknown op: %s\n", op)
		os.Exit(1)
	}

	closeDB(db)
}

func runLatencySet(db *yogadb.FlexDB, cf *CommonFlags, p DatasetProfile, maxKey uint64) {
	// Histogram: one per goroutine to avoid contention, merge at end
	histograms := make([][]atomic.Int64, cf.Threads)
	for i := range histograms {
		histograms[i] = make([]atomic.Int64, latencyBuckets)
	}
	var overflow atomic.Int64

	val := makeValue(p.ValLen)
	opsPerThread := cf.Count / int64(cf.Threads)
	var wg sync.WaitGroup

	start := time.Now()
	for t := 0; t < cf.Threads; t++ {
		wg.Add(1)
		go func(tid int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(tid) * 12345))
			zipf := NewZipfian(rng, 0, maxKey)
			keyBuf := make([]byte, p.KeyLen)
			hist := histograms[tid]

			for i := int64(0); i < opsPerThread; i++ {
				k := string(hexKeyBuf(keyBuf, zipf.Next(), p.KeyLen))
				t0 := time.Now()
				db.Put(k, val)
				us := time.Since(t0).Microseconds()
				if us < int64(latencyBuckets) {
					hist[us].Add(1)
				} else {
					overflow.Add(1)
				}
			}
		}(t)
	}
	wg.Wait()
	elapsed := time.Since(start)

	totalOps := opsPerThread * int64(cf.Threads)
	mops := float64(totalOps) / elapsed.Seconds() / 1e6
	fmt.Printf("set: ops %d  %.4f Mops  (%.2fs)\n", totalOps, mops, elapsed.Seconds())

	printLatencyCDF(histograms, overflow.Load())
}

func runLatencyGet(db *yogadb.FlexDB, cf *CommonFlags, p DatasetProfile, maxKey uint64) {
	histograms := make([][]atomic.Int64, cf.Threads)
	for i := range histograms {
		histograms[i] = make([]atomic.Int64, latencyBuckets)
	}
	var overflow atomic.Int64
	var totalOps atomic.Int64

	deadline := time.Now().Add(cf.Duration)
	var wg sync.WaitGroup

	start := time.Now()
	for t := 0; t < cf.Threads; t++ {
		wg.Add(1)
		go func(tid int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(tid) * 12345))
			zipf := NewZipfian(rng, 0, maxKey)
			keyBuf := make([]byte, p.KeyLen)
			hist := histograms[tid]
			ops := int64(0)

			for time.Now().Before(deadline) {
				for j := 0; j < (1 << 14); j++ {
					k := string(hexKeyBuf(keyBuf, zipf.Next(), p.KeyLen))
					t0 := time.Now()
					db.Get(k)
					us := time.Since(t0).Microseconds()
					if us < int64(latencyBuckets) {
						hist[us].Add(1)
					} else {
						overflow.Add(1)
					}
					ops++
				}
			}
			totalOps.Add(ops)
		}(t)
	}
	wg.Wait()
	elapsed := time.Since(start)

	ops := totalOps.Load()
	mops := float64(ops) / elapsed.Seconds() / 1e6
	fmt.Printf("get: ops %d  %.4f Mops  (%.2fs)\n", ops, mops, elapsed.Seconds())

	printLatencyCDF(histograms, overflow.Load())
}

func printLatencyCDF(histograms [][]atomic.Int64, overflow int64) {
	// Merge histograms
	merged := make([]int64, latencyBuckets)
	for _, hist := range histograms {
		for i := range merged {
			merged[i] += hist[i].Load()
		}
	}

	// Compute total
	total := overflow
	for _, c := range merged {
		total += c
	}
	if total == 0 {
		fmt.Println("no latency data")
		return
	}

	totalD := float64(total)
	sum := int64(0)

	fmt.Println("time_us count delta cdf")
	fmt.Println("0 0 0 0.000")
	lastPrinted := int64(0)
	for i := int64(1); i < int64(latencyBuckets); i++ {
		if merged[i] > 0 {
			if (i - 1) != lastPrinted {
				fmt.Printf("%d %d 0 %.3f\n", i-1, sum, float64(sum)*100.0/totalD)
			}
			sum += merged[i]
			fmt.Printf("%d %d %d %.3f\n", i, sum, merged[i], float64(sum)*100.0/totalD)
			lastPrinted = i
		}
	}
	if overflow > 0 {
		sum += overflow
		fmt.Printf("%d %d %d %.3f (overflow)\n", latencyBuckets, sum, overflow, float64(sum)*100.0/totalD)
	}
	fmt.Printf("total %d\n", total)

	// Compute average latency (weighted)
	weightedSum := float64(0)
	for i, c := range merged {
		weightedSum += float64(i) * float64(c)
	}
	weightedSum += float64(latencyBuckets) * float64(overflow)
	fmt.Printf("avg_latency_us %.2f\n", weightedSum/totalD)
}
