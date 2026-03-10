package main

// bench_all.go — Run all benchmarks sequentially (equivalent to C's reproduce.sh).

import (
	"fmt"
	"os"
)

func runAllBench(args []string) {
	cf, _ := parseCommonFlags(args)
	p := cf.Profile

	fmt.Printf("=== Running ALL benchmarks for dataset=%s ===\n\n", p.Name)

	// Write benchmarks (Fig 11a)
	for _, dist := range []string{"seq", "zipf", "czipf"} {
		fmt.Printf("\n### Write - %s ###\n", dist)
		runWriteBench(append(args, "-dist", dist))
	}

	// Read benchmarks (Fig 11c)
	fmt.Println("\n### Read ###")
	runReadBench(args)

	// Scan benchmarks (Fig 11d)
	for _, sl := range []string{"10", "20", "50", "100"} {
		fmt.Printf("\n### Scan-%s ###\n", sl)
		runScanBench(append(args, "-scanlen", sl))
	}

	// Scale-write (Fig 11e top)
	fmt.Println("\n### Scale-Write ###")
	runScaleWriteBench(args)

	// Scale-read (Fig 11e bottom)
	fmt.Println("\n### Scale-Read ###")
	runScaleReadBench(args)

	// YCSB (Fig 12a)
	fmt.Println("\n### YCSB ###")
	runYCSBBench(args)

	// Latency (Table 4)
	for _, op := range []string{"set", "get"} {
		fmt.Printf("\n### Latency-%s ###\n", op)
		runLatencyBench(append(args, "-op", op))
	}

	// GC (Fig 11f)
	for _, dist := range []string{"zipf", "czipf"} {
		fmt.Printf("\n### GC-%s ###\n", dist)
		runGCBench(append(args, "-dist", dist))
	}

	fmt.Println("\n=== All benchmarks complete ===")
	os.Exit(0)
}
