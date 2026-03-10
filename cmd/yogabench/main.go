// cmd/yogabench/main.go — Go benchmark harness for YogaDB,
// equivalent to the C FlexDB benchmarks in study/eurosys22-artifact/flexdb_benchmark/.
//
// Usage:
//
//	go build -o yogabench ./comparison
//	./yogabench write  -dataset udb -dist seq   -threads 4
//	./yogabench read   -dataset udb -threads 4
//	./yogabench scan   -dataset udb -threads 4 -scanlen 50
//	./yogabench scale  -dataset udb -threads 1,2,3,4,5,6,7,8
//	./yogabench ycsb   -dataset udb -workload A -threads 4
//	./yogabench latency -dataset udb -op set -threads 4
//	./yogabench gc     -dataset udb -dist zipf -threads 4
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "write":
		runWriteBench(args)
	case "read":
		runReadBench(args)
	case "scan":
		runScanBench(args)
	case "scale-write":
		runScaleWriteBench(args)
	case "scale-read":
		runScaleReadBench(args)
	case "ycsb":
		runYCSBBench(args)
	case "latency":
		runLatencyBench(args)
	case "gc":
		runGCBench(args)
	case "all":
		runAllBench(args)
	case "help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `yogabench — YogaDB benchmark suite (Go port of FlexDB C benchmarks)

Commands:
  write        Write throughput (Fig 11a)
  read         Point read throughput (Fig 11c)
  scan         Range scan throughput (Fig 11d)
  scale-write  Write scalability (Fig 11e top)
  scale-read   Read scalability (Fig 11e bottom)
  ycsb         YCSB workloads A-F (Fig 12a)
  latency      Latency CDF (Table 4)
  gc           GC overhead (Fig 11f)
  all          Run all benchmarks

Common flags:
  -dataset     Dataset profile: udb, zippydb, sys (default: udb)
  -dir         Database directory (default: /tmp/yogabench)
  -threads     Number of goroutines (default: %d)
  -count       Override operation count (default: from dataset)
  -nodisk      Run in-memory only (no disk I/O)
`, runtime.NumCPU())
}

// DatasetProfile defines a workload's key/value sizes and default operation count.
type DatasetProfile struct {
	Name    string
	KeyLen  int
	ValLen  int
	FillOps int64 // default fill count
}

var datasets = map[string]DatasetProfile{
	"udb":     {Name: "udb", KeyLen: 27, ValLen: 127, FillOps: 420_000_000},
	"zippydb": {Name: "zippydb", KeyLen: 48, ValLen: 43, FillOps: 720_000_000},
	"sys":     {Name: "sys", KeyLen: 28, ValLen: 396, FillOps: 150_000_000},
}

// CommonFlags parsed from command line.
type CommonFlags struct {
	Dataset     string
	Dir         string
	Threads     int
	Count       int64
	NoDisk      bool
	Dist        string
	Profile     DatasetProfile
	CacheMB     uint64
	OmitWALSync bool
	Duration    time.Duration // for timed benchmarks (default 60s)
}

func parseCommonFlags(args []string) (*CommonFlags, *flag.FlagSet) {
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	cf := &CommonFlags{}
	fs.StringVar(&cf.Dataset, "dataset", "udb", "Dataset: udb, zippydb, sys")
	fs.StringVar(&cf.Dir, "dir", filepath.Join(os.TempDir(), "yogabench"), "Database directory")
	fs.IntVar(&cf.Threads, "threads", runtime.NumCPU(), "Goroutine count")
	fs.Int64Var(&cf.Count, "count", 0, "Override operation count (0 = use dataset default)")
	fs.BoolVar(&cf.NoDisk, "nodisk", false, "Run in-memory (no disk)")
	fs.StringVar(&cf.Dist, "dist", "zipf", "Distribution: seq, zipf, czipf")
	fs.Uint64Var(&cf.CacheMB, "cache", 32, "Cache size in MB")
	fs.BoolVar(&cf.OmitWALSync, "omit-wal-sync", false, "Skip WAL fsync (faster loading)")
	fs.DurationVar(&cf.Duration, "duration", 60*time.Second, "Duration for timed benchmarks")
	// Bench-specific flags (accepted by all, used by relevant benchmarks)
	var scanLen int
	var op, workload, threadList string
	fs.IntVar(&scanLen, "scanlen", 0, "Scan length (scan benchmark)")
	fs.StringVar(&op, "op", "set", "Operation: set or get (latency benchmark)")
	fs.StringVar(&workload, "workload", "ALL", "YCSB workload: A-F or ALL")
	fs.StringVar(&threadList, "thread-list", "", "Comma-separated thread counts (scale benchmarks)")
	fs.Parse(args)

	p, ok := datasets[cf.Dataset]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown dataset: %s\n", cf.Dataset)
		os.Exit(1)
	}
	cf.Profile = p
	if cf.Count == 0 {
		cf.Count = p.FillOps
	}
	return cf, fs
}

func parseThreadList(s string) []int {
	parts := strings.Split(s, ",")
	var result []int
	for _, p := range parts {
		var n int
		fmt.Sscanf(p, "%d", &n)
		if n > 0 {
			result = append(result, n)
		}
	}
	return result
}

// intFlagFromArgs scans args for "-name value" and returns the int value, or defaultVal.
func intFlagFromArgs(args []string, name string, defaultVal int) int {
	for i, a := range args {
		if a == name && i+1 < len(args) {
			var v int
			fmt.Sscanf(args[i+1], "%d", &v)
			return v
		}
	}
	return defaultVal
}

// stringFlagFromArgs scans args for "-name value" and returns the string value, or defaultVal.
func stringFlagFromArgs(args []string, name string, defaultVal string) string {
	for i, a := range args {
		if a == name && i+1 < len(args) {
			return args[i+1]
		}
	}
	return defaultVal
}
