# BENCH_PORT.md — Plan for Porting FlexDB C Benchmarks to Go

This document describes the plan used to port the C benchmark suite from
`study/eurosys22-artifact/flexdb_benchmark/` to Go in `comparison/`.

---

## 1. Study the C Benchmark Architecture

### Source Files

| C File | Purpose |
|--------|---------|
| `c/dbtest1.c` | Primary benchmark harness — supports set/get/del/probe/scan ops with parallel and distribution-based modes |
| `c/dbcdf.c` | Latency CDF variant — same ops as dbtest1 but records per-op latency in microsecond buckets |
| `c/ycsbtest.c` | YCSB mixed workload — weighted random mix of get/set/scan/RMW with configurable percentages |
| `c/lib.c` | Core infrastructure: Zipfian/uniform/latest distributions (`rgen_*`), threading (`forker_*`), timing, hex key generation (`strhex_64`) |
| `c/kv.c` | Key generation: `kv_refill_hex64_klen()` converts u64 to 16-char hex string, pads with `'!'` if klen > 16 |

### Shell Scripts (orchestration)

| Script | Paper Figure | What It Tests |
|--------|-------------|--------------|
| `scripts/write.sh` | Fig 11a,b | Write throughput + write amplification (seq/zipf/czipf × 3 datasets) |
| `scripts/read.sh` | Fig 11c,d | Point read + range scan (fill → warmup → zipf reads → czipf reads → scans at 10/20/50/100) |
| `scripts/scale-write.sh` | Fig 11e top | Write scalability: vary threads 1–8 |
| `scripts/scale.sh` | Fig 11e mid/bot | Read + scan scalability: fill once, vary threads 1–8 |
| `scripts/gc.sh` | Fig 11f | GC overhead: fill + 100M overwrites with constrained max offset |
| `scripts/latency.sh` | Table 4 | Per-op latency CDF for set and get |
| `scripts/ycsb.sh` | Fig 12a | YCSB A–F workloads (in-core) |
| `scripts/ycsb-oc.sh` | Fig 12b | YCSB A–F workloads (out-of-core, larger datasets) |
| `reproduce.sh` | All | Master script running all benchmarks sequentially |

### Dataset Profiles

| Profile | Key Length | Value Length | Fill Count (in-core) | Fill Count (out-of-core) |
|---------|-----------|-------------|---------------------|------------------------|
| UDB | 27 bytes | 127 bytes | 420M | 2.6B |
| ZippyDB | 48 bytes | 43 bytes | 720M | 4.5B |
| SYS | 28 bytes | 396 bytes | 150M | 190M |

### Distribution Generators

| C Generator | Description | Benchmark Usage |
|------------|-------------|-----------------|
| `rgen_new_zipfian(min, max)` | Standard Zipfian (theta=0.99, YCSB-style) | Most read/write benchmarks |
| `rgen_new_zipfuni(min, max, ufactor)` | Clustered Zipfian: `z*ufactor + uniform(ufactor)` | "czipf" write/read variants |
| `rgen_new_latest(zipf_range)` | Latest: reads near head, writes increment atomically | YCSB workload D |
| `incu` (sequential) | Incrementing counter | Sequential fill (S mode) |

### C Forker Framework

The C benchmarks use a `forker_passes` framework that:
- Spawns N pthreads per "pass" (phase)
- Each thread runs a batch function in a loop (count-based or time-based)
- A `damp` (damping average) reports stable Mops/sec periodically
- Operation codes: `S`=parallel fill, `s`=random set, `g`=random get, `n`=seek+next, `G`=parallel get, etc.
- Output format: `set N M mops X.XXXX avg Y.YYYY ravg Z.ZZZZ`

---

## 2. Map C Concepts to Go

| C Concept | Go Equivalent |
|-----------|--------------|
| `struct kv` + `kv_refill_hex64_klen` | `hexKeyBuf(buf, n, klen)` — same hex encoding + `'!'` padding |
| `strhex_64(out, v)` | Big-endian u64 → 16 lowercase hex chars |
| `rgen_new_zipfian` | `NewZipfian(rng, min, max)` with identical zeta computation + precomputed table |
| `rgen_new_zipfuni` | `NewClusteredZipfian(rng, min, max, ufactor)` |
| `rgen_new_latest` | `NewLatest(rng, zipfRange)` with atomic head counter |
| `random_double()` | `rng.Float64()` (per-goroutine `*rand.Rand`) |
| `forker_passes` + pthreads | `RunParallel()` / `RunTimed()` with goroutines + `sync.WaitGroup` |
| `kvmap_kv_put(api, ref, kv)` | `db.Put(key, value)` or `batch.Set(key, value)` |
| `kvmap_kv_get(api, ref, kv, out)` | `db.Get(key)` |
| `kvmap_kv_iter_seek` + `iter_next` | `db.View()` + `iter.Seek()` + `iter.Next()` |
| `kvmap_kv_merge` | `db.Merge(key, fn)` |
| `api flexdb path blocksize` | `yogadb.OpenFlexDB(dir, &Config{...})` |
| `numactl -N 0` | `runtime.GOMAXPROCS()` (Go scheduler handles affinity) |
| `vctr` counters + `damp` averager | `atomic.Int64` counters + direct Mops calculation |
| `smartctl` SSD write metrics | `Metrics.WriteAmp` from `db.Close()` |

---

## 3. Go Benchmark File Layout

```
comparison/
├── main.go            CLI entry point + flag parsing (maps to reproduce.sh + shell scripts)
├── keygen.go          Key generation matching C's kv_refill_hex64_klen
├── zipfian.go         Zipfian, Clustered Zipfian, Latest, Uniform distributions (from lib.c)
├── runner.go          Goroutine management, timing, reporting, DB open/close (from forker_*)
├── bench_write.go     Write throughput: seq/zipf/czipf (write.sh → Fig 11a)
├── bench_read.go      Point read: fill → warmup → zipf/czipf reads (read.sh → Fig 11c)
├── bench_scan.go      Range scan: seek+next at lengths 10/20/50/100 (read.sh → Fig 11d)
├── bench_scale.go     Scalability: vary thread count 1–8 for writes/reads/scans (scale*.sh → Fig 11e)
├── bench_ycsb.go      YCSB A–F mixed workloads (ycsb.sh → Fig 12a)
├── bench_latency.go   Per-op latency CDF histogram (dbcdf.c → Table 4)
├── bench_gc.go        GC overhead: fill + overwrite (gc.sh → Fig 11f)
└── bench_all.go       Run all benchmarks sequentially (reproduce.sh)
```

---

## 4. Key Design Decisions

### 4a. Key Generation Fidelity

The C function `kv_refill_hex64_klen` converts a `u64` to a 16-character lowercase hex
string via `strhex_64`, then pads with `'!'` if `klen > 16`. The Go port replicates this
exactly in `hexKeyBuf` using manual big-endian byte extraction and hex table lookup,
avoiding `fmt.Sprintf` in the hot path.

### 4b. Zipfian Distribution Fidelity

The C Zipfian uses theta=0.99 with a precomputed zeta lookup table (`zetalist[]`) for
large ranges. The Go port copies the exact 17-entry table (as `math.Float64frombits` of
the same u64 constants) and uses the same `zetaFull` → `zetaRange` decomposition.
The `gen_zipfian` core formula is ported verbatim:
```
u = random_double()
uz = u * zetan
if uz < 1.0 → base
if uz < quick1 → base + 1
else → base + floor(mod * pow(eta*u + quick2, alpha))
```

### 4c. Batched Fill vs. Individual Puts

The C benchmarks do individual `kvmap_kv_put` calls even during fill (the "S" mode just
partitions the key range across threads). On disk, individual Go `db.Put` calls are
~22ms each due to WAL fsync overhead, making large fills impractical.

Solution: `parallelFill` uses the `Batch` API (1024-key commits without fsync) for the
fill phase only. Non-fill benchmark phases (zipf writes, gets, scans) use individual
`db.Put`/`db.Get` calls to match the C benchmark's per-operation measurement.

### 4d. Timed vs. Counted Phases

The C benchmarks use two modes:
- **Count-based** (`end_type == FORKER_END_COUNT`): run exactly N operations
- **Time-based** (`end_type == FORKER_END_TIME`): run for T seconds

The Go port mirrors this:
- `RunParallel(label, threads, totalOps, fn, db)` for counted phases (fill, write benchmarks)
- `RunTimed(label, threads, duration, fn, db)` for timed phases (read, scan, YCSB)

The `-duration` flag (default 60s) controls timed phases, matching the C default.

### 4e. YCSB Operation Mix

The C `ycsbtest.c` uses cumulative thresholds scaled to 65536 (matching a `uint16` roll):
```c
cget = pget * 65536 / 100
cscn = (pget + pscn) * 65536 / 100
cset = (pget + pscn + pset) * 65536 / 100
// remainder = RMW (update/merge)
```

The Go port uses the identical threshold scheme. YCSB workload definitions:

| Workload | Get | Scan | Set | RMW | Distribution |
|----------|-----|------|-----|-----|-------------|
| A | 50% | 0% | 50% | 0% | Zipfian |
| B | 95% | 0% | 5% | 0% | Zipfian |
| C | 100% | 0% | 0% | 0% | Zipfian |
| D | 95% | 0% | 5% | 0% | Latest |
| E | 0% | 95% | 5% | 0% | Zipfian |
| F | 50% | 0% | 0% | 50% | Zipfian |

### 4f. Latency Measurement

The C `dbcdf.c` records per-op latency in microsecond buckets (array of 10000 counters).
The Go port uses per-goroutine `[]atomic.Int64` histograms (one per thread to avoid
contention), merged at the end. Output format matches C:
```
time_us count delta cdf
```

### 4g. Metrics and Write Amplification

The C benchmarks use `smartctl` to measure SSD-level write amplification. The Go port
uses YogaDB's built-in `Metrics` from `db.Close()`, which reports:
- `LogicalBytesWritten` (user payload)
- `TotalBytesWritten` (all physical writes)
- `WriteAmp` = total / logical

### 4h. What Was Not Ported

| C Feature | Reason |
|-----------|--------|
| Multi-system comparison (RocksDB, KVell, LMDB) | Go port tests YogaDB only |
| `numactl` / CPU affinity | Go scheduler handles this; use `GOMAXPROCS` if needed |
| `io_uring` async I/O | Go uses synchronous `pread`/`pwrite` |
| Filesystem reformatting (`common.sh`) | Not applicable to Go benchmarks |
| `smartctl` SSD metrics | Replaced by `Metrics.WriteAmp` |
| `probe` operation | `flexdb_probe` not yet ported to Go |
| `seek_skip` operation | `flexdb_iterator_skip` not yet ported to Go |

---

## 5. Usage

```bash
# Build
go build -o yogabench ./comparison/

# Quick smoke test (in-memory, 2-second phases)
./yogabench all -dataset udb -count 10000 -nodisk -duration 2s

# Write throughput benchmark (on disk)
./yogabench write -dataset udb -dist zipf -threads 4 -omit-wal-sync

# Point read benchmark
./yogabench read -dataset udb -threads 4 -duration 60s

# Range scan benchmark
./yogabench scan -dataset udb -threads 4 -scanlen 50 -duration 60s

# YCSB workload A
./yogabench ycsb -dataset udb -workload A -threads 4 -duration 60s

# Scalability test
./yogabench scale-write -dataset udb -count 100000 -thread-list 1,2,4,8

# Latency CDF
./yogabench latency -dataset udb -op get -threads 4 -duration 60s

# GC overhead
./yogabench gc -dataset udb -dist zipf -threads 4

# Full paper reproduction (warning: hours with default counts)
./yogabench all -dataset udb -threads 4 -duration 60s
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-dataset` | `udb` | Dataset profile: `udb`, `zippydb`, `sys` |
| `-dir` | `/tmp/yogabench` | Database directory (on disk) |
| `-threads` | `NumCPU` | Goroutine count |
| `-count` | from dataset | Override operation/fill count |
| `-nodisk` | false | Run in-memory (no disk I/O) |
| `-dist` | `zipf` | Distribution: `seq`, `zipf`, `czipf` |
| `-duration` | `60s` | Duration for timed benchmark phases |
| `-cache` | `32` | Cache size in MB |
| `-omit-wal-sync` | false | Skip WAL fsync for faster loading |
| `-scanlen` | `0` | Scan length (0 = test all: 10,20,50,100) |
| `-op` | `set` | Latency op: `set` or `get` |
| `-workload` | `ALL` | YCSB workload: `A`–`F` or `ALL` |
| `-thread-list` | `1,2,...,8` | Thread counts for scale benchmarks |
