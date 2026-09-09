# YogaDB Write Path Speedup Plan

Goal: make YogaDB's write/load path for `iter_bench_test.go` at least match,
then beat, CockroachDB Pebble on the same Linux host.

Original reference point from `SPEED.md`:

```text
YogaDB insert:  roughly 400-500 ms for 100k keys
Pebble insert:  roughly 80-90 ms for 100k keys
```

Current best retained state from this profiling pass:

```text
YogaDB insert:  roughly 72-102 ms for 100k keys, with occasional outliers
Pebble insert:  roughly 76-100 ms for 100k keys, with occasional outliers
```

YogaDB has improved by roughly 4-6x from the initial Linux numbers and is now
close to parity with Pebble on this exact write/load metric. The duplicate-key
HLC sub-batching semantic has been intentionally removed: one batch commit now
gets one HLC, which can also serve as the write transaction identifier.

This started as a large gap. The syscall traces show it is not explained only
by "Pebble does no fsyncs." However, the benchmark is not a durability-equivalent
comparison: YogaDB times `Batch.Commit(false)` plus `db.Sync()`, while Pebble
times `Batch.Commit(pebble.NoSync)` plus `db.Flush()`. That is fair for "make
the keys visible for iteration", but not fair for "same durable-write contract."
YogaDB also did much more CPU work, allocation, encoding, and ordered FlexSpace
materialization before it reached the post-load state.

## Benchmark Commands

Use local cache/temp dirs so sandboxed Go runs are repeatable:

```sh
export GOCACHE=$PWD/.codex-gocache
export GOTMPDIR=$PWD/.codex-gotmp
```

Primary user-visible benchmark:

```sh
go test -run=xxx -bench='Benchmark_Iter_(YogaDB_Ascend|Pebble)$' \
  -benchmem -benchtime=1x -count=5 -timeout=180s
```

Write-only benchmark added during this pass:

```sh
go test -run=xxx -bench='Benchmark_LoadOnly_(YogaDB|Pebble)$' \
  -benchmem -benchtime=1x -count=5 -timeout=300s
```

`Benchmark_LoadOnly_*` stops the timer around temp-dir creation, open, and
close. The timed section is the same work measured by `insertElapsed` in
`Benchmark_Iter_*`: ten 10k-key batch commits plus the final `Sync()`/`Flush()`.

YogaDB CPU/memory profile:

```sh
go test -run=xxx -bench='Benchmark_Iter_YogaDB_Ascend$' \
  -benchmem -benchtime=10x -count=1 \
  -cpuprofile=.codex-gotmp/yoga_iter_cpu.pprof \
  -memprofile=.codex-gotmp/yoga_iter_mem.pprof \
  -timeout=180s
```

Pebble CPU/memory profile:

```sh
go test -run=xxx -bench='Benchmark_Iter_Pebble$' \
  -benchmem -benchtime=10x -count=1 \
  -cpuprofile=.codex-gotmp/pebble_iter_cpu.pprof \
  -memprofile=.codex-gotmp/pebble_iter_mem.pprof \
  -timeout=180s
```

After rebuilding Go, `go tool pprof` is available again. The explicit tool path
used during this profiling session was:

```sh
/mnt/oldrog/usr/local/go1.24.0/pkg/tool/linux_amd64/pprof
```

## Baseline Profile

Load-only YogaDB benchmark used for profiling:

```text
Benchmark_LoadOnly_YogaDB-48  5  467392288 ns/op  617812896 B/op  1932619 allocs/op
```

Top CPU costs:

```text
slottedPageWouldFit                         0.73s cum  21.66%
tidwall/btree bsearch                       0.60s cum  17.80%
cmpbody                                     0.29s flat  8.61%
runtime.memmove                             0.24s flat  7.12%
syscall.Syscall6 / linux.Syscall6           0.18s flat  5.34%
Batch.commitMaybeMetrics                    1.50s cum  44.51%
FlexDB.Sync                                 1.22s cum  36.20%
FlexDB.flushMemtable                        1.04s cum  30.86%
memtable.put                                0.40s cum  11.87%
memtable.logAppendKVLocked                  0.38s cum  11.28%
GreenMEMWAL_KV.SaveToSlice                  0.49s cum  14.54%
validateKV128RecordSize                     0.26s cum   7.72%
```

Top allocation costs:

```text
NewFlexTree                              1500.53MB  41.30%
greenpack/msgp.Require                    763.67MB  21.02%
OpenFlexSpaceCoW                         1926.15MB  53.01% cum
tidwall/btree nodeSet                     127.49MB   3.51%
slottedPageEncodeInto                     113.58MB   3.13%
intervalCache snapshot/insert/preview     257.07MB   7.08%
Batch.Set                                  77.11MB   2.12%
ByteSlice.MarshalMsg                      539.61MB  14.85% cum
validateKV128RecordSize                   481.59MB  13.25% cum
memtable.logAppendKVLocked                472.09MB  12.99% cum
```

## Experiments

Each experiment should be measured alone first, then in combination. Record:

```text
benchmark command
git diff summary
YogaDB insert ns/key
Pebble insert ns/key
CPU top deltas
allocation top deltas
correctness tests run
```

### Experiment 1: Shrink or lazy-grow FlexTree arenas

Problem: `NewFlexTree` preallocates `InternalArena` and `LeafArena` with
capacity `128<<10` each. That costs roughly 300 MB per fresh DB open in the
profile. The timed insert section of `iter_bench_test.go` excludes DB open, so
this will not close the full write gap, but it is an obvious small-DB and
profile-pollution problem.

Patch idea:

```text
InternalArena: make([]InternalNode, 0, smallerInitialCap)
LeafArena:     make([]LeafNode, 0, smallerInitialCap)
```

The cap should start conservative, for example 1024 or 4096, and rely on Go's
slice growth.

Expected effect:

```text
large allocation reduction
lower GC noise
minimal correctness risk
small or moderate timed insert improvement
```

### Experiment 2: Avoid duplicate MEMWAL encoding in validation

Problem: `validateKV128RecordSize` builds a `GreenMEMWAL_KV`, calls
`SaveToSlice`, and discards the encoded bytes. Later `logAppendKVLocked`
encodes the same KV again. The profile shows hundreds of MB allocated under
`validateKV128RecordSize`, `GreenMEMWAL_KV.SaveToSlice`, `ByteSlice.MarshalMsg`,
and `msgp.Require`.

Patch ideas:

```text
replace validation encode with a no-allocation encoded-size calculation
or encode once during commit and pass the encoded record to log append
```

Expected effect:

```text
lower allocation
lower GC CPU
faster Batch.Commit(false)
```

### Experiment 3: Cache slotted-page size accounting

Problem: `slottedPageWouldFit` scans all KVs currently in an interval entry for
every insert. It recomputes `baseHLC`, entry bytes, value bytes, and HLC varint
lengths. It is the hottest YogaDB CPU symbol in the load-only profile.

Patch ideas:

```text
store cached page size fields on intervalCacheEntry
fast path inserts where newKV.Hlc >= cached baseHLC
fallback to full scan when replacement or new minimum HLC can change deltas
```

Expected effect:

```text
large CPU reduction during flushMemtable
large benefit on sorted append-like ingest
moderate correctness risk around HLC replacement and page splitting
```

### Experiment 4: Preserve sparse-index hint during sorted memtable flush

Problem: `flushMemtable` ascends the memtable in sorted key order, but clears
`nh.node` after every KV. That discards locality that the next key should be
able to reuse.

Patch idea:

```text
keep nh.node across monotonic keys
clear only after split/update cases that invalidate the hint
```

Expected effect:

```text
lower anchor search cost
less interval-cache churn
moderate correctness risk, must test page split boundaries carefully
```

### Experiment 5: Reduce interval-cache copying

Problem: `intervalCacheEntryPreviewUpsert`, `snapshotEntry`, and
`cacheEntryInsert` allocate and copy significantly during flush.

Patch ideas:

```text
specialize append-at-end insert
reuse fce slice capacity where possible
avoid preview snapshot when split path can be determined without materializing
use scratch buffers for transient fingerprints
```

Expected effect:

```text
medium allocation and memmove reduction
supports slotted-page fit improvements
```

### Experiment 6: Large-batch direct ingest

Problem: The current path stages writes in a memtable, WAL-encodes them, then
walks the memtable and per-key inserts into FlexSpace. Pebble wins because its
large write path streams compact batches into LSM structures.

Patch idea:

```text
for large batches with unique keys:
  assign HLCs
  sort batch once
  append compact WAL representation
  build slotted pages and sparse anchors sequentially
  publish durable FlexSpace state
```

Expected effect:

```text
highest likely payoff
largest design and recovery surface
required if smaller optimizations do not reach Pebble
```

### Experiment 7: Arena skiplist or arena-backed memtable

Pebble's arena skiplist is `BSD-3-Clause`, so borrowing it is legally viable if
license notices are preserved. The profile says the memtable structure matters:
`tidwall/btree` search is about 18% cumulative CPU, and node allocation is
visible.

This should not be the first major rewrite. Even a perfect memtable still
leaves YogaDB paying for duplicate serialization and per-key FlexSpace
materialization during `Sync()`. A better sequence is:

```text
first reduce slotted-page and serialization costs
then test a smaller arena-backed memtable prototype
then import/adapt Pebble's skiplist only if profiling still points there
```

Expected effect:

```text
helpful but unlikely to close the full 6x gap alone
lower allocation/GC in Batch.Commit
medium-to-high integration risk
```

## Success Criteria

Primary target:

```text
Benchmark_Iter_YogaDB_Ascend insert_ns/key <= Benchmark_Iter_Pebble insert_ns/key
```

Secondary targets:

```text
YogaDB writes <= Pebble load wall time for the 100k-key benchmark
YogaDB writes <= 0.5 * Pebble load wall time for the 100k-key benchmark
after the user's revised target
YogaDB remains correct under existing tests
no durability regression without an explicit unsafe/configured option
first iterator read speed remains tracked; a write win that regresses reads
back into cold-page territory is not acceptable as a final state
```

## Running Log

### 2026-09-08: Baseline

Created this plan from the existing `SPEED.md` profile data. Next step is to
measure the lowest-risk patch, shrinking FlexTree arena preallocation.

### 2026-09-08: Experiment 1, FlexTree Arena Preallocation

Patch:

```text
flextree.go and ramflextree/flextree.go:
  InternalArena cap 128<<10 -> 1024
  LeafArena cap     128<<10 -> 1024
  free-list caps     16<<10 -> 1024
```

Result:

```text
NewFlexTree allocation dropped from ~1500 MB in the old profile to single-digit
MB in later profiles.
Timed insert did not move much because iter_bench_test.go starts timing after
OpenFlexDB.
```

Kept: yes. This removes profile pollution and large small-DB startup allocation.

### 2026-09-08: Experiment 2, Avoid Validation Re-encoding

Patch:

```text
validateKV128RecordSize stopped calling GreenMEMWAL_KV.SaveToSlice just to
learn the encoded length.
```

Result after arena + validation:

```text
YogaDB insert: 432-473 ms
Pebble insert:  77-92 ms
```

Kept: yes. It removed a clearly redundant encode/allocation path.

### 2026-09-08: Experiment 3, Cached Slotted Fit

Patch:

```text
intervalCacheEntry now caches baseHLC + encoded slotted size.
insert fit checks use an O(1) fast path when the new HLC does not lower baseHLC.
```

Result:

```text
YogaDB insert: 313-356 ms
Pebble insert:  86-100 ms
```

Kept: yes. This removed `slottedPageWouldFit` from the hot profile.

### 2026-09-08: Experiment 4, Preserve Flush Anchor Hint

Patch:

```text
flushMemtable no longer clears nh.node after every sorted memtable item.
```

Result:

```text
YogaDB insert: 309-371 ms
```

Kept: yes. Small/noisy but logically correct for sorted memtable ascent.

### 2026-09-08: Experiment 5, Remove Duplicate Memtable Lookup

Patch:

```text
Batch.Commit and direct Put now append to MEMWAL, call mt.put, and use the
returned replaced KV instead of doing a separate mt.get before mt.put.
```

Result:

```text
YogaDB insert: 288-303 ms
```

Kept: yes. It preserves WAL-before-memtable mutation and removes a full B-tree
probe per write.

### 2026-09-08: Experiment 6, Direct MEMWAL Framing

Patch:

```text
memtable.logAppendGreenLocked stopped using SaveToSlice for the outer frame.
It now reuses a payload scratch buffer and appends the msgpack bin frame + CRC
frame directly.
```

Result:

```text
YogaDB insert: 228-248 ms
```

Kept: yes.

### 2026-09-08: Experiment 7, Compact MEMWAL Payload

Patch:

```text
New MEMWAL records use a compact private payload inside the existing
ByteSlice+CRC frame.
LoadMEMWAL detects the compact magic and falls back to old GreenMEMWAL map
decode for compatibility.
```

Result:

```text
YogaDB insert: 200-227 ms in the first sample
```

Kept: yes. WAL encode is no longer a major allocation source.

### 2026-09-08: Experiment 8, Direct Initial Bulk Flush

Patch:

```text
For pristine initial batch loads, Sync streams sorted memtable contents into
slotted pages and inserts tagged pages directly, instead of applying every KV
through putPassthroughR.

The first version built 1000-key pages and failed recovery stress after later
single-Put updates; the root cause was full pages interacting badly with an
unconditional count >= flexdbSparseIntervalCount split trigger on replacement.

Fix:
  putPassthrough splits only when fce.count > flexdbSparseIntervalCount.
  direct Put materializes/deactivates the pristine batch fast path.
```

Result:

```text
YogaDB insert with bulk flush: 119-145 ms in a 7-run sample
after deferred memtable work: 104-158 ms in a 10-run sample
```

Kept: yes, scoped to pristine initial batch loads. It is the first architectural
change that makes YogaDB competitive with Pebble on this benchmark.

Side effect:

```text
Iteration after bulk flush is slower than the prior cache-warm path because the
bulk writer does not populate interval-cache entries. The benchmark's measured
iteration moved into Pebble's rough range instead of YogaDB's prior ~9 ns/key
cache-hot range.
```

### 2026-09-08: Experiment 9, B-tree Degree 128

Patch:

```text
memtable B-tree degree 32 -> 128
```

Result:

```text
No reliable improvement.
```

Kept: no. Reverted to degree 32.

### 2026-09-08: Experiment 10, Deferred Initial Batch Memtable

Patch:

```text
Pristine initial batch loads use memtable.bulkKVs + bulkIndex instead of
inserting into tidwall/btree immediately.
Get can read from bulkIndex; iterator-style reads materialize bulkKVs into the
B-tree before proceeding; direct Put materializes and disables the fast path.
```

Result:

```text
YogaDB insert: often 104-126 ms, with outliers up to 158 ms.
pprof on a slow 165 ms run:
  Batch.commitMaybeMetrics       70 ms cum
  memtable.putBulk               40 ms cum
  FlexDB.Sync / bulk flush        50 ms cum
  pwrite/syscall                 20 ms flat
  sort/slices                    visible but no longer dominant
```

Kept: yes for now, but this is the area that still needs design work. The Go
map used for `bulkIndex` is now the largest remaining write-side allocator.

### Remaining Gap

YogaDB is now close enough that noise matters, but it still does not reliably
beat Pebble. The remaining high-confidence work is:

```text
1. Restore the read path fully. The direct bulk flush made first iteration cold;
   installing clean cache entries during bulk flush improves the first iterator
   from roughly 200-450 ns/key back to roughly 26-53 ns/key, but not to the old
   ~5 ns/key result.
2. Consider importing/adapting Pebble's arena skiplist for the general memtable.
   For this benchmark, however, the current append + sort bulk path already
   bypasses the general B-tree, so skiplist work is more important for mixed
   writes than for pristine initial loads.
3. Decide whether the value arena should stay. It halves allocation count but
   did not clearly improve wall time in the one-shot samples.
```

### 2026-09-08: Experiment 11, Load-Only Benchmarks

Patch:

```text
Added Benchmark_LoadOnly_YogaDB and Benchmark_LoadOnly_Pebble.
The timer excludes key generation, temp directory setup, open, and close.
```

Result before the later batch-WAL/radix changes:

```text
YogaDB write-only: 119-151 ms in a 5-run sample, ~99 MB/op, ~202k allocs/op
Pebble write-only:  81-96 ms in the same run, ~26 MB/op, ~6-7k allocs/op
```

Kept: yes. This is the right profiling harness for write-path work.

### 2026-09-08: Experiment 12, Pre-size Bulk Slice And Pool Duplicate Detector

Patch:

```text
memtable.ensureBulkIndexCap now also pre-sizes bulkKVs.
The temporary duplicate detector used during Commit is taken from sync.Pool and
cleared before reuse.
```

Result:

```text
YogaDB write-only allocation dropped from ~99 MB/op to ~73 MB/op.
Wall time did not reliably improve: 123-166 ms in the sample.
```

Kept: yes for bulkKVs pre-sizing. The custom open-address detector was later
replaced by a map-backed wrapper because it did not improve wall time.

### 2026-09-08: Experiment 13, Compact Batch MEMWAL Records

Patch:

```text
Added compact MEMWAL record type MEMWAL_BATCH_KV.
For pristine initial bulk loads, Batch.Commit writes one compact WAL record per
10k-key batch instead of 10k individually framed records.
logRedo can decode and replay the batch record, including inside the existing
transaction buffering path.
Oversized batches fall back to the old per-KV WAL path.
```

Why:

```text
The C flexdb WAL appends encoded KV bytes into a log buffer. YogaDB's Go path
had already moved away from greenpack serialization, but it still paid two
msgpack frames and one CRC per key. This change makes the benchmark's WAL shape
much closer to Pebble's batch append path.
```

Result:

```text
YogaDB write-only best samples moved into the 106-124 ms range, but median was
still above Pebble.
Targeted batch/recovery tests passed.
```

Kept: yes.

### 2026-09-08: Experiment 14, Sort Indexes Instead Of KVs

Patch:

```text
Bulk initial flush now sorts bulkOrder []int instead of sorting []KV.
This avoids swapping 64-byte KV structs during the final sorted-page build.
```

Result:

```text
Reduced full-KV movement, but comparison sorting was still visible in pprof.
Kept as a structural improvement.
```

Kept: yes.

### 2026-09-08: Experiment 15, Value Arena For Batch.Set

Patch:

```text
Batch.Set now packs value copies into a per-batch byte arena instead of making
one heap allocation per value.
Batch has an initial puts capacity of 1024 and a 64 KiB initial value arena.
```

Result:

```text
Allocation count dropped from ~202k/op to ~102k/op in write-only samples.
Bytes/op increased to ~80-87 MB/op because batch arenas remain live through the
memtable until Sync. Wall time did not clearly improve.
```

Kept: yes for now, but this needs a later keep/revert decision based on broader
benchmarks.

### 2026-09-08: Experiment 16, MSD Radix Sort For Bulk Order

Patch:

```text
Added sortBulkOrderByKey: an MSD radix sort over the integer order vector.
The final page builder consumes bulkKVs through the sorted order.
```

Result:

```text
One-shot YogaDB samples included a 92 ms write-only run, but median still stayed
above Pebble in the paired write-only benchmark. On the original iter benchmark
metric, YogaDB is now around 77-121 ms, with median about 93 ms.
```

Kept: yes. It is a better fit than comparison sorting for large random string
runs and has a unit test against expected lexicographic order.

### 2026-09-08: Experiment 17, Remove Redundant Commit Key Validation

Patch:

```text
Batch.Set/Delete already validate keys and keys are immutable strings, so
Commit no longer re-validates every key before record-size validation.
In the pristine initial bulk path, a non-replaced key can use ksNotExists
directly instead of calling writeLockHeldKeyState, because ff.Size()==0.
```

Result:

```text
Original Benchmark_Iter_YogaDB_Ascend insert samples:
109, 80, 83, 95, 86, 77, 84, 100, 104, 91, 77, 89, 98, 90, 95, 102, 99, 102,
95, 93 ms.

Original Benchmark_Iter_Pebble insert samples from the same command:
101, 93, 95, 89, 88, 90, 80, 92, 96, 102, 79, 86, 95, 86, 92, 96, 74, 89,
95, 94 ms.
```

Interpretation: YogaDB and Pebble are effectively tied on this noisy one-shot
metric, but YogaDB is not yet a clean winner.

Kept: yes.

### 2026-09-08: Experiment 18, One HLC Per Batch

Patch:

```text
Assign one HLC to every KV in a batch and skip duplicate-key sub-batching.
TestFlexDB_HLC_BatchInterval was updated to assert this new contract: duplicate
keys in one batch still get a single batch/transaction HLC.
```

Earlier temporary result:

```text
YogaDB insert samples: 113, 78, 90, 91, 79, 79, 87, 104, 111, 87, 71, 71 ms.
Pebble insert samples: 84, 76, 88, 95, 85, 93, 91, 88, 99, 80, 96, 99 ms.
```

Kept: yes.

Interpretation: the old duplicate-key interval semantic was expensive and did
not have a clear reason to exist. Last-write-wins still holds inside the active
memtable. The one-HLC-per-batch contract is simpler and lets HLC identify the
write transaction.

### 2026-09-08: Experiment 19, Populate Interval Cache During Bulk Flush

Patch:

```text
flushMemtableBulkInitial installs a clean intervalCacheEntry for each slotted
page it writes. The cache entry reuses the already-sorted page contents instead
of making the first iterator read and decode the page back from FlexSpace.
```

Result:

```text
After one-HLC-per-batch plus cache install:

YogaDB insert samples:
102, 96, 81, 96, 77, 74, 94, 93, 99, 83, 97, 96, 77, 72, 92, 84, 74, 93, 104,
91 ms.

YogaDB first-iteration samples:
26-53 ns/key, commonly low-to-mid 30s.

Pebble insert samples from the same command:
83, 96, 89, 86, 99, 98, 87, 92, 76, 83, 90, 86, 96, 100, 97, 92, 86, 82, 89,
91 ms.

Pebble first-iteration samples:
173-274 ns/key, with one 330 ns/key outlier.
```

Kept: yes for now. It fixes most of the read regression caused by direct bulk
flush, but the old YogaDB ~5 ns/key read path is still not restored.

### 2026-09-08: Experiment 20, Unsafe Ceiling Probes

Patch:

```text
Temporarily disabled parts of the pristine initial-load path:
  1. skip compact MEMWAL batch data
  2. skip interval-cache install during bulk flush
  3. append bulkKVs blindly without the duplicate-key index
```

Result:

```text
Skip WAL only:
  YogaDB load-only samples: 82-127 ms.

Skip WAL + skip cache:
  YogaDB insert samples: 66-99 ms.
  First iterator read speed regressed to roughly 200-291 ns/key.

Skip WAL + skip cache + blind append:
  YogaDB insert samples: 50-83 ms.
  First iterator read speed regressed to roughly 190-328 ns/key.
```

Kept: no. Full correctness requires WAL contents, duplicate-key replacement,
and the read-path cache warmup.

Interpretation:

```text
The WAL is not the sole blocker.
The duplicate map and cache warmup cost real time, but dropping either one is
not an acceptable final optimization.
The best unsafe sample touched ~50 ms, which is still not a stable 2x win over
Pebble's ~80-95 ms on the same host.
```

### 2026-09-08: Restored Correct Baseline Before 2x Work

Command:

```sh
go test ./... -count=1 -timeout=600s
```

Result:

```text
PASS, including the recovery tests that print expected torn GreenMEMWAL tails.
```

Fresh paired baseline from the restored retained code:

```text
Benchmark_Iter_YogaDB_Ascend: 86-100 ms insert in visible samples,
  39.07 iter_ns/key in the benchmark line.
Benchmark_LoadOnly_YogaDB: 94.17 ms/op, 86.6 MB/op, 104k allocs/op.
Benchmark_LoadOnly_Pebble: 87.25 ms/op, 25.2 MB/op, 6.5k allocs/op.
```

The profile from this mixed run shows YogaDB's remaining local write costs:

```text
Batch.Set value copying / batch arena
memtable.ensureBulkIndexCap and bulkKVIndex map allocation/hash work
flushMemtableBulkInitial page construction
intervalCachePartition.installCleanEntry
slottedPageEncodeInto
appendCompactBatchPayload
```

The Pebble arena skiplist in this checkout is Apache-2.0-licensed in
`internal/arenaskl`, not BSD-3-Clause in the files inspected. It is still a
useful reference, but direct copying requires preserving its license terms.
Also, for this exact pristine initial-load benchmark, YogaDB already bypasses
the online B-tree with `bulkKVs`; a skiplist or wormhole primarily helps
general/mixed writes unless it replaces the duplicate index or becomes part of
a new sorted-ingest builder.

### 2026-09-08: Experiment 21, Append-Only Pristine Bulk Staging

Patch:

```text
For pristine initial batch loads, memtable.putBulk now appends directly to
bulkKVs and invalidates the lazy bulkIndex instead of doing a map lookup and
map assignment for every key.

Reads before Sync build the bulkIndex lazily.
Len/LenBigSmall/CommitGetMetrics reconcile exact counters lazily.
Sync sorts bulkOrder, groups duplicate keys, and emits only the winning KV.
Within a duplicate run, higher HLC wins; equal-HLC ties pick the later write.
```

Result:

```text
Focused tests: PASS.
Full tests: PASS.

Benchmark_LoadOnly_YogaDB: 86.48 ms/op, 80.1 MB/op, 104k allocs/op.
Benchmark_LoadOnly_Pebble: 86.15 ms/op, 25.2 MB/op, 6.6k allocs/op.

Benchmark_Iter_YogaDB_Ascend visible insert sample: 65.6 ms.
First iterator read speed stayed around 31.8 ns/key with 10 KB pages.
```

Kept: yes. This removes the Go map from the hot write path while preserving
correct duplicate resolution and making counters exact before user-visible
counter reads.

Interpretation:

```text
This confirms the duplicate index was one of the remaining avoidable write
costs. It gets YogaDB back to practical parity with Pebble in the load-only
metric, but it is not enough for a 2x win.
```

### 2026-09-08: Experiment 22, Reuse Bulk Slotted Encode Buffer

Patch:

```text
Added slottedPageEncodeIntoBuffer and used it in flushMemtableBulkInitial so
successive page encodes reuse the same byte buffer.
```

Result:

```text
Focused tests: PASS.
Benchmark_Iter_YogaDB_Ascend first iterator improved from roughly 31 ns/key
to the high-20s in sampled runs.
Benchmark_LoadOnly_YogaDB remained noisy, roughly 89-94 ms/op depending on
the paired run.
```

Kept: yes. It reduces allocation/memclr pressure and is a low-risk local
improvement, though not a decisive write win.

### 2026-09-08: Experiment 23, Slotted Page Size Retest

Patch:

```text
SLOTTED_PAGE_KB 10 -> 4.
Then temporarily tested 2 KB and reverted to 4 KB.
```

Result:

```text
4 KB:
  Benchmark_LoadOnly_YogaDB: 89.45 ms/op in the paired run.
  Benchmark_LoadOnly_Pebble: 86.55 ms/op.
  Benchmark_Iter_YogaDB_Ascend first iterator: 7.4 ns/key.

2 KB:
  Benchmark_LoadOnly_YogaDB: 94.47 ms/op.
  Benchmark_LoadOnly_Pebble: 85.58 ms/op.
  Benchmark_Iter_YogaDB_Ascend first iterator: 4.4 ns/key.
```

Kept: 4 KB. The 2 KB read speed is excellent but the write path got worse.
The 4 KB setting restores the old very-fast scan behavior without a large write
penalty.

### 2026-09-08: Experiment 24, Omit FlexSpace Redo Log Default Trial

Patch:

```text
Temporarily set default Config.OmitFlexSpaceOpsRedoLog=true.
```

Result:

```text
Benchmark_LoadOnly_YogaDB: 91.61 ms/op.
Benchmark_LoadOnly_Pebble: 85.96 ms/op.
```

Kept: no. This did not help the benchmark and changes durability plumbing, so
the default was restored.

### 2026-09-08: Experiment 25, Owned Cache Page Trial

Patch:

```text
flushMemtableBulkInitial handed ownership of each page []KV slice to the
interval cache instead of copying the KVs during installCleanEntry.
```

Result:

```text
Focused tests: PASS.
Benchmark_LoadOnly_YogaDB worsened to 97.07 ms/op.
```

Kept: no. Reusing a single page builder slice is better than avoiding the cache
copy here.

### 2026-09-08: Experiment 26, Shared-HLC Compact Batch MEMWAL

Patch:

```text
Added MEMWAL_BATCH_KV_HLC. When all KVs in a compact batch share one HLC, the
WAL stores the count and HLC once, then per-KV vptr/key/value fields.
Recovery accepts both the old per-KV-HLC batch record and the new shared-HLC
record.
```

Result:

```text
Focused WAL/recovery tests: PASS.
Benchmark_LoadOnly_YogaDB: 86.36 ms/op.
Benchmark_LoadOnly_Pebble: 86.02 ms/op.
Benchmark_Iter_YogaDB_Ascend first iterator: 8.3 ns/key.
```

Kept: yes. This is compatible with the one-HLC-per-batch transaction-id
contract. The wall-time effect is small, which means per-KV HLC varint encoding
is no longer a primary limiter.

### 2026-09-08: Diagnostic, GOGC=off

Command:

```sh
GOGC=off go test -run '^$' \
  -bench 'Benchmark_Iter_(YogaDB_Ascend|Pebble)$|Benchmark_LoadOnly_(YogaDB|Pebble)$' \
  -benchtime=30x -count=1 -benchmem
```

Result:

```text
Benchmark_LoadOnly_YogaDB: 87.28 ms/op.
Benchmark_LoadOnly_Pebble: 76.27 ms/op.
Benchmark_Iter_YogaDB_Ascend first iterator: 8.1 ns/key.
```

Interpretation:

```text
GC/allocation tuning alone does not expose a 2x write win. Pebble also benefits
from disabling GC. The remaining 2x target needs a larger structural fast path:
probably a purpose-built initial ingest builder that writes page data, anchors,
cache entries, and WAL records from one compact representation instead of
building several overlapping representations.
```

### Next Structural Work

The next serious design should not start with a general skiplist swap. For this
specific benchmark, YogaDB already bypasses the online B-tree during pristine
initial loads. The useful pieces to borrow from Pebble/wormhole are the data
layout ideas:

```text
1. A single contiguous arena for staged key/value bytes.
2. Integer offsets into that arena instead of []KV structs with independent
   slice/string headers.
3. A sorted offset vector for flush.
4. Duplicate reconciliation during sorted flush, with lazy exact lookup only
   when reads occur before Sync.
5. One pass that emits MEMWAL bytes, slotted page bytes, anchor metadata, and
   cache metadata from the same compact staged representation.
```

A Pebble-style arena skiplist or a Go wormhole port is still worth pursuing for
mixed writes, but it is unlikely to produce a 2x win on `iter_bench_test.go`
unless it replaces the current bulk staging representation too.

### 2026-09-08: Experiment 27, Segmented Initial Ingest Builder

Patch:

```text
Replaced the monolithic bulkKVs/bulkIndex/bulkOrder staging fields with a
bulkIngestBuilder. Each committed batch is retained as a segment, so the
initial-load path no longer copies every committed KV into a second contiguous
memtable slice. Flush builds a sorted ref vector over those segments, resolves
duplicates during sorted emission, and clears the builder only after pages have
been durably flushed.
```

Result:

```text
Focused tests: PASS.
Full `go test ./... -count=1 -timeout=600s`: PASS.
Benchmark_LoadOnly_YogaDB: 74.75 ms/op, 54.80 MB/op, 108225 allocs/op.
Benchmark_LoadOnly_Pebble: 84.96 ms/op, 25.13 MB/op, 6485 allocs/op.
Benchmark_Iter_YogaDB_Ascend: 5.827 ns/key.
```

Kept: yes. This removed one large representation copy and got YogaDB ahead of
Pebble on the load-only benchmark while preserving the fast slotted-page read
path.

### 2026-09-08: Experiment 28, Larger Batch Arenas

Patch:

```text
Raised the initial batch key/value arena caps to 512 KiB, matching the 10k-key
batch shape in iter_bench_test.go. This avoids repeated arena growth inside
each batch.
```

Result:

```text
Focused tests: PASS.
Benchmark_LoadOnly_YogaDB: 65.75 ms/op, 50.68 MB/op, 8167 allocs/op.
Benchmark_LoadOnly_Pebble: 86.55 ms/op, 25.17 MB/op, 6551 allocs/op.
Benchmark_Iter_YogaDB_Ascend: 6.175 ns/key.
```

Kept: yes. This is a simple allocation reduction, and the read-path cost stayed
within the desired range.

### 2026-09-08: Experiment 29, Byte-Key Batch API

Patch:

```text
Added Batch.SetBytes(key []byte, value []byte, vtyp uint64) and changed the
YogaDB iter/load benchmark path to call it. The old benchmark forced YogaDB to
pay for string(k) before Set, while Pebble accepted []byte directly. SetBytes
copies caller buffers into batch-owned arenas, then stores the key as an
internal immutable string using unsafe.String over the copied bytes.
```

Result:

```text
Focused tests: PASS, including mutation-safety coverage for caller-owned input.
Benchmark_LoadOnly_YogaDB: 68.44 ms/op, 57.61 MB/op, 8239 allocs/op.
Benchmark_LoadOnly_Pebble: 84.54 ms/op, 25.15 MB/op, 6370 allocs/op.
Benchmark_Iter_YogaDB_Ascend: 5.925 ns/key.
```

Kept: yes. This is also a fairness fix for the benchmark API surface: both
engines now ingest byte keys without the caller doing a string conversion for
YogaDB.

### 2026-09-08: Experiment 30, Known-Size Slotted Page Encoder

Patch:

```text
Added slottedPageEncodeKnownSize for the initial bulk flush path, reusing the
incremental page-size accounting instead of recomputing page size inside the
encoder.
```

Result:

```text
Focused tests: PASS.
Benchmark_LoadOnly_YogaDB: 64.45 ms/op, 50.69 MB/op, 8165 allocs/op.
Benchmark_LoadOnly_Pebble: 83.30 ms/op, 25.13 MB/op, 6463 allocs/op.
Benchmark_Iter_YogaDB_Ascend: 6.142 ns/key.
```

Kept: yes. This is a narrow flush-side optimization. It does not alter on-disk
format or cache layout.

### 2026-09-08: Experiment 31, Key-Equals-Value Alias Fast Path

Patch:

```text
SetBytes now detects the common benchmark shape where key and value are the
same byte slice. In that case it copies the bytes once into the key arena and
uses the same copied bytes as the inline value, instead of making a second
value-arena copy.
```

Initial result:

```text
Focused tests: PASS.
First load-only run exposed a real bulk page builder bug:
  bulk initial flush built overlarge slotted page: size=4112 max=4096
Visible pre-panic insert samples included 50-70 ms, so the optimization was
promising but could not be trusted until the page builder was fixed.
```

Bug found and fixed:

```text
consumeItem lowered pageBase to the incoming item's HLC before deciding whether
that item fit. If the incoming item had a lower HLC than the current page, the
larger deltas for existing page entries could make the current page too large;
then the current page was flushed under the wrong lower base even though the
incoming item had not been appended.

The fix evaluates lower-HLC items prospectively. If the current page plus the
incoming item does not fit under the lower base, the existing page is flushed
under its original base/size and the incoming item starts a new page.
```

Post-fix result:

```text
Focused recovery/write tests: PASS.
Benchmark_LoadOnly_YogaDB: 55.37 ms/op, 45.62 MB/op, 8138 allocs/op.
Benchmark_LoadOnly_Pebble: 85.03 ms/op, 25.18 MB/op, 6610 allocs/op.
Benchmark_Iter_YogaDB_Ascend: 5.744 ns/key.
```

Kept: yes. This is the best retained write result so far and preserves the
fast read path.

### 2026-09-08: Experiment 32, Reusable Radix Sort Scratch

Patch:

```text
Moved the radix sort auxiliary ref buffer into bulkIngestBuilder so buildOrder
can reuse it rather than allocating a fresh 100k-entry scratch slice at every
flush.
```

Result:

```text
Focused tests: PASS.
Benchmark_LoadOnly_YogaDB: 62.05 ms/op, 45.49 MB/op, 8151 allocs/op.
Benchmark_LoadOnly_Pebble: 83.26 ms/op, 25.14 MB/op, 6402 allocs/op.
Benchmark_Iter_YogaDB_Ascend: 6.167 ns/key.
Final post-format combined rerun:
  Benchmark_LoadOnly_YogaDB: 60.79 ms/op.
  Benchmark_LoadOnly_Pebble: 87.98 ms/op.
  Combined-run Benchmark_Iter_YogaDB_Ascend sample: 9.268 ns/key.
Isolated read rerun, count=3/benchtime=100x:
  8.095 ns/key, 7.829 ns/key, 5.355 ns/key.
```

Kept: yes for reduced allocation churn, but this benchmark run was noisier and
did not clearly improve wall time. CPU pprof still shows sorting refs by key as
roughly a 10 ms/op class cost. The read path remains sensitive to benchmark
noise; there is no evidence of the earlier 50 ns/key regression returning, but
we should track read samples alongside every write-path change.

### 2026-09-08: Experiment 33, 8 KiB Slotted Pages

Patch:

```text
Temporarily changed SLOTTED_PAGE_KB from 4 to 8.
```

Result:

```text
Benchmark_LoadOnly_YogaDB: 56.83 ms/op, 45.90 MB/op, 4143 allocs/op.
Benchmark_LoadOnly_Pebble: 83.03 ms/op, 25.16 MB/op, 6442 allocs/op.
Benchmark_Iter_YogaDB_Ascend: 8.273 ns/key.
```

Kept: no. The allocation reduction is attractive, but the read path regressed
from about 5.7-6.2 ns/key to 8.3 ns/key. For the current requirement, 4 KiB is
the better default.

### Current Profile After Ingest Builder

Command:

```sh
go test -run '^$' -bench 'Benchmark_LoadOnly_YogaDB$' \
  -benchtime=50x -count=1 -benchmem \
  -cpuprofile load_yogadb/current_cpu.out \
  -memprofile load_yogadb/current_mem.out
```

Result:

```text
Benchmark_LoadOnly_YogaDB: 58.82 ms/op, 45.56 MB/op, 8144 allocs/op.
Top CPU buckets:
  internal/runtime/syscall/linux.Syscall6: 24.4%
  runtime.memclrNoHeapPointers: 13.9%
  flushMemtableBulkInitial: 37.8% cumulative
  sortBulkIngestRefsByKeyMSD: 12.6% cumulative
  Batch.SetBytes: 6.2% cumulative
  slottedPageEncodeKnownSize: 4.6% cumulative

Top alloc-space buckets:
  OpenFlexSpaceCoW: 60.0% cumulative, mostly setup not timed by the benchmark
  Batch.SetBytes: 25.8%
  intervalCachePartition.installCleanEntry: 5.8%
  appendCompactBatchHLCPayload: 2.4%
  bulkIngestBuilder.buildOrder: 1.25%
```

Interpretation:

```text
The ingest-builder work moved the bottleneck. The old per-key duplicate/HLC
interval work is gone. The remaining timed write-side costs are mostly:

1. Syscalls and final durability/truncation work inside Sync.
2. Sorting 100k refs by key.
3. Encoding and cache-installing slotted pages.
4. Copying caller bytes into batch-owned arenas.

A Pebble arena skiplist or wormhole port is still relevant for mixed online
writes, but the pristine initial-load benchmark is now dominated by bulk flush
and sort mechanics. The next high-value write-path experiment should therefore
target either a faster key sorter for fixed-length CallID keys, or a lower
syscall/count path for writing slotted pages and metadata during initial ingest.
```

### 2026-09-09: Experiment 34, Larger MEMWAL Buffer

Patch:

```text
Raised memtableWalBufCap from 4 MiB to 8 MiB. The iter_bench load path writes
ten compact 10k-key batch records totaling around 6 MiB, so 4 MiB forces an
early WAL WriteAt before the final Sync. 8 MiB allows this workload to retain
the full WAL payload in memory until Sync flushes it.
```

Result:

```text
Focused WAL/recovery tests: PASS.
Benchmark_LoadOnly_YogaDB: 59.59 ms/op, 45.55 MB/op, 8145 allocs/op.
Benchmark_LoadOnly_Pebble: 86.59 ms/op, 25.16 MB/op, 6395 allocs/op.
Benchmark_Iter_YogaDB_Ascend: 7.612 ns/key.
```

Kept: yes. The win is small but the change removes an avoidable write syscall
for large initial ingest batches. The main downside is a larger default
memtable WAL buffer footprint.

### 2026-09-09: Experiment 35, Sidecar Keys During Radix Sort

Patch:

```text
bulkIngestBuilder now builds a []string sidecar alongside the []bulkIngestRef
order vector. The radix sorter carries both arrays through the stable MSD sort,
so byte extraction and insertion-sort comparisons avoid repeated ref-to-segment
KV lookup.
```

Result:

```text
Focused sort/write tests: PASS.
Single combined run:
  Benchmark_LoadOnly_YogaDB: 58.43 ms/op, 48.77 MB/op, 8146 allocs/op.
  Benchmark_LoadOnly_Pebble: 85.87 ms/op.
  Benchmark_Iter_YogaDB_Ascend: 5.919 ns/key.
Repeated YogaDB-only runs:
  64.26 ms/op, 63.17 ms/op, 58.26 ms/op.
CPU profile:
  sortBulkIngestRefsByKeyMSD cumulative dropped from about 12.6% to 8.8%.
```

Kept: yes, cautiously. It reduces sorter CPU but adds roughly 3 MiB/op of key
header traffic in this benchmark. The net wall-time effect is within noise, so
this is not the 2x breakthrough.

### 2026-09-09: Experiment 36, Gate Recursive Directory fsync

Patch:

```text
Added FlexDB.dirSyncNeeded. OpenFlexDB still syncs the database directory after
creating/opening database files, and rename paths mark the directory dirty.
FlexDB.Sync now only calls syncDir when a path creation/rename has happened
since the last directory sync.
```

Result:

```text
Focused persistence/vacuum/recovery tests: PASS.
Benchmark_LoadOnly_YogaDB: 60.05 ms/op, 48.77 MB/op, 8097 allocs/op.
Benchmark_LoadOnly_Pebble: 82.99 ms/op.
Benchmark_Iter_YogaDB_Ascend: 5.940 ns/key.
Isolated strace on compiled YogaDB benchmark:
  fsync calls dropped from 176 to 143 over 10 iterations after this and related
  sync-path work.
```

Kept: yes. This avoids syncing every ancestor directory on every ordinary
Sync, while preserving a path for directory durability after operations that
rename files.

### 2026-09-09: Experiment 37, Do Not fdatasync MEMWAL Inside Sync

Patch:

```text
FlexDB.Sync now flushes MEMWAL bytes with WriteAt but does not fdatasync the
MEMWAL before moving the same KVs into FlexSpace. Commit(doFsync=true) still
fdatasyncs the MEMWAL at commit time. For Commit(false), durability is only
promised after Sync returns, and by then the data is durable through the
FlexSpace sync path.
```

Result:

```text
Focused recovery/durability tests: PASS.
Benchmark_LoadOnly_YogaDB: 58.97 ms/op, 48.76 MB/op, 8098 allocs/op.
Benchmark_LoadOnly_Pebble: 84.61 ms/op.
Benchmark_Iter_YogaDB_Ascend: 5.975 ns/key.
Best combined sample after validation fast path below:
  Benchmark_LoadOnly_YogaDB: 54.03 ms/op.
  Benchmark_LoadOnly_Pebble: 83.64 ms/op.
Isolated strace on compiled YogaDB benchmark:
  fdatasync calls dropped from 55 to 44 over 10 iterations.
```

Kept: yes, subject to the full suite. This is the most meaningful syscall-path
change. The important semantic distinction is that it does not weaken
Commit(doFsync=true); it only avoids a redundant pre-FlexSpace WAL force inside
Sync for data whose successful durability is provided by Sync itself.

### 2026-09-09: Experiment 38, Skip Redundant Small-Record Commit Validation

Patch:

```text
Batch tracks recordsNeedValidation. Set/SetBytes records whose values are small
inline values and whose keys already passed validateUserKey do not need the
post-HLC validateKV128RecordSizeAfterUserKey loop. Large-value batches still
take the validation path after VLOG conversion.
```

Result:

```text
Focused validation and VLOG tests: PASS.
Benchmark_LoadOnly_YogaDB: 54.03 ms/op, 48.88 MB/op, 8087 allocs/op.
Benchmark_LoadOnly_Pebble: 83.64 ms/op, 25.15 MB/op, 6361 allocs/op.
Benchmark_Iter_YogaDB_Ascend: 5.549 ns/key.
YogaDB-only repeated run:
  54.93 ms/op, 56.42 ms/op, 54.15 ms/op, 56.89 ms/op, 51.83 ms/op.
CPU profile after this change:
  internal/runtime/syscall/linux.Syscall6: 18.4% flat
  flushMemtableBulkInitial: 47.1% cumulative
  sortBulkIngestRefsByKeyMSD: 10.3% cumulative
  Batch.SetBytes: 7.4% cumulative
  slottedPageEncodeKnownSize: 3.4% cumulative
```

Kept: yes, subject to the full suite. This is a valid fast path for the common
small-inline record case and does not change large-value safety checks.

### Current Status After Experiments 34-38

YogaDB is now ahead of Pebble on `Benchmark_LoadOnly_*` by roughly 1.5x in the
best comparable runs:

```text
YogaDB: 54.03 ms/op
Pebble: 83.64 ms/op
ratio: 1.55x faster
```

The 2x target has not been reached. The remaining costs are no longer the old
duplicate-key interval semantics or per-record HLC work. The next structural
experiments should be:

```text
1. Replace the ref sorter with a benchmarked fixed-key-width prefix/radix
   sorter, but only keep it if wall time improves and allocation does not grow.
2. Add a true bulk FlexSpace append primitive that appends many already-sized
   slotted pages while updating tree extents/tags and anchors in one tight loop.
3. Consider a page-builder representation that avoids copying KVs into
   interval cache entries, but only if read iteration remains in the 5-8 ns/key
   band.
```

### 2026-09-09: Experiment 39, Accumulate Batch Logical Bytes

Patch:

```text
Batch now accumulates logicalBytes during Set, SetBytes, and Delete. Commit no
longer scans every KV solely to compute LogicalBytesWritten.
```

Result:

```text
Focused write-amplification/batch tests: PASS.
Benchmark_LoadOnly_YogaDB: 53.98 ms/op, 48.90 MB/op, 8085 allocs/op.
Benchmark_LoadOnly_Pebble: 88.36 ms/op.
Benchmark_Iter_YogaDB_Ascend: 5.406 ns/key.
```

Kept: yes. It is a simple hot-loop removal and preserves existing metrics in
the focused WA counter tests.

### 2026-09-09: Experiment 40, Batch Initial Put Capacity

Patch:

```text
Tested batchInitialPutCap at 10_000, 4096, and 8192. The benchmark commits
10k-key chunks, so the old 1024 cap caused repeated []KV growth and copying
inside every batch.
```

Result:

```text
10_000:
  Benchmark_LoadOnly_YogaDB: 55.44 ms/op, 28.37 MB/op, 7975 allocs/op.
  Benchmark_Iter_YogaDB_Ascend: 4.035 ns/key.
4096:
  Benchmark_LoadOnly_YogaDB: 52.55 ms/op, 44.61 MB/op, 8042 allocs/op.
  Benchmark_Iter_YogaDB_Ascend: 5.462 ns/key.
8192:
  Benchmark_LoadOnly_YogaDB: 57.42 ms/op, 33.64 MB/op, 7995 allocs/op.
  Benchmark_Iter_YogaDB_Ascend: 5.577 ns/key.
```

Kept: 4096. It gives the best measured wall time of the three and avoids the
larger default allocation footprint of 10k KVs per new batch.

### 2026-09-09: Experiment 41, Small Inline Slotted Encoder

Patch:

```text
Added slottedPageEncodeKnownSizeSmallInlineZeroVtyp plus matching size helper.
flushMemtableBulkInitial tracks whether a page is entirely live small-inline
vtyp=0 KVs and uses the specialized encoder only for those pages. Other pages
continue through the generic known-size encoder.
```

Result:

```text
Focused slotted/VLOG/persistence tests: PASS.
Benchmark_LoadOnly_YogaDB: 52.74 ms/op, 44.64 MB/op, 8039 allocs/op.
Benchmark_LoadOnly_Pebble: 86.38 ms/op.
Benchmark_Iter_YogaDB_Ascend: 5.576 ns/key.
YogaDB-only profile run:
  Benchmark_LoadOnly_YogaDB: 54.07 ms/op.
  slottedPageEncodeKnownSizeSmallInlineZeroVtyp: 3.8% cumulative.
```

Kept: yes. It is a narrow specialization for the common small-inline page shape
and does not change the read format.

### 2026-09-09: Experiment 42, Preallocate Bulk Flush Page Slice

Patch:

```text
Preallocated the transient bulk-flush page []KV to 128 entries. With 4 KiB
slotted pages and iter_bench key/value sizes, that covers a normal page without
growth.
```

Result:

```text
Focused persistence/slotted tests: PASS.
Benchmark_LoadOnly_YogaDB: 53.57 ms/op, 44.60 MB/op, 8035 allocs/op.
Benchmark_LoadOnly_Pebble: 86.97 ms/op.
Benchmark_Iter_YogaDB_Ascend: 6.267 ns/key.
```

Kept: yes. It is essentially neutral, but avoids needless first-page slice
growth in the hot flush path.

### 2026-09-09: Experiment 43, FlexSpace No-Commit Insert During Bulk Flush

Patch:

```text
Temporarily changed bulk initial page flush to call insertWithTagR(...,
commit=false) and manually update insert metrics, relying on the final ff.Sync
to commit all FlexSpace work.
```

Result:

```text
Focused WA/persistence/GC tests: PASS.
Benchmark_LoadOnly_YogaDB: 58.75 ms/op, 44.56 MB/op, 8039 allocs/op.
Benchmark_LoadOnly_Pebble: 83.30 ms/op.
Benchmark_Iter_YogaDB_Ascend: 5.381 ns/key.
```

Kept: no. The generic InsertWTag path is not the bottleneck; this worsened
wall time.

### 2026-09-09: Experiment 44, Direct Shared-HLC WAL Batch Encoding

Patch:

```text
logAppendBatchLocked now uses compactBatchHLCPayloadMaxSize and directly emits
appendCompactBatchHLCPayload. Batch.Commit already assigned one HLC to the
whole batch, so the hot path no longer scans the batch to prove HLC equality.
```

Result:

```text
Focused WAL/recovery tests: PASS.
Benchmark_LoadOnly_YogaDB: 54.35 ms/op, 44.66 MB/op, 8028 allocs/op.
Benchmark_LoadOnly_Pebble: 85.21 ms/op.
Benchmark_Iter_YogaDB_Ascend: 5.433 ns/key.
```

Kept: yes. The timing effect is small, but it removes redundant work from the
only current caller.

### 2026-09-09: Experiment 45, Radix Sort Cutoff Tuning

Patch:

```text
Retuned bulkRadixInsertionCutoff. Tested 16, 8, and 24 with the current sidecar
key sorter.
```

Result:

```text
cutoff 16, YogaDB-only count=3:
  52.92 ms/op, 52.03 ms/op, 47.94 ms/op.
cutoff 8, YogaDB-only count=3:
  56.03 ms/op, 53.88 ms/op, 51.73 ms/op.
cutoff 24, YogaDB-only count=3:
  53.75 ms/op, 57.42 ms/op, 50.19 ms/op.
Final retained cutoff 16 profile run:
  Benchmark_LoadOnly_YogaDB: 52.98 ms/op, 44.68 MB/op, 8028 allocs/op.
  sortBulkIngestRefsByKeyMSD: 9.55% cumulative.
Combined retained run:
  Benchmark_LoadOnly_YogaDB: 57.62 ms/op.
  Benchmark_LoadOnly_Pebble: 88.44 ms/op.
  Combined read sample: 12.15 ns/key.
Isolated read rerun:
  6.857 ns/key, 4.585 ns/key, 4.867 ns/key.
```

Kept: cutoff 16. It has the best cluster and moves sorter cost down from the
earlier 12-13% range to about 9-10% cumulative. The bad combined read sample
did not reproduce in isolated read runs.

### Updated Assessment

The best YogaDB-only samples are now dipping below 50 ms/op, and the best
side-by-side comparisons remain around 1.5-1.7x faster than Pebble while read
iteration stays in the single-digit ns/key range when measured separately.

The remaining path to a true 2x Pebble win is probably not another tiny branch
removal. The promising larger designs are:

```text
1. A true bulk FlexSpace append that builds tree extents/tags for all slotted
   pages in one append-aware loop, not through page-at-a-time generic insert.
2. A sorter that exploits fixed-width CallID keys without carrying string
   headers through each radix pass.
3. A cache-install path that can transfer ownership of per-page KVs without
   forcing either page-slice reuse bugs or slow first-read disk decodes.
```

### 2026-09-09: Experiment 46, Fixed-Width Key Radix Sort

Patch:

```text
bulkIngestBuilder.buildOrder detects when all staged keys have the same byte
length. Fixed-width batches use a 256-bucket MSD radix sorter that does not
need the variable-length sentinel bucket.
```

Result:

```text
Focused sort tests: PASS.
Side-by-side:
  Benchmark_LoadOnly_YogaDB: 56.22 ms/op, 44.54 MB/op, 8041 allocs/op.
  Benchmark_LoadOnly_Pebble: 82.04 ms/op.
  Benchmark_Iter_YogaDB_Ascend: 5.409 ns/key.
YogaDB-only count=5:
  49.84, 52.15, 52.19, 48.00, 52.86 ms/op.
```

Kept: yes. The iter_bench keys are 28-byte fixed-width base64url CallIDs, so
this path is active on the benchmark and does not change storage/read layout.

### 2026-09-09: Experiment 47, Sequential Sparse-Index Anchor Append

Patch:

```text
Added memSparseIndexTreeHandler.handlerAppend and used it in
flushMemtableBulkInitial. The bulk flush emits pages in sorted key order, so
each new anchor is appended after the previous one instead of doing a fresh
findAnchorPos for every page.
```

Result:

```text
Focused sparse-index/persistence tests: PASS.
Side-by-side:
  Benchmark_LoadOnly_YogaDB: 50.57 ms/op, 44.73 MB/op, 8021 allocs/op.
  Benchmark_LoadOnly_Pebble: 89.12 ms/op.
  Benchmark_Iter_YogaDB_Ascend: 5.777 ns/key.
Profile after change:
  handlerAppend: 0.56% cumulative.
```

Kept: yes. This removes repeated tree descents from pristine bulk flush and
preserves the same anchor/page layout.

### 2026-09-09: Experiment 48, Guardrail Tests and Fixed-Length Detector Fix

Patch:

```text
Added TestSortBulkOrderByFixedWidthKey.
Fixed buildOrder's fixed-length detector so once variable-length keys are
observed it cannot accidentally reset back to fixed-length mode.
Added an explicit error if handlerAppend unexpectedly returns nil during bulk
initial flush.
```

Result:

```text
Focused sort/sparse/persistence tests: PASS.
Full suite after all retained changes: PASS.
```

Kept: yes. The detector bug was exposed by the new fixed-prefix sorter trial
and is a correctness fix independent of benchmark speed.

### 2026-09-09: Experiment 49, Page Size 8 KiB Trial

Patch:

```text
Temporarily changed SLOTTED_PAGE_KB from 4 to 8 to reduce page count, anchor
count, cache entries, and page-encoding work.
```

Result:

```text
Combined load/read, count=3:
  Benchmark_LoadOnly_YogaDB: 50.04, 55.19, 50.39 ms/op.
  Allocations fell to about 4048 allocs/op.
Isolated read, count=3:
  10.70, 8.888, 10.75 ns/key.
```

Kept: no. Larger pages halved allocation count but did not reliably improve
write time and clearly worsened the read iteration path compared with 4 KiB
pages.

### 2026-09-09: Experiment 50, No-Sidecar Comparison Sort Trial

Patch:

```text
Temporarily sorted fixed-width bulk refs with sort.Slice and direct
builder.kv(ref).Key comparisons, skipping the key/keyAux sidecar allocation.
```

Result:

```text
Focused sort tests: PASS.
Benchmark_LoadOnly_YogaDB count=3:
  77.65, 75.03, 73.40 ms/op.
  Allocation dropped to about 40.6 MB/op.
```

Kept: no. Direct comparisons saved memory but lost badly on CPU. The sidecar
radix sorter remains the right tradeoff.

### 2026-09-09: Experiment 51, Batch Sizing Retune

Patch:

```text
Retuned Batch defaults for the benchmark's actual 10,000-key commit chunks:
batchInitialPutCap 4096 -> 10000, and batchInitialKeyArenaCap 512 KiB ->
280 KiB. SetBytes already aliases value to key for SetBytes(k, k, 0), so no
value arena is allocated in this benchmark.
```

Result:

```text
Before this retune, typical retained profile:
  Benchmark_LoadOnly_YogaDB: 51.84 ms/op, 44.62 MB/op, 8033 allocs/op.
After put cap 10000 / key arena 320 KiB:
  47.96, 51.20, 52.19, 48.24, 49.15 ms/op.
  about 26.4 MB/op.
After key arena 280 KiB:
  53.18, 47.87, 53.02, 52.94, 47.46 ms/op.
  about 26.0 MB/op.
```

Kept: yes. Runtime is noisy but allocations drop by roughly 18 MB/op, and this
no longer forces every 10k batch through multiple KV-slice growth/copy steps.

### 2026-09-09: Experiment 52, Cache Install Known Size

Patch:

```text
Bulk initial flush now accumulates the page's kvSizeApprox total while building
the page and passes it to intervalCachePartition.installCleanEntryWithSize.
The cache still copies KVs and builds CRC fingerprints exactly as before, but
does not recompute approximate KV size during cache install.
```

Result:

```text
Focused sort/cache/persistence tests: PASS.
Benchmark_LoadOnly_YogaDB count=5:
  46.25, 51.14, 47.17, 46.70, 50.74 ms/op.
Final side-by-side after full suite:
  Benchmark_Iter_YogaDB_Ascend: 5.522 ns/key.
  Benchmark_LoadOnly_YogaDB: 52.58 ms/op, 26.02 MB/op, 7964 allocs/op.
  Benchmark_LoadOnly_Pebble: 83.11 ms/op, 25.15 MB/op, 6494 allocs/op.
Full suite:
  go test ./... -count=1 -timeout=600s: PASS.
```

Kept: yes. The effect is modest but positive, and it keeps cache warmup for the
fast read path.

### Current Hot Path Notes

The current retained profile after experiments 46-52 is roughly:

```text
Benchmark_LoadOnly_YogaDB: 51.82 ms/op, 26.01 MB/op, 7965 allocs/op.
Top CPU:
  internal/runtime/syscall/linux.Syscall6: 24.79% flat
  runtime.memclrNoHeapPointers: 15.32% flat
  flushMemtableBulkInitial: 43.45% cumulative
  sortBulkIngestRefsByFixedKeyLenMSD: 10.03% cumulative
  installCleanEntry: 7.24% cumulative
  SetBytes: 2.79% cumulative
```

The skiplist/wormhole/Pebble arena-skiplist idea is still valuable for the
general write path, but it is not the main limiter of this particular
iter_bench pristine-load benchmark anymore. The retained fast path bypasses
the memtable B-tree for pristine initial bulk loads; its central data-structure
cost is now sorting staged refs and installing sorted slotted pages/cache
entries.

The remaining credible route to a 2x Pebble win on this exact benchmark is a
larger bulk-load design, not a small constant tweak:

```text
1. Replace page-at-a-time FlexSpace.InsertWTag with an append-specialized bulk
   primitive that writes all page bytes and creates tagged extents in one
   append-aware loop.
2. Find a fixed-width sorter better than the current MSD sidecar radix sorter
   without falling back to indirect string comparisons.
3. Revisit cache ownership with a design that does not regress first scan speed
   and does not allocate one fresh copied KV slice per page.
```

### 2026-09-09: Experiment 53, FlexTree Append Cursor

Patch:

```text
Added a transient append cursor to FlexTree and an InsertWTagAppend path used
when FlexSpace insertWithTagR is appending at MaxLoff. The cursor caches the
rightmost leaf and path, invalidates on generic insert/delete/tag mutations,
and invalidates after splits. Redo logging and extent tags are unchanged.

Added TestFlexTree_InsertWTagAppendMatchesInsertWTag, comparing the append path
against ordinary InsertWTag over 5000 tagged appends.
```

Result:

```text
Focused FlexTree/FlexSpace/DB tests: PASS.
Benchmark_LoadOnly_YogaDB count=5:
  50.66, 46.39, 46.41, 47.14, 50.75 ms/op.
Profile:
  Benchmark_LoadOnly_YogaDB: 49.19 ms/op, 26.06 MB/op, 7960 allocs/op.
```

Kept: yes. It is not a huge win because append-at-MaxLoff already avoided
shift propagation, but it removes repeated root-to-rightmost-leaf descent and
is covered directly.

### 2026-09-09: Experiment 54, Cache Ownership Retry

Patch:

```text
Temporarily added installOwnedCleanEntryWithSize and transferred each completed
bulk page []KV directly into the interval cache, allocating a fresh 128-cap
page for the next page.
```

Result:

```text
Focused cache/persistence tests: PASS.
Benchmark_LoadOnly_YogaDB count=5:
  50.74, 51.77, 50.10, 49.83, 51.61 ms/op.
  about 33.6 MB/op.
Read remained fast in noisy samples:
  4.310, 4.269, 6.278, 4.407, 5.870 ns/key.
```

Kept: no. Avoiding the cache copy lost to fresh page allocation and increased
allocation pressure. The previous copy-into-cache design remains the better
write/read tradeoff.

### 2026-09-09: Experiment 55, Direct Slotted Page Encode Into FlexSpace Block Buffer

Patch:

```text
flushMemtableBulkInitial now encodes each slotted page directly into the
current FlexSpace block-manager buffer for the pristine append path. It then
updates block usage, appends the tagged FlexTree extent via InsertWTagAppend,
and writes the same redo-log entries as InsertWTag. This removes the temporary
pageBuf -> bm.write copy.
```

Result:

```text
Focused FlexTree/FlexSpace/cache/persistence tests: PASS.
Benchmark_LoadOnly_YogaDB count=5:
  45.77, 50.19, 47.52, 47.37, 48.38 ms/op.
Profile:
  Benchmark_LoadOnly_YogaDB: 48.26 ms/op, 26.01 MB/op, 7959 allocs/op.
Side-by-side:
  Benchmark_LoadOnly_YogaDB: 51.91 ms/op.
  Benchmark_LoadOnly_Pebble: 86.15 ms/op.
  Benchmark_Iter_YogaDB_Ascend: 5.651 ns/key.
```

Kept: yes. It preserves page format, tags, redo logging, and read layout while
removing one hot-path data copy.

### 2026-09-09: Experiment 56, MEMWAL Value-Is-Key Batch Record

Patch:

```text
Added MEMWAL_BATCH_KV_HLC_VALUE_IS_KEY. When Batch.SetBytes observes that the
value slice is the same slice as the key slice, Batch records that invariant.
logAppendBatchLocked can then emit one shared-HLC batch record containing each
key once instead of writing both key and equal value bytes. Recovery decodes the
record by reconstructing Value from Key.

Added TestCompactBatchHLCValueIsKeyPayloadRoundTrip and ran recovery tests.
```

Result:

```text
Focused WAL/recovery/batch tests: PASS.
After initial codec with detection scan:
  Benchmark_LoadOnly_YogaDB count=5:
    43.60, 44.35, 45.31, 42.82, 45.28 ms/op.
After threading Batch.allValuesAliasKeys instead of scanning:
  Benchmark_LoadOnly_YogaDB count=5:
    42.93, 43.04, 43.62, 43.67, 40.46 ms/op.
Side-by-side:
  Benchmark_Iter_YogaDB_Ascend: 3.903 ns/key.
  Benchmark_LoadOnly_YogaDB: 44.30 ms/op, 24.39 MB/op, 7947 allocs/op.
  Benchmark_LoadOnly_Pebble: 85.78 ms/op, 25.14 MB/op, 6394 allocs/op.
Repeated load-only side-by-side:
  YogaDB: 47.45, 47.26, 43.56 ms/op.
  Pebble: 85.21, 84.93, 83.52 ms/op.
Profile:
  Benchmark_LoadOnly_YogaDB: 44.26 ms/op, 24.37 MB/op, 7950 allocs/op.
  appendCompactBatchHLCValueIsKeyPayload: 2.22% cumulative.
Full suite:
  go test ./... -count=1 -timeout=600s: PASS.
```

Kept: yes. This is the largest win in this round. It is specialized to
`SetBytes(k, k, 0)`-style loads, which is exactly what iter_bench does, but it
is encoded as an explicit WAL format with recovery coverage rather than a
benchmark-only bypass.

### 2026-09-09: Experiment 57, Bulk Fixed Page Base

Patch:

```text
Tried using one minimum HLC as every bulk slotted page's baseHLC to avoid
mid-page base lowering and page-size recomputation after sorted keys interleave
different commit HLCs.
```

Result:

```text
Full pre-scan variant:
  Benchmark_LoadOnly_YogaDB count=5:
    43.67, 45.38, 47.13, 47.12, 48.89 ms/op.
No-scan first-segment-HLC variant:
  Focused tests: PASS.
  Benchmark_LoadOnly_YogaDB count=5:
    49.12, 46.34, 46.79, 45.97, 46.92 ms/op.
```

Kept: no. The extra scan/branch and larger common base did not beat the current
per-page base logic.

### Current Position After Experiments 53-57

The retained path now has credible single-run 2x behavior in YogaDB-only versus
recent Pebble timings, and side-by-side runs are close but still not a stable
2x average:

```text
Best retained YogaDB-only cluster:
  40.46-43.67 ms/op.
Recent Pebble side-by-side cluster:
  83.52-85.78 ms/op.
Most recent direct side-by-side:
  YogaDB 44.30 ms/op vs Pebble 85.78 ms/op = 1.94x.
```

Remaining hot costs worth attacking next:

```text
1. Fixed-width sorter: still about 10-13% cumulative. A better sorter needs to
   keep radix-like locality without indirect string comparisons.
2. Bulk page consume/build: slotted size calculation and page assembly are now
   a major share of in-timer work.
3. Cache install: still allocates/copies per page for fast first reads. The
   ownership retry showed naive transfer is worse, so a better design must
   reuse page storage without invalidating cached KVs.
```

### 2026-09-09: Experiment 58, Small-Inline Bulk Page Accounting

Patch:

```text
bulkIngestBuilder now tracks allSmallInlineZeroVtyp while it already scans
staged KVs for approximate size. flushMemtableBulkInitial uses that invariant
to skip per-KV HasVPtr/slottedKVSmallInlineZeroVtyp classification and to use a
small-inline-zero-vtyp encoded-size helper.
```

Result:

```text
Focused WAL/recovery/bulk tests: PASS.
Full suite: go test ./... -count=1 -timeout=600s: PASS.
Side-by-side count=3:
  Benchmark_LoadOnly_YogaDB:
    36.68, 41.97, 40.48 ms/op; about 24.4 MB/op.
  Benchmark_LoadOnly_Pebble:
    85.24, 83.65, 85.69 ms/op.
  Benchmark_Iter_YogaDB_Ascend:
    3.892, 6.705, 3.946 ns/key.
Profile:
  Benchmark_LoadOnly_YogaDB: 40.63 ms/op, 24.40 MB/op, 7946 allocs/op.
```

Kept: yes. This is a narrow but legitimate fast path for the benchmark's real
record shape. It preserves the existing slotted encoder and only specializes
classification and encoded-size accounting.

### 2026-09-09: Experiment 59, Base64URL 64-Bucket Fixed-Width Sorter

Patch:

```text
Tried a base64url-specific fixed-width radix sorter. First variant pre-scanned
all key bytes; second variant used a [256]uint8 rank table during recursion and
fell back to the generic fixed-width sorter on the first non-base64url byte.
```

Result:

```text
Pre-scan variant:
  Benchmark_LoadOnly_YogaDB count=3:
    57.45, 58.75, 54.22 ms/op.
Rank-table variant profile:
  Benchmark_LoadOnly_YogaDB: 43.61 ms/op, 24.33 MB/op, 7955 allocs/op.
  sortBulkIngestRefsByFixedBase64URLKeyLenMSD: 8.51% flat, 13.48% cumulative.
```

Kept: no. The pre-scan variant was a clear regression. The table variant was
less bad, but it did not beat the current 256-bucket fixed-width sorter in
practice and added benchmark-shaped complexity.

### 2026-09-09: Experiment 60, Fast HLC-Base Recompute For Small Pages

Patch:

```text
Added intervalCacheEntrySlottedKVsSizeSmallInlineZeroVtyp so that when sorted
bulk input interleaves multiple commit HLCs and lowers the current page base,
the page-size recomputation avoids the generic slottedKVEncodedSize routine.
```

Result:

```text
Focused WAL/recovery/bulk tests: PASS.
Profile run:
  Benchmark_LoadOnly_YogaDB: 46.24 ms/op, 24.35 MB/op, 7951 allocs/op.
  flushMemtableBulkInitial.func3: 16.34% cumulative.
```

Kept: tentatively yes. The timed result was not an immediate win, but the
change is small and removes generic work from a known-fast page shape. Revisit
if later benchmark clusters show regression.

### 2026-09-09: Experiment 61, Exact MEMWAL Preallocation

Patch:

```text
compactBatchHLCValueIsKeyPayloadSize computes the exact encoded payload size
for MEMWAL_BATCH_KV_HLC_VALUE_IS_KEY records. logAppendBatchLocked now grows
memWalEncodeBuf to that capacity before appending the payload.
```

Result:

```text
Focused WAL/recovery/bulk tests: PASS.
Side-by-side count=3:
  Benchmark_LoadOnly_YogaDB:
    40.41, 46.40, 41.95 ms/op; about 23.1-23.2 MB/op.
  Benchmark_LoadOnly_Pebble:
    84.41, 87.08, 86.69 ms/op.
Profile:
  Benchmark_LoadOnly_YogaDB: 36.44 ms/op, 23.28 MB/op, 7906 allocs/op.
  appendCompactBatchHLCValueIsKeyPayload is no longer in the top alloc list.
```

Kept: yes. This is a clean allocation reduction on the retained value-is-key
WAL format. It does not change encoded bytes or recovery semantics.

### Current Position After Experiments 58-61

```text
Best retained YogaDB profile run:
  36.44 ms/op, 23.28 MB/op, 7906 allocs/op.
Latest Pebble side-by-side cluster:
  84.41-87.08 ms/op.
Current side-by-side YogaDB cluster:
  40.41-46.40 ms/op.
```

The profile after experiment 61 shows remaining YogaDB-owned CPU mostly in:

```text
sortBulkIngestRefsByFixedKeyLenMSD: 8.66% flat.
flushMemtableBulkInitial: 36.22% cumulative.
  consume/page assembly: about 20.47% cumulative.
  flush page/encode/cache install: about 15.75% cumulative.
slottedPageEncodeKnownSizeSmallInlineZeroVtyp: 4.72% cumulative.
installCleanEntryWithSize: still a top allocation site because warm reads keep
decoded pages in cache.
```

The write target is now met on the best retained run and often met on
side-by-side samples, but the cluster is noisy enough that the honest statement
is "about 2x faster than Pebble on this benchmark", not a guaranteed 2x every
single sample.

### 2026-09-09: Experiment 62, Exact Generic HLC MEMWAL Sizing

Patch:

```text
Added compactBatchHLCPayloadSize for MEMWAL_BATCH_KV_HLC and changed
logAppendBatchLocked to use exact encoded sizes for both active shared-HLC
batch formats. Removed the now-unused pessimistic max-size helpers.

Added tests that assert the exact-size helpers match the actual encoded
payload lengths for both MEMWAL_BATCH_KV_HLC and
MEMWAL_BATCH_KV_HLC_VALUE_IS_KEY.
```

Result:

```text
Focused WAL/recovery tests: PASS.
Full suite: go test ./... -count=1 -timeout=600s: PASS.
Side-by-side benchtime=10x:
  Benchmark_LoadOnly_YogaDB: 43.24 ms/op, 23.20 MB/op, 7915 allocs/op.
  Benchmark_LoadOnly_Pebble: 84.05 ms/op, 25.19 MB/op, 6630 allocs/op.
```

Kept: yes. The iter benchmark still uses the value-is-key WAL record, so this
mainly finishes the memtable WAL cleanup for the generic HLC batch path and
keeps buffer preallocation exact across both retained compact batch encoders.

### 2026-09-09: Experiment 63, Top-16 Fixed-Key Radix Trial

Patch:

```text
Temporarily tried a two-byte top-level radix split for fixed-width keys before
falling back to the existing MSD sorter inside each bucket.
```

Result:

```text
Focused tests: PASS.
Side-by-side load regressed to about 43.56 ms/op and allocated about 1 MB/op
more than the retained path.
Profile showed runtime.memclrNoHeapPointers rising because the larger scratch
tables were zeroed on every sort.
```

Kept: no. The larger radix table is exactly the wrong tradeoff here: it reduces
some recursive bookkeeping, but the zeroing cost dominates.

### 2026-09-09: Experiment 64, Cache Fingerprint Threading Trial

Patch:

```text
Temporarily threaded per-page key fingerprints from bulk flush into
installCleanEntryWithSize to avoid recomputing them during cache install.
```

Result:

```text
Focused tests: PASS.
Side-by-side load regressed to about 47.05 ms/op.
Profile run was about 41.63 ms/op.
```

Kept: no. The extra per-page scratch and plumbing cost more than recomputing
fingerprints locally during cache install.

### 2026-09-09: Experiment 65, Cheap Duplicate Equality Guard

Patch:

```text
bulkIngestBuilder now records fixedKeyLen when all staged keys have one width.
flushMemtableBulkInitial uses that to reject non-duplicate neighbors by last
byte before doing full string equality in the duplicate-collapse loop. The
variable-width path uses the same cheap last-byte guard via bulkIngestKeysEqual.
```

Result:

```text
Focused bulk/WAL/recovery tests: PASS.
Full suite: go test ./... -count=1 -timeout=600s: PASS.
Side-by-side:
  Benchmark_LoadOnly_YogaDB: 40.92 ms/op.
  Benchmark_LoadOnly_Pebble: 91.64 ms/op.
Profile:
  Benchmark_LoadOnly_YogaDB: 38.40 ms/op, 23.31 MB/op, 7903 allocs/op.
Read check:
  Benchmark_Iter_YogaDB_Ascend isolated samples: 4.06, 4.00, 5.56 ns/key.
```

Kept: yes. This preserves duplicate-key last-HLC-wins semantics while avoiding
most full string equality work in no-duplicate runs.

### 2026-09-09: Experiment 66, All-Small Inline Consume Specialization Trial

Patch:

```text
Temporarily added a separate consumeSmallInlineItem closure for bulk pages that
are known to contain only small inline values with vtyp zero.
```

Result:

```text
Focused tests: PASS.
Side-by-side load regressed to about 44.67 ms/op.
Read remained fast at about 4.14 ns/key.
```

Kept: no. Removing a few item-shape branches made the function larger and did
not reduce the real costs: page flush, cache install, encoded-size accounting,
and block writes.

### 2026-09-09: Experiment 67, Lazy Initial Cache Install Trial

Patch:

```text
Temporarily skipped interval-cache installation from flushMemtableBulkInitial.
```

Result:

```text
Focused tests: PASS.
Load-only improved allocation substantially:
  Benchmark_LoadOnly_YogaDB: 39.40 ms/op, 15.34 MB/op, 3213 allocs/op.
But the first iterator pass paid the deferred cache load:
  Benchmark_Iter_YogaDB_Ascend, benchtime=1x:
    204-294 ns/key, about 38 MB/op, about 225k allocs/op.
```

Kept: no. This is a real write-only knob, but it violates the read-after-load
goal. Eager cache installation remains the right default for the current API and
benchmark semantics.

### 2026-09-09: Experiment 68, Ordered Bulk Ingest Detection

Patch:

```text
bulkIngestBuilder now tracks whether appended bulk segments remain globally
nondecreasing by key. buildOrder still builds the order/key sidecars for
duplicate collapse, but skips the radix sort entirely when input is already
sorted. Disorder detection stops after the first out-of-order key, so random
inputs pay only a short failed check.

Added Benchmark_LoadOnly_YogaDB_Ascending and
Benchmark_LoadOnly_Pebble_Ascending to measure the non-random workload class
directly.
```

Result:

```text
Focused tests: PASS after fixing zero-value builder initialization.
Full suite: go test ./... -count=1 -timeout=600s: PASS.

Existing random NewCallID fixture:
  Benchmark_Iter_YogaDB_Ascend: 4.04 ns/key.
  Benchmark_LoadOnly_YogaDB: 38.37 ms/op, 23.34 MB/op, 7899 allocs/op.
  Benchmark_LoadOnly_Pebble: 84.53 ms/op.

Ascending fixture:
  Benchmark_LoadOnly_YogaDB_Ascending: 21.44 ms/op, 22.66 MB/op, 3884 allocs/op.
  Benchmark_LoadOnly_Pebble_Ascending: 64.01 ms/op, 14.01 MB/op, 2849 allocs/op.

Profile:
  Benchmark_LoadOnly_YogaDB_Ascending: 23.67 ms/op.
  flushMemtableBulkInitial cumulative dropped to about 20%.
  buildOrder cumulative dropped to about 4%.
```

Kept: yes. This is not an optimization for the random NewCallID benchmark. It
targets a realistic ingestion shape: already ordered bulk loads. On that shape,
YogaDB is about 3x faster than Pebble while keeping the read path warm and fast.

### 2026-09-09: Experiment 69, Page Checksum Algorithm Trial

Patch:

```text
Temporarily changed slotted page checksums from CRC32c to xxhash64-truncated-
to-32-bits, with no legacy fallback. Also added a temporary 4 KiB checksum
microbenchmark matching SLOTTED_PAGE_KB=4.
```

Result:

```text
Focused slotted/recovery/cache tests: PASS after updating debug-dump text from
CRC=OK to Checksum=OK.

4 KiB checksum microbench, 5 runs:
  CRC32c: 130-139 ns/op, about 29.5-31.5 GB/s.
  xxhash64 truncated: 243-259 ns/op, about 15.8-16.8 GB/s.

Full database samples with xxhash were noisy and not convincingly better; one
ascending sample regressed to about 24.19 ms/op.
```

Kept: no. On this AMD Linux machine, Go's CRC32c path uses fast Castagnoli
hardware instructions and is faster than xxhash for 4 KiB pages. CRC32c is not
the write bottleneck: the final profile still shows page checksum around 1-2%
of CPU.

### 2026-09-09: Experiment 70, Drop Sorted-Path Sort Aux Allocation

Patch:

```text
bulkIngestBuilder.buildOrder now allocates sortAux/keyAux only when the staged
bulk input is actually unsorted and needs sorting. Already ordered input still
builds order/keys for duplicate collapse at this stage, but no longer zeroes the
merge-sort scratch arrays.
```

Result:

```text
Focused bulk/slotted tests: PASS.

Ascending load, 30x count=3:
  20.47-21.24 ms/op, 20.25 MB/op, 3882 allocs/op.

Previous ascending sample with sorted detection but eager aux allocation:
  about 21.58-22.94 ms/op, 22.66 MB/op, 3880-3884 allocs/op.
```

Kept: yes. This removes about 2.4 MB/op of sorted-ingest allocation and gives a
small time win or time-neutral result depending on run noise.

### 2026-09-09: Experiment 71, Remove Interval-Cache Fingerprint Sidecar

Patch:

```text
Production point reads now use intervalCacheEntryFindKeyGE, the binary-search
path already used by writes. intervalCacheEntryFindKeyEQ remains as a
compatibility wrapper over GE for tests/benchmarks. The fce.fps sidecar and all
fingerprint maintenance were removed from cache load, insert, replace, delete,
split, snapshot/restore, and preview-upsert paths.
```

Result:

```text
Focused interval/get/put/recovery/HLC tests: PASS.
Full suite: go test ./... -count=1 -timeout=600s: PASS.

FindKey benchmark, 8 x 200ms:
  FindKeyEQ wrapper: 35.70-41.02 ns/op, 0 allocs/op.
  FindKeyGE:         35.50-41.46 ns/op, 0 allocs/op.

Side-by-side load/read sample:
  Benchmark_Iter_YogaDB_Ascend: 3.99 ns/key, 1320 B/op, 3 allocs/op.
  Benchmark_LoadOnly_YogaDB: 43.44 ms/op, 22.98 MB/op, 6348 allocs/op.
  Benchmark_LoadOnly_Pebble: 82.47 ms/op.
  Benchmark_LoadOnly_YogaDB_Ascending: 20.52 ms/op, 20.02 MB/op, 3118 allocs/op.
  Benchmark_LoadOnly_Pebble_Ascending: 63.23 ms/op.
```

Kept: yes. It removes dead write/cache maintenance and improves point lookup
relative to the old linear fingerprint scan.

### 2026-09-09: Experiment 72, Stream Already-Sorted Bulk Segments

Patch:

```text
flushMemtableBulkInitial now has a sorted bulk path that streams the appended
segments directly instead of calling buildOrder. Duplicate keys are collapsed
on the fly because sorted input guarantees duplicates are adjacent; the kept
entry is the highest HLC, with later equal HLC winning. This does not assume
fixed-width keys and is independent of the random NewCallID benchmark shape.
```

Result:

```text
Focused bulk/interval/get/put/recovery/slotted/HLC tests: PASS.
Full suite: go test ./... -count=1 -timeout=600s: PASS.

Ascending load, 50x count=5:
  20.49, 20.54, 20.59, 21.11, 22.97 ms/op,
  17.61 MB/op, 3116 allocs/op.

Final profiled ascending load:
  Benchmark_LoadOnly_YogaDB_Ascending: 20.32 ms/op,
  17.61 MB/op, 3116 allocs/op.

Final side-by-side sample after the patch:
  Benchmark_Iter_YogaDB_Ascend: 4.03 ns/key, 1320 B/op, 3 allocs/op.
  Benchmark_LoadOnly_YogaDB: 41.06 ms/op, 23.00 MB/op, 6345 allocs/op.
  Benchmark_LoadOnly_Pebble: 85.65 ms/op.
  Benchmark_LoadOnly_YogaDB_Ascending: 23.56 ms/op in one noisy paired run;
  isolated repeated runs cluster around 20.5-21.1 ms/op.
  Benchmark_LoadOnly_Pebble_Ascending: 65.18 ms/op.
```

Kept: yes. The time result is mostly neutral against the aux-only sorted path,
but it removes another about 2.4 MB/op of sorted-ingest allocation and erases
buildOrder from the sorted-ingest allocation profile. Current profile bottlenecks
are now page encoding, cache KV-copy install, WAL payload construction, syscalls,
and allocation/GC noise from the benchmark harness/open path.

## Current State After Experiment 72

The fair write-path comparison now has two useful shapes:

```text
Random NewCallID fixture:
  YogaDB: about 41-43 ms/op.
  Pebble: about 82-86 ms/op.
  Status: YogaDB is roughly 2x faster.

Ascending fixture:
  YogaDB: about 20.3-21.1 ms/op isolated, with noisy paired outliers.
  Pebble: about 63-65 ms/op.
  Status: YogaDB is roughly 3x faster.

Read path:
  Benchmark_Iter_YogaDB_Ascend remains about 4 ns/key with 3 allocs/op.
```

The remaining high-upside architectural work is still arena ownership: avoid
the pointer-heavy `[]KV` batch/bulk/cache copies by storing staged keys/values
in compact arenas plus offset metadata, then materializing pointer-bearing KVs
only where a warmed cache entry needs them. That is the Go analogue of Pebble's
arena skiplist advantage, but it is a larger format/internal-memory change and
needs its own correctness pass.

### 2026-09-09: Experiment 73, Recheck Smaller Batch Puts Cap

Patch:

```text
Retested lowering batchInitialPutCap from 10000 to 1024.
```

Result:

```text
Rejected. The benchmark commits every 10000 keys, so the smaller starting cap
caused avoidable grows/copies and did not improve write throughput.
```

### 2026-09-09: Experiment 74, Append-Only nextBlock Fast Path

Patch:

```text
Tried avoiding the FlexSpace empty-block search when bulk initial load is
append-only.
```

Result:

```text
Rejected and reverted. It did not improve the write benchmarks and made the
read/noise picture worse.

Post-revert reference:
  Random YogaDB:    39.15, 40.88, 40.12 ms/op.
  Random Pebble:    88.04, 93.51, 94.82 ms/op.
  Ascending YogaDB: 19.86, 19.59, 25.83 ms/op.
  Ascending Pebble: 62.67, 71.53, 61.90 ms/op.
```

### 2026-09-09: Experiment 75, Sorted Unique Small-Page Direct Flush

Patch:

```text
For already-sorted, duplicate-free, all-small-inline-zero-vtyp bulk input,
flushMemtableBulkInitial chunks each bulk segment directly instead of going
through the generic consumeItem page builder. Cache installation can own the
segment subslice, avoiding the per-page KV copy.
```

Result:

```text
Focused correctness tests: PASS.

Ascending load improved into the 16.8-18.8 ms/op range in repeated paired
runs, with iterator reads still mostly 3.9-4.2 ns/key.
```

Kept: yes. This is specific to sorted input but does not depend on fixed-width
random keys.

### 2026-09-09: Experiment 76, Sparse Interval Retune

Patch:

```text
Retuned flexdbSparseIntervalCount around the new bulk path.
```

Result:

```text
1500 before value-is-key:
  Random YogaDB:    37.35-45.80 ms/op.
  Ascending YogaDB: 17.63-20.96 ms/op.
  Rejected.

2000 after value-is-key/cache accounting fix:
  Random YogaDB:    31.68-41.57 ms/op in paired samples.
  Ascending YogaDB: 17.16-20.02 ms/op.
  Kept.

3000 after value-is-key:
  Random YogaDB:    37.59-41.51 ms/op.
  Ascending YogaDB: 17.21-19.86 ms/op.
  Rejected and reverted to 2000.
```

### 2026-09-09: Experiment 77, Bulk Tombstone Metadata

Patch:

```text
bulkIngestBuilder now tracks hasTombstones during appendBatch, allowing
flushMemtableBulkInitial to avoid a full tombstone eligibility scan when data
arrived through the bulk builder.
```

Result:

```text
Focused correctness tests: PASS.
Random samples:    38.38-43.80 ms/op.
Ascending samples: 17.63-20.75 ms/op.
```

Kept: yes. Mostly neutral timing, but it removes a redundant pass and preserves
the correctness gate for tombstones.

### 2026-09-09: Experiment 78, Slotted Value-Is-Key Marker

Patch:

```text
Added slottedValInfoValueIsKey, used when an inline value is byte-identical to
the key and Vtyp is zero. The slotted encoder stores only the key bytes plus
the marker; decode reconstructs Value from Key. No backwards-compatibility
fallback was added, per direction.
```

Important fix:

```text
The first version hurt reads badly because interval cache accounting
double-counted aliased key/value bytes and evicted hot pages. kvSizeApprox now
does not double-count value bytes when Value aliases Key.
```

Result after the cache accounting fix:

```text
Focused correctness tests: PASS.

Iterator read:
  3.70-3.82 ns/key, 1320 B/op, 3 allocs/op.

Random load:
  37.30-44.02 ms/op, about 22.53 MB/op, about 3560 allocs/op.

Ascending load:
  17.54-18.59 ms/op, about 10.29 MB/op, about 1403 allocs/op.
```

Kept: yes. It reduced encoded data volume and allocation pressure while keeping
the read path at the original fast-cache speed.

### 2026-09-09: Experiment 79, Specialized Value-Is-Key Page Encoder

Patch:

```text
Threaded allValuesAliasKeys through Batch -> memtable -> bulkIngestBuilder and
added slottedPageEncodeKnownSizeSmallInlineZeroVtypValueIsKey. For all-value-
is-key pages, the encoder no longer rechecks aliasing per KV and has no value
copy loop.
```

Result:

```text
Focused correctness tests: PASS.

Paired 20x count=5:
  Iterator read:    3.83-4.34 ns/key, 1320 B/op, 3 allocs/op.
  Random YogaDB:    32.44, 35.94, 37.99, 34.21, 35.22 ms/op.
  Random Pebble:    86.33, 96.85, 100.91, 87.90, 94.86 ms/op.
  Ascending YogaDB: 14.49-20.85 ms/op.
  Ascending Pebble: 61.63-65.17 ms/op.
```

Kept: yes. The win is modest on random but significant on ascending and it
removes format work that was provably redundant for this workload.

### 2026-09-09: Experiment 80, Skip MEMWAL Serialization for Initial Commit(false)

Patch:

```text
During pristine bulk initial load, Batch.Commit(false) now appends to the bulk
builder without serializing the batch to MEMWAL. Batch.Commit(true) still uses
the compact MEMWAL path and syncs as before. This follows the documented
durability boundary: Commit(false) is not durable until db.Sync() returns.
```

Result:

```text
Focused correctness tests: PASS.
Full suite: go test ./... -count=1 -timeout=600s: PASS.

Paired 20x count=5:
  Random YogaDB:    26.85, 30.43, 28.65, 29.82, 30.08 ms/op.
  Random Pebble:    83.62, 85.86, 84.25, 85.41, 85.72 ms/op.
  Ascending YogaDB: 11.72-13.90 ms/op.
  Ascending Pebble: 58.90-62.38 ms/op.

Later retained-code 100x profile:
  Random YogaDB: 28.73 ms/op, 21.52 MB/op, 3536 allocs/op.
```

Kept: yes. This is the largest retained win from this round. It removes
short-lived MEMWAL payload construction and write traffic that is unnecessary
for Commit(false) before the final Sync.

### 2026-09-09: Experiment 81, No-Dedup Bulk Flush

Patch:

```text
Tried streaming sorted refs directly after sort without duplicate-key collapse.
```

Result:

```text
Focused correctness tests: PASS.
Random YogaDB: 26.43-33.05 ms/op, average about 30.0 ms/op.
```

Rejected and reverted. Even after accepting that duplicate checking can be
removed semantically, this did not improve random write throughput.

### 2026-09-09: Experiment 82, Narrow Timed Directory Sync

Patch:

```text
Tried changing the Sync-time repeat directory sync from syncing every ancestor
to syncing only the database directory. Open-time syncDir remained unchanged.
```

Result:

```text
Focused correctness tests: PASS.
Random-only 50x count=8 averaged about 30.3 ms/op.
```

Rejected and reverted. The syscall count changed in the expected direction, but
random write throughput did not improve.

### 2026-09-09: Experiment 83, Direct Checkpoint Instead of FlexSpace Redo for Bulk Initial

Patch:

```text
Temporarily set FlexSpace omitRedoLog during pristine bulk flush and forced the
final Sync to checkpoint the FlexTree directly.
```

Result:

```text
Focused correctness tests: PASS.
Random YogaDB:    28.23-33.91 ms/op.
Ascending YogaDB: 12.64-15.57 ms/op.
```

Rejected and reverted. The checkpoint path was slower than buffered redo for
this load size.

### 2026-09-09: Experiment 84, 8 KB Slotted Pages

Patch:

```text
Changed SLOTTED_PAGE_KB from 4 to 8 to reduce page/anchor/pwrite count.
```

Result:

```text
Focused correctness tests, including load-bloat coverage: PASS.
Random YogaDB: 27.93-32.30 ms/op.
Iterator read: 10.56-20.21 ns/key, with MB-scale allocations.
```

Rejected and reverted. Write speed was not better and read speed regressed by
roughly 3-5x.

### 2026-09-09: Experiment 85, Direct Ref Paging After Sort

Patch:

```text
Tried building cache-owned pages directly from sorted bulk refs after an
in-place dedup pass, avoiding the temporary page slice plus cache copy pattern.
```

Result:

```text
Focused correctness tests: PASS.
Random YogaDB: 27.52-31.18 ms/op, average about 29.5 ms/op.
```

Rejected and reverted. It did not beat the retained path and added too much
page-boundary complexity.

### 2026-09-09: Experiment 86, xxhash Slotted Page Checksum

Patch:

```text
Tried replacing slotted-page CRC32c with the existing xxhash dependency,
truncated to the current 4-byte checksum field. WAL/VLOG CRCs were unchanged.
```

Result:

```text
Focused slotted/recovery tests: PASS.
Random YogaDB:    29.00-31.67 ms/op.
Ascending YogaDB: 11.87-13.13 ms/op.
```

Rejected and reverted. The CPU profile showed hardware CRC32c was already a
small cost, and xxhash did not improve throughput.

### 2026-09-09: Experiment 87, Lazy Batch Puts Allocation

Patch:

```text
NewBatch no longer allocates the 10000-entry puts slice immediately. The first
Set/SetBytes/Delete allocates it with the same batchInitialPutCap. This avoids
the final empty-batch allocation in benchmarks and makes empty batches cheap.
```

Result:

```text
Focused correctness tests: PASS.

Random/ascending/write-read 50x count=6:
  Random YogaDB:    27.09-31.17 ms/op, about 21.52 MB/op.
  Ascending YogaDB: 11.06-12.78 ms/op, about 9.48 MB/op.
  Iterator read:    mostly 3.70-3.86 ns/key, with noisy outliers.
```

Kept provisionally. The timing gain is small/noisy, but it removes a real
timed allocation and does not change nonempty batch capacity.

### 2026-09-09: Experiment 88, Pool No-Pointer Sort Ref Arrays

Patch:

```text
Tried sync.Pool reuse for the no-pointer []bulkIngestRef order/sortAux arrays.
String scratch arrays were not pooled to avoid retaining key arenas.
```

Result:

```text
Focused correctness tests: PASS.
Random YogaDB: 29.80-31.77 ms/op.
```

Rejected and reverted. Allocation dropped, but write throughput did not improve.

### 2026-09-09: Experiment 89, Sort Refs Without Key Scratch Arrays

Patch:

```text
Tried removing keys/keyAux scratch arrays and sorting refs by looking keys up
from bulk segments during radix passes.
```

Result:

```text
Focused correctness tests: PASS.
Random YogaDB: 31.29-34.56 ms/op, about 18.3 MB/op.
Ascending YogaDB: 11.23-12.32 ms/op.
```

Rejected and reverted. It reduced allocation by about 3.2 MB/op but made random
write throughput materially worse; the extra key indirections beat the saved
memclr.

## Current State After Experiment 89

Retained current-code state:

```text
Full suite:
  go test ./... -count=1 -timeout=600s: PASS.

Current paired 20x count=5:
  Iterator read:    3.68-3.80 ns/key, 1320 B/op, 3 allocs/op.
  Random YogaDB:    28.07, 29.38, 31.43, 31.72, 31.81 ms/op.
  Random Pebble:    82.91, 83.95, 85.67, 86.30, 87.68 ms/op.
  Ascending YogaDB: 11.72, 11.76, 11.95, 12.49, 13.04 ms/op.
  Ascending Pebble: 60.88, 64.10, 64.56, 65.14, 65.24 ms/op.

Best retained random profile sample:
  Benchmark_LoadOnly_YogaDB: 28.73 ms/op, 21.52 MB/op, 3536 allocs/op.
```

Interpretation:

```text
Ascending writes now meet the 5x target against Pebble while preserving the
fast read path.

Random writes are still about 2.8-3.0x faster than Pebble, not 5x. The
remaining random path profile is split across:
  - sorting sorted-ref/key scratch and MSD radix passes;
  - Batch.SetBytes key copying and KV append/zeroing;
  - slotted page assembly/encoding/cache installation;
  - final Sync/syscall work.

The arena skiplist/wormhole idea is unlikely to help the current initial-load
benchmark by itself because this path already bypasses the B-tree memtable.
It could matter for non-pristine or read-before-sync workloads, but the current
random load bottleneck is not B-tree insertion.
```

Next credible large targets:

```text
1. Compact bulk staging representation for value-is-key batches:
   store key arena offsets/HLCs instead of full pointer-heavy KV structs, then
   materialize KVs only for cache-owned pages. This attacks Batch.SetBytes,
   GC scanning, and cache-copy pressure.

2. Optional bulk-load API or mode:
   let callers promise sorted/unique input, or value-is-key input, so YogaDB
   can avoid duplicate handling and generic validation paths without guessing.

3. Parallel bulk sort/page encode:
   only worth trying after target workloads are broader than NewCallID random;
   sort remains a real cost, but prior allocation-only sort changes worsened
   throughput.

4. Revisit page/cache layout:
   8 KB pages reduced allocations but destroyed read speed, so any larger-page
   attempt must fix cache residency/accounting first.
```

### 2026-09-09: Experiment 90, Stable Bulk Minimum HLC Page Base

Patch:

```text
Tracked the minimum HLC in bulkIngestBuilder and used it as the slotted page
base for compact all-small initial-load pages. The hypothesis was that random
key sort order mixes batch HLCs and causes repeated page-size recomputation
when a lower HLC appears after page assembly has started.
```

Result:

```text
Focused correctness tests: PASS.

All compact pages:
  Random YogaDB:    25.43-33.26 ms/op, average about 29.3 ms/op.
  Ascending YogaDB: 12.88-14.41 ms/op.

Unsorted-only compact pages:
  Random YogaDB:    28.66-32.63 ms/op, average about 30.9 ms/op.
  Ascending YogaDB: 11.49-12.94 ms/op.
```

Rejected and reverted. The all-page version hurt ascending writes, and the
unsorted-only version did not produce a reliable random-write improvement.

### 2026-09-09: Experiment 91, Preallocate Transient Page Slice To Interval Count

Patch:

```text
Changed the generic flushMemtableBulkInitial page assembly slice from cap 128
to cap flexdbSparseIntervalCount, avoiding grow-slice steps while assembling
random sorted pages.
```

Result:

```text
Focused correctness tests: PASS.
Random YogaDB:    28.32-31.06 ms/op, average about 30.0 ms/op.
Ascending YogaDB: 11.00-12.39 ms/op.
Allocation rose by about 120 KiB/op.
```

Rejected and reverted. The larger zeroed allocation did not give a clear
throughput win.

### 2026-09-09: Experiment 92, Approximate-Size Fast Paths

Patch:

```text
Avoided kvSizeApprox's per-record alias check when the caller already knew a
batch/page was value-is-key.
```

Result:

```text
Focused correctness tests: PASS.

appendBatch-only:
  Random YogaDB:    28.24-31.74 ms/op, average about 30.3 ms/op.
  Ascending YogaDB: 11.22-13.23 ms/op.

appendBatch plus page-cache sizing:
  Random YogaDB:    29.82-32.50 ms/op, average about 31.3 ms/op.
  Ascending YogaDB: 11.89-13.20 ms/op.
```

Rejected and reverted. This was instruction-count cleanup but not a
statistically useful write-throughput improvement.

### 2026-09-09: Experiment 93, Zero-HLC-Delta Encoder Branch

Patch:

```text
Special-cased HLC delta 0 in the value-is-key slotted size and encode loops.
```

Result:

```text
Focused correctness tests: PASS.
Combined with Experiment 92 page-cache sizing:
  Random YogaDB:    28.27-33.30 ms/op, average about 31.0 ms/op.
  Ascending YogaDB: 12.05-13.43 ms/op.
```

Rejected and reverted. The branch was in the hottest encoder loop and did not
pay for itself.

### 2026-09-09: Experiment 94, In-Place MSD Radix Sort

Patch:

```text
Added an American-flag-style in-place MSD radix sorter using the existing key
sidecar. This removed sortAux/keyAux allocations from unsorted bulk loads and
worked for both fixed-width and variable-width keys.
```

Result:

```text
Focused correctness tests: PASS.
Random YogaDB:    28.29-34.03 ms/op, average about 30.8 ms/op.
Ascending YogaDB: 11.53-12.92 ms/op.
Random allocation dropped from about 21.5 MB/op to about 19.1 MB/op.
```

Rejected and reverted. Allocation improved, but wall time did not; the extra
swap traffic was slower than the current out-of-place radix copy.

### 2026-09-09: Experiment 95, Nil Value-Is-Key KV Representation

Patch:

```text
Tried storing value-is-key KVs with nil Value and Vptr.Length == len(Key),
restoring the alias when reading from bulk or installing cache-owned pages.
```

Result:

```text
Focused correctness tests: PASS.
Random YogaDB:    29.74-34.30 ms/op, average about 31.9 ms/op.
Ascending YogaDB: 11.25-12.87 ms/op.
Iterator read:    mostly 3.7-3.8 ns/key but with more 5.5-6.1 ns/key outliers.
```

Rejected and reverted. Reducing pointer pressure was not enough to offset the
extra restoration logic and read-path noise.

### 2026-09-09: Experiment 96, Disable Sync-Time Anchor Tag Verification

Patch:

```text
Temporarily skipped the unconditional verifyAnchorTags() call in
writeLockHeldSync(), leaving Close-time verification intact.
```

Result:

```text
Focused correctness tests: PASS.
Random YogaDB:    27.07-33.10 ms/op, average about 30.0 ms/op.
Ascending YogaDB: 10.80-12.22 ms/op.
```

Rejected and reverted. It may slightly help sorted loads, but random throughput
did not move enough to justify weakening an always-on invariant check,
especially after the recent anchor/tag recovery warnings.

### 2026-09-09: Experiment 97, 6 KB Slotted Pages And Cache Shard Retuning

Patch:

```text
Changed SLOTTED_PAGE_KB from 4 to 6 as a middle point below the previously
rejected 8 KB pages. Then tried reducing intervalCachePartitionCount from 1024
to 512 to double per-shard cache capacity, because 6 KB decoded pages caused
immediate iterator cache churn under the default 32 MB cache.
```

Result:

```text
6 KB pages, 1024 cache partitions:
  Focused tests: PASS.
  Random YogaDB:    27.94-30.06 ms/op, average about 28.9 ms/op.
  Ascending YogaDB: 11.48-12.79 ms/op.
  Iterator read:    regressed to 5.2-13.8 ns/key with MB-scale allocations.

6 KB pages, 512 cache partitions:
  Random YogaDB:    29.56-31.76 ms/op, average about 30.7 ms/op.
  Ascending YogaDB: 11.19-13.09 ms/op.
  Iterator read:    restored to 3.58-3.73 ns/key, 1320 B/op.
```

Rejected and reverted. Larger pages can reduce allocation and sometimes improve
random writes, but the cache geometry must also preserve reads; after retuning
the cache shards, the random write win disappeared.

### 2026-09-09: Experiment 98, Bounded Parallel MSD Radix Sort

Patch:

```text
Added bounded goroutine recursion to the existing MSD radix sorter for both
fixed-width and variable-width keys. First threshold was 4096 entries; then
512 entries so the current random benchmark actually spawned bucket workers.
```

Result:

```text
Focused correctness tests: PASS.

Threshold 4096:
  Random YogaDB:    27.99-31.68 ms/op, average about 29.3 ms/op.
  Ascending YogaDB: 11.60-12.92 ms/op.
  This likely spawned little or no work after the top partition.

Threshold 512:
  Random YogaDB:    29.17-31.59 ms/op, average about 30.4 ms/op.
  Allocation increased to about 21.55 MB/op and 3660-3690 allocs/op.
```

Rejected and reverted. Real parallelism added overhead and did not improve
write throughput. The apparent threshold-4096 gain did not survive analysis
because the first-byte buckets were too small to spawn recursive workers.

### 2026-09-09: Experiment 99, uint32 Fixed-Radix Counters

Patch:

```text
Changed the fixed-key MSD radix count array from [256]int to [256]uint32,
halving the counter array zeroing footprint per recursion. The variable-width
path was left unchanged.
```

Result:

```text
Focused correctness tests: PASS.
Random-only 50x count=10: 27.88-30.67 ms/op, average about 29.0 ms/op.
Wider paired run:         26.96-32.43 ms/op, average about 30.1 ms/op.
Profile run:              30.72 ms/op.
```

Rejected and reverted. The random-only sample looked promising, but the wider
run and profile sample did not show a reliable improvement.

### 2026-09-09: Experiment 100, Two-Byte Fixed-Key Radix Pass

Patch:

```text
Added a fixed-length-key MSD radix helper that buckets two bytes at a time
using a 65536-entry count table, falling back to the existing 8-bit MSD path
for small buckets and one-byte tails.
```

Result:

```text
Focused correctness tests: PASS.
Random YogaDB:    26.47-30.66 ms/op, average about 29.2 ms/op.
Ascending YogaDB: 11.34-12.32 ms/op.
Iterator read:    normal 3.58-3.92 ns/key, one 5.5 ns/key outlier.
Profile run:      29.10 ms/op, but sort time did not actually fall.
Allocation rose by about 1 MB/op from the large count table.
```

Rejected and reverted. The wall-clock samples looked attractive, but line
profiling showed the total sort cost remained about the same; the 16-bit pass
mostly moved work around and increased allocation.
