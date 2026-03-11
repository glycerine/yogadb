# Plan: Automatic Background GC for YogaDB

## Context

YogaDB's FlexSpace GC is currently **synchronous and demand-driven**: it only runs when a write needs a new block and free blocks < 64. This mirrors the C reference implementation exactly. For workloads with heavy overwrites/deletes, fragmentation can accumulate in `FLEXSPACE.KV128_BLOCKS` without being reclaimed until the system runs critically low on blocks. The user wants a `Config` option to periodically trigger GC proactively.

## Research Summary

### C Reference (study/flexspace/flexfile.c)
- **No background/periodic GC.** Purely demand-driven via `flexfile_bm_find_empty_block()`.
- 4-round algorithm with progressively aggressive utilization thresholds (6.25% → 12.5% → 25% → 75%).
- Queue-based: scans FlexTree leaves, queues up to 8192 extents, relocates to new blocks.
- Paper says GC overhead is negligible; exploits spatial locality of hot segments.

### Go Port (flexspace.go)
- Faithful port of same 4-round algorithm in `FlexSpace.GC()` (line 1272).
- `gcNeeded()` (line 1099): `freeBlocks < FLEXSPACE_GC_THRESHOLD (64)`.
- Auto-triggered from `findEmptyBlock()` (line 158) during writes.
- `garbageMetrics()` (line 1081) scans 204K `blkusage[]` entries (~800KB, sub-millisecond).
- Config has `LowBlockUtilizationPct` for metrics only.

### LLM Suggestions (gc_suggestions.txt) - Evaluated

| Suggestion | Verdict | Notes |
|-----------|---------|-------|
| 1. Segment-Based GC (LFS) | **Already implemented** | YogaDB's 4-round GC with `blkusage[]` counters *is* segment-based GC. The cost-benefit heuristic is the 4-round threshold progression. |
| 2. Filesystem Hole Punching | **Future consideration** | `fallocate(PUNCH_HOLE)` on Linux could reduce physical disk usage without data movement. macOS lacks support. Not in scope for this task. |
| 3. Locality-Aware Leaf Defrag | **Orthogonal** | Improves read performance, not space reclamation. Could be a separate feature. |
| 4. KV Separation (WiscKey) | **Already implemented** | YogaDB has VLOG for large values (see `valueLog` in db.go). |
| 5. Hybrid In-Place Updates | **Risky** | Breaks append-only invariant. Crash safety concerns. Not recommended. |

**Conclusion:** The existing GC algorithm is sound. The gap is *when* it runs, not *how*. We need proactive triggering, not a new algorithm.

---

## Approach: Two Trigger Modes

### Mode A: Piggyback on Sync/Flush (default, no new goroutine) - we did this

Run GC at the end of `writeLockHeldSync()` and `doFlush()` when the write lock is already held and FlexSpace is in a consistent state.

**Pros:**
- Zero new goroutines (simpler for embedded use)
- Zero new locking complexity - write lock already held
- GC runs at natural consistency points (after Sync)
- Simple to reason about: GC happens when you call Sync, not randomly

**Cons:**
- If the user never calls Sync(), auto-GC only fires from the flush timer (every 5 seconds)
- GC latency is added to Sync() call - callers see higher Sync latency when GC triggers
- Not truly "periodic" - tied to write activity patterns

### Mode B: Dedicated Background Goroutine (opt-in via `AutoGCBackground`) - we skipped for now

A GC goroutine that wakes every `AutoGCEveryDur`, acquires the write lock, checks metrics, and runs GC if warranted. Useful for read-heavy workloads where Sync is infrequent.

**Pros:**
- Truly periodic - runs even if the user is idle (useful for background compaction during read-heavy phases)
- GC latency is NOT added to user-visible Sync() calls
- Familiar goroutine pattern (same as existing flush worker)

**Cons:**
- Adds a background goroutine
- Can stall writers at unpredictable times (GC holds write lock)
- More shutdown/lifecycle complexity
- If GC runs during a burst of writes, it competes for the write lock

---

## Config Fields (db.go ~line 533)

```go
// AutoGCEveryDur sets the minimum interval between automatic GC runs.
// GC is checked at the end of Sync() and flush operations; if this
// duration has elapsed since the last GC AND garbage exceeds
// AutoGCGarbagePct, GC runs. Zero (default) disables automatic GC.
AutoGCEveryDur time.Duration

// AutoGCGarbagePct is the minimum fraction of wasted bytes in used
// blocks (garbage / (garbage + live)) required to trigger automatic GC.
// Default 0.50 (50%) when zero and AutoGCEveryDur is set.
// Prevents GC from running when fragmentation is low.
AutoGCGarbagePct float64

// AutoGCBackground enables a dedicated background goroutine that
// wakes every AutoGCEveryDur to check and run GC. When false
// (default), GC only runs piggybacked on Sync/flush calls.
// Requires AutoGCEveryDur > 0 to have any effect.
AutoGCBackground bool
```

## FlexDB Fields (db.go ~line 541)

```go
autoGCLastRun time.Time
autoGCStats   AutoGCStats
gcStop        chan struct{}   // only used when AutoGCBackground=true
gcWG          sync.WaitGroup // only used when AutoGCBackground=true
```

## New Types

```go
type AutoGCStats struct {
    LastGCTime     time.Time
    LastGCDuration time.Duration
    TotalGCRuns    int64
}
```

## New Methods (db.go)

### `maybeAutoGC()` - shared gate logic (used by both modes)

```go
func (db *FlexDB) maybeAutoGC() {
    if db.cfg.AutoGCEveryDur <= 0 {
        return
    }
    now := time.Now()
    if now.Sub(db.autoGCLastRun) < db.cfg.AutoGCEveryDur {
        return // cooldown not elapsed
    }
    threshold := db.cfg.AutoGCGarbagePct
    if threshold <= 0 {
        threshold = 0.50
    }
    live, garbage, _, _ := db.ff.garbageMetrics(db.cfg.LowBlockUtilizationPct)
    total := live + garbage
    if total == 0 || float64(garbage)/float64(total) < threshold {
        return // not enough garbage to justify GC
    }
    start := time.Now()
    db.ff.GC()
    db.autoGCLastRun = now
    db.autoGCStats.LastGCTime = now
    db.autoGCStats.LastGCDuration = time.Since(start)
    db.autoGCStats.TotalGCRuns++
}
```

### `gcWorker()` - background goroutine (Mode B only)

```go
func (db *FlexDB) gcWorker() {
    defer db.gcWG.Done()
    ticker := time.NewTicker(db.cfg.AutoGCEveryDur)
    defer ticker.Stop()
    for {
        select {
        case <-db.gcStop:
            return
        case <-ticker.C:
            db.topMutRW.Lock()
            if !db.closed {
                db.maybeAutoGC()
            }
            db.topMutRW.Unlock()
        }
    }
}
```

## Call Sites

### Mode A insertion points (always active when AutoGCEveryDur > 0)

1. **`writeLockHeldSync()`** (db.go:1881) - after `db.ff.Sync()`:
   ```go
   db.ff.Sync()
   db.maybeAutoGC()  // <-- insert here
   db.verifyAnchorTags()
   ```

2. **`doFlush()`** (db.go:3461) - after `db.ff.Sync()`:
   ```go
   db.ff.Sync()
   db.maybeAutoGC()  // <-- insert here
   ```

### Mode B startup/shutdown

- **`OpenFlexDB()`**: If `cfg.AutoGCBackground && cfg.AutoGCEveryDur > 0`, init `gcStop = make(chan struct{})`, start `go db.gcWorker()` with `gcWG.Add(1)`.
- **`Close()`**: If `gcStop != nil`, `close(gcStop)` then `gcWG.Wait()`.

## Metrics (db.go Metrics struct ~line 945)

Add to `Metrics`:
```go
AutoGCRuns      int64
AutoGCLastDurMs int64
```

Populate in `SessionMetrics()`, `CumulativeMetrics()`, `finalMetrics()`.

## Defaults
- `AutoGCEveryDur = 0` → disabled (opt-in)
- `AutoGCGarbagePct = 0` → default 0.50 when AutoGCEveryDur > 0
- `AutoGCBackground = false` → sync-piggyback only (Mode A)

## Why the Garbage Threshold?

Without `AutoGCGarbagePct`, every Sync after `AutoGCEveryDur` would call `ff.GC()`, which does a 4-round scan even if `gcNeeded()` returns false (free blocks >= 64). The `garbageMetrics()` check is sub-millisecond (simple loop over blkusage array) and short-circuits when fragmentation is low.

## Files to Modify

| File | Changes |
|------|---------|
| `db.go` | Config fields, FlexDB fields, `maybeAutoGC()`, `gcWorker()`, `AutoGCStats` type, Metrics fields, call sites in `writeLockHeldSync` and `doFlush`, startup in `OpenFlexDB`, shutdown in `Close` |
| `gc_test.go` | Add auto-GC tests (see below) |

## Verification

1. **Unit tests** in `gc_test.go`:
   - `TestAutoGC_TriggersAfterDuration` - set 1ms interval, create fragmentation, Sync twice, verify `TotalGCRuns > 0`
   - `TestAutoGC_RespectsGarbageThreshold` - set threshold=0.99, verify GC does NOT run
   - `TestAutoGC_CooldownPreventsStorm` - set 1s interval, Sync 10x rapidly, verify at most 1 GC run
   - `TestAutoGC_DisabledByDefault` - default config, verify no auto-GC
   - `TestAutoGC_ReclaimsSpace` - write, delete half, enable auto-GC, Sync, check free blocks increased
   - `TestAutoGC_BackgroundGoroutine` - set `AutoGCBackground=true`, create fragmentation, sleep for 2x interval, verify GC ran without explicit Sync

2. **Run full test suite**: `go test ./...` - ensure no regressions

## Critical Files

- `db.go` - Config (line 498), FlexDB struct (line 541), `writeLockHeldSync` (line 1857), `doFlush` (line 3440), `Close` (line 870), Metrics (line 945)
- `flexspace.go` - `FlexSpace.GC()` (line 1272), `garbageMetrics()` (line 1081), `gcNeeded()` (line 1099)
- `gc_test.go` - existing GC test patterns, test helpers (`openTestDB`, etc.)
