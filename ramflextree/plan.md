# Plan: Extract `ramflextree` - Memory-Only KV Store from YogaDB

## Context

YogaDB is a high-performance sorted-order KV store that handles larger-than-memory data via a 3-layer architecture: FlexDB -> FlexSpace -> FlexTree. We want to extract a standalone, in-memory-only package called `ramflextree` at `~/yogadb/ramflextree/` that provides the same user-facing API but never touches disk. This gives developers a lightweight "starter kit" they can later upgrade to full YogaDB without API changes.

The approach: copy source files from `~/yogadb/` into `~/yogadb/ramflextree/`, change the package name to `ramflextree`, and surgically remove all disk I/O code (WAL, VLOG, CoW pages, redo log, recovery, fsync, VFS).

## Target

- Directory: `/Users/jaten/yogadb/ramflextree/`
- Package: `ramflextree`
- Module: `github.com/glycerine/yogadb/ramflextree` (own go.mod)
- No shared code with yogadb (fully independent, duplicated source)

## Dependencies (minimal)

- `github.com/tidwall/btree` - memtable B-tree (required)
- `github.com/glycerine/rbtree` - used by `omap` for deterministic, cache-friendly block storage (required)
- Standard library only otherwise
- Eliminated: `vfs`, `idem`, `greenpack`, `blake3`, `cristalhq/base64`, `4d63.com/tz`, `pebble`, `bbolt`, `porcupine`

---

## Files to Copy and Edit

### Group A: Copy verbatim (package rename only)

| File | Lines | Notes |
|------|-------|-------|
| `hlc.go` | 251 | Pure in-memory HLC. No external imports. |
| `format.go` | 111 | Number formatting. stdlib only. |
| `sparseindextree.go` | 462 | In-memory sparse index B-tree. No external imports. |
| `brute.go` | 312 | BruteForce oracle for testing. No imports. |
| `diff.go` | 97 | DB comparison. stdlib only. |

### Group B: Copy with minor edits

| File | Lines | Edits |
|------|-------|-------|
| `vprint.go` | 298 | Replace `4d63.com/tz` with `time.LoadLocation`. Remove `isNil` if unused. |
| `mathrand.go` | 293 | Replace `cristalhq/base64` with `encoding/base64`. |
| `rand.go` | 210 | Replace `cristalhq/base64` with `encoding/base64`. |
| `slotted.go` | 727 | Remove `slottedPageDumpWithVLog` (takes `*valueLog` param). Keep rest as-is. |
| `intervalcache.go` | 563 | No structural changes - disk I/O is behind FlexSpace abstraction which becomes RAM-only. |
| `tx.go` | 611 | Minor: FetchLarge delegates to db which becomes trivial. |
| `iter_doc.go` | 111 | Package rename only. |
| `omap.go` | 471 | Package rename only. Used by block manager for deterministic, cache-friendly RAM block storage. |

### Group C: Copy with moderate edits

| File | Lines | Key changes |
|------|-------|-------------|
| `flextree.go` | 1823 | Remove `vfs` import. Remove CoW fields from FlexTree struct (`fs`, `metaFD`, `nodeFD`, `maxSlotID`, `nodesFileCap`, `freeSlots`, `cowEnabled`, `metaNextOff`, `metaFileCap`, etc.). Remove `SlotID` from CommonNode. Remove `ChildSlotIDs` from InternalNode. Change `NewFlexTree(fs)` to `NewFlexTree()`. Remove `//go:generate greenpack`. Remove `zid` struct tags. All tree operations (Insert/Delete/Query/SetTag/Pos) unchanged. |
| `memtable.go` | 159 | Remove `vfs` import. Remove `memWalFD`, `memWalBuf`, `memWalMut`, `memWalBytesWritten`. Remove all WAL methods (`logAppend`, `logFlush`, `logSync`, `logTruncateWithVersion`, etc.). Change `newMemtable(fd)` to `newMemtable()`. Keep: btree, `put()`, `get()`, `reset()`, `empty`, `size`. Result: ~40-50 lines. |
| `iter.go` | 1753 | Simplify `FetchV()` to return `it.pKV.Value, nil`. Simplify `iterResolvedValue()` to always return inline value. `Large()` always returns false. Rest of iterator (Seek/Next/Prev/prefetch) unchanged. |

### Group D: Copy with heavy edits

| File | Lines | Key changes |
|------|-------|-------------|
| `flexspace.go` | 1503 | See "FlexSpace RAM Conversion" below. |
| `db.go` | 4351 | See "FlexDB Stripping" below. |

### Group E: Do NOT copy

| File | Reason |
|------|--------|
| `pages.go` (821) | CoW persistence - entirely disk-specific |
| `vlog.go` (405) | Value log for large values - eliminated |
| `b3.go` (22) | Blake3 for VLOG dedup |
| `flextree_gen.go` (5324) | greenpack serialization |
| `exists.go` (177) | VFS file helpers |
| ~~`omap.go`~~ | Moved to Group B (used by block manager for deterministic RAM blocks) |
| `btree_demo.go` (29) | Demo code |

---

## FlexSpace RAM Conversion (`flexspace.go`)

The core change: replace the single-buffer + file-descriptor block manager with an `omap[uint64, []byte]` holding all blocks in RAM. We use `omap` (from `omap.go`) instead of a builtin `map[uint64][]byte` because omap is deterministic (reproducible iteration order for testing) and much faster than the builtin map for repeated iteration thanks to its contiguous `ordercache` slice.

### blockManager changes

```
Current:                           RAM-only:
  buf []byte (single 4MB block)     blocks *omap[uint64, []byte] (all blocks in RAM)
  fdKV128blocks vfs.File             (removed)
  flushedOff uint64                  (removed - no flush/unflushed distinction)
```

- `write()`: write to `blocks.get(blkid)` instead of single `buf`. On block full, call `nextBlock()`.
- `nextBlock()`: remove `WriteAt` to file. Just switch `blkid`/`blkoff`. Allocate new `[]byte` via `blocks.set(newBlkid, make([]byte, FLEXSPACE_BLOCK_SIZE))`.
- `flush()`: no-op (no file to sync).
- `read()`: read from `blocks.get(blkid)` instead of trying single `buf` then falling back to file.
- `findEmptyBlock()`: unchanged (operates on `blkusage[]`).

### FlexSpace struct removals

Remove: `vfs`, `fdKV128blocks`, `redoLogFD`, `logBuf`, `logBufSize`, `logTotalSize`, `omitRedoLog`, `REDOLogBytesWritten`.

### Function changes

- `OpenFlexSpaceCoW()` -> `NewFlexSpace()`: no path, no files, create tree via `NewFlexTree()`, init block manager.
- `Close()`: nil out refs, return 0.
- `Sync()`/`syncR()`: no-ops.
- `readR()`: remove `ff.fdKV128blocks.ReadAt()` fallback - all reads from block manager's omap.
- `insertR()`: remove `ff.logWrite()` calls.
- `collapseR()`: remove `ff.logWrite()` calls.
- `setTagR()`: remove `ff.logWrite()` calls.
- `Overwrite()`: remove file `WriteAt` for flushed regions - just update the omap block.
- Remove: `logFull()`, `logWrite()`, `redoLogFlushAndSync()`, `logTruncate()`, `writeLogVersion()`, `logRedo()`, `bmCreateFromFile()`.
- Keep GC: it prevents unbounded memory growth by repacking blocks. Remove file I/O inside it.
- `truncateTrailingBlocks()`: free trailing blocks from omap via `blocks.delkey(blkid)` instead of file truncate.
- `garbageMetrics()`: keep as-is (reads `blkusage[]`).

### init() changes

Remove the `memtableWalBufCap` assertion (WAL buffer no longer exists).

---

## FlexDB Stripping (`db.go`)

### Imports to remove

`"os"`, `"path/filepath"`, `"github.com/glycerine/idem"`, `"github.com/glycerine/vfs"`.

### Config simplification

Remove: `NoDisk`, `FS`, `DisableVLOG`, `OmitFlexSpaceOpsRedoLog`, `OmitMemWalFsync`, `PiggybackGC_on_SyncOrFlush`, `GCGarbagePct`, `LowBlockUtilizationPct`.
Keep: `CacheMB`, `DisableBackgroundFlush`, `PaddedSplits`.

### FlexDB struct removals

Remove: `vfs`, `piggyGCStats`, `vlog`, `flushTrigger`, `flushHalt`, `MemWALBytesWritten`, `totalLogicalBase`, `totalPhysicalBase`.

### OpenFlexDB rewrite

- Path is informational only (stored but no dirs created).
- Create FlexSpace via `NewFlexSpace()`.
- Create memtable via `newMemtable()` (no WAL fd).
- Create sparse index tree and cache.
- No file creation, no recovery, no WAL truncation, no flush worker goroutine.

### Close simplification

- Remove flush worker halt.
- Flush memtable to in-memory FlexSpace.
- Destroy cache. Return metrics.
- No WAL truncation, no vlog sync/close, no directory sync.

### Sync simplification

- Flush memtable to FlexSpace (in-memory).
- No WAL sync, no vlog sync, no syncDir, no piggyback GC.

### Put simplification

- Remove VLOG path (all values inline regardless of size).
- Remove `mt.logAppend()` (no WAL).
- Keep: HLC tick, key size check, memtable put, key counter, inline flush when full.

### Get simplification

- Remove `resolveVPtr()` - always return inline value.

### Other removals

- `FetchLarge()`: return `kv.Value, nil`.
- `VacuumVLOG()`/`VacuumKV()`: return empty stats, nil error.
- `recovery()`, `replayLegacyWALs()`, `logRedo()`: remove entirely.
- `persistCounters()`: remove (no CoW metadata).
- `syncDir()`: remove entirely.
- `flushWorker()`: remove (flushes happen on Sync/Close/memtable-full only).
- `PiggybackGCStats`: remove.
- `lookupOldVPtr()`: remove.

### Kept intact

- Batch (Set/Delete/Commit - simplified to skip WAL/VLOG).
- KV, VPtr, KVcloser types (for API compat; Large() always false).
- kv128 encoding/decoding (used by slotted pages).
- `flushMemtable()`, `putPassthrough()`, `putPassthroughR()`, `treeInsertAnchor()`.
- `getPassthrough()`, `flexdbReadKVFromHandler()`.
- SearchModifier constants (LAZY_LARGE accepted but no-op).
- Metrics (simplified - zero disk fields, WriteAmp always 1.0).
- CheckIntegrity (keep in-memory consistency checks).
- DeleteRange/Clear/Merge (keep logic, VLOG paths removed).

---

## Test Files

### Tests to copy and adapt

| Source | Edits |
|--------|-------|
| `db_test.go` | New `openTestDB` helper (no VFS, calls `OpenFlexDB("", cfg)`). Remove VLOG/recovery tests. Remove `newTestFS`. |
| `batch_test.go` | Remove asset-loading tests (Test640, Test641). Remove doFsync-specific tests. |
| `iter_test.go` | Remove VLOG fetch tests. Simplify Large() assertions. |
| `tx_test.go` | Mostly as-is. Remove VLOG assertions. |
| `find_test.go` | Remove LAZY_LARGE VLOG tests. |
| `write_test.go` | Should work after VLOG path removal. |
| `flextree_test.go` | Change `NewFlexTree(vfs.NewMem())` to `NewFlexTree()`. Remove VFS imports. |
| `brute_test.go` | Same as flextree_test.go. |
| `flexspace_test.go` | Replace `OpenFlexSpaceCoW(...)` with `NewFlexSpace()`. Remove file assertions. |
| `slotted_test.go` | Package rename, remove vlog dump test if any. |
| `sparseindextree_test.go` | Package rename only. |
| `intervalcache_test.go` | Replace DB setup with RAM-only. |
| `hlc_test.go` | Package rename only. |
| `format_test.go` | Package rename only. |

### Tests NOT copied

recovery_test.go, pages_test.go, gc_test.go, gc_debug_test.go, vlog_test.go, load_bloat_test.go, vacuum_bloat_test.go, vacuum_vlog_then_kv_test.go, porc_test.go, tesser_test.go, memfs_test.go, realfs_test.go, flextree_gen_test.go, all *_fuzz_test.go, all *_bench_test.go with external deps.

### New test helper

```go
func openTestDB(t *testing.T, cfg *Config) *FlexDB {
    t.Helper()
    if cfg == nil {
        cfg = &Config{}
    }
    db, err := OpenFlexDB("", cfg)
    if err != nil {
        t.Fatalf("OpenFlexDB: %v", err)
    }
    t.Cleanup(func() { db.Close() })
    return db
}
```

---

## Implementation Order

1. Create `~/yogadb/ramflextree/` dir and `go.mod`
2. Copy Group A files (verbatim + package rename): hlc, format, sparseindextree, brute, diff
3. Copy Group B files (minor edits): vprint, mathrand, rand, slotted, intervalcache, tx, iter_doc
4. Copy and edit `flextree.go` (remove CoW/VFS fields, greenpack tags)
5. Copy and edit `memtable.go` (strip WAL to ~40 lines)
6. Copy and edit `flexspace.go` (RAM block manager - hardest step)
7. Copy and edit `db.go` (strip VLOG/WAL/recovery/flush worker - largest step)
8. Copy and edit `iter.go` (simplify VLOG resolution)
9. `go build ./ramflextree/` - fix compilation errors iteratively
10. Copy and adapt test files (start with trivial ones, work up to db_test.go)
11. `go test ./ramflextree/` - fix test failures

## Verification

- `go build ./ramflextree/` compiles cleanly
- `go test ./ramflextree/` - all copied tests pass
- `go vet ./ramflextree/` - no issues
- Grep for `vfs`, `idem`, `greenpack`, `blake3` in ramflextree/ - zero hits
- Grep for `ReadAt`, `WriteAt`, `Sync()`, `Truncate`, `MkdirAll` in ramflextree/ - zero file I/O
- Verify `go.mod` has only `tidwall/btree` and `glycerine/rbtree` as dependencies
