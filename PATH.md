-------------

me: this is about func (db *FlexDB) rebuildAnchorsFromTags() at db.go:4052 right?
I think the line numbers have shifted(!)

Claude transcript:


Response:
       So the function returns KV{}, false if:
       1. Read fails (line 4169)
       2. Header size check fails (line 4178)
       3. Buffer too small (line 4184)
       4. Read of full data fails (line 4188)
       5. slottedPageFirstKey fails (line 4192)
       6. Legacy format parsing fails (lines 4199-4208)

       If any of these fail, the anchor is silently skipped during recovery!

       This is a MAJOR BUG. If an anchor's first KV can't be read for any reason, it will be completely missing from the rebuilt sparse index, and all keys in that anchor will be lost!

       The key question is: could a valid anchor that was flushed and had its tag set be in a state where flexdbReadKVFromHandler returns false?

       Yes! Especially if there's a crash between when the anchor's tag is set and when the data is fully written/synced. Or if there's a bug in the key/value encoding/decoding.

       Let me now provide my analysis to the user by writing a summary document. But wait - I'm in read-only mode. Let me just provide my findings directly.

       Summary of Investigation: Key Loss in FlexDB Recovery Path

       I've completed a thorough exploration of the FlexDB recovery and reopen path. Here are the key findings:

       Critical Issue Found: Silent Anchor Loss During Recovery

       Location: /Users/jaten/go/src/github.com/glycerine/yogadb/db.go, function rebuildAnchorsFromTags() lines 4046-4111

       Root Cause: When rebuilding the sparse index tree from FlexSpace tags during recovery (OpenFlexDB → recovery() → rebuildAnchorsFromTags()), the code scans all extents and looks for those with anchor tags. However, if the first KV in an anchor cannot be read (via flexdbReadKVFromHandler at line 4070), the entire anchor is silently skipped and never added to the rebuilt sparse index (see lines 4070-4077):

       kv, ok := flexdbReadKVFromHandler(fh, kvbuf)
       if ok {
           var anchorKey string
           if loff > 0 {
               anchorKey = kv.Key
           }
           anchors = append(anchors, anchorInfo{key: anchorKey, loff: loff, unsorted: unsorted})
       }
       // If !ok, the anchor is silently NOT added to the anchors list

       This means all keys in that anchor become unreachable after reopen, even though they exist in FlexSpace.

       Why This Happens: Multiple Failure Modes in flexdbReadKVFromHandler

       The function flexdbReadKVFromHandler() (lines 4163-4209) can return ok=false in several scenarios:

       1. Read failure: Lines 4169, 4188, 4203-4205
       2. Buffer too small: Lines 4184
       3. Invalid header: Line 4178
       4. Decoding failure: Lines 4192, 4200, 4208

       Any of these could cause a valid but damaged anchor to be skipped during recovery.

       Why Key "k0016" Could Be Lost

       If a key like "k0016" was:
       1. Flushed to FlexSpace and tagged as an anchor (SetTag called at line 3679 or 3827/3830)
       2. Subsequently, the FlexSpace data becomes partially corrupted or unreadable
       3. On reopen, flexdbReadKVFromHandler fails to decode the first key
       4. The anchor is silently skipped (line 4076 condition fails)
       5. The entire interval/anchor is orphaned in FlexSpace
       6. Get("k0016") then searches the rebuilt sparse index, can't find the anchor, and returns "not found"

       Recovery Path Flow

       The recovery path is:
       OpenFlexDB()
         ├─ OpenFlexSpaceCoW()    [restores FlexTree and CoW pages]
         ├─ recovery()            [called if ffSize > 0, line 970]
         │   ├─ rebuildAnchorsFromTags()
         │   │   ├─ GetHandler(0)
         │   │   ├─ Iterate all extents
         │   │   ├─ For each extent with anchor tag:
         │   │   │   ├─ flexdbReadKVFromHandler() <- CRITICAL: may silently fail
         │   │   │   └─ Add to anchors list (only if read succeeds)
         │   │   └─ Build sparse index from collected anchors
         │   └─ logRedo()        [replay WAL if present]
         └─ Return to caller

       Related Code: Serialization Format

       The anchor data is serialized in two formats:
       1. Slotted page format (checked at line 4174): Header magic byte + slots
       2. Legacy kv128 format (fallback at line 4198): Size prefix + encoded KV

       Line 4174 checks: if header[0] == slottedPageMagic

       If the first byte is corrupted or not set correctly, the wrong format parser is used, leading to decode failures.

       Existing Diagnostic Code

       There's a function verifyAnchorTags() (lines 3835-3885) that verifies anchors have tags, and clampAnchorPsizes() (lines 3972-4041) that handles oversized anchors from missing split tags. These hint at the developers being aware of tag-related issues.

       Get Path After Reopen

       After recovery, Get() uses someLockHeldGet() (line 2774) which:
       1. Checks active and inactive memtables
       2. Falls through to getPassthrough() (line 2818)
       3. getPassthrough() searches the sparse index via findAnchorPos() (line 3577)
       4. If the anchor for key k0016 was skipped during recovery, findAnchorPos() returns the previous anchor
       5. The interval cache loads that anchor and searches for k0016 within it
       6. Since k0016 is in a different (orphaned) anchor, it's not found and returns "not found"

       Critical Code Sections

       - Recovery scan: /Users/jaten/go/src/github.com/glycerine/yogadb/db.go lines 4063-4080 (iterates extents, collects anchors with tags)
       - Anchor list building: lines 4082-4111 (builds sparse index from collected anchors)
       - Silent failure point: lines 4070-4077 (skips anchor if KV can't be read)
       - Get via sparse index: /Users/jaten/go/src/github.com/glycerine/yogadb/db.go lines 3575-3605 (searches sparse index, not orphaned anchors)

       Immediate Action

       The fix should be to either:
       1. Log/panic on anchor read failure instead of silently skipping (make the corruption visible)
       2. Gracefully handle partially-readable anchors (e.g., use a generic key name if the first key can't be decoded)
       3. Validate anchor tags during flush to ensure they're only set when data is fully written
  Done (41 tool uses  70.3k tokens  4m 6s)

 Good exploration results, but I need to verify the actual failure more precisely. The agents gave me two theories - let me dig deeper into the specific code paths. Let me first decode the actual operations from the seed, and look more carefully at the recovery path.

--------------
All passing. Here's a summary of what was done:

  Changes for fail-stop panics on anchor corruption

  db.go - flexdbReadKVFromHandler: All 8 failure paths now:
  - Always call alwaysPrintf (visible even if panic is recovered)
  - Call panicf if panicOnFailure is true

  db.go - rebuildAnchorsFromTags: Now takes panicOnFailure bool:
  - panicOnFailure=true (from VacuumKV): panics with diagnostic if any anchor's first key can't be read - this is data corruption
  - panicOnFailure=false (from crash recovery): logs a WARNING via alwaysPrintf but skips the anchor - this is expected when WAL replay creates partial
  extents from unsynced writes

  Call sites:
  - VacuumKV (line 1843): rebuildAnchorsFromTags(true) - data must be intact
  - recovery() (line 4134): rebuildAnchorsFromTags(false) - incomplete WAL replay is expected after crash

 Worked for 4m 53s
commit atop of 516234f
------------

All pass. To summarize the two fixes:

Fix 1 - VacuumKV cross-block extent (db.go ~line 1709): VacuumKV 
wrote extents sequentially without checking 4 MB
block boundaries. An extent could straddle two blocks, corrupting 
blkusage[] tracking. Fixed by skipping to the next
block boundary when an extent won't fit.
  
Fix 2 - bmInit partial-block reuse (flexspace.go bmInit): 
After close+reopen, bmInit always picked a new empty block
to start writing. Even a handful of small ff.Update calls (from 
HLC delta changes) would allocate an entire new 4 MB
block, adding ~4 MB of "free" space per session. Fixed by 
tracking the high-water mark within each block during the
tree walk and continuing in the partially-filled block with 
the most remaining space, loading its existing data into
the write buffer.

Debug counters (updateCount, updateGarbageBytes, insertCount, insertBytes) 
remain on FlexSpace and are incremented on every Update/Insert call.
Print statements are commented out but easily re-enabled.

----

# Plan: Eliminate kv128 format from KV128_BLOCKS

## Context

KV128_BLOCKS currently contains two on-disk formats: slotted pages and kv128 (with kv128ExtentMagic prefix). The kv128 format only enters KV128_BLOCKS via VacuumVLOG, which re-encodes intervals after updating VPtrs. All normal write paths (putPassthroughInitial, putPassthroughR, treeInsertAnchor, flushDirtyPages, flushDirtyEntry) already use slotted page format. Having two formats creates complexity in every read path (loadInterval, decodeIntervalDirect, flexdbReadKVFromHandler, VacuumKV, CheckIntegrity) and caused the bloat regression (format mismatch -> ff.Update garbage).

The MEMWAL continues to use kv128 encoding (no magic prefix) - that is unaffected.

## Approach

**Change VacuumVLOG to write slotted page format instead of kv128.**

The one concern: kv128 is open-ended (no size limit), while slotted pages have a target size. However, VacuumVLOG replaces VPtrs (8-byte offset + 8-byte length = 16 bytes) with... new VPtrs (same 16 bytes). The interval size doesn't change. The only size change is the format overhead difference between kv128 and slotted page encoding of the same KVs. Since slotted pages already handle VPtrs natively (slottedValInfoVPtr sentinel), this is safe.

For intervals that would exceed slottedPageMaxSize after re-encoding as slotted page: use slottedPageEncode (tight, no padding) which has no size limit - it just produces a page without zero-padding.

### Step 1 - VacuumVLOG: write slotted page instead of kv128

**db.go VacuumVLOG (~line 1517-1523):**
```
// OLD:
buf := db.itvbuf[:0]
buf = append(buf, kv128ExtentMagic[:]...)
for i := 0; i < fce.count; i++ {
    buf = kv128Encode(buf, fce.kvs[i])
}

// NEW:
buf := slottedPageEncode(fce.kvs[:fce.count])
```

NOTE: Step 1 has already been applied to the code.

### Step 2 - Simplify read paths (remove kv128 detection from KV128_BLOCKS reads)

Remove kv128ExtentMagic handling from:
- `loadInterval` (intervalcache.go ~line 421-436)
- `decodeIntervalDirect` (db.go ~line 3495-3507)
- `flexdbReadKVFromHandler` (db.go ~line 4290-4320)
- `VacuumKV` read path (db.go ~line 1695-1704) - already handles slotted pages
- `CheckIntegrity` (db.go) - if it has kv128 detection

For safety, replace kv128 detection with a panic("unexpected kv128 format in KV128_BLOCKS") so old-format databases fail loudly rather than silently.

### Step 3 - Update tests

- Update `TestVacuumVLOG_ThenVacuumKV` to verify slotted page format after vacuum
- Verify `TestVacuumThenOverwrite_DiskSizeBounded` still passes (should improve since no format mismatch)
- Run full test suite

## Files to modify

- `db.go` - VacuumVLOG write path, decodeIntervalDirect, flexdbReadKVFromHandler, CheckIntegrity
- `intervalcache.go` - loadInterval read path
- `vacuum_vlog_then_kv_test.go` - verify format after vacuum

## Verification

```bash
go test -tags memfs -timeout 300s -count=1 ./...
go test -run TestVacuumThenOverwrite_DiskSizeBounded -v -count=1 -timeout 600s
go test -run TestVacuumVLOG_ThenVacuumKV -v -count=1 -timeout 60s
```


All tests pass. Here's a summary of the changes:

  Step 1 (already done): VacuumVLOG already uses slottedPageEncode at db.go:1521.

  Step 2 - Replaced kv128 read paths with panics on unexpected format:

  - intervalcache.go:419-438 (loadInterval): Replaced kv128 decode loop with a panic if non-slotted trailing data is
  found.
  - db.go:3492-3507 (decodeIntervalDirect): Same replacement.
  - db.go:4321-4369 (flexdbReadKVFromHandler): Removed the entire kv128 extent handling block (40+ lines). Unknown
  magic now always produces an error/panic.
  - db.go:2148-2178 (CheckIntegrity): Replaced kv128 decode loop with an error report for unexpected non-slotted
  trailing bytes.

  Step 3 - Updated test comments in vacuum_vlog_then_kv_test.go to reflect that both vacuums now use slotted page
  format exclusively.

  The kv128 encode/decode functions themselves remain - they're still used by MEMWAL (memtable.go:66, db.go:250) and
  WAL replay (logRedo at db.go:4400), plus unit tests in db_test.go.

---------
  Why dead space isn't reused inline - it's a structural property of log-structured allocation:

  The block manager maintains a single write cursor (blkid + blkoff) that only moves forward within a block. bm.write() copies data at bm.blkoff and advances it. There is no mechanism to seek backward within a partially-used block to fill a "hole" left by a collapsed extent.

  When ff.Update runs (line 971-974), it does:
  1. collapseR(loff, olen) - this decrements blkusage[blkid] for the old extent's bytes, but the physical bytes remain in the block untouched
  2. insertR(buf, loff, length) - this appends the new data at bm.offset() (the current write cursor), which is always at the end of the current block

  The dead bytes from step 1 are still physically sitting in the block. The blkusage counter knows they're dead, but the block manager has no free-space map within a block - it only tracks one number per block (total live bytes). It doesn't know where the holes are within the 4 MB block.

  Why it's designed this way (not just philosophical purity):

  1. No intra-block free map: blkusage[] is a single uint32 per block - total live bytes. To reuse holes you'd need a proper allocator (free list or bitmap) within each 4 MB block, tracking the offset and size of every dead region. That's a significant data structure.
  2. Extent fragmentation: If you filled holes, a single logical page might span multiple non-contiguous physical regions within a block (or across blocks). The FlexTree can handle this (it already splits extents), but reads become more scattered.
  3. GC is the designed reclamation path: The architecture's answer is: when a block's blkusage drops low enough, GC rewrites the entire block - copying only live extents to fresh space. This is the standard log-structured approach (like LSM compaction or log-structured filesystems).
  4. findEmptyBlock only recycles completely empty blocks: Line 162-164 shows it only reuses blocks where blkusage[i] == 0. A block that's 90% dead but 10% live is never reused - only GC can reclaim it.

  So the real question isn't "why no free list" but rather "why isn't GC running during loads?" And the answer is: PiggybackGC_on_SyncOrFlush is off by default, and the test disables it. The dead space accumulates because nothing triggers GC during a bulk load.

  This is also why Option B (tight splits) helps so much - it doesn't eliminate dead space, but it makes the dead extents much smaller (tight ~27 KB instead of padded 64 KB), so less total garbage accumulates.

  Let me update the plan file with this understanding and the implementation plan.

⏺ Reading Plan(~/.claude/plans/rippling-chasing-matsumoto.md)
  ⎿  Read 124 lines

⏺ The plan file already matches what the user's prompt asked me to implement. This is the same task. Let me update it with the dead-space understanding and keep it focused.

⏺ Updated plan
  ⎿  Added 23 lines
       88  increment (e.g. 2x content or +4KB). This is more complex but eliminates all padding waste.
       89  Risk: more `ff.Update` calls during update workloads, more FlexSpace garbage.
       90
       91 +## Why Dead Space Is Not Reused Inline
       92 +
       93 +FlexSpace is log-structured. The block manager has a single write cursor (`blkid` + `blkoff`)
       94 +that only moves forward. `bm.write()` appends at `bm.blkoff`; there is no mechanism to
       95 +seek backward within a block to fill holes left by collapsed extents.
       96 +
       97 +`ff.Update` (flexspace.go:962) does `collapseR` then `insertR`:
       98 +- `collapseR` decrements `blkusage[blkid]` but the physical bytes stay in the block
       99 +- `insertR` appends new data at `bm.offset()` (end of current block)
      100 +
      101 +The dead bytes are tracked only as a count (`blkusage` is one `uint32` per block - total
      102 +live bytes, not a map of where holes are). To reuse holes you'd need a per-block free-space
      103 +allocator (bitmap or free list) tracking offset+size of every dead region.
      104 +
      105 +`findEmptyBlock` (line 157) only recycles **completely empty** blocks (`blkusage[i] == 0`).
      106 +A block that is 90% dead / 10% live is never reused - only GC can reclaim it by rewriting
      107 +the live extents to fresh space and freeing the entire block.
      108 +
      109 +This is the standard log-structured design tradeoff: simple append-only writes at the cost
      110 +of background GC. The issue here is that GC doesn't run during bulk loads (PiggybackGC is off).
      111 +
      112  ## Recommended Approach: Idea B (tight splits)
      113
      114  Simplest change with the biggest impact. One function to modify.
      115 +Does not eliminate dead space, but makes dead extents much smaller (tight ~27 KB vs padded 64 KB),
      116 +so less total garbage accumulates.
      117
      118  ### Implementation
      119
  ⎿  1 PostToolUse hook ran
-----------

# Reduce Space Amplification at SLOTTED_PAGE_KB=64

## Context

At `SLOTTED_PAGE_KB=64`, loading 434K keys (22.8 MB raw) produces 68 MB on disk (3.13x).
At `SLOTTED_PAGE_KB=4`, the same load produces 49 MB (2.27x).
The difference is entirely in `KV.SLOT_BLOCKS`: 64 MB vs 47 MB.
Goal: get SLOTTED_PAGE_KB=64 under 3x (ideally close to 2.27x).

## Root Cause Analysis

### Byte-level accounting (SLOTTED_PAGE_KB=64)

| Component | Bytes | Notes |
|-----------|-------|-------|
| Raw key+value data | 22.8 MB | 434K keys, value=key |
| Per-key overhead | ~4.7 MB | 2B keyLen + 2B valInfo + ~1B HLC varint = ~5B * 434K |
| Page headers + CRCs | ~14 KB | 27B header + 4B CRC per page, ~435 pages |
| **Subtotal (tight)** | **~27.5 MB** | What all pages would total if tight-encoded |
| VLOG (large vals) | 3.7 MB | Stored separately |
| REDO.LOG + metadata | ~0.4 MB | Small |
| **Subtotal (logical)** | **~31.6 MB** | |

So where does the other ~36 MB come from?

### Source 1: Split padding waste (~14 MB)

When an anchor reaches `flexdbSparseIntervalCount` (1000 keys), `treeInsertAnchor` splits it 50/50.
Both halves are padded to `slottedPageMaxSize` = 64 KB via `slottedPageEncodePadded`.
A half-full 64 KB page wastes ~32 KB. With ~435 anchors, that's ~435 * 32KB / 2 = ~7 MB average.
But the left half was also grown from tight to 64KB, adding more waste.

### Source 2: FlexSpace garbage from Update/Insert during splits (~20 MB)

When `treeInsertAnchor` fires:
1. Left half: `ff.Update(padded64KB, ..., oldTightSize)` - the old tight extent becomes garbage
2. Right half: `ff.Insert(padded64KB, ...)` - 64 KB new extent

Each split generates `oldTightSize` bytes of garbage (the old tight page).
With ~435 splits and average tight size ~27.5MB/435 = ~63KB each, that's ~27 MB of garbage.
Without `PiggybackGC_on_SyncOrFlush`, this garbage is never reclaimed during the load.

At SLOTTED_PAGE_KB=4, the same splits only pad to 4 KB, so waste per split is 16x smaller.

### Source 3: Block-level rounding (up to 4 MB)

FlexSpace allocates in 4 MB blocks. The last block can waste up to 4 MB.
At 64 MB total, 16 blocks are fully used (metrics show 65 KB free = nearly zero block waste).

## Reduction Ideas

### Idea A: Tight-encode on initial flush, pad only on first update (ALREADY DONE)

`putPassthroughInitial` already encodes tight (line 3531). But `treeInsertAnchor` immediately
pads both halves to 64 KB (lines 3630, 3644). The split is the main padding point.

### Idea B: Tight-encode splits too (defer padding to first dirty flush)

Change `treeInsertAnchor` to encode both halves **tight** instead of padded.
The page will be grown to `slottedPageMaxSize` only when `flushDirtyPages` or
`flushDirtyEntry` finds `content > psize` (which already handles this case, lines 143-173).

**Savings**: ~14 MB (split padding waste) + ~14 MB (less garbage from smaller Updates)
**Risk**: The first update to each post-split page will trigger an `ff.Update` to grow it.
For a pure initial load followed by Sync, there are no updates - so pages stay tight.
For update-heavy workloads, the first update grows the page (one-time cost, same as today
for `putPassthroughInitial` tight pages).

**Files**: `db.go` lines 3629-3647 (`treeInsertAnchor`)

### Idea C: Enable PiggybackGC during load

Already supported via Config. Reclaims garbage from splits. But adds I/O during load.
The test commented this out. This is orthogonal to Idea B.

### Idea D: Compact/VacuumKV after initial load

Already exists. Would reclaim garbage and re-encode tight. But adds a full rewrite pass.

### Idea E: Smaller split ratio (e.g. 75/25 instead of 50/50)

Keep left page fuller, right page smaller. Reduces total padding.
But doesn't address the fundamental issue of padding to 64KB at split time.

### Idea F: Variable page sizes (no fixed slottedPageMaxSize pad target)

Pages stay tight forever. Updates that overflow trigger an `ff.Update` to grow by some
increment (e.g. 2x content or +4KB). This is more complex but eliminates all padding waste.
Risk: more `ff.Update` calls during update workloads, more FlexSpace garbage.

## Why Dead Space Is Not Reused Inline

FlexSpace is log-structured. The block manager has a single write cursor (`blkid` + `blkoff`)
that only moves forward. `bm.write()` appends at `bm.blkoff`; there is no mechanism to
seek backward within a block to fill holes left by collapsed extents.

`ff.Update` (flexspace.go:962) does `collapseR` then `insertR`:
- `collapseR` decrements `blkusage[blkid]` but the physical bytes stay in the block
- `insertR` appends new data at `bm.offset()` (end of current block)

The dead bytes are tracked only as a count (`blkusage` is one `uint32` per block - total
live bytes, not a map of where holes are). To reuse holes you'd need a per-block free-space
allocator (bitmap or free list) tracking offset+size of every dead region.

`findEmptyBlock` (line 157) only recycles **completely empty** blocks (`blkusage[i] == 0`).
A block that is 90% dead / 10% live is never reused - only GC can reclaim it by rewriting
the live extents to fresh space and freeing the entire block.

This is the standard log-structured design tradeoff: simple append-only writes at the cost
of background GC. The issue here is that GC doesn't run during bulk loads (PiggybackGC is off).

## Recommended Approach: Idea B (tight splits)

Simplest change with the biggest impact. One function to modify.
Does not eliminate dead space, but makes dead extents much smaller (tight ~27 KB vs padded 64 KB),
so less total garbage accumulates.

### Implementation

In `treeInsertAnchor` (db.go ~line 3619):

1. **Left half** (lines 3629-3638): Instead of `slottedPageEncodePadded(..., slottedPageMaxSize)`,
   use `slottedPageEncode(fce.kvs[:leftCount])`. Still need `ff.Update` if psize changed.

2. **Right half** (lines 3643-3647): Instead of `slottedPageEncodePadded(..., slottedPageMaxSize)`,
   use `slottedPageEncode(fce.kvs[leftCount:fce.count])`.

3. The `flushDirtyPages`/`flushDirtyEntry` paths already handle `content > psize` by growing
   to `slottedPageMaxSize` on the first dirty flush. No changes needed there.

### Files to modify

- `db.go`: `treeInsertAnchor` (~lines 3629-3647) - use tight encoding for both halves

### Verification

```bash
go test -v -tags memfs -run TestLoadBloat_SpaceAmplification
# Should show space amplification < 3.0x (target: ~2.3x like SLOTTED_PAGE_KB=4)

go test -v -tags memfs -run TestVacuumThenOverwrite_DiskSizeBounded
# Existing test must still pass

go test -tags memfs ./...
# Full test suite must pass
```


------------
 Two questions: a) I have been reluctant to run GC during a data load, because it seemed like a global large cost, but perhaps a targeted GC on just one or two blocks could be acceptable and amortize
well. b) Let me understand the impact of Option B, tight splits: would we not, after packing tight, immediately just split again? It seems the risk is alot of cascading re-writes and work-amplification
if we pack tight after a split. No?


  (b) Would tight splits cause cascading re-splits?

  No - and here's why. The split threshold is key count (flexdbSparseIntervalCount = 1000 keys), not byte size. Look at line 3507:

  if fce.count >= flexdbSparseIntervalCount {
      db.treeInsertAnchor(nh, partition, fce)
  }

  After a split, left has 500 keys, right has 500 keys. Whether the on-disk encoding is tight (32 KB) or padded (64 KB) doesn't matter - neither half will split again until it
  accumulates another 500 keys. Tight encoding doesn't change fce.count, so no cascading splits.

  The concern about write amplification would be: what happens when the next key lands in a tight post-split page? During a bulk load, it goes through putPassthroughR (line 3505,
   since anchor.psize != 0). putPassthroughR checks slottedPageWouldFit (line 3563) using fitTarget = slottedPageMaxSize (line 3560-3561) - so it knows the page can grow to 64 KB
   even though psize is currently ~32 KB. The key is accepted into the cache, and the page is just marked dirty (putPassthroughMarkDirty). No immediate disk I/O.

  The growth to 64 KB only happens later, when flushDirtyPages runs (on Sync or eviction). At that point (intervalcache.go line 143-169), it does one ff.Update to grow the ~32 KB
   tight page to 64 KB padded. That's a one-time cost per page, and it's the same cost that putPassthroughInitial tight pages already pay today.

  During a pure bulk load (load all keys, then Sync), many of those 500-key post-split pages will accumulate more keys before being flushed. Some may even reach 1000 and split
  again - still tight. The growth-to-64KB only fires if a page is dirty at Sync time and content exceeds psize. For a load-then-Sync, pages that split and then re-split are
  re-encoded tight both times. Only the final state gets flushed.

  (a) Targeted GC on one or two blocks

  You're right that full GC is heavy. Looking at the code, GC() (which findEmptyBlock calls at line 159) does scan for candidate blocks. But the real question is: during a bulk
  load, blocks don't become fully empty (they have a mix of live and dead extents from splits), so even targeted GC would need to rewrite a block to compact it.

  A lighter approach: since Option B makes dead extents much smaller (~32 KB per split instead of ~63 KB), the total dead space drops from ~27 MB to ~14 MB. That might be
  acceptable without any GC. If not, a targeted "GC the worst N blocks" after the load completes (but before Sync returns) could reclaim that remaining 14 MB with bounded I/O
  cost - rewriting just 2-3 of the worst blocks rather than scanning all 16.

  This could be a good follow-up after Option B, since Option B alone should get you from 3.13x to roughly 2.4-2.5x.
-----------
