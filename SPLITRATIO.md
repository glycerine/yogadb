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

## Dead Space Explained

FlexSpace is log-structured. When `ff.Update` replaces a page, the old physical bytes
are not overwritten - they remain in the block as "dead space". The block's `blkusage`
counter is decremented by the old size, but the physical bytes sit there until GC
rewrites the entire block. Without GC running, dead space accumulates permanently.

During a load of 434K keys with SLOTTED_PAGE_KB=64, each of the ~435 splits generates
~63 KB of dead space (the old tight page replaced by the padded 64 KB page). That is
~27 MB of dead space that is never reclaimed unless PiggybackGC is enabled or VacuumKV
is called afterward.

At SLOTTED_PAGE_KB=4, the same splits generate ~63 KB of dead space each but the
replacement pages are only 4 KB (not 64 KB), so total live data per block is much
smaller and blocks fill more efficiently.

## Options

### Option A: Tight-encode on initial flush, pad only on first update (ALREADY DONE)

`putPassthroughInitial` already encodes tight. But `treeInsertAnchor` immediately
pads both halves to 64 KB. The split is the main padding point. No further action needed
here - this is already in place.

### Option B: Tight-encode splits too (defer padding to first dirty flush)

Change `treeInsertAnchor` to encode both halves **tight** instead of padded.
The page will be grown to `slottedPageMaxSize` only when `flushDirtyPages` or
`flushDirtyEntry` finds `content > psize` (which already handles this case).

**Savings**: ~14 MB (split padding waste) + ~14 MB (less garbage from smaller Updates)
**Risk**: The first update to each post-split page will trigger an `ff.Update` to grow it.
For a pure initial load followed by Sync, there are no updates - so pages stay tight.
For update-heavy workloads, the first update grows the page (one-time cost, same as today
for `putPassthroughInitial` tight pages).

**Tradeoffs**:
- Pro: Simplest change, biggest impact, one function to modify.
- Pro: Load + Sync workloads see ~28 MB savings immediately.
- Con: Update-heavy workloads pay a one-time grow cost per page on first dirty flush.
- Con: Does not address dead space from the Update itself (old tight bytes still become garbage).

**Files**: `db.go` lines 3629-3647 (`treeInsertAnchor`)

### Option C: Enable PiggybackGC during load

Already supported via `Config.PiggybackGC_on_SyncOrFlush`. Reclaims garbage from splits
by running GC on blocks with low utilization during Sync/Flush.

**Savings**: Reclaims dead space (~20+ MB) from split garbage.
**Risk**: Adds I/O during load (GC rewrites blocks). Slower loads.

**Tradeoffs**:
- Pro: Reclaims dead space that Option B alone cannot.
- Pro: Already implemented, just a config flag.
- Con: Extra I/O during load - GC rewrites entire blocks to compact them.
- Con: Orthogonal to Option B - can be combined or used independently.

### Option D: Compact/VacuumKV after initial load

Already exists. Would reclaim all garbage and re-encode tight in a single pass.

**Savings**: Gets to theoretical minimum (~31.6 MB).
**Risk**: Adds a full rewrite pass after load.

**Tradeoffs**:
- Pro: Gets closest to theoretical minimum space.
- Pro: Already implemented.
- Con: Full rewrite doubles the I/O of the load.
- Con: Only helps if you explicitly call it - not automatic.

### Option E: Smaller split ratio (e.g. 75/25 instead of 50/50)

Keep left page fuller, right page smaller. Reduces total padding when padded.

**Savings**: Moderate - reduces padding waste per split.
**Risk**: Right pages fill up faster, triggering more splits over time.

**Tradeoffs**:
- Pro: Left pages stay ~75% full instead of ~50% full after split.
- Con: Right pages start very small, wasting padding if padded to 64 KB.
- Con: Does not address the fundamental issue of padding to 64KB at split time.
- Con: More frequent splits on the right side during ongoing inserts.
- Con: Only helps if pages are padded; irrelevant if Option B (tight splits) is used.

### Option F: Variable page sizes (no fixed slottedPageMaxSize pad target)

Pages stay tight forever. Updates that overflow trigger an `ff.Update` to grow by some
increment (e.g. 2x content or +4KB). Pages never pad to a fixed maximum.

**Savings**: Eliminates all padding waste permanently.
**Risk**: More `ff.Update` calls during update workloads, more FlexSpace garbage.

**Tradeoffs**:
- Pro: Eliminates padding waste for all workloads, not just loads.
- Pro: Space usage stays close to theoretical minimum.
- Con: Most complex to implement - needs changes to flush paths, not just splits.
- Con: Every update that grows the page generates garbage (old extent becomes dead).
- Con: Without GC, many small growths produce many small dead extents.
- Con: Harder to reason about page sizes during debugging.

## Recommendation

Option B (tight splits) is the best starting point:
- Simplest change (one function, ~6 lines)
- Biggest immediate impact (~28 MB savings on the 64 KB test case)
- No downside for load-then-Sync workloads
- Can combine with Option C (PiggybackGC) later if dead space is still too high

-----
Q: which "padding" we are talking about. The slotted 
page design was intentionally update-in-place with
free space filled in towards either end. Are we talking 
about that key-value free space when we say padding? Or something about
the anchors and intervals?

A: Yes, it's exactly the slotted page free space you designed. 
Here's what's happening:

  The slotted page layout is: 
  [header | entry records -->  gap  <-- values | CRC]

  - slottedPageEncode (tight): totalSize = contentSize - entries and values are packed with no gap between them. The page is exactly as large as the data requires.
  
  - slottedPageEncodePadded (padded to slottedPageMaxSize): totalSize = targetSize (64 KB). Entry records grow forward, values
  grow backward from the CRC, and there's a zero-filled gap in the middle. That gap is the free space for future in-place updates.

  The old treeInsertAnchor was calling slottedPageEncodePadded(..., slottedPageMaxSize) for both split halves. So a page that was
  full at 64 KB gets split into two ~half-full 64 KB pages - each with ~32 KB of free space gap. That doubles the disk footprint
  at the moment of split.

The tight-encode approach writes each half as ~32 KB (no gap). 
Then when flushDirtyPages later writes a dirty page to disk, it
grows it to slottedPageMaxSize via ff.Update at that point;
but only if the page has actually been modified with new puts. 
So you don't pay the 64 KB cost until the page actually needs
update-in-place room.

The waste was: every split (on 64KB slotted pages) 
instantly allocates 128 KB of FlexSpace for data 
that only needs ~64 KB, and those zero-filled gaps
stay on disk as dead weight until the page gets dirty again.
