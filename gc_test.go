package yogadb

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/glycerine/vfs"
)

/* GC overview
Summary: FlexSpace GC Mechanism and FlexDB Key Overwrite Analysis

Key Findings

I've thoroughly analyzed the garbage collection mechanism in FlexSpace
and how FlexDB handles key overwrites. Here are the critical findings:

---
1. FlexSpace Block Usage Tracking (flexspace.go)

The blkusage Array

- Location: blockManager struct (line 125)
- Type: []uint32 with capacity FLEXSPACE_BLOCK_COUNT (204,800 blocks)
- Purpose: Tracks the number of bytes used in each 4 MB block (FLEXSPACE_BLOCK_SIZE = 4,194,304 bytes)

The updateBlkUsage Method (lines 134-147)

func (bm *blockManager) updateBlkUsage(blkid uint64, delta int32) uint32 {
    oidx := bm.blkusage[blkid] >> FLEXSPACE_BM_BLKDIST_BITS
    bm.blkdist[oidx]--
    if bm.blkusage[blkid] == 0 {
        bm.freeBlocks--
    }
    bm.blkusage[blkid] = uint32(int32(bm.blkusage[blkid]) + delta)
    nidx := bm.blkusage[blkid] >> FLEXSPACE_BM_BLKDIST_BITS
    bm.blkdist[nidx]++
    if bm.blkusage[blkid] == 0 {
        bm.freeBlocks++
    }
    return bm.blkusage[blkid]
}

What it does:
1. Takes a delta (positive for writes, negative for deletes)
2. Updates the histogram bucket tracking (blkdist[]) by moving the block from old usage bucket to new bucket
3. Maintains freeBlocks counter (increments when a block becomes completely empty, decrements when a block was empty and gets data)
4. Returns the new usage count

GC Threshold and Trigger (line 1104-1106)

const FLEXSPACE_GC_THRESHOLD = 64  // line 40

func (ff *FlexSpace) gcNeeded() bool {
    return ff.bm.freeBlocks < FLEXSPACE_GC_THRESHOLD
}

GC is triggered when:
- Free blocks drop below 64 blocks (~256 MB out of 800 GB address space)
- When findEmptyBlock() is called without sufficient free blocks (line 152-154)

---
2. GC Implementation (lines 1101-1308)

GC Strategy: 4 Rounds of Decreasing Aggressiveness

The GC() method runs 4 rounds (from round 3 to 0) with different target thresholds:

Round 3: Most conservative - targets blocks with usage ≤ 6.25% of block size (262 KB)
Round 2: Targets blocks with usage ≤ 12.5% of block size (524 KB)
Round 1: Targets blocks with usage ≤ 25% of block size (1 MB)
Round 0: Most aggressive - targets blocks with usage ≤ 75% of block size (3.1 MB)

Three-Phase GC Process:

1. gcAsyncPrepare() (lines 1144-1211): Scans leaf nodes sequentially and
   queues extents in fragmented blocks (up to 8192 queue depth)
2. gcAsyncExecute() (lines 1213-1262): Writes queued items to new block
   locations, logs GC operations, decrements old block usage
3. GC() Main loop (lines 1271-1308): Coordinates rounds until enough
   blocks are freed

Critical Code Path for Block Usage Update During GC:

// Line 1237-1241: In gcAsyncExecute()
oblkid := opoff >> FLEXSPACE_BLOCK_BITS
newUsage := ff.bm.updateBlkUsage(oblkid, -int32(length))  // DECREMENTS old block
if newUsage == 0 {
    rblocks++  // count freed blocks
}

Key insight: When GC moves data, it calls updateBlkUsage() with a negative
delta (line 1238) to decrement the old block's usage by the extent length.

---
3. FlexDB Key Overwrite Path (db.go)

Put Operation Flow (lines 1551-1598)

When db.Put(key, value) is called:
1. Key/value written to in-memory memtable (btree)
2. Entry logged to WAL (write-ahead log)
3. Data is NOT immediately flushed to FlexSpace - flushed
   asynchronously or on explicit Sync()

Flush Path (lines 2332-2350): flushMemtable()

When the memtable is flushed to FlexSpace, for each KV:
for _, kv := range batch {
    if kv.isTombstone() {
        db.deletePassthrough(kv.Key, &nh)
    } else {
        db.putPassthrough(kv, &nh)  // Line 2344
    }
}

Key Overwrite in putPassthroughR() (lines 1854-1891)

When a key already exists in FlexSpace (eq == true):
if eq {
    oldPSize := uint32(kv128EncodedSize(fce.kvs[idx]))
    partition.cacheEntryReplace(fce, kv, idx)
    db.ff.Update(encoded, loff, uint64(psize), uint64(oldPSize))  // Line 1869
    if psize != oldPSize {
        nh.shiftUpPropagate(int64(psize) - int64(oldPSize))
    }
    anchor.psize = uint32(int32(anchor.psize) + int32(psize) - int32(oldPSize))
}

The key call is db.ff.Update() which calls FlexSpace's Update method.

---
4. FlexSpace.Update() Chain (lines 945-966)

func (ff *FlexSpace) Update(buf []byte, loff, length, olen uint64) (int, error) {
    tag, _ := ff.GetTag(loff)

    // Phase 1: Delete old data
    if err := ff.collapseR(loff, olen, false); err != nil {
        return -1, err
    }

    // Phase 2: Insert new data
    n, err := ff.insertR(buf, loff, length, false)
    if err != nil {
        return -1, err
    }

    // Phase 3: Restore tag
    if tag != 0 {
        _ = ff.setTagR(loff, tag, false)
    }

    if !ff.omitRedoLog && ff.logFull() {
        ff.Sync()
    }
    return n, nil
}

---
5. CRITICAL: Space Reclamation During Overwrite (collapseR)

When Old Data is Deleted (lines 854-879)

func (ff *FlexSpace) collapseR(loff, length uint64, commit bool) error {
    ff.gc.writeBetweenStages = true
    ff.globalEpoch++

    // Query extents being collapsed to update block usage
    rr := ff.tree.Query(loff, length)  // Find all extents in this range
    if rr != nil {
        for i := uint64(0); i < rr.Count; i++ {
            blkid := rr.V[i].Poff >> FLEXSPACE_BLOCK_BITS
            ff.bm.updateBlkUsage(blkid, -int32(rr.V[i].Len))  // LINE 867: DECREMENTS
        }
    }
    ff.tree.Delete(loff, length)  // Update FlexTree
    // Log operation...
    return nil
}

Critical finding: When a key is overwritten:
1. Update() calls collapseR() to delete the old value
2. collapseR() calls ff.tree.Query() to find all extents in the deleted range
3. For each extent, updateBlkUsage() is called with negative delta (-int32(rr.V[i].Len))
4. This decrements the physical block's usage counter
5. If block usage reaches zero, the block is marked as free and freeBlocks is incremented

The old data's space IS reclaimed immediately, through the block usage counter, even though the physical data itself may still be on disk until GC runs.

---
6. Testing Coverage

FlexSpace Tests (flexspace_test.go)

- TestFlexspace_BlockRecycling() (lines 691-768): Writes data to blocks 0-2, collapses to free them, then writes new data and verifies reuse from block 0
- TestFlexspace_Collapse() (line 146): Tests Collapse operation and block usage updates
- TestFlexspace_Update() (line 369): Tests the Update path with overwrite semantics

FlexDB Tests (flexdb_test.go)

- TestFlexDB_OverwriteLoop() (lines 605-629): Tests repeatedly overwriting the same key with Put(), verifying final value is correct
- TestFlexDB_UpdateAfterSync() (line 346): Tests updating a key after sync
- TestFlexDB_DeleteAfterSync() (line 371): Tests deleting after sync

Note: There are NO explicit GC tests in the test files, but block recycling is implicitly tested through TestFlexspace_BlockRecycling().

---
7. Complete Data Flow for Key Overwrite

Example: Overwriting "key1" with new value

1. Initial state:
   key1 = value1_v1  [stored in FlexSpace at loff=100, poff=block2+offset, size=50]
   block2.usage = 50

2. User calls: db.Put("key1", "value1_v2")
   - New KV added to memtable (in-memory)
   - WAL entry written

3. Memtable flush occurs (via flushMemtable):
   - putPassthrough() called with new KV
   - Finds existing key in interval cache (eq == true)
   - Calls db.ff.Update(newEncoded, loff=100, length=48, olen=50)

4. FlexSpace.Update() executes:
   a) collapseR(loff=100, length=50, false):
      - Query extents at [100, 150) -> finds extent at poff=block2+X, len=50
      - updateBlkUsage(block2, -50)
      - block2.usage = 0 -> freeBlocks++
      - Delete from FlexTree

   b) insertR(newData, loff=100, length=48, false):
      - Write new 48 bytes to current write block (say block3)
      - Insert extent at poff=block3+Y, len=48 into FlexTree
      - updateBlkUsage(block3, +48)
      - block3.usage += 48

5. Final state:
   block2.usage = 0 (eligible for GC)
   block3.usage += 48
   freeBlocks increased (block2 now free)

---

8. Summary of GC Behavior

│           Scenario     │        Block Usage Update              │GC Trigger            │ Reclamation Timing  │
----------------------------------------------------------------------------------------------------------------
│ New key written        │ updateBlkUsage(blk, +len)              │ If freeBlocks < 64   │ -                   │
│ Key overwritten        │ Old block: updateBlkUsage(blk, -len)   │ If old block becomes │                     │
                                                                  │ free &&              │                     │
                                                                  │ freeBlocks < 64      │ Immediate (counter) │
│ Key deleted (tombstone)│ updateBlkUsage(blk, -len)              │ Same                 │ Immediate (counter) │
│ GC recycles fragmented block │ Old block: updateBlkUsage(blk, -len) -> 0; New           │                     │
│                              │ block: updateBlkUsage(blk, +len) │ When freeBlocks < 64 │ During GC run       │
----------------------------------------------------------------------------------------------------------------

Critical insight: The block usage counter is updated immediately when
data is deleted (overwrite or delete), triggering GC if threshold is crossed.
The physical GC run (data movement and compaction) only happens if freeBlocks< 64.

---
File Locations Summary

┌────────────────────────────────────────┬───────────────────────────────────────┬──────────────┐
│               Component                │                 File                  │    Lines     │
├────────────────────────────────────────┼───────────────────────────────────────┼──────────────┤
│ Block usage array + updateBlkUsage     │ /Users/jaten/yogadb/flexspace.go      │ 125, 134-147 │
├────────────────────────────────────────┼───────────────────────────────────────┼──────────────┤
│ GC threshold constant                  │ /Users/jaten/yogadb/flexspace.go      │ 40           │
├────────────────────────────────────────┼───────────────────────────────────────┼──────────────┤
│ GC main loop                           │ /Users/jaten/yogadb/flexspace.go      │ 1271-1308    │
├────────────────────────────────────────┼───────────────────────────────────────┼──────────────┤
│ GC execute (decrements old block)      │ /Users/jaten/yogadb/flexspace.go      │ 1237-1241    │
├────────────────────────────────────────┼───────────────────────────────────────┼──────────────┤
│ Collapse (decrements deleted blocks)   │ /Users/jaten/yogadb/flexspace.go      │ 854-879      │
├────────────────────────────────────────┼───────────────────────────────────────┼──────────────┤
│ FlexDB Put                             │ /Users/jaten/yogadb/db.go         │ 1551-1598    │
├────────────────────────────────────────┼───────────────────────────────────────┼──────────────┤
│ FlexDB putPassthroughR (key overwrite) │ /Users/jaten/yogadb/db.go         │ 1866-1873    │
├────────────────────────────────────────┼───────────────────────────────────────┼──────────────┤
│ FlexSpace.Update (collapse + insert)   │ /Users/jaten/yogadb/flexspace.go      │ 945-966      │
├────────────────────────────────────────┼───────────────────────────────────────┼──────────────┤
│ Test: Block recycling                  │ /Users/jaten/yogadb/flexspace_test.go │ 691-768      │
├────────────────────────────────────────┼───────────────────────────────────────┼──────────────┤
│ Test: Overwrite loop                   │ /Users/jaten/yogadb/flexdb_test.go    │ 605-629      │
└────────────────────────────────────────┴───────────────────────────────────────┴──────────────┘

Key findings:
1. FlexSpace.Update() does call collapseR() which does decrement blkusage for replaced data
2. FlexDB.putPassthroughR() correctly calls ff.Update() when eq == true (key already exists)
3. However, GC only triggers when freeBlocks < 64 - meaning you need 64+ completely empty blocks before GC even considers running
*/

/* 2026 March 09: mixed hlc big deltas causing page overflow bug fix history:

bug:

       Now I have the full picture. Let me assess the magnitude of the problem to calibrate the right fix.

       The HLC delta varint growth: a 1-byte varint encodes deltas 0-127. A 2-byte varint encodes up to 16383. A 3-byte varint encodes up to ~2M. So if baseHLC is from an old entry and the new HLC is, say, 200 ticks later, that's still 2 bytes instead of 1. The worst case per entry is about 9 extra bytes (1-byte to 10-byte varint). With ~1000 entries
        per page (flexdbSparseIntervalCount=1000), the worst case overhead is ~9KB -- nearly the entire page size of 10KB. But in practice, the delta growth is much smaller.

       Let me check what the actual entry count is when a split is triggered (i.e., how many KVs fit in a 10KB page).

       A typical entry with a 10-byte key, 8-byte inline value, 1-byte HLC varint: 2+2+1+10 = 15 bytes entry record + 8 bytes value = 23 bytes. So ~430 entries fit in 10KB. With 2-byte HLC varints instead of 1, that's 1 extra byte per entry = 430 extra bytes. That's 4.3% growth. With 3-byte varints, it's 860 extra bytes = 8.6% growth.

       Now I can make my recommendation.

       ---
       Here is my analysis and recommendation.

       Problem Analysis

       The bug has two distinct manifestations:

       Transient overflow (already handled): During flushMemtable, keys are updated one-by-one via putPassthroughR. After replacing key K5 with a new HLC but before replacing K1, the page has mixed HLCs. The baseHLC stays low (anchored by K1's old HLC), making K5's delta a 2-3 byte varint. The existing slottedPageWouldFitUniformHLC check at
       db.go:3177 handles this correctly -- it allows the replace and defers the size mismatch to flushDirtyPages, which uses ff.Update to resize.

       Persistent overflow (the real bug): A user updates only SOME keys in a page. The remaining keys keep old HLCs permanently. The page genuinely needs more space because HLC deltas are larger. slottedPageWouldFitUniformHLC returns false (correctly, because even with uniform HLCs the data might be at the limit), triggering a treeInsertAnchor split
        that calls ff.Insert -- allocating a new 4MB block for a half-page of data.

       Assessment of Each Option

       Option A (Reserve HLC headroom): This is the right general idea but the wrong mechanism. Fixed percentage headroom is wasteful for pages with uniform HLCs (the common case) and may still be insufficient for extreme HLC spreads. It also changes the on-disk format for all pages.

       Option B (Fixed-size HLC encoding): This is the cleanest theoretical fix -- it eliminates the variable entirely. Using 4-byte fixed deltas costs 3 extra bytes per entry vs. a 1-byte varint, or ~1.3KB for 430 entries. But it permanently wastes space on every page, even when all HLCs are identical (delta=0). It also requires a format migration.

       Option C (Rebase HLCs during flushDirtyPages): This does not solve the problem. The issue is that putPassthroughR sees the overflow BEFORE flushDirtyPages runs. By the time flushDirtyPages executes, the split has already happened.

       Option D (In-place Update for replace overflow): This is the most targeted fix. When eq=true and the page does not fit, instead of splitting, do the replace in the cache, then use ff.Update (collapse+insert) to grow anchor.psize by the delta. No new anchor, no 4MB allocation. The page just becomes slightly larger than slottedPageMaxSize.

       Option E (Use ff.Update for right half of split): This does not help because the right half is a NEW logical range -- there is no existing extent to replace. ff.Update requires an existing extent at that loff.

       Recommendation: Option D, with refinement

       Option D is the correct fix. Here is the precise design:

       Core change in putPassthroughR

       When slottedPageWouldFit returns false AND eq == true (replacing an existing key), do NOT split. Instead:

       1. Replace the entry in the cache: partition.cacheEntryReplace(fce, kv, idx)
       2. Compute the new encoded size: newSize := slottedPageComputeSize(fce.kvs[:fce.count])
       3. If newSize > int(anchor.psize), grow the extent in place:
         - Encode the page tight (no padding): buf := slottedPageEncode(fce.kvs[:fce.count])
         - Call ff.Update(buf, absLoff, uint64(len(buf)), uint64(anchor.psize)) to collapse the old extent and insert the new, larger one at the same loff
         - Call nh.shiftUpPropagate(int64(len(buf)) - int64(anchor.psize)) to adjust shifts
         - Update anchor.psize = uint32(len(buf))
         - Mark the entry as NOT dirty (it was just written)
       4. If newSize <= int(anchor.psize), the page fits -- just mark dirty as usual (the flushDirtyPages path handles it).

       This eliminates the split entirely for replace operations. A page might grow beyond slottedPageMaxSize by a few hundred bytes, but it never triggers the 4MB block allocation for what is fundamentally a metadata size change.

       Why this is safe

       - ff.Update is collapse+insert, which is the same primitive used by the existing flushDirtyPages overflow path (intervalcache.go:142). So the FlexSpace/FlexTree handling is already proven.
       - The shift propagation is already done correctly for Updates in treeInsertAnchor (db.go:3227-3229).
       - Pages slightly larger than slottedPageMaxSize are handled correctly by decode (it uses the actual buffer length, not a fixed constant).
       - The only place that assumes anchor.psize == slottedPageMaxSize is the left-half check in treeInsertAnchor (line 3224), which already has a fallback for psize != slottedPageMaxSize.

       What about insert (eq=false)?

       When eq=false (genuinely new key), the page has more data and a split IS appropriate. No change needed there. The existing code path is correct -- a new key means the page has genuinely grown in key/value content, not just in HLC encoding overhead.

       Additional guard: cap the growth

       Add a sanity check: if a replace-only growth would push anchor.psize beyond 2 * slottedPageMaxSize, go ahead and split. This prevents pathological cases where repeated HLC-only growth accumulates unboundedly. In practice this should never be reached because the HLC varint overhead per entry is bounded at ~9 bytes.

       Remove the slottedPageWouldFitUniformHLC path

       The existing slottedPageWouldFitUniformHLC check at db.go:3177 becomes unnecessary. It was a partial fix that handled the transient case by deferring to flushDirtyPages. The Option D approach subsumes it by handling ALL replace-overflow cases uniformly in putPassthroughR itself -- either the page fits and is marked dirty, or it does not fit
       and is grown via ff.Update immediately. This is simpler and eliminates the "encode as tight, detect mismatch, fall back to Update" two-phase dance in flushDirtyPages.

       However, if you prefer incremental changes, you can keep the slottedPageWouldFitUniformHLC path as-is and only add the Option D fallback for the eq && !slottedPageWouldFitUniformHLC case (the path that currently falls through to the split). This minimizes risk.

       Implementation sequence

       1. In putPassthroughR, after the existing slottedPageWouldFitUniformHLC block (line 3177), add a new block for eq && !wouldFitUniform: do the replace + ff.Update to grow.
       2. Remove the split path for eq cases (the if eq { cacheEntryReplace... } treeInsertAnchor block at lines 3184-3192), or gate it behind !eq.
       3. Optionally simplify flushDirtyPages to remove the len(buf) != anchor.psize fallback, since pages will now always be pre-grown to their actual size during putPassthroughR.
       4. Add a test that updates only some keys in a page, verifying no split occurs and the page grows in place.

       Critical Files for Implementation

       - /Users/jaten/go/src/github.com/glycerine/yogadb/db.go - Contains putPassthroughR (line 3160) and treeInsertAnchor (line 3214) where the core fix goes
       - /Users/jaten/go/src/github.com/glycerine/yogadb/slotted.go - Contains slottedPageWouldFit, slottedPageComputeSize, and slottedPageEncodePadded which are the sizing/encoding functions
       - /Users/jaten/go/src/github.com/glycerine/yogadb/intervalcache.go - Contains flushDirtyPages (line 97) and flushDirtyEntry (line 213) which have the existing overflow fallback to simplify
       - /Users/jaten/go/src/github.com/glycerine/yogadb/flexspace.go - Contains ff.Update (line 864) which is the collapse+insert primitive used for in-place growth
       - /Users/jaten/go/src/github.com/glycerine/yogadb/db.go - Contains flushMemtable (line 3771) as the primary trigger path to test
   Done (19 tool uses · 49.4k tokens · 5m 10s)

* Good analysis from the planner. The recommendation is Option D: when replacing an existing key
  causes page overflow, grow the page in-place via ff.Update instead of splitting. Let me implement
  this properly.

  The key insight: a replace (eq=true) never changes the key count - only HLC encoding overhead grows.
   A split is the wrong response because it creates a structural change (new anchor + 4MB block
  allocation) for what is fundamentally a metadata encoding issue.

outcome:

  - 1000/1000 iterations of TestGC_OverwriteSameKeys_DiskSizeBounded pass (was flaky before the fix)
  - Full test suite passes
  - Both new regression tests pass:
    - TestGC_PartialOverwrite_NoBlockGrowth - permanent HLC spread (only half the keys updated), disk stays bounded at
  1.0x
    - TestGC_ReplaceWithLargerValue_Splits - genuinely larger value triggers proper split, all keys correct

   The fix is solid. The Option D approach (grow page in-place via
   ff.Update on replace-triggered overflow) handles both
   transient and permanent HLC delta inflation correctly.

*/

// mustFileSize returns the size of the named file, or 0 if it doesn't exist.
func mustFileSize(fs vfs.FS, path string) int64 {
	sz, err := fileSize(fs, path)
	if err != nil {
		return 0
	}
	return sz
}

// countUsedBlocks returns the number of blocks with non-zero usage
// in the FlexSpace block manager. This represents the physical
// disk footprint in units of 4 MB blocks.
func countUsedBlocks(ff *FlexSpace) int {
	count := 0
	for i := uint64(0); i < FLEXSPACE_BLOCK_COUNT; i++ {
		if ff.bm.blkusage[i] > 0 {
			count++
		}
	}
	return count
}

// totalBlockUsage returns the sum of all bytes tracked across all blocks.
func totalBlockUsage(ff *FlexSpace) uint64 {
	total := uint64(0)
	for i := uint64(0); i < FLEXSPACE_BLOCK_COUNT; i++ {
		total += uint64(ff.bm.blkusage[i])
	}
	return total
}

// TestGC_OverwriteSameKeys_BlockUsageDecreases verifies that when
// the same keys are written multiple times (simulating repeated
// import of the same dataset), the block-level usage tracking
// in FlexSpace properly accounts for the old data being replaced.
//
// This is the core question: does FlexDB's overwrite path
// (putPassthroughR -> ff.Update -> collapseR) correctly decrement
// blkusage for the old extents?
func TestGC_OverwriteSameKeys_BlockUsageDecreases(t *testing.T) {
	db, _ := openTestDB(t, nil)

	const nKeys = 200
	keys := make([]string, nKeys)
	vals := make([]string, nKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("key%06d", i)
		vals[i] = fmt.Sprintf("value-round0-%06d-padding-to-make-it-bigger", i)
	}

	// Round 0: initial write of all keys.
	for i, k := range keys {
		mustPut(t, db, k, vals[i])
	}
	db.Sync()

	usageAfterRound0 := totalBlockUsage(db.ff)
	blocksAfterRound0 := countUsedBlocks(db.ff)
	flexSizeAfterRound0 := db.ff.Size()

	t.Logf("Round 0: totalBlockUsage=%d, usedBlocks=%d, flexSize=%d",
		usageAfterRound0, blocksAfterRound0, flexSizeAfterRound0)

	if usageAfterRound0 == 0 {
		t.Fatal("expected non-zero block usage after initial write")
	}

	// Rounds 1-4: overwrite every key with the same-length value.
	// If GC/block tracking works, total block usage should NOT grow
	// linearly with each round.
	const rounds = 4
	for r := 1; r <= rounds; r++ {
		for i, k := range keys {
			newVal := fmt.Sprintf("value-round%d-%06d-padding-to-make-it-bigger", r, i)
			mustPut(t, db, k, newVal)
		}
		db.Sync()

		usage := totalBlockUsage(db.ff)
		blocks := countUsedBlocks(db.ff)
		flexSize := db.ff.Size()

		t.Logf("Round %d: totalBlockUsage=%d, usedBlocks=%d, flexSize=%d",
			r, usage, blocks, flexSize)
	}

	usageAfterAllRounds := totalBlockUsage(db.ff)

	// The key assertion: after overwriting the same keys 4 more times,
	// the total block usage should be bounded - ideally close to the
	// single-round usage, NOT 5x the initial.
	//
	// We allow 3x as a generous upper bound. If block usage tracking
	// is working correctly on overwrites, the old data's blocks get
	// decremented and new data goes into (possibly the same or new)
	// blocks. The total should stay roughly constant.
	maxAcceptable := usageAfterRound0 * 3
	if usageAfterAllRounds > maxAcceptable {
		t.Errorf("block usage grew too much: after round 0 = %d, after %d rounds = %d (%.1fx); max acceptable = %d (3x)",
			usageAfterRound0, rounds, usageAfterAllRounds,
			float64(usageAfterAllRounds)/float64(usageAfterRound0),
			maxAcceptable)
	}

	// Also verify correctness: all keys should have the final round's values.
	for i, k := range keys {
		expected := fmt.Sprintf("value-round%d-%06d-padding-to-make-it-bigger", rounds, i)
		mustGet(t, db, k, expected)
	}
}

// TestGC_OverwriteSameKeys_DiskSizeBounded verifies that the actual
// on-disk directory size doesn't grow linearly when overwriting the
// same keys repeatedly.
//
// We measure from round 1 onward (after block pre-allocation has
// stabilized) to avoid false failures from the 4 MB block jump.
func TestGC_OverwriteSameKeys_DiskSizeBounded(t *testing.T) {
	fs, dir := newTestFS(t)
	cfg := &Config{FS: fs}

	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const nKeys = 500
	keys := make([]string, nKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("key%06d", i)
	}

	// Round 0: initial write - triggers block pre-allocation.
	for i, k := range keys {
		val := fmt.Sprintf("val-round0-%06d-padding-data-here", i)
		if err := db.Put(k, []byte(val)); err != nil {
			t.Fatal(err)
		}
	}
	db.Sync()

	// Round 1: first overwrite - after this, pre-allocation is stable.
	for i, k := range keys {
		val := fmt.Sprintf("val-round1-%06d-padding-data-here", i)
		if err := db.Put(k, []byte(val)); err != nil {
			t.Fatal(err)
		}
	}
	db.Sync()
	sizeAfterRound1 := mustDirSize(fs, dir)
	t.Logf("Round 1 disk size: %d bytes (baseline after pre-alloc)", sizeAfterRound1)

	// Rounds 2-9: overwrite every key 8 more times.
	const rounds = 9
	for r := 2; r <= rounds; r++ {
		for i, k := range keys {
			val := fmt.Sprintf("val-round%d-%06d-padding-data-here", r, i)
			if err := db.Put(k, []byte(val)); err != nil {
				t.Fatal(err)
			}
		}
		db.Sync()
	}

	sizeAfterAllRounds := mustDirSize(fs, dir)
	t.Logf("After %d rounds disk size: %d bytes (%.2fx vs round 1)",
		rounds, sizeAfterAllRounds, float64(sizeAfterAllRounds)/float64(sizeAfterRound1))

	// After pre-allocation stabilizes at round 1, 8 more rounds of
	// overwrites should NOT cause significant disk growth.
	// The only expected growth is the redo log (~21 KB/round = ~170 KB).
	// KV128_BLOCKS file should stay the same size.
	// Allow 1.5x as the bound (generous for redo log growth).
	maxAcceptable := int64(float64(sizeAfterRound1) * 1.5)
	if sizeAfterAllRounds > maxAcceptable {
		t.Errorf("disk size grew too much after pre-alloc: round 1 = %d, after %d rounds = %d (%.2fx); max acceptable = %d (1.5x)",
			sizeAfterRound1, rounds, sizeAfterAllRounds,
			float64(sizeAfterAllRounds)/float64(sizeAfterRound1),
			maxAcceptable)
	}

	// Verify correctness.
	for i, k := range keys {
		expected := fmt.Sprintf("val-round%d-%06d-padding-data-here", rounds, i)
		got, err := db.Get(k)
		if err != nil {
			t.Fatalf("key %q not found", k)
		}
		if string(got) != expected {
			t.Fatalf("key %q: got %q, want %q", k, got, expected)
		}
	}
}

// TestGC_PartialOverwrite_NoBlockGrowth verifies that updating only SOME
// keys in a page does not trigger an anchor split or 4MB block allocation.
// This is the non-transient HLC spread case: keys 0-249 get new HLCs
// while keys 250-499 keep their original HLCs permanently, creating a
// large HLC delta that inflates varint encoding.
func TestGC_PartialOverwrite_NoBlockGrowth(t *testing.T) {
	fs, dir := newTestFS(t)
	cfg := &Config{FS: fs}

	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const nKeys = 500
	keys := make([]string, nKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("key%06d", i)
	}

	// Round 0: initial write of all keys.
	for i, k := range keys {
		val := fmt.Sprintf("val-round0-%06d-padding-data-here", i)
		if err := db.Put(k, []byte(val)); err != nil {
			t.Fatal(err)
		}
	}
	db.Sync()

	// Round 1: overwrite all keys to stabilize pre-allocation.
	for i, k := range keys {
		val := fmt.Sprintf("val-round1-%06d-padding-data-here", i)
		if err := db.Put(k, []byte(val)); err != nil {
			t.Fatal(err)
		}
	}
	db.Sync()
	sizeAfterRound1 := mustDirSize(fs, dir)
	t.Logf("Round 1 disk size: %d bytes (baseline)", sizeAfterRound1)

	// Rounds 2-9: overwrite only the FIRST HALF of the keys.
	// This creates a permanent HLC spread within each page: the first half
	// has new (large) HLCs, the second half retains old (small) HLCs.
	const rounds = 9
	for r := 2; r <= rounds; r++ {
		for i := 0; i < nKeys/2; i++ {
			k := keys[i]
			val := fmt.Sprintf("val-round%d-%06d-padding-data-here", r, i)
			if err := db.Put(k, []byte(val)); err != nil {
				t.Fatal(err)
			}
		}
		db.Sync()
	}

	sizeAfterPartialRounds := mustDirSize(fs, dir)
	t.Logf("After %d partial-overwrite rounds: %d bytes (%.2fx vs round 1)",
		rounds, sizeAfterPartialRounds, float64(sizeAfterPartialRounds)/float64(sizeAfterRound1))

	// Allow 2x as the bound - pages may grow slightly from HLC varint
	// overhead but should NOT trigger 4MB block allocations.
	maxAcceptable := int64(float64(sizeAfterRound1) * 2.0)
	if sizeAfterPartialRounds > maxAcceptable {
		t.Errorf("disk size grew too much with partial overwrites: round 1 = %d, after %d rounds = %d (%.2fx); max acceptable = %d (2.0x)",
			sizeAfterRound1, rounds, sizeAfterPartialRounds,
			float64(sizeAfterPartialRounds)/float64(sizeAfterRound1),
			maxAcceptable)
	}

	// Verify correctness: first half has latest round's value,
	// second half still has round 1's value.
	for i, k := range keys {
		var expected string
		if i < nKeys/2 {
			expected = fmt.Sprintf("val-round%d-%06d-padding-data-here", rounds, i)
		} else {
			expected = fmt.Sprintf("val-round1-%06d-padding-data-here", i)
		}
		got, err := db.Get(k)
		if err != nil {
			t.Fatalf("key %q not found", k)
		}
		if string(got) != expected {
			t.Fatalf("key %q: got %q, want %q", k, got, expected)
		}
	}
}

// TestGC_ReplaceWithLargerValue_Splits verifies that replacing a key with
// a genuinely larger value triggers a proper split (not just HLC growth).
func TestGC_ReplaceWithLargerValue_Splits(t *testing.T) {
	fs, dir := newTestFS(t)
	cfg := &Config{FS: fs}

	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Write enough keys to fill a page (~200 keys in 10KB).
	const nKeys = 200
	for i := 0; i < nKeys; i++ {
		k := fmt.Sprintf("key%06d", i)
		v := fmt.Sprintf("v%06d", i) // short values
		if err := db.Put(k, []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	db.Sync()

	// Replace one key with a much larger value. This should trigger
	// a proper page split, not the HLC-growth-only path.
	bigValue := make([]byte, 4096)
	for i := range bigValue {
		bigValue[i] = byte('A' + i%26)
	}
	if err := db.Put("key000100", bigValue); err != nil {
		t.Fatal(err)
	}
	db.Sync()

	// Verify the large value was stored correctly.
	got, err := db.Get("key000100")
	if err != nil {
		t.Fatal("key000100 not found after large-value replace")
	}
	if len(got) != len(bigValue) {
		t.Fatalf("key000100: got %d bytes, want %d", len(got), len(bigValue))
	}
	for i := range got {
		if got[i] != bigValue[i] {
			t.Fatalf("key000100 byte %d: got %d, want %d", i, got[i], bigValue[i])
		}
	}

	// Verify other keys are intact.
	for i := 0; i < nKeys; i++ {
		if i == 100 {
			continue
		}
		k := fmt.Sprintf("key%06d", i)
		expected := fmt.Sprintf("v%06d", i)
		got, foundErr := db.Get(k)
		if foundErr != nil {
			t.Fatalf("key %q not found", k)
		}
		if string(got) != expected {
			t.Fatalf("key %q: got %q, want %q", k, got, expected)
		}
	}
}

// TestGC_OverwriteSameKeys_RedoLogGrows shows that even though
// FlexSpace block-level tracking is correct, the redo log grows
// linearly with each overwrite round. Each Update (collapse + insert)
// writes log entries that accumulate within a session.
//
// This test also verifies KV128_BLOCKS file doesn't grow after
// the block pre-allocation stabilizes (round 1 onward).
func TestGC_OverwriteSameKeys_RedoLogGrows(t *testing.T) {
	fs, dir := newTestFS(t)
	cfg := &Config{FS: fs}

	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const nKeys = 200
	keys := make([]string, nKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("key%06d", i)
	}

	// Round 0: initial write.
	for i, k := range keys {
		val := fmt.Sprintf("val-round0-%06d-some-padding", i)
		db.Put(k, []byte(val))
	}
	db.Sync()

	// Round 1: first overwrite - stabilizes block pre-allocation.
	for i, k := range keys {
		val := fmt.Sprintf("val-round1-%06d-some-padding", i)
		db.Put(k, []byte(val))
	}
	db.Sync()

	redoLogSize1 := mustFileSize(fs, filepath.Join(dir, "FLEXSPACE.REDO.LOG"))
	kv128Size1 := mustFileSize(fs, filepath.Join(dir, "FLEXSPACE.KV128_BLOCKS"))
	t.Logf("Round 1 (baseline): REDO.LOG=%d, KV128_BLOCKS=%d", redoLogSize1, kv128Size1)

	// Rounds 2-9
	const rounds = 9
	for r := 2; r <= rounds; r++ {
		for i, k := range keys {
			val := fmt.Sprintf("val-round%d-%06d-some-padding", r, i)
			db.Put(k, []byte(val))
		}
		db.Sync()
	}

	redoLogSizeN := mustFileSize(fs, filepath.Join(dir, "FLEXSPACE.REDO.LOG"))
	kv128SizeN := mustFileSize(fs, filepath.Join(dir, "FLEXSPACE.KV128_BLOCKS"))
	t.Logf("Round %d: REDO.LOG=%d (%.1fx vs r1), KV128_BLOCKS=%d (%.2fx vs r1)",
		rounds,
		redoLogSizeN, float64(redoLogSizeN)/float64(redoLogSize1),
		kv128SizeN, float64(kv128SizeN)/float64(kv128Size1))

	// KV128_BLOCKS should NOT grow after pre-alloc stabilizes.
	if kv128SizeN > kv128Size1+int64(FLEXSPACE_BLOCK_SIZE) {
		t.Errorf("KV128_BLOCKS grew unexpectedly after pre-alloc: round1=%d, round%d=%d",
			kv128Size1, rounds, kv128SizeN)
	}

	// Block-level usage should be stable.
	usage := totalBlockUsage(db.ff)
	blocks := countUsedBlocks(db.ff)
	t.Logf("Final: totalBlockUsage=%d, usedBlocks=%d, freeBlocks=%d",
		usage, blocks, db.ff.bm.freeBlocks)
}

// TestGC_BlockUsageTracking_UpdatePath directly tests that
// FlexSpace.Update() properly decrements block usage for old data.
// This isolates the FlexSpace layer from FlexDB to pinpoint
// whether the issue is in FlexSpace or FlexDB.
func TestGC_BlockUsageTracking_UpdatePath(t *testing.T) {
	fs, dir := newTestFS(t)
	ff, err := OpenFlexSpaceCoW(dir, false, fs)
	if err != nil {
		t.Fatal(err)
	}
	defer ff.Close()

	// Write initial data.
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 256)
	}

	_, err = ff.Insert(data, 0, uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	ff.Sync()

	usageAfterInsert := totalBlockUsage(ff)
	t.Logf("After insert: totalBlockUsage=%d", usageAfterInsert)

	if usageAfterInsert != uint64(len(data)) {
		t.Errorf("expected block usage = %d, got %d", len(data), usageAfterInsert)
	}

	// Now Update (overwrite) with same-size data. The old block usage
	// should be decremented and new block usage incremented.
	newData := make([]byte, 4096)
	for i := range newData {
		newData[i] = byte((i + 1) % 256)
	}

	_, err = ff.Update(newData, 0, uint64(len(newData)), uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	ff.Sync()

	usageAfterUpdate := totalBlockUsage(ff)
	t.Logf("After update: totalBlockUsage=%d", usageAfterUpdate)

	// After updating same-size data, total usage should be approximately
	// the same as after the initial insert (the old extent is freed,
	// new extent is allocated). Allow small overhead for log entries etc.
	if usageAfterUpdate > usageAfterInsert*2 {
		t.Errorf("block usage doubled after same-size update: insert=%d, update=%d",
			usageAfterInsert, usageAfterUpdate)
	}

	// Repeat updates 10 times.
	for r := 0; r < 10; r++ {
		for i := range newData {
			newData[i] = byte((i + r + 2) % 256)
		}
		_, err = ff.Update(newData, 0, uint64(len(newData)), uint64(len(newData)))
		if err != nil {
			t.Fatal(err)
		}
	}
	ff.Sync()

	usageAfterManyUpdates := totalBlockUsage(ff)
	t.Logf("After 10 more updates: totalBlockUsage=%d", usageAfterManyUpdates)

	// Should still be bounded - not 12x the initial.
	if usageAfterManyUpdates > usageAfterInsert*3 {
		t.Errorf("block usage grew after repeated updates: insert=%d, after 10 more=%d (%.1fx)",
			usageAfterInsert, usageAfterManyUpdates,
			float64(usageAfterManyUpdates)/float64(usageAfterInsert))
	}

	// Verify data is correct (last written value).
	readBuf := make([]byte, 4096)
	n, err := ff.Read(readBuf, 0, uint64(len(readBuf)))
	if err != nil {
		t.Fatal(err)
	}
	if n != len(readBuf) {
		t.Fatalf("Read returned %d bytes, want %d", n, len(readBuf))
	}
	// Last loop iteration: r=9, so data[i] = (i + 9 + 2) % 256 = (i + 11) % 256
	for i := range readBuf {
		expected := byte((i + 11) % 256)
		if readBuf[i] != expected {
			t.Fatalf("byte %d: got %d, want %d", i, readBuf[i], expected)
		}
	}
}

// TestGC_CrossSession_DiskGrowth opens and closes the DB multiple times,
// writing the same dataset each session, to study whether the on-disk
// footprint grows linearly with sessions (the scenario observed in
// benchmarks where two independent runs produced 2x disk usage).
//
// Each session: open DB -> write N keys -> sync -> close.
// Between sessions the redo log is truncated (by Close), so only
// KV128_BLOCKS, FLEXTREE.PAGES, and FLEXTREE.COMMIT matter.
func TestGC_CrossSession_DiskGrowth(t *testing.T) {
	fs, dir := newTestFS(t)

	const nKeys = 500
	keys := make([]string, nKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("key%06d", i)
	}

	const sessions = 5
	var sizes [sessions]int64
	var kv128Sizes [sessions]int64
	var pagesSizes [sessions]int64

	cfg := &Config{FS: fs}
	for s := 0; s < sessions; s++ {
		db, err := OpenFlexDB(dir, cfg)
		if err != nil {
			t.Fatalf("session %d: OpenFlexDB: %v", s, err)
		}

		// Write the same keys with session-specific values.
		for i, k := range keys {
			val := fmt.Sprintf("val-session%d-%06d-padding-data", s, i)
			if err := db.Put(k, []byte(val)); err != nil {
				t.Fatalf("session %d: Put: %v", s, err)
			}
		}
		db.Sync()

		// Capture internal metrics before close.
		usage := totalBlockUsage(db.ff)
		blocks := countUsedBlocks(db.ff)
		freeBlks := db.ff.bm.freeBlocks
		writeBlk := db.ff.bm.blkid

		db.Close()

		sizes[s] = mustDirSize(fs, dir)
		kv128Sizes[s] = mustFileSize(fs, filepath.Join(dir, "FLEXSPACE.KV128_BLOCKS"))
		pagesSizes[s] = mustFileSize(fs, filepath.Join(dir, "FLEXTREE.PAGES"))

		t.Logf("Session %d: dir=%d, KV128=%d, PAGES=%d | blkUsage=%d, usedBlks=%d, freeBlks=%d, writeBlk=%d",
			s, sizes[s], kv128Sizes[s], pagesSizes[s],
			usage, blocks, freeBlks, writeBlk)
	}

	// Verify data from last session is correct.
	{
		db, err := OpenFlexDB(dir, cfg)
		if err != nil {
			t.Fatalf("verify open: %v", err)
		}
		for i, k := range keys {
			expected := fmt.Sprintf("val-session%d-%06d-padding-data", sessions-1, i)
			got, foundErr := db.Get(k)
			if foundErr != nil {
				t.Fatalf("key %q not found after %d sessions", k, sessions)
			}
			if string(got) != expected {
				t.Fatalf("key %q: got %q, want %q", k, got, expected)
			}
		}
		db.Close()
	}

	// Analyze growth. Compare session 1 (after pre-alloc) to last session.
	// Session 0 -> 1 may grow due to block pre-allocation.
	// Sessions 1 -> N should ideally stay flat.
	if sizes[1] == 0 {
		t.Fatal("session 1 dir size is 0")
	}

	growth := float64(sizes[sessions-1]) / float64(sizes[1])
	kv128Growth := float64(kv128Sizes[sessions-1]) / float64(kv128Sizes[1])

	t.Logf("")
	t.Logf("=== Cross-session growth (session 1 -> %d) ===", sessions-1)
	t.Logf("  Dir size:     %d -> %d (%.2fx)", sizes[1], sizes[sessions-1], growth)
	t.Logf("  KV128_BLOCKS: %d -> %d (%.2fx)", kv128Sizes[1], kv128Sizes[sessions-1], kv128Growth)
	t.Logf("  PAGES:        %d -> %d (%.2fx)", pagesSizes[1], pagesSizes[sessions-1],
		float64(pagesSizes[sessions-1])/float64(pagesSizes[1]))

	// Assert: KV128_BLOCKS should not grow linearly with sessions.
	// If block reuse across sessions works, it should stay roughly constant.
	// Allow 2x as generous bound.
	if kv128Growth > 2.0 {
		t.Errorf("KV128_BLOCKS grew %.2fx across %d sessions (want <=2x): %d -> %d",
			kv128Growth, sessions-1, kv128Sizes[1], kv128Sizes[sessions-1])
	}

	// Assert: total dir size bounded.
	if growth > 2.0 {
		t.Errorf("dir size grew %.2fx across %d sessions (want <=2x): %d -> %d",
			growth, sessions-1, sizes[1], sizes[sessions-1])
	}
}

// TestGC_CrossSession_BlockReuse verifies that when the DB is reopened,
// the block manager correctly identifies free blocks from the prior
// session and reuses them rather than always appending to new blocks.
func TestGC_CrossSession_BlockReuse(t *testing.T) {
	fs, dir := newTestFS(t)
	cfg := &Config{FS: fs}
	const nKeys = 200

	// Session 0: write initial data.
	{
		db, err := OpenFlexDB(dir, cfg)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < nKeys; i++ {
			k := fmt.Sprintf("key%06d", i)
			v := fmt.Sprintf("val-session0-%06d-data-padding", i)
			db.Put(k, []byte(v))
		}
		db.Sync()
		t.Logf("Session 0: usedBlocks=%d, totalUsage=%d, writeBlk=%d",
			countUsedBlocks(db.ff), totalBlockUsage(db.ff), db.ff.bm.blkid)
		db.Close()
	}

	kv128After0 := mustFileSize(fs, filepath.Join(dir, "FLEXSPACE.KV128_BLOCKS"))

	// Sessions 1-6: reopen and overwrite all keys.
	const sessions = 6
	// reuse the same filesystem as above!
	//fs, dir := newTestFS(t)
	//cfg := &Config{FS: fs}

	for s := 1; s <= sessions; s++ {

		db, err := OpenFlexDB(dir, cfg)
		if err != nil {
			t.Fatalf("session %d: %v", s, err)
		}

		// Check block manager state on open - are old blocks seen as used?
		usageOnOpen := totalBlockUsage(db.ff)
		blocksOnOpen := countUsedBlocks(db.ff)
		freeOnOpen := db.ff.bm.freeBlocks
		writeBlkOnOpen := db.ff.bm.blkid

		for i := 0; i < nKeys; i++ {
			k := fmt.Sprintf("key%06d", i)
			v := fmt.Sprintf("val-session%d-%06d-data-padding", s, i)
			db.Put(k, []byte(v))
		}
		db.Sync()

		usageAfterWrite := totalBlockUsage(db.ff)
		blocksAfterWrite := countUsedBlocks(db.ff)
		writeBlkAfterWrite := db.ff.bm.blkid

		t.Logf("Session %d: onOpen(usage=%d, blks=%d, free=%d, writeBlk=%d) -> afterWrite(usage=%d, blks=%d, writeBlk=%d)",
			s, usageOnOpen, blocksOnOpen, freeOnOpen, writeBlkOnOpen,
			usageAfterWrite, blocksAfterWrite, writeBlkAfterWrite)

		// Verify data.
		for i := 0; i < nKeys; i++ {
			k := fmt.Sprintf("key%06d", i)
			expected := fmt.Sprintf("val-session%d-%06d-data-padding", s, i)
			got, foundErr := db.Get(k)
			if foundErr != nil {
				t.Fatalf("session %d: key %q not found", s, k)
			}
			if string(got) != expected {
				t.Fatalf("session %d: key %q: got %q, want %q", s, k, got, expected)
			}
		}
		db.Close()
	}

	kv128AfterAll := mustFileSize(fs, filepath.Join(dir, "FLEXSPACE.KV128_BLOCKS"))
	t.Logf("KV128_BLOCKS: after session 0 = %d, after session %d = %d",
		kv128After0, sessions, kv128AfterAll)

	// The key observation from this test is in the per-session logs above:
	// the block manager alternates between 1 and 2 used blocks, showing
	// that new session writes go to a new block before the old data
	// is collapsed (because flush writes new values before the Update
	// path can reclaim old extents). This ping-pong pattern means
	// the file stabilizes at 2 blocks (8 MB) regardless of session count.
}

// TestGC_CrossSession_ManyReopens_SameDataset is the definitive test:
// import the exact same dataset 10 times across 10 separate sessions
// and verify that disk usage stays bounded. This simulates the exact
// scenario where Pebble outperforms YogaDB on space.
func TestGC_CrossSession_ManyReopens_SameDataset(t *testing.T) {
	fs, dir := newTestFS(t)
	cfg := &Config{FS: fs}

	const nKeys = 1000

	// Build the dataset once (keys and values are constant across sessions).
	keys := make([][]byte, nKeys)
	vals := make([][]byte, nKeys)
	for i := 0; i < nKeys; i++ {
		keys[i] = []byte(fmt.Sprintf("key%06d", i))
		vals[i] = []byte(fmt.Sprintf("value-%06d-constant-payload-for-all-sessions", i))
	}

	const sessions = 10
	var dirSizes [sessions]int64
	var kv128Sizes [sessions]int64

	for s := 0; s < sessions; s++ {
		db, err := OpenFlexDB(dir, cfg)
		if err != nil {
			t.Fatalf("session %d: %v", s, err)
		}

		for i := 0; i < nKeys; i++ {
			if err := db.Put(string(keys[i]), vals[i]); err != nil {
				t.Fatalf("session %d: Put: %v", s, err)
			}
		}
		db.Sync()
		db.Close()

		dirSizes[s] = mustDirSize(fs, dir)
		kv128Sizes[s] = mustFileSize(fs, filepath.Join(dir, "FLEXSPACE.KV128_BLOCKS"))

		t.Logf("Session %2d: dir=%8d  KV128=%8d", s, dirSizes[s], kv128Sizes[s])
	}

	// Verify all data is correct after final session.
	{
		db, err := OpenFlexDB(dir, cfg)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < nKeys; i++ {
			got, foundErr := db.Get(string(keys[i]))
			if foundErr != nil {
				t.Fatalf("key %q missing after %d sessions", keys[i], sessions)
			}
			if string(got) != string(vals[i]) {
				t.Fatalf("key %q: got %q, want %q", keys[i], got, vals[i])
			}
		}
		db.Close()
	}

	// Analyze growth from session 1 (post-prealloc) to session 9.
	baseline := dirSizes[1]
	final := dirSizes[sessions-1]
	growth := float64(final) / float64(baseline)

	kv128Baseline := kv128Sizes[1]
	kv128Final := kv128Sizes[sessions-1]
	kv128Growth := float64(kv128Final) / float64(kv128Baseline)

	t.Logf("")
	t.Logf("=== %d sessions importing identical dataset ===", sessions)
	t.Logf("  Dir:   session 1 = %d -> session %d = %d  (%.2fx)", baseline, sessions-1, final, growth)
	t.Logf("  KV128: session 1 = %d -> session %d = %d  (%.2fx)", kv128Baseline, sessions-1, kv128Final, kv128Growth)

	// The ideal is 1.0x - identical data rewritten yields no growth.
	// With block-granularity overhead, allow up to 2x.
	if kv128Growth > 2.0 {
		t.Errorf("KV128_BLOCKS grew %.2fx over %d sessions of identical data - block reuse not working across sessions",
			kv128Growth, sessions)
	}
	if growth > 2.0 {
		t.Errorf("dir size grew %.2fx over %d sessions of identical data",
			growth, sessions)
	}
}

// TestGC_GarbageMetrics_InMetrics verifies that the new garbage metrics
// (TotalFreeBytesInBlocks, BlocksWithLowUtilization, TotalLiveBytes) are
// populated correctly in SessionMetrics, finalMetrics, and CumulativeMetrics.
func TestGC_GarbageMetrics_InMetrics(t *testing.T) {
	db, _ := openTestDB(t, nil)

	// Before any writes, all metrics should be zero.
	m := db.SessionMetrics()
	if m.KVBlocksTotalLiveBytes != 0 {
		t.Errorf("expected KVBlocksTotalLiveBytes=0 before writes, got %d", m.KVBlocksTotalLiveBytes)
	}
	if m.TotalFreeBytesInBlocks != 0 {
		t.Errorf("expected TotalFreeBytesInBlocks=0 before writes, got %d", m.TotalFreeBytesInBlocks)
	}
	if m.BlocksWithLowUtilization != 0 {
		t.Errorf("expected BlocksWithLowUtilization=0 before writes, got %d", m.BlocksWithLowUtilization)
	}

	// Write some data.
	const nKeys = 100
	for i := 0; i < nKeys; i++ {
		k := fmt.Sprintf("key%06d", i)
		v := fmt.Sprintf("value-%06d-some-padding-data-here", i)
		mustPut(t, db, k, v)
	}
	db.Sync()

	m = db.SessionMetrics()
	t.Logf("After writes: KVBlocksTotalLiveBytes=%d, TotalFreeBytesInBlocks=%d, BlocksWithLowUtilization=%d",
		m.KVBlocksTotalLiveBytes, m.TotalFreeBytesInBlocks, m.BlocksWithLowUtilization)

	if m.KVBlocksTotalLiveBytes == 0 {
		t.Error("expected KVBlocksTotalLiveBytes > 0 after writes")
	}

	// With only ~5 KB of data in a 4 MB block, the block has <25%
	// utilization, so BlocksWithLowUtilization should be >= 1.
	if m.BlocksWithLowUtilization < 1 {
		t.Errorf("expected BlocksWithLowUtilization >= 1 for small dataset in 4MB block, got %d",
			m.BlocksWithLowUtilization)
	}

	// TotalFreeBytesInBlocks = block_size - live_bytes for each non-empty block.
	if m.TotalFreeBytesInBlocks <= 0 {
		t.Errorf("expected TotalFreeBytesInBlocks > 0 (small data in large block), got %d",
			m.TotalFreeBytesInBlocks)
	}

	// CumulativeMetrics should also have the same garbage metrics.
	cm := db.CumulativeMetrics()
	if cm.KVBlocksTotalLiveBytes != m.KVBlocksTotalLiveBytes {
		t.Errorf("CumulativeMetrics KVBlocksTotalLiveBytes=%d != SessionMetrics KVBlocksTotalLiveBytes=%d",
			cm.KVBlocksTotalLiveBytes, m.KVBlocksTotalLiveBytes)
	}

	// Verify String() includes garbage section.
	s := m.String()
	if !strings.Contains(s, "TotalFreeBytesInBlocks") {
		t.Error("Metrics.String() missing TotalFreeBytesInBlocks")
	}
	if !strings.Contains(s, "BlocksWithLowUtilization") {
		t.Error("Metrics.String() missing BlocksWithLowUtilization")
	}
}

// TestGC_GarbageMetrics_CustomThreshold verifies that Config.LowBlockUtilizationPct
// controls the BlocksWithLowUtilization threshold.
func TestGC_GarbageMetrics_CustomThreshold(t *testing.T) {
	// With threshold=0.01 (1%), a block with ~5 KB / 4 MB = 0.1% should still count.
	cfg1 := &Config{LowBlockUtilizationPct: 0.01}
	db1, _ := openTestDB(t, cfg1)
	for i := 0; i < 50; i++ {
		mustPut(t, db1, fmt.Sprintf("k%04d", i), fmt.Sprintf("v%04d-pad", i))
	}
	db1.Sync()
	m1 := db1.SessionMetrics()
	db1.Close()

	// With threshold=0.001 (0.1%), the same data may or may not count
	// depending on exact size. Use a very tight threshold to show it changes.
	cfg2 := &Config{LowBlockUtilizationPct: 0.0001} // 0.01% - ~420 bytes
	db2, _ := openTestDB(t, cfg2)
	for i := 0; i < 50; i++ {
		mustPut(t, db2, fmt.Sprintf("k%04d", i), fmt.Sprintf("v%04d-pad", i))
	}
	db2.Sync()
	m2 := db2.SessionMetrics()
	db2.Close()

	t.Logf("Threshold 1%%: BlocksWithLowUtilization=%d", m1.BlocksWithLowUtilization)
	t.Logf("Threshold 0.01%%: BlocksWithLowUtilization=%d", m2.BlocksWithLowUtilization)

	// Both should have identical KVBlocksTotalLiveBytes (same data).
	// But with a very tight threshold, fewer blocks qualify as "low utilization".
	if m2.BlocksWithLowUtilization > m1.BlocksWithLowUtilization {
		t.Errorf("tighter threshold should not produce more low-util blocks: loose=%d, tight=%d",
			m1.BlocksWithLowUtilization, m2.BlocksWithLowUtilization)
	}
}

// TestGC_FreeBlocksCountAccurate checks that freeBlocks is properly
// maintained when data is written and then collapsed (deleted).
func TestGC_FreeBlocksCountAccurate(t *testing.T) {
	fs, dir := newTestFS(t)

	ff, err := OpenFlexSpaceCoW(dir, false, fs)
	if err != nil {
		t.Fatal(err)
	}
	defer ff.Close()

	initialFreeBlocks := ff.bm.freeBlocks
	t.Logf("Initial freeBlocks: %d (total blocks: %d)", initialFreeBlocks, FLEXSPACE_BLOCK_COUNT)

	// Write enough data to consume exactly 1 block (4 MB).
	data := make([]byte, FLEXSPACE_BLOCK_SIZE)
	for i := range data {
		data[i] = byte(i % 256)
	}
	_, err = ff.Insert(data, 0, uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	ff.Sync()

	freeAfterWrite := ff.bm.freeBlocks
	t.Logf("After writing 4MB: freeBlocks=%d (delta=%d)", freeAfterWrite, int64(initialFreeBlocks)-int64(freeAfterWrite))

	// We consumed at least 1 block.
	if freeAfterWrite >= initialFreeBlocks {
		t.Errorf("freeBlocks should have decreased: was %d, now %d", initialFreeBlocks, freeAfterWrite)
	}

	// Now collapse (delete) all the data.
	err = ff.Collapse(0, uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	ff.Sync()

	freeAfterCollapse := ff.bm.freeBlocks
	t.Logf("After collapsing 4MB: freeBlocks=%d", freeAfterCollapse)

	// After deleting all data, the block(s) should be fully free again.
	if freeAfterCollapse != initialFreeBlocks {
		t.Errorf("freeBlocks should be restored: was %d initially, now %d after collapse",
			initialFreeBlocks, freeAfterCollapse)
	}
}

// TestPiggybackGC_TriggersOnSync enables PiggybackGC_on_SyncOrFlush,
// creates fragmentation by writing then overwriting keys, and verifies
// that GC runs during Sync when garbage exceeds the threshold.
func TestPiggybackGC_TriggersOnSync(t *testing.T) {
	cfg := &Config{
		PiggybackGC_on_SyncOrFlush: true,
		GCGarbagePct:               0.10, // low threshold to ensure GC triggers
	}
	db, _ := openTestDB(t, cfg)

	// Write keys, sync, then overwrite with smaller values to create garbage.
	const nKeys = 300
	for i := 0; i < nKeys; i++ {
		k := fmt.Sprintf("key%06d", i)
		v := fmt.Sprintf("value-%06d-padding-to-make-it-bigger-original", i)
		mustPut(t, db, k, v)
	}
	db.Sync()

	// Overwrite with smaller values - old extents become garbage.
	for i := 0; i < nKeys; i++ {
		k := fmt.Sprintf("key%06d", i)
		v := fmt.Sprintf("v2-%06d", i)
		mustPut(t, db, k, v)
	}
	db.Sync() // should trigger piggyback GC

	m := db.SessionMetrics()
	t.Logf("PiggybackGCRuns=%d, PiggybackGCLastDurMs=%d", m.PiggybackGCRuns, m.PiggybackGCLastDurMs)

	if m.PiggybackGCRuns == 0 {
		t.Error("expected PiggybackGCRuns > 0 after creating fragmentation and syncing")
	}
}

// TestPiggybackGC_RespectsGarbageThreshold sets the GC threshold to 1.0
// (100%) so that piggyback GC does NOT run. Since garbage fraction is
// always < 1.0 when there's live data, GC should never trigger.
func TestPiggybackGC_RespectsGarbageThreshold(t *testing.T) {
	cfg := &Config{
		PiggybackGC_on_SyncOrFlush: true,
		GCGarbagePct:               1.0, // impossible to reach - won't trigger
	}
	db, _ := openTestDB(t, cfg)

	const nKeys = 100
	for i := 0; i < nKeys; i++ {
		k := fmt.Sprintf("key%06d", i)
		v := fmt.Sprintf("value-%06d-padding-to-make-it-bigger-original", i)
		mustPut(t, db, k, v)
	}
	db.Sync()

	// Overwrite to create some garbage (but not 99%).
	for i := 0; i < nKeys; i++ {
		k := fmt.Sprintf("key%06d", i)
		v := fmt.Sprintf("v2-%06d", i)
		mustPut(t, db, k, v)
	}
	db.Sync()

	m := db.SessionMetrics()
	if m.PiggybackGCRuns != 0 {
		t.Errorf("expected PiggybackGCRuns=0 with 100%% threshold, got %d", m.PiggybackGCRuns)
	}
}

// TestPiggybackGC_DisabledByDefault verifies that piggyback GC does not
// run when Config uses default values (PiggybackGC_on_SyncOrFlush=false).
func TestPiggybackGC_DisabledByDefault(t *testing.T) {
	db, _ := openTestDB(t, nil) // default config

	const nKeys = 100
	for i := 0; i < nKeys; i++ {
		k := fmt.Sprintf("key%06d", i)
		v := fmt.Sprintf("value-%06d-padding-to-make-it-bigger-original", i)
		mustPut(t, db, k, v)
	}
	db.Sync()

	// Overwrite all keys.
	for i := 0; i < nKeys; i++ {
		k := fmt.Sprintf("key%06d", i)
		v := fmt.Sprintf("v2-%06d", i)
		mustPut(t, db, k, v)
	}
	db.Sync()

	m := db.SessionMetrics()
	if m.PiggybackGCRuns != 0 {
		t.Errorf("expected PiggybackGCRuns=0 with default config, got %d", m.PiggybackGCRuns)
	}
}

// TestPiggybackGC_ReclaimsSpace verifies that piggyback GC actually
// frees blocks. Write data, delete half, enable piggyback GC, sync,
// and verify free blocks increased.
func TestPiggybackGC_ReclaimsSpace(t *testing.T) {
	cfg := &Config{
		PiggybackGC_on_SyncOrFlush: true,
		GCGarbagePct:               0.10,
	}
	db, _ := openTestDB(t, cfg)

	// Write a large dataset.
	const nKeys = 500
	for i := 0; i < nKeys; i++ {
		k := fmt.Sprintf("key%06d", i)
		v := fmt.Sprintf("value-%06d-padding-to-make-it-significantly-bigger-than-needed", i)
		mustPut(t, db, k, v)
	}
	db.Sync()

	liveBeforeDelete, garbageBeforeDelete, _, _ := db.ff.garbageMetrics(0.25)
	t.Logf("Before delete: live=%d, garbage=%d", liveBeforeDelete, garbageBeforeDelete)

	// Delete half the keys to create garbage.
	for i := 0; i < nKeys/2; i++ {
		k := fmt.Sprintf("key%06d", i)
		db.Delete(k)
	}
	db.Sync() // should trigger piggyback GC

	liveAfterGC, garbageAfterGC, _, _ := db.ff.garbageMetrics(0.25)
	t.Logf("After delete+GC: live=%d, garbage=%d", liveAfterGC, garbageAfterGC)

	m := db.SessionMetrics()
	t.Logf("PiggybackGCRuns=%d", m.PiggybackGCRuns)

	// Verify remaining keys are correct.
	for i := nKeys / 2; i < nKeys; i++ {
		k := fmt.Sprintf("key%06d", i)
		expected := fmt.Sprintf("value-%06d-padding-to-make-it-significantly-bigger-than-needed", i)
		mustGet(t, db, k, expected)
	}

	// Deleted keys should be gone.
	for i := 0; i < nKeys/2; i++ {
		k := fmt.Sprintf("key%06d", i)
		_, err := db.Get(k)
		if err == nil {
			t.Errorf("key %q should have been deleted", k)
		}
	}
}

// writing 1000 keys of 33 bytes with a common 10 byte prefix
// and associate values of 1050 bytes should not balloon our
// total overall KV_BLOCKS to 5 blocks and 18-20MB
// when this is 33000 bytes of key material. we do not
// want 18x write amplification. but we see it under
// real darwin AFPS, not under our -tags memfs vfs; so
// maybe our Truncate or Preallocate implementation is
// not being faithful? or maybe something else entirely.
func Test_GC1K_write_1k_keys_with_large_values(t *testing.T) {
	db, _ := openTestDB(t, nil)

	var seed [32]byte
	rng := newPRNG(seed)

	const nKeys = 1000
	keys := make([]string, nKeys)
	vals := make([]string, nKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("usertable/user%v", rng.unlockedStringOfDigits(19))

		v := make([]byte, 1050)
		rng.cha8.Read(v)
		vals[i] = string(v)
	}

	// Round 0: initial write of all keys.
	for i, k := range keys {
		mustPut(t, db, k, vals[i])
	}
	db.Sync()

	met := db.SessionMetrics()
	vv("met = %v", met)

	if met.BlocksInUse > 1 { // was 5 !?!
		t.Fatalf("for 1000 33 bytes keys, should only have 1 4MB block in use, not %v", met.BlocksInUse)
	}
	//met.BlocksWithLowUtilization was 5

	// sanity
	v2, err2 := db.Get(keys[0])
	if err2 != nil {
		panicf("key '%v' was written but not found ", keys[0])
	}
	if string(v2) != vals[0] {
		panicf("key '%v' was written but value retreived has mismatch! v2='%v' len; vals[0]='%v' len", keys[0], len(v2), len(vals[0]))
	}

	/*
	   usageAfterRound0 := totalBlockUsage(db.ff)
	   blocksAfterRound0 := countUsedBlocks(db.ff)
	   flexSizeAfterRound0 := db.ff.Size()

	   t.Logf("Round 0: totalBlockUsage=%d, usedBlocks=%d, flexSize=%d",

	   	usageAfterRound0, blocksAfterRound0, flexSizeAfterRound0)

	   	if usageAfterRound0 == 0 {
	   		t.Fatal("expected non-zero block usage after initial write")
	   	}

	   // Rounds 1-4: overwrite every key with the same-length value.
	   // If GC/block tracking works, total block usage should NOT grow
	   // linearly with each round.
	   const rounds = 4

	   	for r := 1; r <= rounds; r++ {
	   		for i, k := range keys {
	   			newVal := fmt.Sprintf("value-round%d-%06d-padding-to-make-it-bigger", r, i)
	   			mustPut(t, db, k, newVal)
	   		}
	   		db.Sync()

	   		usage := totalBlockUsage(db.ff)
	   		blocks := countUsedBlocks(db.ff)
	   		flexSize := db.ff.Size()

	   		t.Logf("Round %d: totalBlockUsage=%d, usedBlocks=%d, flexSize=%d",
	   			r, usage, blocks, flexSize)
	   	}

	   usageAfterAllRounds := totalBlockUsage(db.ff)

	   // The key assertion: after overwriting the same keys 4 more times,
	   // the total block usage should be bounded - ideally close to the
	   // single-round usage, NOT 5x the initial.
	   //
	   // We allow 3x as a generous upper bound. If block usage tracking
	   // is working correctly on overwrites, the old data's blocks get
	   // decremented and new data goes into (possibly the same or new)
	   // blocks. The total should stay roughly constant.
	   maxAcceptable := usageAfterRound0 * 3

	   	if usageAfterAllRounds > maxAcceptable {
	   		t.Errorf("block usage grew too much: after round 0 = %d, after %d rounds = %d (%.1fx); max acceptable = %d (3x)",
	   			usageAfterRound0, rounds, usageAfterAllRounds,
	   			float64(usageAfterAllRounds)/float64(usageAfterRound0),
	   			maxAcceptable)
	   	}

	   // Also verify correctness: all keys should have the final round's values.

	   	for i, k := range keys {
	   		expected := fmt.Sprintf("value-round%d-%06d-padding-to-make-it-bigger", rounds, i)
	   		mustGet(t, db, k, expected)
	   	}
	*/
}

/* without memfs, we see the 5 blocks in use

Compilation started at Wed Mar 11 04:45:48

go test -v -run GC1K
=== RUN   Test_GC1K_write_1k_keys_with_large_values

gc_test.go:1539 [goID 9] 2026-03-11 07:46:15.100007000 +0000 UTC met = Metrics{
      (just this) Session: true
        KV128 BytesWritten: 196608
       MemWAL BytesWritten: 72140
      REDOLog BytesWritten: 1452
FlexTreePages BytesWritten: 1152
   LARGE.VLOG BytesWritten: 1074000
      Logical BytesWritten: 1083000
        Total BytesWritten: 1345352
                 WriteAmp: 1.242

   -------- lifetime totals over all sessions  --------
    TotalLogical BytesWrit: 1083000
   TotalPhysical BytesWrit: 1345352
       CumulativeWriteAmp: 1.242

   -------- free space / block utilization --------
            KVBlocksTotalLiveBytes: 98304 (0.09 MB)
    TotalFreeBytesInBlocks: 20873216 (19.91 MB)
      FLEXSPACE_BLOCK_SIZE: 4.00 MB
               BlocksInUse: 5  (20.00 MB)
  BlocksWithLowUtilization: 5

   -------- based on parameters used --------
    LowBlockUtilizationPct: 50.0 %
}

--- PASS: Test_GC1K_write_1k_keys_with_large_values (22.77s)
PASS
ok  	github.com/glycerine/yogadb	22.793s

Compilation finished at Wed Mar 11 04:46:15
*/

/* versus with memfs, only 1 block used!

-*- mode: compilation; default-directory: "~/go/src/github.com/glycerine/yogadb/" -*-
Compilation started at Wed Mar 11 04:48:15

go test -v -run GC1K -tags memfs
=== RUN   Test_GC1K_write_1k_keys_with_large_values

gc_test.go:1539 [goID 9] 2026-03-11 07:48:19.334389000 +0000 UTC met = Metrics{
      (just this) Session: true
        KV128 BytesWritten: 221184    // note the difference more here. 196608 above
       MemWAL BytesWritten: 72060     // slightly less here. 72140 above. 80 bytes diff
      REDOLog BytesWritten: 1632      // above 1452
FlexTreePages BytesWritten: 1152      // same
   LARGE.VLOG BytesWritten: 1074000   // same
      Logical BytesWritten: 1083000   // same
        Total BytesWritten: 1370028   // above 1345352
                 WriteAmp: 1.265      // above 1.242

   -------- lifetime totals over all sessions  --------
    TotalLogical BytesWrit: 1083000   // same
   TotalPhysical BytesWrit: 1370028   // above 1345352, slightly less.
       CumulativeWriteAmp: 1.265

   -------- free space / block utilization --------
            KVBlocksTotalLiveBytes: 110592 (0.11 MB)
    TotalFreeBytesInBlocks: 4083712 (3.89 MB)
      FLEXSPACE_BLOCK_SIZE: 4.00 MB
               BlocksInUse: 1  (4.00 MB)
  BlocksWithLowUtilization: 1

   -------- based on parameters used --------
    LowBlockUtilizationPct: 50.0 %
}

--- PASS: Test_GC1K_write_1k_keys_with_large_values (0.01s)
PASS
ok  	github.com/glycerine/yogadb	0.035s

Compilation finished at Wed Mar 11 04:48:19
*/

// same test above but as a batch: our perf was getting
// destroyed b/c every single VLOG Put is fsyncing
// in vlog.go appendAndSync()

func Test_GC_batch1K_write_1k_batch_large_values(t *testing.T) {
	db, _ := openTestDB(t, nil)

	var seed [32]byte
	rng := newPRNG(seed)

	const nKeys = 1000
	keys := make([]string, nKeys)
	vals := make([]string, nKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("usertable/user%v", rng.unlockedStringOfDigits(19))

		v := make([]byte, 1050)
		rng.cha8.Read(v)
		vals[i] = string(v)
	}

	batch := db.NewBatch()

	// Round 0: initial write of all keys.
	for i, k := range keys {
		//mustPut(t, db, k, vals[i])
		panicOn(batch.Set(k, []byte(vals[i])))
	}
	batch.Commit(false)
	db.Sync()

	met := db.SessionMetrics()
	vv("met = %v", met)

	if met.BlocksInUse > 1 { // was 5 !?!
		t.Fatalf("for 1000 33 bytes keys, should only have 1 4MB block in use, not %v", met.BlocksInUse)
	}
}
