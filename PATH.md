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
