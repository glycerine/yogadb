package yogadb

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
)

// ========================================================================
// Invariant Checkers for bruteForce Extent Oracle
//
// These verify the theoretical properties that the extent array must
// maintain at all times. See the plan file for formal definitions.
// ========================================================================

/*

# Plan: Fuzz Test for bruteForce Extent Oracle (`brute_test.go`)

## Context

bruteForce is the ground-truth oracle for FlexTree correctness. Everything in the system
(FlexSpace, FlexDB/YogaDB) builds on FlexTree, and FlexTree is validated against bruteForce.
If bruteForce itself has a bug, every higher-level test is silently wrong. We need a
standalone fuzz test that exercises bruteForce operations with randomized inputs and checks
that its internal invariants hold after every operation.

## The FlexTree/bruteForce Extent Model (from the EuroSys 2022 paper + code)

### What is the domain?

The domain is a **linear logical byte-address space** `[0, MaxLoff)`. This is analogous to a
file's address space: each byte position in `[0, MaxLoff)` maps to exactly one physical
location (or is a hole). The extents collectively describe this mapping.

An **extent** is a triple `(Loff, Len, Poff)` plus metadata `Tag`. It says:
"logical bytes `[Loff, Loff+Len)` are stored at physical bytes `[Poff, Poff+Len)`."
The mapping is linear within an extent: `logical_byte_i → Poff + (i - Loff)`.

### Key operations (from paper Section 3.2)

- **insert-range(loff, poff, len)**: Insert new data at `loff`. All existing data at
  `loff` and beyond is shifted forward by `len` bytes. The new extent maps
  `[loff, loff+len)` → `[poff, poff+len)`. This is the key FlexTree innovation:
  O(log N) shift cost vs O(N) for conventional B+-Trees.

- **collapse-range(loff, len)** (= Delete): Remove the mapping at `[loff, loff+len)`.
  All data beyond `loff+len` shifts backward by `len`. MaxLoff decreases by `len`.
  This "closes the gap" - no hole is left behind.

- **PQuery(loff)**: Point lookup - return the physical offset for logical byte `loff`.

- **Query(loff, len)**: Range lookup - return a list of `(poff, len)` chunks covering
  `[loff, loff+len)`.

- **SetTag(loff, tag)** / **GetTag(loff)**: Attach/retrieve a 16-bit tag at the start of
  the extent containing `loff`. SetTag may split an extent to create a boundary.

- **PDelete(loff)**: Shorthand for `Delete(loff, 1)`.

### Holes

When inserting at `loff > MaxLoff`, the gap `[MaxLoff, loff)` is automatically filled
with **hole extents** - extents whose `Poff` has bit 47 set (`FLEXTREE_HOLE = 1 << 47`).
Holes are real extents in the array; they participate in all invariants. They represent
unmapped logical space (like sparse file regions).

### Merging

Adjacent extents merge into one when ALL of these hold:
1. Physically contiguous: `prev.Poff + prev.Len == new.Poff`
2. Logically contiguous: `prev.Loff + prev.Len == new.Loff`
3. Combined size ≤ MaxExtentSize
4. Tag == 0 (only untagged extents merge)

This is an optimization, not a semantic requirement - merging reduces extent count
but doesn't change the logical-to-physical mapping.

---

## Invariants to Check

These are the properties that must hold after **every** operation on bruteForce.
They constitute the fuzz test's assertion suite.

### INV-1: Strict Loff Ordering (Sorted)
```
∀ i ∈ [0, len(Extents)-1): Extents[i].Loff < Extents[i+1].Loff
```
Extents are strictly sorted by logical offset. No two extents share the same Loff.

### INV-2: Perfect Contiguity (No Gaps)
```
∀ i ∈ [0, len(Extents)-1): Extents[i].Loff + Extents[i].Len == Extents[i+1].Loff
```
Every extent's end is exactly the next extent's start. There are no unmapped bytes
in the logical space. (Holes are explicit extents, not gaps.)

### INV-3: MaxLoff Consistency
```
if len(Extents) == 0: MaxLoff == 0
else: Extents[last].Loff + Extents[last].Len == MaxLoff
```
MaxLoff is the exclusive upper bound of the logical space.

### INV-4: First Extent Starts at Zero
```
if len(Extents) > 0: Extents[0].Loff == 0
```
The logical space always starts at byte 0.

### INV-5: No Zero-Length Extents
```
∀ i: Extents[i].Len > 0
```
Every extent covers at least one byte.

### INV-6: Extent Size Bounded
```
∀ i: Extents[i].Len ≤ MaxExtentSize
```
No single extent exceeds MaxExtentSize.

### INV-7: No Mergeable Neighbors (maximality)
```
∀ i ∈ [0, len(Extents)-1):
  ¬(Extents[i].Poff + Extents[i].Len == Extents[i+1].Poff
    AND Extents[i].Len + Extents[i+1].Len ≤ MaxExtentSize
    AND Extents[i].Tag == 0
    AND Extents[i+1].Tag == 0)
```
If two adjacent extents could be merged (physically contiguous, both untagged,
combined size within limit), they should have been merged already by the Insert path.

**Note**: This invariant is subtle. Delete and SetTag can create adjacent extents that
are physically contiguous but have different tags (one tag set to 0 by Delete, the
other not). Only the Insert path performs merging. So this invariant may need to be
relaxed or skipped after Delete/SetTag operations. We should check whether this holds
in practice and adjust accordingly. If it turns out that Delete can leave mergeable
neighbors, we document that as expected behavior and omit INV-7 from post-Delete checks.

### INV-8: Poff Mask (48-bit)
```
∀ i: Extents[i].Poff == Extents[i].Poff & 0xffffffffffff
```
Physical offsets use at most 48 bits. (The hole flag is within the 48-bit range.)

### INV-9: PQuery Consistency
```
∀ loff ∈ [0, MaxLoff): PQuery(loff) != ^uint64(0)
∀ loff ≥ MaxLoff:       PQuery(loff) == ^uint64(0)
```
Every logical byte within `[0, MaxLoff)` resolves to a valid physical offset.
Every byte outside that range returns the "not found" sentinel.

We can spot-check this (sample N random positions) rather than checking all bytes.

### INV-10: PQuery-Extent Agreement
```
For extent e covering [e.Loff, e.Loff+e.Len):
  ∀ offset ∈ [e.Loff, e.Loff+e.Len):
    PQuery(offset) == e.Poff + (offset - e.Loff)
```
PQuery must return the linear interpolation within the extent. We spot-check
by picking a random byte within each extent.

### INV-11: Query Covers Full Range
```
For any valid range [loff, loff+len) ⊆ [0, MaxLoff):
  q = Query(loff, len)
  q != nil
  sum(q.V[i].Len for all i) == len
```
A range query over valid addresses always succeeds and covers exactly the requested length.

### INV-12: Insert Shifts All Subsequent Bytes
```
Before: PQuery(loff) == p  (for loff ≥ insertion point, loff in valid range)
After Insert(ins_loff, ins_poff, ins_len):
  PQuery(loff + ins_len) == p  (the old byte shifted forward)
```
This is the fundamental insert-range semantic from the paper. We can verify it
by sampling a few positions before and after an insert.

### INV-13: Delete Shifts All Subsequent Bytes Backward
```
Before: PQuery(loff) == p  (for loff > del_loff + del_len, loff in valid range)
After Delete(del_loff, del_len):
  PQuery(loff - del_len) == p  (the surviving byte shifted backward)
```
The collapse-range semantic. We verify by sampling.

### INV-14: Tag Semantics
```
After SetTag(loff, tag):
  GetTag(loff) returns (tag, 0) if tag != 0
  GetTag(loff) returns (0, -1) if tag == 0
For loff in the middle of an extent (not at its Loff):
  GetTag(loff) returns (0, -1) even if the extent has a tag
```
Tags are only retrievable at extent boundaries.

---

## Answers to High-Level Questions

**Do extents partition the domain space?**
Yes - they form a **perfect partition** of `[0, MaxLoff)`. Every byte is in exactly one
extent. There are no gaps and no overlaps. Holes are explicit extents (with FLEXTREE_HOLE
in their Poff), not empty gaps.

**What is the domain space?**
`[0, MaxLoff)` - a linear byte-addressable logical address space. It starts at 0 and
grows with Insert operations, shrinks with Delete operations.

**Must extents be contiguous?**
Yes - always. INV-2 requires `Extents[i].Loff + Extents[i].Len == Extents[i+1].Loff`.
The array covers the entire `[0, MaxLoff)` range with no gaps.

**Are extents allowed to overlap?**
No - never. INV-1 (strict ordering) + INV-2 (contiguity) together guarantee non-overlap.
Each logical byte maps to exactly one physical byte.

---

## Fuzz Test Design

### File: `brute_test.go` (NEW)

### Approach: Operation-sequence fuzzing

The Go fuzzer provides a `[]byte` seed. We interpret it as a sequence of operations:
each operation is decoded from the byte stream as an opcode + parameters. After
every operation, we run the structural invariant checks (INV-1 through INV-8).
Periodically (every N ops) we run the more expensive spot-checks (INV-9 through INV-14).

### Operations to fuzz (6 opcodes)

| Opcode | Operation | Parameters |
|--------|-----------|------------|
| 0 | Insert | loff (relative to MaxLoff), poff, len |
| 1 | Delete | loff (relative to MaxLoff), len |
| 2 | PDelete | loff (relative to MaxLoff) |
| 3 | SetTag | loff (relative to MaxLoff), tag |
| 4 | InsertWTag | loff (relative to MaxLoff), poff, len, tag |
| 5 | InsertBeyond | loff (> MaxLoff, creates holes), poff, len |

Parameters are derived from fuzz bytes modulo valid ranges, ensuring we exercise
both typical and boundary cases. When MaxLoff == 0, only Insert operations are valid.

### MaxExtentSize

Use a small MaxExtentSize (e.g., 256 or 1024) for the fuzz test to trigger more
extent splitting and merging with smaller inputs.

### Structural invariant checker function

```go
func checkInvariants(t testing.TB, bf *bruteForce) {
    // INV-1: Sorted
    // INV-2: Contiguous
    // INV-3: MaxLoff consistency
    // INV-4: Starts at zero
    // INV-5: No zero-length
    // INV-6: Size bounded
    // INV-7: No mergeable neighbors (conditional)
    // INV-8: Poff 48-bit mask
}
```

### Spot-check function (run periodically)

```go
func spotCheckQueries(t testing.TB, bf *bruteForce, rng *rand.Rand) {
    // INV-9: PQuery consistency (sample N random positions + boundary)
    // INV-10: PQuery-extent agreement (one random byte per extent)
    // INV-11: Query covers full range (a few random ranges)
}
```

### Shift verification (run on each Insert/Delete)

```go
func verifyInsertShift(t testing.TB, bf *bruteForce, ...) { // INV-12 }
func verifyDeleteShift(t testing.TB, bf *bruteForce, ...) { // INV-13 }
```

### Fuzz function signature

```go
func FuzzbruteForce(f *testing.F) {
    // Seed corpus: empty, single insert, insert+delete, insert with holes
    f.Fuzz(func(t *testing.T, data []byte) {
        bf := openBruteForce(256) // small MaxExtentSize for more splits
        // decode + execute operations from data
        // checkInvariants after each op
        // spotCheckQueries every 8 ops
    })
}
```

### Additionally: a deterministic randomized stress test

```go
func TestbruteForce_RandomizedInvariants(t *testing.T) {
    // Fixed seed, 10,000 random ops, checks all invariants.
    // Quick regression test that always runs.
}
```

## Files Modified

| File | Changes |
|------|---------|
| `brute_test.go` (NEW) | FuzzbruteForce, TestbruteForce_RandomizedInvariants, checkInvariants, spotCheckQueries, shift verifiers |

## Verification

```bash
# Run the deterministic test
go test -v -run TestbruteForce_RandomizedInvariants -count=1 ./...

# Run the fuzz test for 30 seconds
go test -fuzz FuzzbruteForce -fuzztime 30s -run=xxx -v

# Run the fuzz test for longer (find deeper bugs)
go test -fuzz FuzzbruteForce -fuzztime 5m -run=xxx -v
```

*/

/*
  Tests (6):

  ┌─────────────────────────────────────────┬───────────────────────────────────────────────────────────────────────┐
  │                  Test                   │                            What it checks                             │
  ├─────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────┤
  │                                         │ Go native fuzz test: decodes random byte streams as operation         │
  │ FuzzbruteForce                          │ sequences (Insert/Delete/PDelete/SetTag/InsertWTag/InsertBeyond),     │
  │                                         │ checks all invariants after every op                                  │
  ├─────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────┤
  │ TestbruteForce_RandomizedInvariants     │ 10,000 random ops with fixed seed, checks all invariants including    │
  │                                         │ shift verification                                                    │
  ├─────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────┤
  │ TestbruteForce_InsertOnlyMergeInvariant │ 5,000 insert-only ops, checks INV-7 (no mergeable neighbors)          │
  ├─────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────┤
  │ TestbruteForce_HoleInvariants           │ Insert beyond MaxLoff, verifies hole extents are created correctly    │
  ├─────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────┤
  │ TestbruteForce_DeleteAllAndRebuild      │ Delete everything, verify clean empty state, rebuild                  │
  ├─────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────┤
  │ TestbruteForce_TagSplitInvariants       │ SetTag splits extents correctly, GetTag only works at boundaries      │
  │                                         │ (INV-14)                                                              │
  └─────────────────────────────────────────┴───────────────────────────────────────────────────────────────────────┘
*/

// checkInvariants verifies structural invariants INV-1 through INV-8
// after every bruteForce operation. Failures are fatal.
func checkInvariants(t testing.TB, bf *bruteForce, opDesc string) {
	t.Helper()
	n := len(bf.Extents)

	// INV-3: MaxLoff consistency
	if n == 0 {
		if bf.MaxLoff != 0 {
			t.Fatalf("[INV-3] after %s: empty Extents but MaxLoff = %d, want 0", opDesc, bf.MaxLoff)
		}
		return // all other invariants are trivially satisfied for empty
	}

	// INV-4: First extent starts at zero
	if bf.Extents[0].Loff != 0 {
		t.Fatalf("[INV-4] after %s: Extents[0].Loff = %d, want 0", opDesc, bf.Extents[0].Loff)
	}

	for i := 0; i < n; i++ {
		e := &bf.Extents[i]

		// INV-5: No zero-length extents
		if e.Len == 0 {
			t.Fatalf("[INV-5] after %s: Extents[%d].Len == 0 (Loff=%d)", opDesc, i, e.Loff)
		}

		// INV-6: Extent size bounded by MaxExtentSize
		if e.Len > bf.MaxExtentSize {
			t.Fatalf("[INV-6] after %s: Extents[%d].Len = %d > MaxExtentSize = %d",
				opDesc, i, e.Len, bf.MaxExtentSize)
		}

		// INV-8: Poff fits in 48 bits
		if e.Poff != e.Poff&0xffffffffffff {
			t.Fatalf("[INV-8] after %s: Extents[%d].Poff = 0x%x exceeds 48 bits",
				opDesc, i, e.Poff)
		}

		if i+1 < n {
			next := &bf.Extents[i+1]

			// INV-1: Strict Loff ordering (sorted)
			if e.Loff >= next.Loff {
				t.Fatalf("[INV-1] after %s: Extents[%d].Loff = %d >= Extents[%d].Loff = %d (not sorted)",
					opDesc, i, e.Loff, i+1, next.Loff)
			}

			// INV-2: Perfect contiguity (no gaps)
			end := e.Loff + uint64(e.Len)
			if end != next.Loff {
				t.Fatalf("[INV-2] after %s: gap between Extents[%d] and [%d]: %d + %d = %d != %d",
					opDesc, i, i+1, e.Loff, e.Len, end, next.Loff)
			}
		}
	}

	// INV-3: MaxLoff == last extent end
	last := &bf.Extents[n-1]
	lastEnd := last.Loff + uint64(last.Len)
	if lastEnd != bf.MaxLoff {
		t.Fatalf("[INV-3] after %s: last extent ends at %d but MaxLoff = %d",
			opDesc, lastEnd, bf.MaxLoff)
	}
}

// checkNoMergeableNeighbors checks INV-7: no two adjacent extents could be
// merged. This is only guaranteed after Insert sequences (Delete/SetTag
// may leave mergeable neighbors). Pass skipCheck=true to skip.
func checkNoMergeableNeighbors(t testing.TB, bf *bruteForce, opDesc string) {
	t.Helper()
	for i := 0; i+1 < len(bf.Extents); i++ {
		a := &bf.Extents[i]
		b := &bf.Extents[i+1]
		if a.Poff+uint64(a.Len) == b.Poff &&
			a.Len+b.Len <= bf.MaxExtentSize &&
			a.Tag == 0 && b.Tag == 0 {
			t.Fatalf("[INV-7] after %s: Extents[%d] and [%d] are mergeable: "+
				"a={Loff:%d Len:%d Poff:%d Tag:%d} b={Loff:%d Len:%d Poff:%d Tag:%d}",
				opDesc, i, i+1,
				a.Loff, a.Len, a.Poff, a.Tag,
				b.Loff, b.Len, b.Poff, b.Tag)
		}
	}
}

// spotCheckQueries verifies INV-9, INV-10, INV-11 by sampling.
func spotCheckQueries(t testing.TB, bf *bruteForce, rng *rand.Rand) {
	t.Helper()
	if bf.MaxLoff == 0 {
		// INV-9 boundary: out-of-range query should fail
		if bf.PQuery(0) != ^uint64(0) {
			t.Fatalf("[INV-9] empty BF: PQuery(0) != not-found")
		}
		return
	}

	// INV-9: spot-check random positions inside and outside range
	for i := 0; i < 20; i++ {
		loff := uint64(rng.Int63n(int64(bf.MaxLoff)))
		r := bf.PQuery(loff)
		if r == ^uint64(0) {
			t.Fatalf("[INV-9] PQuery(%d) returned not-found, but loff < MaxLoff=%d", loff, bf.MaxLoff)
		}
	}
	// Out-of-range should return not-found
	if bf.PQuery(bf.MaxLoff) != ^uint64(0) {
		t.Fatalf("[INV-9] PQuery(MaxLoff=%d) should return not-found", bf.MaxLoff)
	}
	if bf.PQuery(bf.MaxLoff+100) != ^uint64(0) {
		t.Fatalf("[INV-9] PQuery(MaxLoff+100) should return not-found")
	}

	// INV-10: PQuery-extent agreement - check one random byte per extent
	for idx := range bf.Extents {
		e := &bf.Extents[idx]
		// Pick a random offset within this extent
		off := e.Loff
		if e.Len > 1 {
			off += uint64(rng.Int63n(int64(e.Len)))
		}
		got := bf.PQuery(off)
		want := e.Poff + (off - e.Loff)
		if got != want {
			t.Fatalf("[INV-10] extent[%d] Loff=%d Len=%d Poff=%d: PQuery(%d) = %d, want %d",
				idx, e.Loff, e.Len, e.Poff, off, got, want)
		}
	}

	// INV-11: Query covers full range - a few random ranges
	for i := 0; i < 5; i++ {
		start := uint64(rng.Int63n(int64(bf.MaxLoff)))
		maxLen := bf.MaxLoff - start
		if maxLen == 0 {
			continue
		}
		length := uint64(rng.Int63n(int64(maxLen))) + 1
		q := bf.Query(start, length)
		if q == nil {
			t.Fatalf("[INV-11] Query(%d, %d) returned nil, range is within [0, %d)",
				start, length, bf.MaxLoff)
		}
		var totalLen uint64
		for j := uint64(0); j < q.Count; j++ {
			totalLen += q.V[j].Len
		}
		if totalLen != length {
			t.Fatalf("[INV-11] Query(%d, %d) returned total len %d, want %d",
				start, length, totalLen, length)
		}
	}
}

// verifyInsertShift checks INV-12: insert shifts all subsequent bytes forward.
// Samples a few positions beyond the insertion point before and after.
func verifyInsertShift(t testing.TB, bf *bruteForce, insLoff uint64, insLen uint32, preSnapshot map[uint64]uint64) {
	t.Helper()
	for loff, oldPoff := range preSnapshot {
		if loff < insLoff {
			// Bytes before insertion point should be unchanged
			got := bf.PQuery(loff)
			if got != oldPoff {
				t.Fatalf("[INV-12] pre-insert byte at loff=%d: PQuery = %d, want %d (unchanged)",
					loff, got, oldPoff)
			}
		} else {
			// Bytes at or after insertion point should be shifted forward by insLen
			newLoff := loff + uint64(insLen)
			got := bf.PQuery(newLoff)
			if got != oldPoff {
				t.Fatalf("[INV-12] shifted byte: old loff=%d, new loff=%d: PQuery = %d, want %d",
					loff, newLoff, got, oldPoff)
			}
		}
	}
}

// verifyDeleteShift checks INV-13: delete shifts all subsequent bytes backward.
func verifyDeleteShift(t testing.TB, bf *bruteForce, delLoff, delLen uint64, preSnapshot map[uint64]uint64) {
	t.Helper()
	for loff, oldPoff := range preSnapshot {
		if loff < delLoff {
			// Bytes before deletion should be unchanged
			got := bf.PQuery(loff)
			if got != oldPoff {
				t.Fatalf("[INV-13] pre-delete byte at loff=%d: PQuery = %d, want %d (unchanged)",
					loff, got, oldPoff)
			}
		} else if loff >= delLoff+delLen {
			// Bytes after deleted range should shift backward by delLen
			newLoff := loff - delLen
			got := bf.PQuery(newLoff)
			if got != oldPoff {
				t.Fatalf("[INV-13] shifted byte: old loff=%d, new loff=%d: PQuery = %d, want %d",
					loff, newLoff, got, oldPoff)
			}
		}
		// Bytes inside the deleted range are gone - no check needed
	}
}

// snapshotPositions samples N random valid positions and records their PQuery results.
func snapshotPositions(bf *bruteForce, rng *rand.Rand, n int) map[uint64]uint64 {
	snap := make(map[uint64]uint64, n)
	if bf.MaxLoff == 0 {
		return snap
	}
	for i := 0; i < n; i++ {
		loff := uint64(rng.Int63n(int64(bf.MaxLoff)))
		snap[loff] = bf.PQuery(loff)
	}
	return snap
}

// ========================================================================
// Byte-stream operation decoder for fuzz testing
// ========================================================================

// consumeU16 reads a uint16 from data, advancing the position.
// Returns 0 if not enough bytes.
func consumeU16(data []byte, pos *int) uint16 {
	if *pos+2 > len(data) {
		*pos = len(data)
		return 0
	}
	v := binary.LittleEndian.Uint16(data[*pos:])
	*pos += 2
	return v
}

// consumeU32 reads a uint32 from data, advancing the position.
func consumeU32(data []byte, pos *int) uint32 {
	if *pos+4 > len(data) {
		*pos = len(data)
		return 0
	}
	v := binary.LittleEndian.Uint32(data[*pos:])
	*pos += 4
	return v
}

// ========================================================================
// FuzzbruteForce - Go native fuzz test
// ========================================================================

func FuzzBruteForce(f *testing.F) {
	// Seed corpus: a few interesting operation sequences.
	// Each byte sequence will be decoded as: [opcode, params...]*

	// Seed 1: empty input (no operations - just check empty invariants)
	f.Add([]byte{})

	// Seed 2: single insert at offset 0
	f.Add([]byte{0, 0, 0, 0, 0, 100, 0, 0, 0, 50, 0, 0, 0})

	// Seed 3: insert then delete all
	f.Add([]byte{
		0, 0, 0, 0, 0, 200, 0, 0, 0, 100, 0, 0, 0, // insert(0, 200, 100)
		1, 0, 0, 0, 0, 100, 0, 0, 0, // delete(0, 100)
	})

	// Seed 4: multiple inserts with tags
	f.Add([]byte{
		0, 0, 0, 0, 0, 0, 0, 0, 0, 20, 0, 0, 0, // insert(0, 0, 20)
		4, 5, 0, 0, 0, 100, 0, 0, 0, 10, 0, 0, 0, 42, 0, // insertWTag(5, 100, 10, tag=42)
		3, 5, 0, 0, 0, 99, 0, // setTag(5, 99)
	})

	// Seed 5: insert beyond MaxLoff (creates holes)
	f.Add([]byte{
		0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, // insert(0, 0, 10)
		5, 0, 2, 0, 0, 0, 1, 0, 0, 5, 0, 0, 0, // insertBeyond(512, 256, 5)
	})

	const fuzzMaxExtentSize = 256

	f.Fuzz(func(t *testing.T, data []byte) {
		bf := openBruteForce(fuzzMaxExtentSize)
		rng := rand.New(rand.NewSource(12345))

		pos := 0
		opCount := 0
		// Cap total logical space to prevent fuzz from creating enormous state
		const maxLogicalSpace = uint64(100_000)

		for pos < len(data) {
			opcode := data[pos] % 6
			pos++

			opCount++
			var opDesc string

			switch opcode {
			case 0: // Insert(loff, poff, len)
				rawLoff := consumeU32(data, &pos)
				poff := consumeU32(data, &pos)
				length := consumeU32(data, &pos)%fuzzMaxExtentSize + 1
				var loff uint64
				if bf.MaxLoff > 0 {
					loff = uint64(rawLoff) % bf.MaxLoff
				}
				if bf.MaxLoff+uint64(length) > maxLogicalSpace {
					continue
				}
				opDesc = fmt.Sprintf("Insert(%d, %d, %d)", loff, poff, length)
				//vv("%v", opDesc)

				snap := snapshotPositions(bf, rng, 8)
				bf.Insert(loff, uint64(poff), length)
				verifyInsertShift(t, bf, loff, length, snap)

			case 1: // Delete(loff, len)
				if bf.MaxLoff == 0 {
					continue
				}
				rawLoff := consumeU32(data, &pos)
				rawLen := consumeU32(data, &pos)
				loff := uint64(rawLoff) % bf.MaxLoff
				maxDel := bf.MaxLoff - loff
				length := uint64(rawLen)%maxDel + 1
				opDesc = fmt.Sprintf("Delete(%d, %d)", loff, length)
				//vv("%v", opDesc)

				snap := snapshotPositions(bf, rng, 8)
				bf.Delete(loff, length)
				verifyDeleteShift(t, bf, loff, length, snap)

			case 2: // PDelete(loff)
				if bf.MaxLoff == 0 {
					continue
				}
				rawLoff := consumeU32(data, &pos)
				loff := uint64(rawLoff) % bf.MaxLoff
				opDesc = fmt.Sprintf("PDelete(%d)", loff)
				//vv("%v", opDesc)

				snap := snapshotPositions(bf, rng, 8)
				bf.PDelete(loff)
				verifyDeleteShift(t, bf, loff, 1, snap)

			case 3: // SetTag(loff, tag)
				if bf.MaxLoff == 0 {
					continue
				}
				rawLoff := consumeU32(data, &pos)
				tag := consumeU16(data, &pos)
				loff := uint64(rawLoff) % bf.MaxLoff
				opDesc = fmt.Sprintf("SetTag(%d, %d)", loff, tag)
				//vv("%v", opDesc)

				bf.SetTag(loff, tag)

				// INV-14: verify tag was set
				if tag != 0 {
					gotTag, rc := bf.GetTag(loff)
					if rc != 0 || gotTag != tag {
						t.Fatalf("[INV-14] after SetTag(%d, %d): GetTag returned (%d, %d)",
							loff, tag, gotTag, rc)
					}
				}

			case 4: // InsertWTag(loff, poff, len, tag)
				rawLoff := consumeU32(data, &pos)
				poff := consumeU32(data, &pos)
				length := consumeU32(data, &pos)%fuzzMaxExtentSize + 1
				tag := consumeU16(data, &pos)
				var loff uint64
				if bf.MaxLoff > 0 {
					loff = uint64(rawLoff) % bf.MaxLoff
				}
				if bf.MaxLoff+uint64(length) > maxLogicalSpace {
					continue
				}
				opDesc = fmt.Sprintf("InsertWTag(%d, %d, %d, tag=%d)", loff, poff, length, tag)
				//vv("%v", opDesc)

				snap := snapshotPositions(bf, rng, 8)
				bf.InsertWTag(loff, uint64(poff), length, tag)
				verifyInsertShift(t, bf, loff, length, snap)

			case 5: // InsertBeyond (insert past MaxLoff, creates holes)
				gap := uint64(consumeU16(data, &pos)) + 1 // 1..65536
				poff := consumeU32(data, &pos)
				length := consumeU32(data, &pos)%fuzzMaxExtentSize + 1
				loff := bf.MaxLoff + gap
				if loff+uint64(length) > maxLogicalSpace {
					continue
				}
				opDesc = fmt.Sprintf("InsertBeyond(loff=%d, poff=%d, len=%d, gap=%d)", loff, poff, length, gap)
				//vv("%v", opDesc)

				bf.Insert(loff, uint64(poff), length)
			}

			if opDesc == "" {
				opDesc = fmt.Sprintf("op#%d", opCount)
			}

			// Check structural invariants after every operation
			checkInvariants(t, bf, opDesc)

			// Spot-check queries every 8 ops (more expensive)
			if opCount%8 == 0 {
				spotCheckQueries(t, bf, rng)
			}
		}

		// Final comprehensive check
		checkInvariants(t, bf, "final")
		spotCheckQueries(t, bf, rng)
	})
}

// ========================================================================
// TestbruteForce_RandomizedInvariants - deterministic stress test
// ========================================================================

func TestBruteForce_RandomizedInvariants(t *testing.T) {
	const (
		maxExtentSize = 256
		numOps        = 10_000
		seed          = 98765
	)

	bf := openBruteForce(maxExtentSize)
	rng := rand.New(rand.NewSource(seed))

	// Start with a base extent so we have something to work with
	bf.Insert(0, 1000, 50)
	checkInvariants(t, bf, "initial insert")
	checkNoMergeableNeighbors(t, bf, "initial insert")

	for i := 0; i < numOps; i++ {
		op := rng.Intn(6)
		var opDesc string

		switch op {
		case 0: // Insert
			var loff uint64
			if bf.MaxLoff > 0 {
				loff = uint64(rng.Int63n(int64(bf.MaxLoff)))
			}
			poff := uint64(rng.Int63n(1 << 30))
			length := uint32(rng.Intn(maxExtentSize)) + 1
			if bf.MaxLoff+uint64(length) > 500_000 {
				continue // cap growth
			}
			opDesc = fmt.Sprintf("op%d: Insert(%d, %d, %d)", i, loff, poff, length)

			snap := snapshotPositions(bf, rng, 5)
			bf.Insert(loff, poff, length)
			verifyInsertShift(t, bf, loff, length, snap)

		case 1: // Delete
			if bf.MaxLoff < 2 {
				continue
			}
			loff := uint64(rng.Int63n(int64(bf.MaxLoff - 1)))
			maxDel := bf.MaxLoff - loff
			length := uint64(rng.Int63n(int64(maxDel))) + 1
			opDesc = fmt.Sprintf("op%d: Delete(%d, %d)", i, loff, length)

			snap := snapshotPositions(bf, rng, 5)
			bf.Delete(loff, length)
			verifyDeleteShift(t, bf, loff, length, snap)

		case 2: // PDelete
			if bf.MaxLoff == 0 {
				continue
			}
			loff := uint64(rng.Int63n(int64(bf.MaxLoff)))
			opDesc = fmt.Sprintf("op%d: PDelete(%d)", i, loff)

			snap := snapshotPositions(bf, rng, 5)
			bf.PDelete(loff)
			verifyDeleteShift(t, bf, loff, 1, snap)

		case 3: // SetTag
			if bf.MaxLoff == 0 {
				continue
			}
			loff := uint64(rng.Int63n(int64(bf.MaxLoff)))
			tag := uint16(rng.Intn(0x10000))
			opDesc = fmt.Sprintf("op%d: SetTag(%d, %d)", i, loff, tag)
			bf.SetTag(loff, tag)

			// INV-14: verify
			if tag != 0 {
				gotTag, rc := bf.GetTag(loff)
				if rc != 0 || gotTag != tag {
					t.Fatalf("[INV-14] after %s: GetTag = (%d, %d)", opDesc, gotTag, rc)
				}
			}

		case 4: // InsertWTag
			var loff uint64
			if bf.MaxLoff > 0 {
				loff = uint64(rng.Int63n(int64(bf.MaxLoff)))
			}
			poff := uint64(rng.Int63n(1 << 30))
			length := uint32(rng.Intn(maxExtentSize)) + 1
			tag := uint16(rng.Intn(0x10000))
			if bf.MaxLoff+uint64(length) > 500_000 {
				continue
			}
			opDesc = fmt.Sprintf("op%d: InsertWTag(%d, %d, %d, tag=%d)", i, loff, poff, length, tag)

			snap := snapshotPositions(bf, rng, 5)
			bf.InsertWTag(loff, poff, length, tag)
			verifyInsertShift(t, bf, loff, length, snap)

		case 5: // InsertBeyond (with holes)
			gap := uint64(rng.Intn(1000)) + 1
			poff := uint64(rng.Int63n(1 << 30))
			length := uint32(rng.Intn(maxExtentSize)) + 1
			loff := bf.MaxLoff + gap
			if loff+uint64(length) > 500_000 {
				continue
			}
			opDesc = fmt.Sprintf("op%d: InsertBeyond(loff=%d, gap=%d, poff=%d, len=%d)", i, loff, gap, poff, length)
			bf.Insert(loff, poff, length)
		}

		if opDesc == "" {
			opDesc = fmt.Sprintf("op%d", i)
		}

		checkInvariants(t, bf, opDesc)

		// Spot-check every 100 ops
		if i%100 == 0 {
			spotCheckQueries(t, bf, rng)
		}
	}

	// Final exhaustive check
	checkInvariants(t, bf, "final")
	spotCheckQueries(t, bf, rng)

	t.Logf("Completed %d ops. Final state: %d extents, MaxLoff=%d",
		numOps, len(bf.Extents), bf.MaxLoff)
}

// TestBruteForce_InsertOnlyMergeInvariant runs insert-only sequences
// and verifies INV-7 (no mergeable neighbors) which is only guaranteed
// when no Delete/SetTag operations intervene.
func TestBruteForce_InsertOnlyMergeInvariant(t *testing.T) {
	const maxExtentSize = 128
	bf := openBruteForce(maxExtentSize)
	rng := rand.New(rand.NewSource(54321))

	for i := 0; i < 5000; i++ {
		var loff uint64
		if bf.MaxLoff > 0 {
			loff = uint64(rng.Int63n(int64(bf.MaxLoff)))
		}
		poff := uint64(rng.Int63n(1 << 30))
		length := uint32(rng.Intn(maxExtentSize)) + 1
		if bf.MaxLoff+uint64(length) > 200_000 {
			continue
		}
		opDesc := fmt.Sprintf("Insert#%d(%d, %d, %d)", i, loff, poff, length)
		bf.Insert(loff, poff, length)
		checkInvariants(t, bf, opDesc)
		checkNoMergeableNeighbors(t, bf, opDesc)
	}

	t.Logf("Insert-only: %d extents, MaxLoff=%d", len(bf.Extents), bf.MaxLoff)
}

// TestBruteForce_HoleInvariants verifies that inserting beyond MaxLoff
// creates proper hole extents that satisfy all invariants.
func TestBruteForce_HoleInvariants(t *testing.T) {
	const maxExtentSize = 256
	bf := openBruteForce(maxExtentSize)

	// Insert at offset 0 first
	bf.Insert(0, 1000, 10)
	checkInvariants(t, bf, "base insert")

	if bf.MaxLoff != 10 {
		t.Fatalf("MaxLoff = %d, want 10", bf.MaxLoff)
	}

	// Insert at offset 500 - should create holes [10, 500)
	bf.Insert(500, 2000, 20)
	checkInvariants(t, bf, "insert with holes")

	// Verify hole extents exist with FLEXTREE_HOLE flag
	const FLEXTREE_HOLE = 1 << 47
	foundHole := false
	for _, e := range bf.Extents {
		if e.Poff&FLEXTREE_HOLE != 0 {
			foundHole = true
			break
		}
	}
	if !foundHole {
		t.Fatalf("expected hole extents after inserting beyond MaxLoff")
	}

	// Verify MaxLoff is correct: 10 (original) + 490 (holes) + 20 (new) = 520
	if bf.MaxLoff != 520 {
		t.Fatalf("MaxLoff = %d, want 520", bf.MaxLoff)
	}

	// Every byte in [0, 520) should be queryable
	for loff := uint64(0); loff < bf.MaxLoff; loff++ {
		r := bf.PQuery(loff)
		if r == ^uint64(0) {
			t.Fatalf("PQuery(%d) returned not-found, but loff < MaxLoff=%d", loff, bf.MaxLoff)
		}
	}
}

// TestBruteForce_DeleteAllAndRebuild verifies that deleting everything
// and rebuilding from scratch maintains invariants.
func TestBruteForce_DeleteAllAndRebuild(t *testing.T) {
	const maxExtentSize = 256
	bf := openBruteForce(maxExtentSize)

	// Build up some state
	bf.Insert(0, 100, 50)
	bf.Insert(25, 200, 30)
	bf.Insert(10, 300, 20)
	checkInvariants(t, bf, "after 3 inserts")

	// Delete everything
	bf.Delete(0, bf.MaxLoff)
	checkInvariants(t, bf, "after delete all")

	if bf.MaxLoff != 0 || len(bf.Extents) != 0 {
		t.Fatalf("after delete all: MaxLoff=%d len(Extents)=%d, want 0, 0",
			bf.MaxLoff, len(bf.Extents))
	}

	// Rebuild
	bf.Insert(0, 500, 100)
	checkInvariants(t, bf, "after rebuild")
	spotCheckQueries(t, bf, rand.New(rand.NewSource(1)))
}

// TestBruteForce_TagSplitInvariants verifies that SetTag correctly
// splits extents and that GetTag only works at extent boundaries.
func TestBruteForce_TagSplitInvariants(t *testing.T) {
	const maxExtentSize = 256
	bf := openBruteForce(maxExtentSize)

	bf.Insert(0, 1000, 100)
	checkInvariants(t, bf, "initial")

	// Should be 1 extent: [0, 100) -> poff 1000
	if len(bf.Extents) != 1 {
		t.Fatalf("expected 1 extent, got %d", len(bf.Extents))
	}

	// SetTag at middle of extent - should split
	bf.SetTag(50, 0xBEEF)
	checkInvariants(t, bf, "SetTag(50, 0xBEEF)")

	// Should now be 2 extents: [0,50) and [50,100)
	if len(bf.Extents) != 2 {
		t.Fatalf("expected 2 extents after SetTag split, got %d", len(bf.Extents))
	}

	// INV-14: GetTag at the split boundary should return the tag
	tag, rc := bf.GetTag(50)
	if rc != 0 || tag != 0xBEEF {
		t.Fatalf("GetTag(50) = (%d, %d), want (0xBEEF, 0)", tag, rc)
	}

	// INV-14: GetTag in the middle of an extent should fail
	_, rc = bf.GetTag(25)
	if rc != -1 {
		t.Fatalf("GetTag(25) in middle of extent should return rc=-1, got %d", rc)
	}

	// INV-14: GetTag at extent boundary with tag=0 should also fail
	_, rc = bf.GetTag(0)
	if rc != -1 {
		t.Fatalf("GetTag(0) with tag=0 should return rc=-1, got %d", rc)
	}

	// PQuery should still work correctly across the split
	if bf.PQuery(0) != 1000 {
		t.Fatalf("PQuery(0) = %d, want 1000", bf.PQuery(0))
	}
	if bf.PQuery(50) != 1050 {
		t.Fatalf("PQuery(50) = %d, want 1050", bf.PQuery(50))
	}
	if bf.PQuery(99) != 1099 {
		t.Fatalf("PQuery(99) = %d, want 1099", bf.PQuery(99))
	}
}
