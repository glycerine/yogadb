package yogadb

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	//"github.com/glycerine/vfs"
)

// ========================================================================
// FlexTree Fuzz Test
//
// Uses bruteForce as the ground-truth oracle and verifies both shared
// invariants (INV-1 through INV-14) and FlexTree-specific invariants
// (INV-FT-1 through INV-FT-6) covering: leaf linked list integrity,
// shift consistency, tree height bounds, node count consistency,
// O(log N) operation cost, and oracle agreement.
// ========================================================================

/*
# Plan: FlexTree Fuzz Test (`flextree_fuzz_test.go`)

## Context

bruteForce has been fuzz-tested with 14 structural invariants (brute_test.go). Now we apply
the same invariants to the real FlexTree implementation as a "belt and suspenders" check.
Additionally, we add FlexTree-specific invariants that test the B+-tree structure, the shift
propagation mechanism, the leaf linked list, and the O(log N) operation cost guarantee.

## Approach

### Part 1: Add performance counters to FlexTree (flextree.go)

Add unexported fields to `FlexTree` struct for measuring per-operation cost:

```go
// Per-operation performance counters (unexported, reset before each op)
nodesVisited    int64  // nodes touched during findLeafNode + shiftUpPropagate
splitCount      int64  // leaf + internal splits triggered
shiftPropNodes  int64  // internal nodes updated by shiftUpPropagate
```

Instrument these functions:
- `findLeafNode()`: increment `nodesVisited` for each level descended
- `shiftUpPropagate()`: increment `shiftPropNodes` per internal node updated
- `splitLeafNode()` / `splitInternalNode()`: increment `splitCount`
- `insertR()`: reset counters before, read after

Add a method to reset and read:
```go
func (tree *FlexTree) resetOpCounters()
func (tree *FlexTree) opCounters() (visited, splits, shiftNodes int64)
```

### Part 2: Extract effective extents from FlexTree for invariant checking

We need a function that walks the FlexTree and reconstructs the same flat extent array
that bruteForce maintains, with effective (global) Loff values. This uses the Pos cursor:

```go
func (tree *FlexTree) allExtents() []bruteForceExtent
```

Walk via `PosGet(0)` + `Forward()`, collecting extents as `bruteForceExtent` structs.
This allows direct comparison with bruteForce and reuse of `checkInvariants()`.

### Part 3: FlexTree-specific invariants

On top of INV-1 through INV-14 (applied to extracted extents), add:

**INV-FT-1: Leaf linked list integrity**
```
Starting from LeafHead, following Next pointers visits every leaf exactly once.
Each leaf's Prev pointer points back to its predecessor.
Total extents across all leaves == total count from allExtents().
```

**INV-FT-2: Internal node shift consistency**
```
For any leaf L reached via path (X₀,S₀), (X₁,S₁), ..., (Xₙ,Sₙ):
  effectiveLoff(extent) = extent.Loff + Σ(Sᵢ for all i on path)
  This must match the global Loff from PQuery.
```
We verify by comparing `allExtents()` output against PQuery spot-checks.

**INV-FT-3: Tree height bounded by O(log N)**
```
path.Level ≤ FLEXTREE_PATH_DEPTH (= 7)
Actual height ≤ ceil(log₃₀(N / 60)) + 1  (approximate)
```

**INV-FT-4: Node count consistency**
```
(non-free leaves in LeafArena) + (non-free internals in InternalArena)
== number of nodes reachable from Root
```

**INV-FT-5: O(log N) operation cost**
```
After each Insert or Delete:
  nodesVisited ≤ C × (tree_height + splitCount)
where C is a small constant (e.g., 3) and tree_height ≤ 7.
```
This asserts that operations don't degenerate to O(N). With height ≤ 7 and
FLEXTREE_INTERNAL_CAP = 30 siblings per level, worst case shiftUpPropagate touches
at most 7 nodes × 30 pivot updates = 210 pivot updates per insert.
The key assertion: `nodesVisited` is bounded by a constant independent of N.

**INV-FT-6: Oracle agreement**
```
After each operation performed on BOTH FlexTree and bruteForce:
  FlexTree.PQuery(loff) == bruteForce.PQuery(loff)  for all sampled loff
  FlexTree.GetMaxLoff() == bruteForce.GetMaxLoff()
  FlexTree.Query(loff, len) == bruteForce.Query(loff, len)  for sampled ranges
```
This is the ultimate correctness check - the FlexTree matches its oracle.

### Part 4: Fuzz test structure

```go
func FuzzFlexTree(f *testing.F) {
    f.Fuzz(func(t *testing.T, data []byte) {
        ft := OpenFlexTree("")
        bf := OpenbruteForce(256)
        // decode operations from data (same opcodes as FuzzbruteForce)
        // apply each op to BOTH ft and bf
        // after each op:
        //   - check INV-FT-5 (O(log N) cost)
        //   - check INV-FT-6 (oracle agreement: MaxLoff + sampled PQuery)
        // every 8 ops:
        //   - extract allExtents(ft), run checkInvariants()
        //   - check INV-FT-1 (leaf linked list)
        //   - full oracle agreement spot-check
    })
}
```

### Part 5: Deterministic stress test

```go
func TestFlexTree_RandomizedInvariants(t *testing.T) {
    // Fixed seed, 10,000 ops on both FlexTree and bruteForce
    // Full invariant suite after every op
    // Log final stats: tree height, node count, max nodesVisited
}
```

## Files Modified

| File | Changes |
|------|---------|
| `flextree.go` | Add 3 unexported counter fields, `resetOpCounters()`, `opCounters()`, `allExtents()`, instrument `findLeafNode`, `shiftUpPropagate`, `splitLeafNode`, `splitInternalNode` |
| `flextree_fuzz_test.go` (NEW) | `FuzzFlexTree`, `TestFlexTree_RandomizedInvariants`, `checkLeafLinkedList()`, `checkNodeCountConsistency()`, `checkOracleAgreement()` |

## Verification

```bash
# Deterministic test
go test -v -run TestFlexTree_RandomizedInvariants -count=1 ./...

# Fuzz test for 30s
go test -fuzz FuzzFlexTree -fuzztime 30s -v -run=xxx

# longer
go test -fuzz FuzzFlexTree -fuzztime 5m -v -run=xxx

# Full suite (ensure no regressions)
go test -count=1 ./...
```
*/

/*
 flextree.go - Performance counters and allExtents()

  1. Added 3 unexported counter fields to FlexTree struct:
    - nodesVisited - incremented in findLeafNode() for each internal node descended
    - splitCount - incremented in splitLeafNode() and splitInternalNode()
    - shiftPropNodes - incremented in shiftUpPropagate() for each internal node updated

  2. Added methods: resetOpCounters() and opCounters() to reset/read counters

  3. Added allExtents() - walks the leaf linked list via PosGet(0) +
     ForwardExtent(), returning []bruteForceExtent with effective (global) Loff
     values for comparison with bruteForce

  4. Instrumented public methods (Insert, InsertWTag, Delete, PDelete, SetTag)
     to call resetOpCounters() before operation

  flextree_fuzz_test.go (NEW) - 5 tests + 1 fuzz test

  ┌─────────────────────────────────────┬───────────────────────────────────────────────┐
  │                Test                 │                What It Checks                 │
  ├─────────────────────────────────────┼───────────────────────────────────────────────┤
  │                                     │ Fuzz: applies random ops to both FlexTree and │
  │ FuzzFlexTree                        │  bruteForce, checks INV-FT-1 through INV-FT-6 │
  │                                     │  + shared INV-1 through INV-14                │
  ├─────────────────────────────────────┼───────────────────────────────────────────────┤
  │ TestFlexTree_RandomizedInvariants   │ 10,000 deterministic random ops, full         │
  │                                     │ invariant suite every 50 ops                  │
  ├─────────────────────────────────────┼───────────────────────────────────────────────┤
  │ TestFlexTree_LeafLinkedListBasic    │ 200 inserts + 100 deletes, verifying leaf     │
  │                                     │ linked list integrity (INV-FT-1)              │
  ├─────────────────────────────────────┼───────────────────────────────────────────────┤
  │                                     │ 500 inserts + 200 deletes with full           │
  │ TestFlexTree_OracleAgreementFocused │ extent-by-extent oracle comparison            │
  │                                     │ (INV-FT-2/6)                                  │
  ├─────────────────────────────────────┼───────────────────────────────────────────────┤
  │ TestFlexTree_TreeHeightBound        │ 5000 inserts verifying height ≤ theoretical   │
  │                                     │ log₃₀(N/60)+2 (INV-FT-3)                      │
  └─────────────────────────────────────┴───────────────────────────────────────────────┘

  FlexTree-Specific Invariants Checked

  ┌───────────┬─────────────────────────────────────────────────────────────────────────┐
  │ Invariant │                               Description                               │
  ├───────────┼─────────────────────────────────────────────────────────────────────────┤
  │ INV-FT-1  │ Leaf linked list: Prev/Next consistency, all leaves reachable, extent   │
  │           │ count matches                                                           │
  ├───────────┼─────────────────────────────────────────────────────────────────────────┤
  │ INV-FT-2  │ Shift consistency: extracted extents match bruteForce extent-by-extent  │
  ├───────────┼─────────────────────────────────────────────────────────────────────────┤
  │ INV-FT-3  │ Tree height ≤ FLEXTREE_PATH_DEPTH (7)                                   │
  ├───────────┼─────────────────────────────────────────────────────────────────────────┤
  │ INV-FT-4  │ Node count: reachable nodes == non-free arena nodes                     │
  ├───────────┼─────────────────────────────────────────────────────────────────────────┤
  │ INV-FT-5  │ O(log N) cost: nodesVisited bounded for single Insert operations        │
  ├───────────┼─────────────────────────────────────────────────────────────────────────┤
  │ INV-FT-6  │ Oracle agreement: PQuery, Query, MaxLoff all match bruteForce           │
  └───────────┴─────────────────────────────────────────────────────────────────────────┘

*/

// --- FlexTree-specific invariant checkers ---

// checkLeafLinkedList verifies INV-FT-1: the leaf linked list is consistent.
// Starting from LeafHead, following Next pointers visits every leaf exactly
// once. Each leaf's Prev pointer points back to its predecessor. Total
// extents across all leaves equals the total from allExtents().
func checkLeafLinkedList(t testing.TB, ft *FlexTree, opDesc string) {
	t.Helper()

	if ft.Root.IsIllegal() {
		return
	}

	// Count total reachable leaves and extents via linked list
	seen := make(map[NodeID]bool)
	var totalExtents uint32
	var leafCount int

	cur := ft.LeafHead
	var prevID NodeID = IllegalID

	for !cur.IsIllegal() {
		if seen[cur] {
			t.Fatalf("[INV-FT-1] after %s: cycle in leaf linked list at NodeID %v", opDesc, cur)
		}
		seen[cur] = true
		leafCount++

		le := ft.GetLeaf(cur)
		if le.Prev != prevID {
			t.Fatalf("[INV-FT-1] after %s: leaf %v Prev=%v, expected %v",
				opDesc, cur, le.Prev, prevID)
		}
		totalExtents += le.Count

		prevID = cur
		cur = le.Next
	}

	// Verify extent count matches allExtents
	allExts := ft.allExtents()
	if int(totalExtents) != len(allExts) {
		t.Fatalf("[INV-FT-1] after %s: leaf linked list has %d extents, allExtents() has %d",
			opDesc, totalExtents, len(allExts))
	}

	// Verify all leaves in arenas that are not freed are reachable
	reachableLeaves := countReachableLeaves(ft)
	if reachableLeaves != leafCount {
		t.Fatalf("[INV-FT-1] after %s: %d reachable leaves from root, but %d in linked list",
			opDesc, reachableLeaves, leafCount)
	}
}

// countReachableLeaves counts leaves reachable from the root via tree traversal.
func countReachableLeaves(ft *FlexTree) int {
	if ft.Root.IsIllegal() {
		return 0
	}
	return countReachableLeavesFrom(ft, ft.Root)
}

func countReachableLeavesFrom(ft *FlexTree, id NodeID) int {
	if id.IsIllegal() {
		return 0
	}
	if id.IsLeaf() {
		return 1
	}
	ie := ft.GetInternal(id)
	count := 0
	for i := uint32(0); i <= ie.Count; i++ {
		count += countReachableLeavesFrom(ft, ie.Children[i].NodeID)
	}
	return count
}

// checkNodeCountConsistency verifies INV-FT-4: the number of non-free nodes
// reachable from Root equals the total non-free nodes in arenas.
func checkNodeCountConsistency(t testing.TB, ft *FlexTree, opDesc string) {
	t.Helper()

	if ft.Root.IsIllegal() {
		return
	}

	reachableLeaves, reachableInternals := countReachableNodes(ft)

	// Count non-free leaves
	freeLeafSet := make(map[int32]bool, len(ft.FreeLeaves))
	for _, idx := range ft.FreeLeaves {
		freeLeafSet[idx] = true
	}
	arenaLeaves := 0
	for i := range ft.LeafArena {
		if !ft.LeafArena[i].Freed && !freeLeafSet[int32(i)] {
			arenaLeaves++
		}
	}

	// Count non-free internals
	freeInternalSet := make(map[int32]bool, len(ft.FreeInternals))
	for _, idx := range ft.FreeInternals {
		freeInternalSet[idx] = true
	}
	arenaInternals := 0
	for i := range ft.InternalArena {
		if !ft.InternalArena[i].Freed && !freeInternalSet[int32(i)] {
			arenaInternals++
		}
	}

	if reachableLeaves != arenaLeaves {
		t.Fatalf("[INV-FT-4] after %s: %d reachable leaves vs %d non-free leaves in arena",
			opDesc, reachableLeaves, arenaLeaves)
	}
	if reachableInternals != arenaInternals {
		t.Fatalf("[INV-FT-4] after %s: %d reachable internals vs %d non-free internals in arena",
			opDesc, reachableInternals, arenaInternals)
	}
}

func countReachableNodes(ft *FlexTree) (leaves, internals int) {
	if ft.Root.IsIllegal() {
		return 0, 0
	}
	countNodesFrom(ft, ft.Root, &leaves, &internals)
	return
}

func countNodesFrom(ft *FlexTree, id NodeID, leaves, internals *int) {
	if id.IsIllegal() {
		return
	}
	if id.IsLeaf() {
		*leaves++
		return
	}
	*internals++
	ie := ft.GetInternal(id)
	for i := uint32(0); i <= ie.Count; i++ {
		countNodesFrom(ft, ie.Children[i].NodeID, leaves, internals)
	}
}

// treeHeight returns the height of the tree (1 = root is leaf).
func treeHeight(ft *FlexTree) int {
	if ft.Root.IsIllegal() {
		return 0
	}
	h := 1
	node := ft.Root
	for node.IsInternal() {
		ie := ft.GetInternal(node)
		node = ie.Children[0].NodeID
		h++
	}
	return h
}

// checkTreeHeight verifies INV-FT-3: tree height is bounded.
func checkTreeHeight(t testing.TB, ft *FlexTree, opDesc string) {
	t.Helper()
	h := treeHeight(ft)
	if h > FLEXTREE_PATH_DEPTH {
		t.Fatalf("[INV-FT-3] after %s: tree height %d exceeds FLEXTREE_PATH_DEPTH=%d",
			opDesc, h, FLEXTREE_PATH_DEPTH)
	}
}

// checkInsertOpCost verifies INV-FT-5: O(log N) insert operation cost.
// After each Insert or InsertWTag, nodesVisited should be bounded by a
// constant proportional to tree height plus splits.
// Note: Delete is excluded because it loops internally (calling findLeafNode
// once per extent crossed), so its total cost is O(k * height) where k is
// the number of extents in the delete range.
func checkInsertOpCost(t testing.TB, ft *FlexTree, opDesc string) {
	t.Helper()
	visited, splits, _ := ft.opCounters()
	h := treeHeight(ft)

	// For a single insertR call:
	//   findLeafNode: at most h-1 internal node visits
	//   shiftUpPropagate: at most h-1 internal nodes
	//   splits: each split calls findLeafNode-like traversal
	// For insert beyond MaxLoff, insertR calls itself recursively for holes,
	// each of which does its own findLeafNode. So bound needs to be generous.
	// Use: 10 * (height + splits) + 50 as a generous but still meaningful bound.
	maxVisited := int64(10*h + 10*int(splits) + 50)
	if visited > maxVisited {
		t.Fatalf("[INV-FT-5] after %s: nodesVisited=%d exceeds bound %d (height=%d, splits=%d)",
			opDesc, visited, maxVisited, h, splits)
	}
}

// checkOracleAgreement verifies INV-FT-6: FlexTree matches bruteForce.
func checkOracleAgreement(t testing.TB, ft *FlexTree, bf *bruteForce, rng *rand.Rand, opDesc string) {
	t.Helper()

	// MaxLoff must match
	if ft.GetMaxLoff() != bf.MaxLoff {
		t.Fatalf("[INV-FT-6] after %s: FlexTree.MaxLoff=%d != bruteForce.MaxLoff=%d",
			opDesc, ft.GetMaxLoff(), bf.MaxLoff)
	}

	if bf.MaxLoff == 0 {
		return
	}

	// Spot-check PQuery at random positions
	nSamples := 20
	if int(bf.MaxLoff) < nSamples {
		nSamples = int(bf.MaxLoff)
	}
	for i := 0; i < nSamples; i++ {
		loff := uint64(rng.Int63n(int64(bf.MaxLoff)))
		ftResult := ft.PQuery(loff)
		bfResult := bf.PQuery(loff)
		if ftResult != bfResult {
			t.Fatalf("[INV-FT-6] after %s: PQuery(%d): FlexTree=%d, bruteForce=%d",
				opDesc, loff, ftResult, bfResult)
		}
	}

	// Out-of-range
	ftOOB := ft.PQuery(bf.MaxLoff)
	bfOOB := bf.PQuery(bf.MaxLoff)
	if ftOOB != bfOOB {
		t.Fatalf("[INV-FT-6] after %s: PQuery(MaxLoff=%d): FlexTree=%d, bruteForce=%d",
			opDesc, bf.MaxLoff, ftOOB, bfOOB)
	}

	// Spot-check Query
	for i := 0; i < 5; i++ {
		start := uint64(rng.Int63n(int64(bf.MaxLoff)))
		maxLen := bf.MaxLoff - start
		if maxLen == 0 {
			continue
		}
		length := uint64(rng.Int63n(int64(maxLen))) + 1
		ftQ := ft.Query(start, length)
		bfQ := bf.Query(start, length)
		if (ftQ == nil) != (bfQ == nil) {
			t.Fatalf("[INV-FT-6] after %s: Query(%d, %d): FlexTree nil=%v, bruteForce nil=%v",
				opDesc, start, length, ftQ == nil, bfQ == nil)
		}
		if ftQ != nil && bfQ != nil {
			var ftTotal, bfTotal uint64
			for j := uint64(0); j < ftQ.Count; j++ {
				ftTotal += ftQ.V[j].Len
			}
			for j := uint64(0); j < bfQ.Count; j++ {
				bfTotal += bfQ.V[j].Len
			}
			if ftTotal != bfTotal {
				t.Fatalf("[INV-FT-6] after %s: Query(%d, %d) total len: FlexTree=%d, bruteForce=%d",
					opDesc, start, length, ftTotal, bfTotal)
			}
		}
	}
}

// checkExtentsMatchOracle verifies INV-FT-2 and INV-FT-6 together:
// the flat extent array extracted from FlexTree matches bruteForce
// when checked via checkInvariants.
func checkExtentsMatchOracle(t testing.TB, ft *FlexTree, bf *bruteForce, opDesc string) {
	t.Helper()

	ftExts := ft.allExtents()
	bfExts := bf.Extents

	if len(ftExts) != len(bfExts) {
		t.Fatalf("[INV-FT-2] after %s: FlexTree has %d extents, bruteForce has %d",
			opDesc, len(ftExts), len(bfExts))
	}

	for i := range ftExts {
		if ftExts[i].Loff != bfExts[i].Loff {
			t.Fatalf("[INV-FT-2] after %s: extent[%d] Loff: FlexTree=%d, bruteForce=%d",
				opDesc, i, ftExts[i].Loff, bfExts[i].Loff)
		}
		if ftExts[i].Len != bfExts[i].Len {
			t.Fatalf("[INV-FT-2] after %s: extent[%d] Len: FlexTree=%d, bruteForce=%d",
				opDesc, i, ftExts[i].Len, bfExts[i].Len)
		}
		if ftExts[i].Poff != bfExts[i].Poff {
			t.Fatalf("[INV-FT-2] after %s: extent[%d] Poff: FlexTree=0x%x, bruteForce=0x%x",
				opDesc, i, ftExts[i].Poff, bfExts[i].Poff)
		}
		if ftExts[i].Tag != bfExts[i].Tag {
			t.Fatalf("[INV-FT-2] after %s: extent[%d] Tag: FlexTree=%d, bruteForce=%d",
				opDesc, i, ftExts[i].Tag, bfExts[i].Tag)
		}
	}
}

// checkFlexTreeInvariants runs the bruteForce structural invariants on the
// extracted FlexTree extent array by constructing a temporary bruteForce.
func checkFlexTreeInvariants(t testing.TB, ft *FlexTree, opDesc string) {
	t.Helper()

	tmpBF := &bruteForce{
		MaxLoff:       ft.GetMaxLoff(),
		MaxExtentSize: ft.MaxExtentSize,
		Extents:       ft.allExtents(),
	}
	checkInvariants(t, tmpBF, opDesc+" [FlexTree extents]")
}

// ========================================================================
// FuzzFlexTree - Go native fuzz test
// ========================================================================

func FuzzFlexTree(f *testing.F) {
	// Seed corpus
	f.Add([]byte{})

	// Single insert
	f.Add([]byte{0, 0, 0, 0, 0, 100, 0, 0, 0, 50, 0, 0, 0})

	// Insert then delete
	f.Add([]byte{
		0, 0, 0, 0, 0, 200, 0, 0, 0, 100, 0, 0, 0,
		1, 0, 0, 0, 0, 100, 0, 0, 0,
	})

	// Multiple inserts with tags
	f.Add([]byte{
		0, 0, 0, 0, 0, 0, 0, 0, 0, 20, 0, 0, 0,
		4, 5, 0, 0, 0, 100, 0, 0, 0, 10, 0, 0, 0, 42, 0,
		3, 5, 0, 0, 0, 99, 0,
	})

	// Insert beyond MaxLoff (creates holes)
	f.Add([]byte{
		0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0,
		5, 0, 2, 0, 0, 0, 1, 0, 0, 5, 0, 0, 0,
	})

	const fuzzMaxExtentSize = 256

	f.Fuzz(func(t *testing.T, data []byte) {
		// only real os based:
		//dir := cryRand15B() + "_FuzzFlexTree.out"
		//fs := vfs.Default
		//panicOn(fs.MkdirAll(dir, 0777))
		//defer fs.RemoveAll(dir)

		// vs either -tags memfs or not:
		fs, dir := newTestFS(t)

		ft, err := OpenFlexTreeCoW(dir, fs)
		panicOn(err)
		bf := openBruteForce(fuzzMaxExtentSize)
		ft.MaxExtentSize = fuzzMaxExtentSize
		rng := rand.New(rand.NewSource(12345))

		pos := 0
		opCount := 0
		const maxLogicalSpace = uint64(100_000)

		for pos < len(data) {
			opcode := data[pos] % 6
			pos++

			opCount++
			var opDesc string
			isSimpleInsert := false // true only for Insert/InsertWTag at loff ≤ MaxLoff

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
				isSimpleInsert = true

				bf.Insert(loff, uint64(poff), length)
				ft.Insert(loff, uint64(poff), length)

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

				bf.Delete(loff, length)
				ft.Delete(loff, length)

			case 2: // PDelete(loff)
				if bf.MaxLoff == 0 {
					continue
				}
				rawLoff := consumeU32(data, &pos)
				loff := uint64(rawLoff) % bf.MaxLoff
				opDesc = fmt.Sprintf("PDelete(%d)", loff)

				bf.PDelete(loff)
				ft.PDelete(loff)

			case 3: // SetTag(loff, tag)
				if bf.MaxLoff == 0 {
					continue
				}
				rawLoff := consumeU32(data, &pos)
				tag := consumeU16(data, &pos)
				loff := uint64(rawLoff) % bf.MaxLoff
				opDesc = fmt.Sprintf("SetTag(%d, %d)", loff, tag)

				bf.SetTag(loff, tag)
				ft.SetTag(loff, tag)

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
				isSimpleInsert = true

				bf.InsertWTag(loff, uint64(poff), length, tag)
				ft.InsertWTag(loff, uint64(poff), length, tag)

			case 5: // InsertBeyond (insert past MaxLoff, creates holes)
				// InsertBeyond cost is O(gap/MaxExtentSize * height) due to
				// hole extent insertion, so we don't check cost here.
				gap := uint64(consumeU16(data, &pos)) + 1
				poff := consumeU32(data, &pos)
				length := consumeU32(data, &pos)%fuzzMaxExtentSize + 1
				loff := bf.MaxLoff + gap
				if loff+uint64(length) > maxLogicalSpace {
					continue
				}
				opDesc = fmt.Sprintf("InsertBeyond(loff=%d, poff=%d, len=%d, gap=%d)", loff, poff, length, gap)

				bf.Insert(loff, uint64(poff), length)
				ft.Insert(loff, uint64(poff), length)
			}

			if opDesc == "" {
				opDesc = fmt.Sprintf("op#%d", opCount)
			}
			//vv("opDesc = %v", opDesc)

			// INV-FT-5: check O(log N) cost for simple insert operations only.
			// Must run BEFORE checkFlexTreeContiguity, which calls PQuery
			// and inflates nodesVisited.
			if isSimpleInsert {
				checkInsertOpCost(t, ft, opDesc)
			}
			// avoid cross talk: run AFTER checkInsertOpCost!
			checkFlexTreeContiguity(t, ft, opDesc)

			// INV-FT-6: oracle agreement (MaxLoff + sampled PQuery)
			checkOracleAgreement(t, ft, bf, rng, opDesc)

			// Every 8 ops: full invariant suite
			if opCount%8 == 0 {
				checkFlexTreeInvariants(t, ft, opDesc)
				checkLeafLinkedList(t, ft, opDesc)
				checkExtentsMatchOracle(t, ft, bf, opDesc)
				checkTreeHeight(t, ft, opDesc)
			}
		}

		// Final comprehensive checks
		checkFlexTreeInvariants(t, ft, "final")
		checkLeafLinkedList(t, ft, "final")
		checkExtentsMatchOracle(t, ft, bf, "final")
		checkTreeHeight(t, ft, "final")
		checkOracleAgreement(t, ft, bf, rng, "final")
	})
}

// ========================================================================
// TestFlexTree_RandomizedInvariants - deterministic stress test
// ========================================================================

func TestFlexTree_RandomizedInvariants(t *testing.T) {
	const (
		maxExtentSize = 256
		numOps        = 10_000
		seed          = 98765
	)

	fs, dir := newTestFS(t)
	ft, err := OpenFlexTreeCoW(dir, fs)
	panicOn(err)
	ft.MaxExtentSize = maxExtentSize
	bf := openBruteForce(maxExtentSize)
	rng := rand.New(rand.NewSource(seed))

	// Start with a base extent
	bf.Insert(0, 1000, 50)
	ft.Insert(0, 1000, 50)
	checkFlexTreeInvariants(t, ft, "initial insert")
	checkOracleAgreement(t, ft, bf, rng, "initial insert")

	var maxNodesVisited int64
	var maxHeight int

	opsCompleted := 0
	for i := 0; i < numOps; i++ {
		op := rng.Intn(6)
		var opDesc string
		isSimpleInsert := false

		switch op {
		case 0: // Insert
			var loff uint64
			if bf.MaxLoff > 0 {
				loff = uint64(rng.Int63n(int64(bf.MaxLoff)))
			}
			poff := uint64(rng.Int63n(1 << 30))
			length := uint32(rng.Intn(maxExtentSize)) + 1
			if bf.MaxLoff+uint64(length) > 500_000 {
				continue
			}
			opDesc = fmt.Sprintf("op%d: Insert(%d, %d, %d)", i, loff, poff, length)
			isSimpleInsert = true

			bf.Insert(loff, poff, length)
			ft.Insert(loff, poff, length)

		case 1: // Delete
			if bf.MaxLoff < 2 {
				continue
			}
			loff := uint64(rng.Int63n(int64(bf.MaxLoff - 1)))
			maxDel := bf.MaxLoff - loff
			length := uint64(rng.Int63n(int64(maxDel))) + 1
			opDesc = fmt.Sprintf("op%d: Delete(%d, %d)", i, loff, length)

			bf.Delete(loff, length)
			ft.Delete(loff, length)

		case 2: // PDelete
			if bf.MaxLoff == 0 {
				continue
			}
			loff := uint64(rng.Int63n(int64(bf.MaxLoff)))
			opDesc = fmt.Sprintf("op%d: PDelete(%d)", i, loff)

			bf.PDelete(loff)
			ft.PDelete(loff)

		case 3: // SetTag
			if bf.MaxLoff == 0 {
				continue
			}
			loff := uint64(rng.Int63n(int64(bf.MaxLoff)))
			tag := uint16(rng.Intn(0x10000))
			opDesc = fmt.Sprintf("op%d: SetTag(%d, %d)", i, loff, tag)

			bf.SetTag(loff, tag)
			ft.SetTag(loff, tag)

			// INV-14: verify tag was set on both
			if tag != 0 {
				bfTag, bfRc := bf.GetTag(loff)
				ftTag, ftRc := ft.GetTag(loff)
				if bfRc != ftRc || bfTag != ftTag {
					t.Fatalf("[INV-14] after %s: bruteForce=(%d,%d), FlexTree=(%d,%d)",
						opDesc, bfTag, bfRc, ftTag, ftRc)
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
			isSimpleInsert = true

			bf.InsertWTag(loff, poff, length, tag)
			ft.InsertWTag(loff, poff, length, tag)

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
			ft.Insert(loff, poff, length)
		}

		if opDesc == "" {
			opDesc = fmt.Sprintf("op%d", i)
		}
		opsCompleted++

		// INV-FT-5: O(log N) cost (simple insert operations only)
		if isSimpleInsert {
			checkInsertOpCost(t, ft, opDesc)
		}

		// Track stats
		visited, _, _ := ft.opCounters()
		if visited > maxNodesVisited {
			maxNodesVisited = visited
		}
		h := treeHeight(ft)
		if h > maxHeight {
			maxHeight = h
		}

		// INV-FT-6: oracle agreement
		checkOracleAgreement(t, ft, bf, rng, opDesc)

		// Full invariant checks every 50 ops
		if i%50 == 0 {
			checkFlexTreeInvariants(t, ft, opDesc)
			checkLeafLinkedList(t, ft, opDesc)
			checkExtentsMatchOracle(t, ft, bf, opDesc)
			checkTreeHeight(t, ft, opDesc)
			checkNodeCountConsistency(t, ft, opDesc)
		}
	}

	// Final exhaustive checks
	checkFlexTreeInvariants(t, ft, "final")
	checkLeafLinkedList(t, ft, "final")
	checkExtentsMatchOracle(t, ft, bf, "final")
	checkTreeHeight(t, ft, "final")
	checkNodeCountConsistency(t, ft, "final")
	checkOracleAgreement(t, ft, bf, rng, "final")

	t.Logf("Completed %d ops. Final state: FlexTree %d extents, MaxLoff=%d, height=%d, maxNodesVisited=%d",
		opsCompleted, len(ft.allExtents()), ft.GetMaxLoff(), treeHeight(ft), maxNodesVisited)
}

// ========================================================================
// TestFlexTree_LeafLinkedListBasic - targeted leaf linked list test
// ========================================================================

func TestFlexTree_LeafLinkedListBasic(t *testing.T) {
	fs, dir := newTestFS(t)
	ft, err := OpenFlexTreeCoW(dir, fs)
	panicOn(err)

	ft.MaxExtentSize = 128

	// Fill up enough extents to force several leaf splits
	for i := uint32(0); i < 200; i++ {
		ft.Insert(uint64(i*10), uint64(1000+i*10), 10)
	}

	checkLeafLinkedList(t, ft, "after 200 inserts")
	checkTreeHeight(t, ft, "after 200 inserts")
	checkNodeCountConsistency(t, ft, "after 200 inserts")

	// Delete half the extents
	for i := 0; i < 100; i++ {
		if ft.MaxLoff > 0 {
			ft.Delete(0, 1)
		}
	}

	checkLeafLinkedList(t, ft, "after 100 deletes")
	checkNodeCountConsistency(t, ft, "after 100 deletes")
}

// ========================================================================
// TestFlexTree_OracleAgreementFocused - intensive oracle comparison
// ========================================================================

func TestFlexTree_OracleAgreementFocused(t *testing.T) {
	const maxExtentSize = 256

	fs, dir := newTestFS(t)
	ft, err := OpenFlexTreeCoW(dir, fs)
	panicOn(err)

	ft.MaxExtentSize = maxExtentSize
	bf := openBruteForce(maxExtentSize)
	rng := rand.New(rand.NewSource(42))

	// Insert at various positions
	for i := 0; i < 500; i++ {
		var loff uint64
		if bf.MaxLoff > 0 {
			loff = uint64(rng.Int63n(int64(bf.MaxLoff)))
		}
		poff := uint64(rng.Int63n(1 << 20))
		length := uint32(rng.Intn(maxExtentSize)) + 1

		bf.Insert(loff, poff, length)
		ft.Insert(loff, poff, length)

		checkOracleAgreement(t, ft, bf, rng, fmt.Sprintf("insert#%d", i))
	}

	checkExtentsMatchOracle(t, ft, bf, "after 500 inserts")

	// Interleave deletes
	for i := 0; i < 200; i++ {
		if bf.MaxLoff < 2 {
			break
		}
		loff := uint64(rng.Int63n(int64(bf.MaxLoff - 1)))
		length := uint64(rng.Intn(100)) + 1
		if loff+length > bf.MaxLoff {
			length = bf.MaxLoff - loff
		}

		bf.Delete(loff, length)
		ft.Delete(loff, length)

		checkOracleAgreement(t, ft, bf, rng, fmt.Sprintf("delete#%d", i))
	}

	checkExtentsMatchOracle(t, ft, bf, "after deletes")
}

// ========================================================================
// TestFlexTree_TreeHeightBound - verify height grows logarithmically
// ========================================================================

func TestFlexTree_TreeHeightBound(t *testing.T) {
	fs, dir := newTestFS(t)

	ft, err := OpenFlexTreeCoW(dir, fs)
	panicOn(err)

	ft.MaxExtentSize = 128

	// Insert enough extents to get several levels deep.
	// Each leaf holds up to 60 extents, each internal holds 30 children.
	// So 60*30 = 1800 extents should give height 2-3.
	// 60*30*30 = 54000 should give height 3-4.
	n := 5000
	for i := 0; i < n; i++ {
		ft.Insert(ft.MaxLoff, uint64(i*100), 10)
	}

	h := treeHeight(ft)
	// Theoretical max: ceil(log30(n/60)) + 1
	nExtents := len(ft.allExtents())
	theoreticalMax := int(math.Ceil(math.Log(float64(nExtents)/60.0)/math.Log(30.0))) + 2
	if theoreticalMax < 2 {
		theoreticalMax = 2
	}

	if h > theoreticalMax+1 {
		t.Fatalf("tree height %d exceeds theoretical bound %d for %d extents",
			h, theoreticalMax+1, nExtents)
	}

	checkLeafLinkedList(t, ft, "height test")
	t.Logf("Height=%d for %d extents (theoretical max ~%d)", h, nExtents, theoreticalMax)
}

// Note: consumeU16 and consumeU32 are defined in brute_test.go
// and are shared across all test files in this package.
