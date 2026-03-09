package yogadb

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

/*
# Plan: Sparse Index Tree Test Suite + Bug Fixes

## Context

The sparse index tree (`sparseindextree.go`, 411 lines) is an in-memory B+-tree mapping
keys to FlexSpace intervals (anchors). It's rebuilt from FlexSpace tags on every open.
This tree has no dedicated tests. The goal is to:

1. Fix identified bugs and memory leaks
2. Add observability counters to the tree struct
3. Create `sparseindextree_test.go` with comprehensive unit tests, fuzz tests, and benchmarks
4. Use a brute-force oracle for correctness verification

## Bug Fixes (in `sparseindextree.go`)

### Fix 1: `treeNodeHandlerNextAnchor` — rightmost walk-up never updates `node`

**Location:** Lines 271-284

**Bug:** When the starting leaf is the rightmost child at every level, the `if pID+1 > cnode.parent.count` branch only updates `cnode` but never `node`. If the loop exits via `cnode.parent == nil`, `node` stays as the original leaf. The search-down phase (`for !node.isLeaf`) is skipped, producing a wrong result for keys not in that leaf.

**Fix:** Update `node` as we walk up in the rightmost-child branch:
```go
if pID+1 > cnode.parent.count {
    cnode = cnode.parent
    node = cnode          // <-- ADD: track highest ancestor visited
}
```

### Fix 2: Stale anchors after `memSparseIndexTreeSplitLeaf`

**Location:** After line 145

**Bug:** After copying anchors to `node2` and nil'ing stale slots in `node`, the stale slots in `node` beyond `node.count` at positions `[count, old_count)` are correctly nil'd (lines 143-145). No additional fix needed for `node`. However, stale pivots in parent are not cleaned (see Fix 4).

### Fix 3: Stale pivots/children after `memSparseIndexTreeSplitInternal`

**Location:** After line 174

**Bug:** After `node.count = count`, positions `node.pivots[count:]` and `node.children[count+1:]` still reference moved data, preventing GC.

**Fix:** Add cleanup:
```go
node.count = count
// Clear stale pivots and children to allow GC
for i := count; i < count + node2.count + 1; i++ {
    node.pivots[i] = nil
}
for i := count + 1; i <= count + node2.count + 1; i++ {
    node.children[i] = memSparseIndexTreeChild{}
}
```

### Fix 4: Stale pivots/children after `memSparseIndexTreeRecycleNode` (pIdx == 0)

**Location:** After line 395 (the `pIdx == 0` branch)

**Bug:** After shifting pivots and children left, `parent.pivots[parent.count]` and `parent.children[parent.count+1]` still reference old data.

**Fix:**
```go
parent.pivots[parent.count] = nil
parent.children[parent.count+1] = memSparseIndexTreeChild{}
```

### Fix 5: Stale pivots/children after `memSparseIndexTreeRecycleNode` (pIdx > 0)

**Location:** After line 404 (the `pIdx > 0` branch)

**Same fix:**
```go
parent.pivots[parent.count] = nil
parent.children[parent.count+1] = memSparseIndexTreeChild{}
```

## Struct Additions (in `sparseindextree.go`)

Add counters to `memSparseIndexTree`:
```go
type memSparseIndexTree struct {
    root           *memSparseIndexTreeNode
    leafHead       *memSparseIndexTreeNode
    leafSplits     int // number of leaf splits performed
    internalSplits int // number of internal splits performed
    recycleCount   int // number of nodes recycled
    rebaseCount    int // number of rebases triggered
}
```

Instrument:
- `memSparseIndexTreeSplitLeaf`: `node.tree.leafSplits++`
- `memSparseIndexTreeSplitInternal`: `node.tree.internalSplits++`
- `memSparseIndexTreeRecycleNode`: `node.tree.recycleCount++`
- `memSparseIndexTreeNodeRebase`: `node.tree.rebaseCount++` (when `newBase != 0`)

## Invariants

| ID | Description |
|----|-------------|
| INV-ST-1 | **Root exists:** `tree.root != nil` |
| INV-ST-2 | **Root has no parent:** `tree.root.parent == nil` |
| INV-ST-3 | **Sentinel present:** leafHead.anchors[0].key == "" |
| INV-ST-4 | **Leaf linked list integrity:** next/prev consistent, covers all leaves, no cycles |
| INV-ST-5 | **Keys sorted within leaf:** anchors[i].key < anchors[i+1].key for all consecutive non-nil pairs |
| INV-ST-6 | **Keys sorted across leaves:** last key of leaf N < first key of leaf N+1 |
| INV-ST-7 | **Pivots match subtree minimums:** pivots[i] == smallest key in children[i+1]'s subtree |
| INV-ST-8 | **Parent/parentID consistency:** node.parent.children[node.parentID].node == node |
| INV-ST-9 | **Count bounds:** leaf 1..122, internal 1..40 (root can be leaf with 1+) |
| INV-ST-10 | **No nil children:** children[0..count].node != nil for internal nodes |
| INV-ST-11 | **Shift consistency:** effectiveLoff monotonically non-decreasing across anchors in key order |
| INV-ST-12 | **Tree pointer:** node.tree == tree for all nodes |
| INV-ST-13 | **Stale slot cleanup:** node.anchors[count:] == nil, node.pivots[count:] == nil, node.children[count+1:] == zero |
| INV-ST-14 | **Oracle agreement:** all anchors' keys and effective loffs match brute-force oracle |

## Tests

### Unit Tests (~35 tests)

| Test | What It Tests |
|------|---------------|
| `TestSparseIndexTree_Create` | New tree: root==leafHead, isLeaf, count==1, sentinel |
| `TestSparseIndexTree_FindAnchorPosEmpty` | Any key → idx==0 (sentinel) on single-node tree |
| `TestSparseIndexTree_FindAnchorPosNilKey` | nil key → idx==0 |
| `TestSparseIndexTree_SingleInsert` | One anchor after sentinel, count==2 |
| `TestSparseIndexTree_InsertMultipleOrdered` | 20 sequential keys, all in one leaf |
| `TestSparseIndexTree_InsertMultipleRandom` | 50 random keys vs oracle |
| `TestSparseIndexTree_LeafSplitTrigger` | 121 anchors → split, leafSplits>0, root becomes internal |
| `TestSparseIndexTree_LeafSplitBalance` | Both halves ~equal, pivot == right child's first key |
| `TestSparseIndexTree_InternalSplitTrigger` | ~5000 anchors → internalSplits>0, height==3 |
| `TestSparseIndexTree_FindPosInternal` | Direct internal node binary search test |
| `TestSparseIndexTree_FindPosLeafLE` | LE search: exact, between, before-all, after-all |
| `TestSparseIndexTree_ShiftUpPropagateBasic` | Shift within one leaf, verify sibling loffs change |
| `TestSparseIndexTree_ShiftUpPropagateAcrossLevels` | Shift after splits, verify all right siblings affected |
| `TestSparseIndexTree_ShiftConsistency` | 500 inserts with psize, effective loffs match oracle |
| `TestSparseIndexTree_DeleteSingleAnchor` | Remove from multi-anchor leaf |
| `TestSparseIndexTree_DeleteUntilRecycle` | Delete all → recycleNode fires, recycleCount>0 |
| `TestSparseIndexTree_DeleteLeftmostPivotUpdate` | Delete idx==0, verify pivot updated |
| `TestSparseIndexTree_RecycleNodeSiblingBecomesRoot` | Recycle causes parent collapse, sibling becomes root |
| `TestSparseIndexTree_HandlerInfoUpdate` | Verify nh.shift == sum of parent shifts to root |
| `TestSparseIndexTree_HintedSearchSameLeaf` | Hinted search within same leaf |
| `TestSparseIndexTree_HintedSearchForwardLeaf` | Hinted search to a different leaf |
| `TestSparseIndexTree_HintedSearchVsCold` | 100 random keys: hinted == cold findAnchorPos |
| `TestSparseIndexTree_HintedSearchRightmostFixed` | Verify Fix 1: rightmost leaf + smaller key → correct result |
| `TestSparseIndexTree_UpdateSmallestKey` | Leftmost chain (no-op) + non-leftmost (updates pivot) |
| `TestSparseIndexTree_RebaseTrigger` | loff >= 0x3FFFFFFF → rebase fires, rebaseCount>0 |
| `TestSparseIndexTree_RebasePreservesEffectiveLoff` | effective loffs unchanged after rebase |
| `TestSparseIndexTree_NodeShiftApply` | Apply shift to leaf and internal nodes |
| `TestSparseIndexTree_RecycleLinkedList` | Remove middle/head leaf from linked list |
| `TestSparseIndexTree_FindSmallestKey` | Descends to leftmost leaf's first key |
| `TestSparseIndexTree_LargeSequential` | 5000 sequential inserts, invariants + oracle |
| `TestSparseIndexTree_LargeRandom` | 5000 random inserts, invariants + oracle |
| `TestSparseIndexTree_InsertDeleteMixed` | 500 insert, 200 delete, 300 insert, 400 delete |
| `TestSparseIndexTree_CallerPatternInsert` | Simulate db.go insert pattern with findAnchorPos/nh.idx++/handlerInsert |
| `TestSparseIndexTree_CallerPatternDelete` | Simulate db.go delete pattern |
| `TestSparseIndexTree_StaleSlotCleanup` | Verify INV-ST-13 after splits and recycles |

### Fuzz Tests (2 tests)

| Test | Strategy |
|------|----------|
| `FuzzSparseIndexTree` | Byte-stream decoded as operations (insert/delete/find/shift). Both tree and oracle updated. All invariants checked after each op. |
| `TestSparseIndexTree_RandomizedStress` | Deterministic PRNG, 10000 random ops, always runs (not fuzz), oracle agreement |

### Benchmarks (3 benchmarks)

| Benchmark | What It Measures |
|-----------|-----------------|
| `BenchmarkSparseIndexTree_InsertSequential` | 10K sequential inserts |
| `BenchmarkSparseIndexTree_FindAnchorPos` | Lookup on 10K-anchor tree |
| `BenchmarkSparseIndexTree_ShiftUpPropagate` | Shift propagation on 10K-anchor tree |

## Oracle Design

```go
type bfAnchor struct {
    key   []byte
    loff  uint64 // absolute effective loff
    psize uint32
}
type sparseIndexBruteForce struct {
    anchors []bfAnchor // sorted by key, nil sentinel at index 0
}
```

Methods: `insert`, `findLE`, `remove`, `shiftAfter`, `allAnchors`, `totalCount`.
Stores absolute loffs (no shifts). Trivial O(N) algorithms as ground truth.

## Helper Functions

```go
func checkSparseIndexTreeInvariants(t testing.TB, tree *memSparseIndexTree, opDesc string)
func effectiveLoff(node *memSparseIndexTreeNode, anchorIdx int) uint64
func allTreeAnchors(tree *memSparseIndexTree) []bfAnchor
func checkOracleAgreement(t testing.TB, tree *memSparseIndexTree, bf *sparseIndexBruteForce, opDesc string)
func sparseIndexTreeSubtreeMinKey(node *memSparseIndexTreeNode) string
func sparseIndexTreeHeight(tree *memSparseIndexTree) int
func countSparseIndexTreeLeaves(tree *memSparseIndexTree) int
func buildTreeWithNAnchors(n int) (*memSparseIndexTree, *sparseIndexBruteForce)
func testKeyFromInt(i int) string
```

## Files Modified

| File | Changes |
|------|---------|
| `sparseindextree.go` | Add 4 counter fields, instrument 4 functions, fix 5 bugs |
| `sparseindextree_test.go` (NEW) | All tests, fuzz, benchmarks, oracle, helpers |

## Verification

```bash
go test -v -run TestSparseIndexTree -count=1 .
go test -fuzz FuzzSparseIndexTree -fuzztime 30s -run=xxx
go test -fuzz FuzzSparseIndexTree -fuzztime 5m -run=xxx
go test -bench BenchmarkSparseIndexTree -benchmem -run=xxx
go test -count=1 .   # full suite, no regressions
```
*/

// ====================== Brute-force oracle ======================

type bfAnchor struct {
	key   string
	loff  uint64 // absolute effective loff
	psize uint32
}

type sparseIndexBruteForce struct {
	anchors []bfAnchor // sorted by key, nil sentinel at index 0
}

func newSparseIndexBruteForce() *sparseIndexBruteForce {
	return &sparseIndexBruteForce{
		anchors: []bfAnchor{{key: "", loff: 0, psize: 0}},
	}
}

func bfKeyCompare(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func (bf *sparseIndexBruteForce) findLE(key string) int {
	best := 0
	for i, a := range bf.anchors {
		if bfKeyCompare(a.key, key) <= 0 {
			best = i
		}
	}
	return best
}

func (bf *sparseIndexBruteForce) insert(key string, loff uint64, psize uint32) {
	a := bfAnchor{key: key, loff: loff, psize: psize}
	idx := sort.Search(len(bf.anchors), func(i int) bool {
		return bfKeyCompare(bf.anchors[i].key, key) > 0
	})
	bf.anchors = append(bf.anchors, bfAnchor{})
	copy(bf.anchors[idx+1:], bf.anchors[idx:])
	bf.anchors[idx] = a
}

func (bf *sparseIndexBruteForce) remove(key string) bool {
	for i, a := range bf.anchors {
		if bfKeyCompare(a.key, key) == 0 {
			bf.anchors = append(bf.anchors[:i], bf.anchors[i+1:]...)
			return true
		}
	}
	return false
}

func (bf *sparseIndexBruteForce) shiftAfter(idx int, shift int64) {
	for i := idx + 1; i < len(bf.anchors); i++ {
		bf.anchors[i].loff = uint64(int64(bf.anchors[i].loff) + shift)
	}
}

func (bf *sparseIndexBruteForce) totalCount() int {
	return len(bf.anchors)
}

func (bf *sparseIndexBruteForce) allAnchors() []bfAnchor {
	out := make([]bfAnchor, len(bf.anchors))
	copy(out, bf.anchors)
	return out
}

// ====================== Helper functions ======================

func testKeyFromInt(i int) string {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return string(buf)
}

// effectiveLoff computes the absolute loff for an anchor at anchorIdx in node.
func effectiveLoff(node *memSparseIndexTreeNode, anchorIdx int) uint64 {
	shift := int64(0)
	n := node
	for n.parent != nil {
		pIdx := n.parentID
		n = n.parent
		shift += n.children[pIdx].shift
	}
	return uint64(node.anchors[anchorIdx].loff + shift)
}

// allTreeAnchors collects all anchors from the tree in linked-list order.
func allTreeAnchors(tree *memSparseIndexTree) []bfAnchor {
	var out []bfAnchor
	leaf := tree.leafHead
	for leaf != nil {
		for i := 0; i < leaf.count; i++ {
			a := leaf.anchors[i]
			out = append(out, bfAnchor{
				key:   a.key,
				loff:  effectiveLoff(leaf, i),
				psize: a.psize,
			})
		}
		leaf = leaf.next
	}
	return out
}

// sparseIndexTreeSubtreeMinKey descends to leftmost leaf and returns first key.
func sparseIndexTreeSubtreeMinKey(node *memSparseIndexTreeNode) string {
	return memSparseIndexTreeFindSmallestKey(node)
}

// sparseIndexTreeHeight returns the height of the tree (1 = just root leaf).
func sparseIndexTreeHeight(tree *memSparseIndexTree) int {
	h := 1
	n := tree.root
	for !n.isLeaf {
		h++
		n = n.children[0].node
	}
	return h
}

// countSparseIndexTreeLeaves counts leaves via the linked list.
func countSparseIndexTreeLeaves(tree *memSparseIndexTree) int {
	count := 0
	leaf := tree.leafHead
	for leaf != nil {
		count++
		leaf = leaf.next
	}
	return count
}

// buildTreeWithNAnchors builds a tree with n sequential anchors (plus sentinel).
func buildTreeWithNAnchors(n int) (*memSparseIndexTree, *sparseIndexBruteForce) {
	tree := memSparseIndexTreeCreate()
	bf := newSparseIndexBruteForce()
	var nh memSparseIndexTreeHandler
	loff := uint64(0)
	for i := 0; i < n; i++ {
		key := testKeyFromInt(i)
		psize := uint32(100)
		tree.findAnchorPos(key, &nh)
		nh.idx++
		nh.handlerInsert(key, loff+uint64(bf.anchors[bf.findLE(key)].psize), psize)
		nh.idx--
		nh.shiftUpPropagate(int64(psize))
		bf.anchors[bf.findLE(key)].psize += psize

		bfIdx := bf.findLE(key)
		bfLoff := bf.anchors[bfIdx].loff + uint64(bf.anchors[bfIdx].psize)
		bf.insert(key, bfLoff, 0)
		bf.shiftAfter(bf.findLE(key), int64(psize))
		bf.anchors[bf.findLE(key)].psize = 0

		// Fix: the oracle needs the anchor's psize to be 0 and loff at the right position.
		// Let's use a simpler approach.
		nh = memSparseIndexTreeHandler{}
		loff += uint64(psize)
	}
	return tree, bf
}

// buildTreeSimple creates a tree with n sequential anchors with simple monotonic loffs.
// Each anchor at index i has loff = i*100, psize = 100.
func buildTreeSimple(n int) (*memSparseIndexTree, *sparseIndexBruteForce) {
	tree := memSparseIndexTreeCreate()
	bf := newSparseIndexBruteForce()
	// sentinel has psize = 0 initially; we'll set it
	bf.anchors[0].psize = 100

	var nh memSparseIndexTreeHandler
	for i := 1; i <= n; i++ {
		key := testKeyFromInt(i)
		loff := uint64(i * 100)
		psize := uint32(100)

		tree.findAnchorPos(key, &nh)
		nh.idx++
		nh.handlerInsert(key, loff, psize)
		nh.idx--

		bf.insert(key, loff, psize)
		nh = memSparseIndexTreeHandler{}
	}
	return tree, bf
}

// checkSparseIndexTreeInvariants validates all structural invariants of the sparse index tree.
func checkSparseIndexTreeInvariants(t testing.TB, tree *memSparseIndexTree, opDesc string) {
	t.Helper()

	// INV-ST-1: Root exists
	if tree.root == nil {
		t.Fatalf("%s: INV-ST-1 violated: root is nil", opDesc)
	}

	// INV-ST-2: Root has no parent
	if tree.root.parent != nil {
		t.Fatalf("%s: INV-ST-2 violated: root has parent", opDesc)
	}

	// INV-ST-3: Sentinel present (leafHead's first anchor has nil key)
	if tree.leafHead == nil {
		t.Fatalf("%s: INV-ST-3 violated: leafHead is nil", opDesc)
	}
	if tree.leafHead.count < 1 || tree.leafHead.anchors[0] == nil {
		t.Fatalf("%s: INV-ST-3 violated: no sentinel at leafHead.anchors[0]", opDesc)
	}
	if tree.leafHead.anchors[0].key != "" {
		t.Fatalf("%s: INV-ST-3 violated: sentinel key is not nil: %v", opDesc, tree.leafHead.anchors[0].key)
	}

	// Collect all nodes via tree traversal for further checks
	var allNodes []*memSparseIndexTreeNode
	var allLeaves []*memSparseIndexTreeNode
	var walkTree func(node *memSparseIndexTreeNode)
	walkTree = func(node *memSparseIndexTreeNode) {
		allNodes = append(allNodes, node)
		if node.isLeaf {
			allLeaves = append(allLeaves, node)
		} else {
			for i := 0; i <= node.count; i++ {
				if node.children[i].node == nil {
					t.Fatalf("%s: INV-ST-10 violated: internal node has nil child at index %d", opDesc, i)
				}
				walkTree(node.children[i].node)
			}
		}
	}
	walkTree(tree.root)

	// INV-ST-12: Tree pointer
	for _, node := range allNodes {
		if node.tree != tree {
			t.Fatalf("%s: INV-ST-12 violated: node.tree != tree", opDesc)
		}
	}

	// INV-ST-8: Parent/parentID consistency
	for _, node := range allNodes {
		if node.parent != nil {
			if node.parent.children[node.parentID].node != node {
				t.Fatalf("%s: INV-ST-8 violated: parent.children[parentID].node != node (parentID=%d)", opDesc, node.parentID)
			}
		}
	}

	// INV-ST-9: Count bounds
	for _, node := range allNodes {
		if node.isLeaf {
			if node.count < 1 {
				t.Fatalf("%s: INV-ST-9 violated: leaf count %d < 1", opDesc, node.count)
			}
			if node.count >= flexMemSparseIndexTreeLeafCap {
				t.Fatalf("%s: INV-ST-9 violated: leaf count %d >= cap %d", opDesc, node.count, flexMemSparseIndexTreeLeafCap)
			}
		} else {
			if node.count < 1 {
				t.Fatalf("%s: INV-ST-9 violated: internal count %d < 1", opDesc, node.count)
			}
			if node.count >= flexMemSparseIndexTreeInternalCap {
				t.Fatalf("%s: INV-ST-9 violated: internal count %d >= cap %d", opDesc, node.count, flexMemSparseIndexTreeInternalCap)
			}
		}
	}

	// INV-ST-5: Keys sorted within leaf
	for _, leaf := range allLeaves {
		for i := 0; i+1 < leaf.count; i++ {
			if leaf.anchors[i] == nil || leaf.anchors[i+1] == nil {
				t.Fatalf("%s: INV-ST-5 violated: nil anchor at index %d or %d", opDesc, i, i+1)
			}
			if bfKeyCompare(leaf.anchors[i].key, leaf.anchors[i+1].key) >= 0 {
				t.Fatalf("%s: INV-ST-5 violated: keys not sorted in leaf: [%d]=%x >= [%d]=%x",
					opDesc, i, leaf.anchors[i].key, i+1, leaf.anchors[i+1].key)
			}
		}
	}

	// INV-ST-4: Leaf linked list integrity
	linkedLeaves := make(map[*memSparseIndexTreeNode]bool)
	leaf := tree.leafHead
	var prevLeaf *memSparseIndexTreeNode
	for leaf != nil {
		if linkedLeaves[leaf] {
			t.Fatalf("%s: INV-ST-4 violated: cycle in leaf linked list", opDesc)
		}
		linkedLeaves[leaf] = true
		if leaf.prev != prevLeaf {
			t.Fatalf("%s: INV-ST-4 violated: prev pointer mismatch", opDesc)
		}
		prevLeaf = leaf
		leaf = leaf.next
	}
	if len(linkedLeaves) != len(allLeaves) {
		t.Fatalf("%s: INV-ST-4 violated: linked list has %d leaves, tree has %d",
			opDesc, len(linkedLeaves), len(allLeaves))
	}
	for _, l := range allLeaves {
		if !linkedLeaves[l] {
			t.Fatalf("%s: INV-ST-4 violated: tree leaf not in linked list", opDesc)
		}
	}

	// INV-ST-6: Keys sorted across leaves
	leaf = tree.leafHead
	for leaf != nil && leaf.next != nil {
		lastKey := leaf.anchors[leaf.count-1].key
		firstKey := leaf.next.anchors[0].key
		if bfKeyCompare(lastKey, firstKey) >= 0 {
			t.Fatalf("%s: INV-ST-6 violated: cross-leaf key order: %x >= %x", opDesc, lastKey, firstKey)
		}
		leaf = leaf.next
	}

	// INV-ST-7: Pivots match subtree minimums
	for _, node := range allNodes {
		if !node.isLeaf {
			for i := 0; i < node.count; i++ {
				minKey := sparseIndexTreeSubtreeMinKey(node.children[i+1].node)
				if node.pivots[i] != minKey {
					t.Fatalf("%s: INV-ST-7 violated: pivot[%d]=%q != subtreeMin=%q", opDesc, i, node.pivots[i], minKey)
				}
			}
		}
	}

	// INV-ST-13: Stale slot cleanup
	for _, node := range allNodes {
		if node.isLeaf {
			for i := node.count; i < flexMemSparseIndexTreeLeafCap; i++ {
				if node.anchors[i] != nil {
					t.Fatalf("%s: INV-ST-13 violated: stale anchor at leaf[%d]", opDesc, i)
				}
			}
		} else {
			for i := node.count; i < flexMemSparseIndexTreeInternalCap; i++ {
				if node.pivots[i] != "" {
					t.Fatalf("%s: INV-ST-13 violated: stale pivot at internal[%d]", opDesc, i)
				}
			}
			for i := node.count + 1; i <= flexMemSparseIndexTreeInternalCap; i++ {
				if node.children[i].node != nil {
					t.Fatalf("%s: INV-ST-13 violated: stale child at internal[%d]", opDesc, i)
				}
			}
		}
	}
}

// checkOracleAgreement verifies that all anchors in the tree match the brute-force oracle.
func checkSparseOracleAgreement(t testing.TB, tree *memSparseIndexTree, bf *sparseIndexBruteForce, opDesc string) {
	t.Helper()
	treeAnchors := allTreeAnchors(tree)
	bfAnchors := bf.allAnchors()

	if len(treeAnchors) != len(bfAnchors) {
		t.Fatalf("%s: INV-ST-14 oracle count mismatch: tree=%d bf=%d", opDesc, len(treeAnchors), len(bfAnchors))
	}
	for i := range treeAnchors {
		if bfKeyCompare(treeAnchors[i].key, bfAnchors[i].key) != 0 {
			t.Fatalf("%s: INV-ST-14 key mismatch at %d: tree=%x bf=%x", opDesc, i, treeAnchors[i].key, bfAnchors[i].key)
		}
		if treeAnchors[i].loff != bfAnchors[i].loff {
			t.Fatalf("%s: INV-ST-14 loff mismatch at %d (key=%x): tree=%d bf=%d", opDesc, i, treeAnchors[i].key, treeAnchors[i].loff, bfAnchors[i].loff)
		}
	}
}

// totalTreeAnchors counts all anchors in the tree.
func totalTreeAnchors(tree *memSparseIndexTree) int {
	count := 0
	leaf := tree.leafHead
	for leaf != nil {
		count += leaf.count
		leaf = leaf.next
	}
	return count
}

// treeInsertAnchor is a helper that inserts an anchor into the tree like db.go does.
func treeInsertAnchor(tree *memSparseIndexTree, key string, loff uint64, psize uint32) {
	var nh memSparseIndexTreeHandler
	tree.findAnchorPos(key, &nh)
	nh.idx++
	nh.handlerInsert(key, loff, psize)
	nh.idx--
}

// treeDeleteAnchor deletes an anchor from the tree like db.go does.
func treeDeleteAnchor(tree *memSparseIndexTree, key string) bool {
	var nh memSparseIndexTreeHandler
	tree.findAnchorPos(key, &nh)
	node := nh.node
	// Find exact match
	for i := 0; i < node.count; i++ {
		if bfKeyCompare(node.anchors[i].key, key) == 0 {
			if i < node.count-1 {
				copy(node.anchors[i:], node.anchors[i+1:node.count])
			}
			node.anchors[node.count-1] = nil
			node.count--
			if node.count == 0 {
				memSparseIndexTreeRecycleNode(node)
			} else if i == 0 {
				memSparseIndexTreeUpdateSmallestKey(node, node.anchors[0].key)
			}
			return true
		}
	}
	return false
}

// ====================== Unit Tests ======================

func TestSparseIndexTree_Create(t *testing.T) {
	tree := memSparseIndexTreeCreate()
	if tree.root == nil {
		t.Fatal("root is nil")
	}
	if tree.root != tree.leafHead {
		t.Fatal("root != leafHead for new tree")
	}
	if !tree.root.isLeaf {
		t.Fatal("root should be leaf")
	}
	if tree.root.count != 1 {
		t.Fatalf("root count should be 1, got %d", tree.root.count)
	}
	if tree.root.anchors[0] == nil || tree.root.anchors[0].key != "" {
		t.Fatal("sentinel missing or has non-nil key")
	}
	if tree.root.parent != nil {
		t.Fatal("root should have no parent")
	}
	checkSparseIndexTreeInvariants(t, tree, "Create")
}

func TestSparseIndexTree_FindAnchorPosEmpty(t *testing.T) {
	tree := memSparseIndexTreeCreate()
	var nh memSparseIndexTreeHandler
	tree.findAnchorPos("anything", &nh)
	if nh.idx != 0 {
		t.Fatalf("expected idx=0 for single-node tree, got %d", nh.idx)
	}
	if nh.node != tree.root {
		t.Fatal("expected node == root")
	}
}

func TestSparseIndexTree_FindAnchorPosNilKey(t *testing.T) {
	tree := memSparseIndexTreeCreate()
	var nh memSparseIndexTreeHandler
	tree.findAnchorPos("", &nh)
	if nh.idx != 0 {
		t.Fatalf("expected idx=0 for empty key, got %d", nh.idx)
	}
}

func TestSparseIndexTree_SingleInsert(t *testing.T) {
	tree := memSparseIndexTreeCreate()
	key := "hello"
	treeInsertAnchor(tree, key, 100, 50)
	if tree.root.count != 2 {
		t.Fatalf("expected count=2, got %d", tree.root.count)
	}
	if tree.root.anchors[1].key != key {
		t.Fatalf("expected key %q at index 1, got %q", key, tree.root.anchors[1].key)
	}
	checkSparseIndexTreeInvariants(t, tree, "SingleInsert")
}

func TestSparseIndexTree_InsertMultipleOrdered(t *testing.T) {
	tree := memSparseIndexTreeCreate()
	for i := 1; i <= 20; i++ {
		key := testKeyFromInt(i)
		treeInsertAnchor(tree, key, uint64(i*100), 100)
	}
	if tree.root.count != 21 {
		t.Fatalf("expected count=21, got %d", tree.root.count)
	}
	// Verify all keys sorted
	for i := 1; i < tree.root.count; i++ {
		if bfKeyCompare(tree.root.anchors[i-1].key, tree.root.anchors[i].key) >= 0 {
			t.Fatalf("keys not sorted at %d", i)
		}
	}
	checkSparseIndexTreeInvariants(t, tree, "InsertMultipleOrdered")
}

func TestSparseIndexTree_InsertMultipleRandom(t *testing.T) {
	tree := memSparseIndexTreeCreate()
	bf := newSparseIndexBruteForce()
	rng := rand.New(rand.NewSource(42))
	used := make(map[int]bool)

	for i := 0; i < 50; i++ {
		v := rng.Intn(100000)
		for used[v] {
			v = rng.Intn(100000)
		}
		used[v] = true
		key := testKeyFromInt(v)
		loff := uint64(i * 100)
		psize := uint32(50)
		treeInsertAnchor(tree, key, loff, psize)
		bf.insert(key, loff, psize)
	}
	checkSparseIndexTreeInvariants(t, tree, "InsertMultipleRandom")
	checkSparseOracleAgreement(t, tree, bf, "InsertMultipleRandom")
}

func TestSparseIndexTree_LeafSplitTrigger(t *testing.T) {
	tree := memSparseIndexTreeCreate()
	n := flexMemSparseIndexTreeLeafCap - 2 // enough to trigger split
	for i := 1; i <= n; i++ {
		key := testKeyFromInt(i)
		treeInsertAnchor(tree, key, uint64(i*100), 100)
	}
	if tree.leafSplits == 0 {
		t.Fatal("expected at least one leaf split")
	}
	if tree.root.isLeaf {
		t.Fatal("root should be internal after split")
	}
	checkSparseIndexTreeInvariants(t, tree, "LeafSplitTrigger")
}

func TestSparseIndexTree_LeafSplitBalance(t *testing.T) {
	tree := memSparseIndexTreeCreate()
	n := flexMemSparseIndexTreeLeafCap - 2
	for i := 1; i <= n; i++ {
		key := testKeyFromInt(i)
		treeInsertAnchor(tree, key, uint64(i*100), 100)
	}
	if tree.root.isLeaf {
		t.Fatal("root should be internal")
	}
	// After split, left and right child should have roughly equal counts
	leftCount := tree.root.children[0].node.count
	rightCount := tree.root.children[1].node.count
	total := leftCount + rightCount
	if total != n+1 { // n keys + sentinel
		t.Fatalf("total count mismatch: %d != %d", total, n+1)
	}
	diff := leftCount - rightCount
	if diff < 0 {
		diff = -diff
	}
	if diff > leftCount/2 {
		t.Fatalf("unbalanced split: left=%d right=%d", leftCount, rightCount)
	}
	// Pivot should equal right child's first key
	pivot := tree.root.pivots[0]
	rightFirstKey := tree.root.children[1].node.anchors[0].key
	if pivot != rightFirstKey {
		t.Fatalf("pivot %q != right child first key %q", pivot, rightFirstKey)
	}
	checkSparseIndexTreeInvariants(t, tree, "LeafSplitBalance")
}

func TestSparseIndexTree_InternalSplitTrigger(t *testing.T) {
	tree := memSparseIndexTreeCreate()
	for i := 1; i <= 5000; i++ {
		key := testKeyFromInt(i)
		treeInsertAnchor(tree, key, uint64(i*100), 100)
	}
	if tree.internalSplits == 0 {
		t.Fatal("expected at least one internal split")
	}
	h := sparseIndexTreeHeight(tree)
	if h < 3 {
		t.Fatalf("expected height >= 3 for 5000 anchors, got %d", h)
	}
	checkSparseIndexTreeInvariants(t, tree, "InternalSplitTrigger")
}

func TestSparseIndexTree_FindPosInternal(t *testing.T) {
	// Create a synthetic internal node
	tree := memSparseIndexTreeCreate()
	node := &memSparseIndexTreeNode{tree: tree}
	node.pivots[0] = "bbb"
	node.pivots[1] = "ddd"
	node.pivots[2] = "fff"
	node.count = 3

	tests := []struct {
		key    string
		expect int
	}{
		{"aaa", 0},
		{"bbb", 1},
		{"ccc", 1},
		{"ddd", 2},
		{"eee", 2},
		{"fff", 3},
		{"zzz", 3},
	}
	for _, tc := range tests {
		got := memSparseIndexTreeFindPosInternal(node, tc.key)
		if got != tc.expect {
			t.Errorf("FindPosInternal(%q) = %d, want %d", tc.key, got, tc.expect)
		}
	}
}

func TestSparseIndexTree_FindPosLeafLE(t *testing.T) {
	tree := memSparseIndexTreeCreate()
	node := tree.root
	// Add some anchors
	skeys := []string{"", "bbb", "ddd", "fff"}
	for i, k := range skeys {
		node.anchors[i] = &dbAnchor{key: k, loff: int64(i * 100)}
	}
	node.count = 4

	tests := []struct {
		key    string
		expect int
	}{
		{"", 0},       // exact match
		{"aaa", 0},    // between "" and bbb
		{"bbb", 1},    // exact match
		{"ccc", 1},    // between bbb and ddd
		{"ddd", 2},    // exact match
		{"eee", 2},    // between ddd and fff
		{"fff", 3},    // exact match
		{"zzz", 3},    // after all
		{"aaaa", 0},   // before bbb
		{"\x00", 0},   // before bbb (but after "")
	}
	for _, tc := range tests {
		got := memSparseIndexTreeFindPosLeafLE(node, tc.key)
		if got != tc.expect {
			t.Errorf("FindPosLeafLE(%q) = %d, want %d", tc.key, got, tc.expect)
		}
	}
}

func TestSparseIndexTree_ShiftUpPropagateBasic(t *testing.T) {
	tree := memSparseIndexTreeCreate()
	// Insert a few anchors
	for i := 1; i <= 5; i++ {
		key := testKeyFromInt(i)
		treeInsertAnchor(tree, key, uint64(i*100), 100)
	}
	// Record loffs before shift
	before := make([]uint64, tree.root.count)
	for i := 0; i < tree.root.count; i++ {
		before[i] = effectiveLoff(tree.root, i)
	}

	// Apply shift at index 2
	var nh memSparseIndexTreeHandler
	nh.node = tree.root
	nh.idx = 2
	nh.shiftUpPropagate(50)

	// Anchors at indices 0,1,2 should be unchanged; 3,4,5 shifted
	for i := 0; i < tree.root.count; i++ {
		after := effectiveLoff(tree.root, i)
		if i <= 2 {
			if after != before[i] {
				t.Errorf("anchor %d should be unchanged: before=%d after=%d", i, before[i], after)
			}
		} else {
			if after != before[i]+50 {
				t.Errorf("anchor %d should be shifted by 50: before=%d after=%d", i, before[i], after)
			}
		}
	}
	checkSparseIndexTreeInvariants(t, tree, "ShiftUpPropagateBasic")
}

func TestSparseIndexTree_ShiftUpPropagateAcrossLevels(t *testing.T) {
	tree := memSparseIndexTreeCreate()
	bf := newSparseIndexBruteForce()

	// Insert enough to cause splits
	n := 200
	for i := 1; i <= n; i++ {
		key := testKeyFromInt(i)
		loff := uint64(i * 100)
		psize := uint32(100)
		treeInsertAnchor(tree, key, loff, psize)
		bf.insert(key, loff, psize)
	}
	checkSparseIndexTreeInvariants(t, tree, "ShiftAcrossLevels-before")

	// Apply shift at an early anchor
	var nh memSparseIndexTreeHandler
	tree.findAnchorPos(testKeyFromInt(50), &nh)
	shift := int64(500)
	nh.shiftUpPropagate(shift)
	bf.shiftAfter(bf.findLE(testKeyFromInt(50)), shift)

	checkSparseIndexTreeInvariants(t, tree, "ShiftAcrossLevels-after")
	checkSparseOracleAgreement(t, tree, bf, "ShiftAcrossLevels-after")
}

func TestSparseIndexTree_ShiftConsistency(t *testing.T) {
	tree := memSparseIndexTreeCreate()
	bf := newSparseIndexBruteForce()

	for i := 1; i <= 500; i++ {
		key := testKeyFromInt(i)
		loff := uint64(i * 100)
		psize := uint32(100)
		treeInsertAnchor(tree, key, loff, psize)
		bf.insert(key, loff, psize)

		var nh memSparseIndexTreeHandler
		tree.findAnchorPos(key, &nh)
		nh.shiftUpPropagate(int64(psize))
		bf.shiftAfter(bf.findLE(key), int64(psize))
	}
	checkSparseIndexTreeInvariants(t, tree, "ShiftConsistency")
	checkSparseOracleAgreement(t, tree, bf, "ShiftConsistency")
}

func TestSparseIndexTree_DeleteSingleAnchor(t *testing.T) {
	tree := memSparseIndexTreeCreate()
	for i := 1; i <= 10; i++ {
		treeInsertAnchor(tree, testKeyFromInt(i), uint64(i*100), 100)
	}
	before := totalTreeAnchors(tree)
	ok := treeDeleteAnchor(tree, testKeyFromInt(5))
	if !ok {
		t.Fatal("delete should succeed")
	}
	after := totalTreeAnchors(tree)
	if after != before-1 {
		t.Fatalf("expected count %d, got %d", before-1, after)
	}
	checkSparseIndexTreeInvariants(t, tree, "DeleteSingleAnchor")
}

func TestSparseIndexTree_DeleteUntilRecycle(t *testing.T) {
	tree := memSparseIndexTreeCreate()
	n := 200
	for i := 1; i <= n; i++ {
		treeInsertAnchor(tree, testKeyFromInt(i), uint64(i*100), 100)
	}
	initialRecycles := tree.recycleCount
	// Delete all non-sentinel anchors
	for i := 1; i <= n; i++ {
		treeDeleteAnchor(tree, testKeyFromInt(i))
	}
	if tree.recycleCount <= initialRecycles {
		t.Fatal("expected recycle events during deletion")
	}
	// Should be back to just sentinel
	if totalTreeAnchors(tree) != 1 {
		t.Fatalf("expected 1 anchor (sentinel), got %d", totalTreeAnchors(tree))
	}
	checkSparseIndexTreeInvariants(t, tree, "DeleteUntilRecycle")
}

func TestSparseIndexTree_DeleteLeftmostPivotUpdate(t *testing.T) {
	tree := memSparseIndexTreeCreate()
	n := flexMemSparseIndexTreeLeafCap - 2
	for i := 1; i <= n; i++ {
		treeInsertAnchor(tree, testKeyFromInt(i), uint64(i*100), 100)
	}
	if tree.root.isLeaf {
		t.Fatal("expected internal root after splits")
	}
	// Delete the first key of the right child
	rightChild := tree.root.children[1].node
	firstKey := rightChild.anchors[0].key
	treeDeleteAnchor(tree, firstKey)
	checkSparseIndexTreeInvariants(t, tree, "DeleteLeftmostPivotUpdate")
}

func TestSparseIndexTree_RecycleNodeSiblingBecomesRoot(t *testing.T) {
	tree := memSparseIndexTreeCreate()
	// Insert enough to split into exactly 2 leaves
	n := flexMemSparseIndexTreeLeafCap - 2
	for i := 1; i <= n; i++ {
		treeInsertAnchor(tree, testKeyFromInt(i), uint64(i*100), 100)
	}
	if tree.root.isLeaf {
		t.Fatal("expected internal root after split")
	}
	// Delete all non-sentinel keys from the right child to trigger recycle
	rightChild := tree.root.children[1].node
	keysToDelete := make([]string, rightChild.count)
	for i := 0; i < rightChild.count; i++ {
		keysToDelete[i] = rightChild.anchors[i].key
	}
	for _, key := range keysToDelete {
		treeDeleteAnchor(tree, key)
		if tree.root.isLeaf {
			break // recycle happened, sibling became root
		}
	}
	// After recycling the right child, the left child should become root
	if tree.recycleCount == 0 {
		t.Fatal("expected recycle to fire")
	}
	checkSparseIndexTreeInvariants(t, tree, "RecycleNodeSiblingBecomesRoot")
}

func TestSparseIndexTree_HandlerInfoUpdate(t *testing.T) {
	tree := memSparseIndexTreeCreate()
	// Insert enough to have internal nodes
	for i := 1; i <= 200; i++ {
		treeInsertAnchor(tree, testKeyFromInt(i), uint64(i*100), 100)
	}
	// Find an anchor and verify shift
	var nh memSparseIndexTreeHandler
	tree.findAnchorPos(testKeyFromInt(100), &nh)
	expectedShift := nh.shift

	// Reset shift and recalculate
	nh.shift = 999
	memSparseIndexTreeHandlerInfoUpdate(&nh)
	if nh.shift != expectedShift {
		t.Fatalf("shift mismatch after InfoUpdate: expected %d, got %d", expectedShift, nh.shift)
	}
}

func TestSparseIndexTree_HintedSearchSameLeaf(t *testing.T) {
	tree := memSparseIndexTreeCreate()
	for i := 1; i <= 10; i++ {
		treeInsertAnchor(tree, testKeyFromInt(i), uint64(i*100), 100)
	}
	var nh memSparseIndexTreeHandler
	tree.findAnchorPos(testKeyFromInt(3), &nh)
	origNode := nh.node

	tree.treeNodeHandlerNextAnchor(&nh, testKeyFromInt(5))
	if nh.node != origNode {
		t.Fatal("expected same leaf for nearby key")
	}
	if nh.idx != 5 { // sentinel + keys 1-5, so key 5 is at index 5
		t.Fatalf("expected idx=5, got %d", nh.idx)
	}
}

func TestSparseIndexTree_HintedSearchForwardLeaf(t *testing.T) {
	tree := memSparseIndexTreeCreate()
	n := flexMemSparseIndexTreeLeafCap - 2
	for i := 1; i <= n; i++ {
		treeInsertAnchor(tree, testKeyFromInt(i), uint64(i*100), 100)
	}
	// This should have split into multiple leaves
	if countSparseIndexTreeLeaves(tree) < 2 {
		t.Fatal("expected multiple leaves")
	}
	var nh memSparseIndexTreeHandler
	tree.findAnchorPos(testKeyFromInt(1), &nh)
	firstNode := nh.node

	// Search for a key that should be in a different leaf
	tree.treeNodeHandlerNextAnchor(&nh, testKeyFromInt(n))
	if nh.node == firstNode {
		t.Fatal("expected different leaf for distant key")
	}
}

func TestSparseIndexTree_HintedSearchVsCold(t *testing.T) {
	tree := memSparseIndexTreeCreate()
	for i := 1; i <= 200; i++ {
		treeInsertAnchor(tree, testKeyFromInt(i), uint64(i*100), 100)
	}
	// Hinted search is optimized for forward-moving key lookups (like db.go's
	// sequential put/delete patterns). Test with monotonically increasing keys.
	var nhHinted memSparseIndexTreeHandler
	for i := 1; i <= 200; i++ {
		key := testKeyFromInt(i)
		// Cold search
		var nhCold memSparseIndexTreeHandler
		tree.findAnchorPos(key, &nhCold)
		// Hinted search
		tree.treeNodeHandlerNextAnchor(&nhHinted, key)

		if nhHinted.node != nhCold.node || nhHinted.idx != nhCold.idx {
			t.Fatalf("iter %d: hinted (%p,%d) != cold (%p,%d) for key %x",
				i, nhHinted.node, nhHinted.idx, nhCold.node, nhCold.idx, key)
		}
		if nhHinted.shift != nhCold.shift {
			t.Fatalf("iter %d: hinted shift %d != cold shift %d", i, nhHinted.shift, nhCold.shift)
		}
	}
}

func TestSparseIndexTree_HintedSearchRightmostFixed(t *testing.T) {
	// This test verifies Fix 1: when the starting leaf is the rightmost child
	// at every level, the walk-up should correctly track the ancestor.
	tree := memSparseIndexTreeCreate()
	n := flexMemSparseIndexTreeLeafCap - 2
	for i := 1; i <= n; i++ {
		treeInsertAnchor(tree, testKeyFromInt(i), uint64(i*100), 100)
	}
	// Position handler at rightmost leaf
	var nh memSparseIndexTreeHandler
	tree.findAnchorPos(testKeyFromInt(n), &nh)

	// Now search for a key in the left half — this exercises the rightmost walk-up
	tree.treeNodeHandlerNextAnchor(&nh, testKeyFromInt(1))

	var nhCold memSparseIndexTreeHandler
	tree.findAnchorPos(testKeyFromInt(1), &nhCold)

	if nh.node != nhCold.node || nh.idx != nhCold.idx {
		t.Fatalf("Fix1: hinted (%p,%d) != cold (%p,%d)", nh.node, nh.idx, nhCold.node, nhCold.idx)
	}
	if nh.shift != nhCold.shift {
		t.Fatalf("Fix1: hinted shift %d != cold shift %d", nh.shift, nhCold.shift)
	}
}

func TestSparseIndexTree_UpdateSmallestKey(t *testing.T) {
	tree := memSparseIndexTreeCreate()
	n := flexMemSparseIndexTreeLeafCap - 2
	for i := 1; i <= n; i++ {
		treeInsertAnchor(tree, testKeyFromInt(i), uint64(i*100), 100)
	}
	checkSparseIndexTreeInvariants(t, tree, "UpdateSmallestKey-before")

	// Leftmost chain: updating smallest key of leftmost subtree should be a no-op
	// (memSparseIndexTreeUpdateSmallestKey only updates if pIdx > 0)
	leftLeaf := tree.leafHead
	memSparseIndexTreeUpdateSmallestKey(leftLeaf, "newkey")
	// This is a no-op because leftLeaf is always child 0 up the chain
	checkSparseIndexTreeInvariants(t, tree, "UpdateSmallestKey-leftmost")

	// Non-leftmost: updating first key should update the pivot
	if tree.root.count >= 1 {
		rightChild := tree.root.children[1].node
		var targetLeaf *memSparseIndexTreeNode
		if rightChild.isLeaf {
			targetLeaf = rightChild
		} else {
			n := rightChild
			for !n.isLeaf {
				n = n.children[0].node
			}
			targetLeaf = n
		}
		newKey := "updated"
		targetLeaf.anchors[0].key = newKey
		memSparseIndexTreeUpdateSmallestKey(targetLeaf, newKey)
		// The relevant pivot should now equal newKey
		// (checking via invariant check would fail since we changed the actual key but not consistently)
		// Just verify pivot was updated
		pIdx := targetLeaf.parentID
		p := targetLeaf.parent
		for p != nil && pIdx == 0 {
			pIdx = p.parentID
			p = p.parent
		}
		if p != nil {
			if p.pivots[pIdx-1] != newKey {
				t.Fatalf("pivot not updated: got %q, want %q", p.pivots[pIdx-1], newKey)
			}
		}
	}
}

func TestSparseIndexTree_RebaseTrigger(t *testing.T) {
	tree := memSparseIndexTreeCreate()
	// Rebase triggers when anchor.loff >= 0x3FFFFFFF AND parent.count > 1.
	// parent.count > 1 means at least 2 splits have happened under the same parent.
	// Insert many keys with large loffs to trigger multiple splits + rebase.
	n := flexMemSparseIndexTreeLeafCap * 3 // enough for multiple splits
	for i := 1; i <= n; i++ {
		treeInsertAnchor(tree, testKeyFromInt(i), uint64(0x3FFFFFFF)+uint64(i*100), 100)
	}
	if tree.rebaseCount == 0 {
		t.Fatal("expected rebase to trigger for large loffs")
	}
	checkSparseIndexTreeInvariants(t, tree, "RebaseTrigger")
}

func TestSparseIndexTree_RebasePreservesEffectiveLoff(t *testing.T) {
	tree := memSparseIndexTreeCreate()
	bf := newSparseIndexBruteForce()
	baseLoff := uint64(0x3FFFFFFF)
	for i := 1; i <= flexMemSparseIndexTreeLeafCap-1; i++ {
		key := testKeyFromInt(i)
		loff := baseLoff + uint64(i*100)
		treeInsertAnchor(tree, key, loff, 100)
		bf.insert(key, loff, 100)
	}
	// After rebase, effective loffs should still match oracle
	checkSparseIndexTreeInvariants(t, tree, "RebasePreservesEffectiveLoff")
	checkSparseOracleAgreement(t, tree, bf, "RebasePreservesEffectiveLoff")
}

func TestSparseIndexTree_NodeShiftApply(t *testing.T) {
	tree := memSparseIndexTreeCreate()
	// Test leaf shift
	node := tree.root
	node.anchors[0].loff = 100
	treeInsertAnchor(tree, testKeyFromInt(1), 200, 50)
	treeInsertAnchor(tree, testKeyFromInt(2), 300, 50)

	loffsBefore := make([]int64, node.count)
	for i := 0; i < node.count; i++ {
		loffsBefore[i] = node.anchors[i].loff
	}
	memSparseIndexTreeNodeShiftApply(node, 1000)
	for i := 0; i < node.count; i++ {
		expected := loffsBefore[i] + 1000
		if node.anchors[i].loff != expected {
			t.Errorf("leaf anchor %d: expected loff %d, got %d", i, expected, node.anchors[i].loff)
		}
	}

	// Test internal shift
	tree2 := memSparseIndexTreeCreate()
	for i := 1; i <= flexMemSparseIndexTreeLeafCap-1; i++ {
		treeInsertAnchor(tree2, testKeyFromInt(i), uint64(i*100), 100)
	}
	if tree2.root.isLeaf {
		t.Fatal("expected internal root")
	}
	root := tree2.root
	shiftsBefore := make([]int64, root.count+1)
	for i := 0; i <= root.count; i++ {
		shiftsBefore[i] = root.children[i].shift
	}
	memSparseIndexTreeNodeShiftApply(root, 500)
	for i := 0; i <= root.count; i++ {
		if root.children[i].shift != shiftsBefore[i]+500 {
			t.Errorf("internal child %d shift: expected %d, got %d", i, shiftsBefore[i]+500, root.children[i].shift)
		}
	}
}

func TestSparseIndexTree_RecycleLinkedList(t *testing.T) {
	tree := memSparseIndexTreeCreate()
	n := flexMemSparseIndexTreeLeafCap - 2
	for i := 1; i <= n; i++ {
		treeInsertAnchor(tree, testKeyFromInt(i), uint64(i*100), 100)
	}
	leavesBefore := countSparseIndexTreeLeaves(tree)
	if leavesBefore < 2 {
		t.Fatal("need multiple leaves")
	}
	// Delete enough from one leaf to trigger recycle
	for i := 1; i <= n; i++ {
		treeDeleteAnchor(tree, testKeyFromInt(i))
	}
	leavesAfter := countSparseIndexTreeLeaves(tree)
	if leavesAfter >= leavesBefore {
		t.Fatalf("expected fewer leaves after mass delete: before=%d after=%d", leavesBefore, leavesAfter)
	}
	checkSparseIndexTreeInvariants(t, tree, "RecycleLinkedList")
}

func TestSparseIndexTree_FindSmallestKey(t *testing.T) {
	tree := memSparseIndexTreeCreate()
	// Smallest key in tree with just sentinel should be nil
	got := memSparseIndexTreeFindSmallestKey(tree.root)
	if got != "" {
		t.Fatalf("expected empty, got %q", got)
	}

	// After inserts, smallest should still be "" (sentinel)
	for i := 1; i <= 200; i++ {
		treeInsertAnchor(tree, testKeyFromInt(i), uint64(i*100), 100)
	}
	got = memSparseIndexTreeFindSmallestKey(tree.root)
	if got != "" {
		t.Fatalf("expected empty (sentinel), got %q", got)
	}
}

func TestSparseIndexTree_LargeSequential(t *testing.T) {
	tree, bf := buildTreeSimple(5000)
	checkSparseIndexTreeInvariants(t, tree, "LargeSequential")
	checkSparseOracleAgreement(t, tree, bf, "LargeSequential")
	if totalTreeAnchors(tree) != 5001 { // 5000 + sentinel
		t.Fatalf("expected 5001 anchors, got %d", totalTreeAnchors(tree))
	}
}

func TestSparseIndexTree_LargeRandom(t *testing.T) {
	tree := memSparseIndexTreeCreate()
	bf := newSparseIndexBruteForce()
	rng := rand.New(rand.NewSource(12345))
	used := make(map[int]bool)

	for i := 0; i < 5000; i++ {
		v := rng.Intn(1000000)
		for used[v] {
			v = rng.Intn(1000000)
		}
		used[v] = true
		key := testKeyFromInt(v)
		loff := uint64(i * 100)
		psize := uint32(50 + rng.Intn(200))
		treeInsertAnchor(tree, key, loff, psize)
		bf.insert(key, loff, psize)
	}
	checkSparseIndexTreeInvariants(t, tree, "LargeRandom")
	checkSparseOracleAgreement(t, tree, bf, "LargeRandom")
}

func TestSparseIndexTree_InsertDeleteMixed(t *testing.T) {
	tree := memSparseIndexTreeCreate()
	bf := newSparseIndexBruteForce()
	rng := rand.New(rand.NewSource(777))
	var insertedKeys []string
	used := make(map[int]bool)

	// 500 inserts with unique keys
	for i := 0; i < 500; i++ {
		v := rng.Intn(500000)
		for used[v] {
			v = rng.Intn(500000)
		}
		used[v] = true
		key := testKeyFromInt(v)
		loff := uint64(i * 100)
		psize := uint32(100)
		treeInsertAnchor(tree, key, loff, psize)
		bf.insert(key, loff, psize)
		insertedKeys = append(insertedKeys, key)
	}
	checkSparseIndexTreeInvariants(t, tree, "Mixed-after500insert")
	checkSparseOracleAgreement(t, tree, bf, "Mixed-after500insert")

	// 200 deletes
	deleted := 0
	for _, key := range insertedKeys {
		if deleted >= 200 {
			break
		}
		if treeDeleteAnchor(tree, key) {
			bf.remove(key)
			deleted++
		}
	}
	checkSparseIndexTreeInvariants(t, tree, "Mixed-after200delete")
	checkSparseOracleAgreement(t, tree, bf, "Mixed-after200delete")

	// 300 more inserts with unique keys
	for i := 0; i < 300; i++ {
		v := rng.Intn(500000)
		for used[v] {
			v = rng.Intn(500000)
		}
		used[v] = true
		key := testKeyFromInt(v)
		loff := uint64((500 + i) * 100)
		psize := uint32(100)
		treeInsertAnchor(tree, key, loff, psize)
		bf.insert(key, loff, psize)
		insertedKeys = append(insertedKeys, key)
	}
	checkSparseIndexTreeInvariants(t, tree, "Mixed-after300insert")
	checkSparseOracleAgreement(t, tree, bf, "Mixed-after300insert")

	// 400 more deletes
	deleted = 0
	for i := len(insertedKeys) - 1; i >= 0; i-- {
		if deleted >= 400 {
			break
		}
		if treeDeleteAnchor(tree, insertedKeys[i]) {
			bf.remove(insertedKeys[i])
			deleted++
		}
	}
	checkSparseIndexTreeInvariants(t, tree, "Mixed-after400delete")
	checkSparseOracleAgreement(t, tree, bf, "Mixed-after400delete")
}

func TestSparseIndexTree_CallerPatternInsert(t *testing.T) {
	// Simulate the exact pattern db.go uses:
	// findAnchorPos → grab anchor pointer → nh.idx++ → handlerInsert → nh.idx-- → shiftUpPropagate
	tree := memSparseIndexTreeCreate()

	for i := 1; i <= 300; i++ {
		key := testKeyFromInt(i)
		psize := uint32(100)
		// Find position
		var nh memSparseIndexTreeHandler
		tree.findAnchorPos(key, &nh)
		anchor := nh.node.anchors[nh.idx] // hold pointer before split
		loff := uint64(anchor.loff) + uint64(nh.shift) + uint64(anchor.psize)

		nh.idx++
		nh.handlerInsert(key, loff, psize)
		nh.idx--
		nh.shiftUpPropagate(int64(psize))
		anchor.psize += psize // use held pointer, safe across splits
	}
	checkSparseIndexTreeInvariants(t, tree, "CallerPatternInsert")
	if totalTreeAnchors(tree) != 301 { // 300 + sentinel
		t.Fatalf("expected 301 anchors, got %d", totalTreeAnchors(tree))
	}
}

func TestSparseIndexTree_CallerPatternDelete(t *testing.T) {
	// Build a tree then delete using db.go's pattern
	tree := memSparseIndexTreeCreate()
	bf := newSparseIndexBruteForce()

	// Insert
	for i := 1; i <= 100; i++ {
		key := testKeyFromInt(i)
		loff := uint64(i * 100)
		psize := uint32(100)
		treeInsertAnchor(tree, key, loff, psize)
		bf.insert(key, loff, psize)
	}

	// Delete pattern: find anchor, remove from node, update tree, recycle if empty
	for i := 1; i <= 50; i++ {
		key := testKeyFromInt(i)
		var nh memSparseIndexTreeHandler
		tree.findAnchorPos(key, &nh)
		node := nh.node
		// Find and remove
		for j := 0; j < node.count; j++ {
			if bfKeyCompare(node.anchors[j].key, key) == 0 {
				psize := node.anchors[j].psize
				// Shift
				nh.idx = j
				nh.shiftUpPropagate(-int64(psize))
				bf.shiftAfter(bf.findLE(key), -int64(psize))

				if j < node.count-1 {
					copy(node.anchors[j:], node.anchors[j+1:node.count])
				}
				node.anchors[node.count-1] = nil
				node.count--
				if node.count == 0 {
					memSparseIndexTreeRecycleNode(node)
				} else if j == 0 {
					memSparseIndexTreeUpdateSmallestKey(node, node.anchors[0].key)
				}
				bf.remove(key)
				break
			}
		}
	}
	checkSparseIndexTreeInvariants(t, tree, "CallerPatternDelete")
	checkSparseOracleAgreement(t, tree, bf, "CallerPatternDelete")
}

func TestSparseIndexTree_StaleSlotCleanup(t *testing.T) {
	// Specifically test INV-ST-13 after many splits and recycles
	tree := memSparseIndexTreeCreate()
	for i := 1; i <= 2000; i++ {
		treeInsertAnchor(tree, testKeyFromInt(i), uint64(i*100), 100)
	}
	checkSparseIndexTreeInvariants(t, tree, "StaleSlotCleanup-afterInsert")

	for i := 1; i <= 1500; i++ {
		treeDeleteAnchor(tree, testKeyFromInt(i))
	}
	checkSparseIndexTreeInvariants(t, tree, "StaleSlotCleanup-afterDelete")
}

// ====================== Fuzz Tests ======================

func FuzzSparseIndexTree(f *testing.F) {
	// Seed corpus
	f.Add([]byte{0, 1, 0, 0, 0, 50, 0, 100})
	f.Add([]byte{1, 5, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		tree := memSparseIndexTreeCreate()
		bf := newSparseIndexBruteForce()
		var insertedKeys []string
		usedKeys := make(map[int]bool)
		nextUnique := 100000 // start high to avoid collisions with keyVal

		i := 0
		for i < len(data) {
			op := data[i] % 4
			i++

			switch op {
			case 0: // insert
				if i+4 > len(data) {
					return
				}
				keyVal := int(binary.BigEndian.Uint16(data[i : i+2]))
				psize := uint32(binary.BigEndian.Uint16(data[i+2 : i+4]))
				i += 4
				if psize == 0 {
					psize = 1
				}
				// Ensure unique key
				if usedKeys[keyVal] {
					nextUnique++
					keyVal = nextUnique
				}
				usedKeys[keyVal] = true
				key := testKeyFromInt(keyVal)
				loff := uint64(len(insertedKeys) * 100)
				treeInsertAnchor(tree, key, loff, psize)
				bf.insert(key, loff, psize)
				insertedKeys = append(insertedKeys, key)

			case 1: // delete
				if len(insertedKeys) == 0 {
					continue
				}
				if i+1 > len(data) {
					return
				}
				idx := int(data[i]) % len(insertedKeys)
				i++
				key := insertedKeys[idx]
				treeDeleteAnchor(tree, key)
				bf.remove(key)
				// Remove from insertedKeys
				insertedKeys = append(insertedKeys[:idx], insertedKeys[idx+1:]...)

			case 2: // find
				if i+2 > len(data) {
					return
				}
				keyVal := int(binary.BigEndian.Uint16(data[i : i+2]))
				i += 2
				key := testKeyFromInt(keyVal)
				var nh memSparseIndexTreeHandler
				tree.findAnchorPos(key, &nh)
				bfIdx := bf.findLE(key)
				// Verify agreement on found key
				if bfKeyCompare(nh.node.anchors[nh.idx].key, bf.anchors[bfIdx].key) != 0 {
					t.Fatalf("findLE mismatch: tree=%x bf=%x",
						nh.node.anchors[nh.idx].key, bf.anchors[bfIdx].key)
				}

			case 3: // shift (positive and negative, now safe with int64 loff)
				if len(insertedKeys) == 0 || i+2 > len(data) {
					continue
				}
				idx := int(data[i]) % len(insertedKeys)
				shift := int64(int8(data[i+1])) * 10 // bidirectional [-1270..1270]
				i += 2
				if shift == 0 {
					shift = 1
				}
				key := insertedKeys[idx]
				var nh memSparseIndexTreeHandler
				tree.findAnchorPos(key, &nh)
				bfIdx := bf.findLE(key)
				nh.shiftUpPropagate(shift)
				bf.shiftAfter(bfIdx, shift)
			}

			checkSparseIndexTreeInvariants(t, tree, fmt.Sprintf("fuzz-op%d", op))
			checkSparseOracleAgreement(t, tree, bf, fmt.Sprintf("fuzz-op%d", op))
		}
	})
}

func TestSparseIndexTree_RandomizedStress(t *testing.T) {
	tree := memSparseIndexTreeCreate()
	bf := newSparseIndexBruteForce()
	rng := rand.New(rand.NewSource(31415))
	var insertedKeys []string
	usedKeys := make(map[int]bool)
	nextKey := 0

	for iter := 0; iter < 10000; iter++ {
		op := rng.Intn(4)
		switch op {
		case 0, 1: // insert (weighted higher)
			nextKey++
			for usedKeys[nextKey] {
				nextKey++
			}
			usedKeys[nextKey] = true
			key := testKeyFromInt(nextKey)
			loff := uint64(iter * 100)
			psize := uint32(10 + rng.Intn(200))
			treeInsertAnchor(tree, key, loff, psize)
			bf.insert(key, loff, psize)
			insertedKeys = append(insertedKeys, key)

		case 2: // delete
			if len(insertedKeys) == 0 {
				continue
			}
			idx := rng.Intn(len(insertedKeys))
			key := insertedKeys[idx]
			treeDeleteAnchor(tree, key)
			bf.remove(key)
			insertedKeys = append(insertedKeys[:idx], insertedKeys[idx+1:]...)

		case 3: // shift (positive and negative, now safe with int64 loff)
			if len(insertedKeys) == 0 {
				continue
			}
			idx := rng.Intn(len(insertedKeys))
			key := insertedKeys[idx]
			shift := int64(rng.Intn(1000)) - 500
			if shift == 0 {
				shift = 1
			}
			var nh memSparseIndexTreeHandler
			tree.findAnchorPos(key, &nh)
			bfIdx := bf.findLE(key)
			nh.shiftUpPropagate(shift)
			bf.shiftAfter(bfIdx, shift)
		}

		// Check invariants periodically to keep test fast
		if iter%500 == 0 || iter == 9999 {
			checkSparseIndexTreeInvariants(t, tree, fmt.Sprintf("stress-iter%d", iter))
			checkSparseOracleAgreement(t, tree, bf, fmt.Sprintf("stress-iter%d", iter))
		}
	}
	t.Logf("Final stats: anchors=%d leaves=%d height=%d splits(leaf=%d,internal=%d) recycles=%d rebases=%d",
		totalTreeAnchors(tree), countSparseIndexTreeLeaves(tree), sparseIndexTreeHeight(tree),
		tree.leafSplits, tree.internalSplits, tree.recycleCount, tree.rebaseCount)
}

// ====================== Benchmarks ======================

func BenchmarkSparseIndexTree_InsertSequential(b *testing.B) {
	for range b.N {
		tree := memSparseIndexTreeCreate()
		for i := 1; i <= 10000; i++ {
			key := testKeyFromInt(i)
			treeInsertAnchor(tree, key, uint64(i*100), 100)
		}
	}
}

func BenchmarkSparseIndexTree_FindAnchorPos(b *testing.B) {
	tree := memSparseIndexTreeCreate()
	for i := 1; i <= 10000; i++ {
		treeInsertAnchor(tree, testKeyFromInt(i), uint64(i*100), 100)
	}
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = testKeyFromInt(i + 1)
	}
	b.ResetTimer()
	var nh memSparseIndexTreeHandler
	for range b.N {
		for _, key := range keys {
			tree.findAnchorPos(key, &nh)
		}
	}
}

func BenchmarkSparseIndexTree_ShiftUpPropagate(b *testing.B) {
	tree := memSparseIndexTreeCreate()
	for i := 1; i <= 10000; i++ {
		treeInsertAnchor(tree, testKeyFromInt(i), uint64(i*100), 100)
	}
	var nh memSparseIndexTreeHandler
	tree.findAnchorPos(testKeyFromInt(5000), &nh)
	b.ResetTimer()
	for range b.N {
		nh.shiftUpPropagate(1)
		nh.shiftUpPropagate(-1)
	}
}
