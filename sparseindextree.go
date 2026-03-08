package yogadb

import (
	"bytes"
)

// ========== Sparse index tree: memSparseIndexTree ==========
//
// In-memory B-tree mapping keys to FlexSpace intervals (anchors).
// Rebuilt from FlexSpace tags on every open (not persisted independently).
// Uses pointer-based nodes (not arena-based) since GC overhead is acceptable here.

/* fix to the C design:

   promote dbAnchor.loff from uint32 to int64. dbAnchor
   is shared/defined in intervalcache.go

   The sparse index tree (used to) store each anchor's logical offset as
   uint32 loff (relative to accumulated int64 parent shifts).
   When shiftUpPropagate or memSparseIndexTreeNodeShiftApply
   adds shifts to these uint32 loffs, silent overflow wraps the value,
   producing wrong effective loffs. The existing rebase mechanism
   only fires during leaf splits with parent.count > 1,
   leaving a window for corruption.

   The stress test demonstrated this: after ~8000 random ops,
   an anchor's effective loff was off by exactly 2^32 due to uint32
   truncation in memSparseIndexTreeNodeShiftApply (called during
   node recycle when a sibling absorbs a large accumulated
   parent shift).

   Fix: Change dbAnchor.loff from uint32 to int64. This eliminates
   the overflow class entirely. Rebase remains as a nice-to-have
   optimization but is no longer needed for correctness.
*/

type memSparseIndexTreeChild struct {
	node  *memSparseIndexTreeNode
	shift int64
}

type memSparseIndexTreeNode struct {
	parent   *memSparseIndexTreeNode
	parentID int // index in parent's children
	tree     *memSparseIndexTree
	count    int
	isLeaf   bool
	// leaf fields
	anchors [flexMemSparseIndexTreeLeafCap]*dbAnchor
	prev    *memSparseIndexTreeNode
	next    *memSparseIndexTreeNode
	// internal fields
	pivots   [flexMemSparseIndexTreeInternalCap][]byte
	children [flexMemSparseIndexTreeInternalCap + 1]memSparseIndexTreeChild
}

type memSparseIndexTree struct {
	root           *memSparseIndexTreeNode
	leafHead       *memSparseIndexTreeNode
	leafSplits     int // number of leaf splits performed
	internalSplits int // number of internal splits performed
	recycleCount   int // number of nodes recycled
	rebaseCount    int // number of rebases triggered
}

type memSparseIndexTreeHandler struct {
	node  *memSparseIndexTreeNode
	shift int64
	idx   int
}

func memSparseIndexTreeCreate() *memSparseIndexTree {
	t := &memSparseIndexTree{}
	root := &memSparseIndexTreeNode{isLeaf: true, tree: t}
	// Insert sentinel anchor (null key at loff=0)
	root.anchors[0] = &dbAnchor{key: nil, loff: 0}
	root.count = 1
	t.root = root
	t.leafHead = root
	return t
}

// findAnchorPos traverses the tree and finds the leaf + position for key.
func (t *memSparseIndexTree) findAnchorPos(key []byte, nh *memSparseIndexTreeHandler) {
	var shift int64
	node := t.root
	for !node.isLeaf {
		idx := memSparseIndexTreeFindPosInternal(node, key)
		shift += node.children[idx].shift
		node = node.children[idx].node
	}
	nh.node = node
	nh.shift = shift
	nh.idx = memSparseIndexTreeFindPosLeafLE(node, key)
}

// memSparseIndexTreeFindPosInternal: binary search for child index in internal node.
// Returns i such that all pivots[j] <= key for j < i.
func memSparseIndexTreeFindPosInternal(node *memSparseIndexTreeNode, key []byte) int {
	lo, hi := 0, node.count
	for lo < hi {
		mid := (lo + hi) >> 1
		if bytes.Compare(key, node.pivots[mid]) >= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// memSparseIndexTreeFindPosLeafLE: returns index of anchor with the largest key <= search key.
func memSparseIndexTreeFindPosLeafLE(node *memSparseIndexTreeNode, key []byte) int {
	lo, hi := 0, node.count
	for lo+1 < hi {
		mid := (lo + hi) >> 1
		cmp := bytes.Compare(key, node.anchors[mid].key)
		if cmp > 0 {
			lo = mid
		} else if cmp < 0 {
			hi = mid
		} else {
			return mid
		}
	}
	return lo
}

// handlerInsert inserts a new anchor at nh.idx.
// loff is the actual absolute loff; stored relative: anchor.loff = int64(loff) - nh.shift.
func (nh *memSparseIndexTreeHandler) handlerInsert(key []byte, loff uint64, psize uint32) *dbAnchor {
	node := nh.node
	anchor := &dbAnchor{
		key:   key,
		loff:  int64(loff) - nh.shift,
		psize: psize,
	}
	target := nh.idx
	if target == node.count {
		node.anchors[node.count] = anchor
	} else {
		copy(node.anchors[target+1:node.count+1], node.anchors[target:node.count])
		node.anchors[target] = anchor
	}
	node.count++
	if memSparseIndexTreeNodeFull(node) {
		memSparseIndexTreeSplitLeaf(node)
	}
	return anchor
}

func memSparseIndexTreeNodeFull(node *memSparseIndexTreeNode) bool {
	if node.isLeaf {
		return node.count >= flexMemSparseIndexTreeLeafCap-1
	}
	return node.count >= flexMemSparseIndexTreeInternalCap-1
}

func memSparseIndexTreeSplitLeaf(node *memSparseIndexTreeNode) {
	t := node.tree
	t.leafSplits++
	node2 := &memSparseIndexTreeNode{isLeaf: true, tree: t, parent: node.parent}
	// link into doubly-linked list
	node2.prev = node
	node2.next = node.next
	node.next = node2
	if node2.next != nil {
		node2.next.prev = node2
	}
	count := (node.count + 1) / 2
	node2.count = node.count - count
	copy(node2.anchors[:], node.anchors[count:node.count])
	for i := count; i < node.count; i++ {
		node.anchors[i] = nil
	}
	node.count = count

	parent := node.parent
	if parent == nil {
		parent = &memSparseIndexTreeNode{tree: t}
		node.parent = parent
		node2.parent = parent
		t.root = parent
	}
	pivotKey := dupBytes(node2.anchors[0].key)
	memSparseIndexTreeInsertIntoParent(node, node2, pivotKey, parent)
	if parent.count > 1 {
		memSparseIndexTreeNodeRebase(node)
		memSparseIndexTreeNodeRebase(node2)
	}
	if memSparseIndexTreeNodeFull(parent) {
		memSparseIndexTreeSplitInternal(parent)
	}
}

func memSparseIndexTreeSplitInternal(node *memSparseIndexTreeNode) {
	t := node.tree
	t.internalSplits++
	node2 := &memSparseIndexTreeNode{tree: t, parent: node.parent}
	count := (node.count + 1) / 2
	newBase := node.pivots[count]
	node2.count = node.count - count - 1
	copy(node2.pivots[:], node.pivots[count+1:node.count])
	copy(node2.children[:], node.children[count+1:node.count+1])
	oldCount := node.count
	node.count = count
	// Clear stale pivots and children to allow GC
	for i := count; i < oldCount; i++ {
		node.pivots[i] = nil
	}
	for i := count + 1; i <= oldCount; i++ {
		node.children[i] = memSparseIndexTreeChild{}
	}

	parent := node.parent
	if parent == nil {
		parent = &memSparseIndexTreeNode{tree: t}
		node.parent = parent
		node2.parent = parent
		t.root = parent
	}
	memSparseIndexTreeInsertIntoParent(node, node2, newBase, parent)
	// fix child parent pointers for node2
	for i := 0; i <= node2.count; i++ {
		node2.children[i].node.parent = node2
		node2.children[i].node.parentID = i
	}
	if memSparseIndexTreeNodeFull(parent) {
		memSparseIndexTreeSplitInternal(parent)
	}
}

func memSparseIndexTreeInsertIntoParent(left, right *memSparseIndexTreeNode, pivot []byte, parent *memSparseIndexTreeNode) {
	if parent.count == 0 {
		parent.children[0] = memSparseIndexTreeChild{node: left, shift: 0}
		parent.children[1] = memSparseIndexTreeChild{node: right, shift: 0}
		parent.pivots[0] = pivot
		parent.count = 1
		left.parentID = 0
		left.parent = parent
		right.parentID = 1
		right.parent = parent
	} else {
		pIdx := left.parentID
		origShift := parent.children[pIdx].shift
		copy(parent.pivots[pIdx+1:], parent.pivots[pIdx:parent.count])
		copy(parent.children[pIdx+2:], parent.children[pIdx+1:parent.count+1])
		parent.children[pIdx+1] = memSparseIndexTreeChild{node: right, shift: origShift}
		parent.pivots[pIdx] = pivot
		parent.count++
		right.parentID = pIdx + 1
		right.parent = parent
		for i := pIdx + 2; i <= parent.count; i++ {
			parent.children[i].node.parentID = i
		}
	}
}

// memSparseIndexTreeNodeRebase prevents uint32 overflow of anchor.loff by rebasing.
func memSparseIndexTreeNodeRebase(node *memSparseIndexTreeNode) {
	if !node.isLeaf || node.count == 0 || node.parent == nil {
		return
	}
	if node.anchors[node.count-1].loff >= 0x3FFFFFFF {
		newBase := node.anchors[0].loff
		if newBase == 0 {
			return
		}
		node.tree.rebaseCount++
		node.parent.children[node.parentID].shift += newBase
		for i := 0; i < node.count; i++ {
			node.anchors[i].loff -= newBase
		}
	}
}

// shiftUpPropagate propagates a loff shift through the sparse index tree.
func (nh *memSparseIndexTreeHandler) shiftUpPropagate(shift int64) {
	node := nh.node
	for i := nh.idx + 1; i < node.count; i++ {
		node.anchors[i].loff += shift
	}
	for node.parent != nil {
		pIdx := node.parentID
		node = node.parent
		for i := pIdx; i < node.count; i++ {
			node.children[i+1].shift += shift
		}
	}
}

// memSparseIndexTreeHandlerInfoUpdate recalculates nh.shift by traversing from leaf to root.
func memSparseIndexTreeHandlerInfoUpdate(nh *memSparseIndexTreeHandler) {
	node := nh.node
	shift := int64(0)
	for node.parent != nil {
		pIdx := node.parentID
		node = node.parent
		shift += node.children[pIdx].shift
	}
	nh.shift = shift
}

// treeNodeHandlerNextAnchor updates nh to the anchor for key (hinted search).
func (t *memSparseIndexTree) treeNodeHandlerNextAnchor(nh *memSparseIndexTreeHandler, key []byte) {
	if nh.node == nil {
		t.findAnchorPos(key, nh)
		return
	}
	// Hinted: walk up to find suitable ancestor, then down.
	node := nh.node
	cnode := nh.node
	for cnode.parent != nil {
		pID := cnode.parentID
		if pID+1 > cnode.parent.count {
			cnode = cnode.parent
			node = cnode // track highest ancestor visited
		} else {
			if bytes.Compare(key, cnode.parent.pivots[pID]) < 0 {
				node = cnode
				break
			}
			node = cnode.parent
			cnode = node
		}
	}
	for !node.isLeaf {
		idx := memSparseIndexTreeFindPosInternal(node, key)
		node = node.children[idx].node
	}
	nh.node = node
	nh.idx = memSparseIndexTreeFindPosLeafLE(node, key)
	memSparseIndexTreeHandlerInfoUpdate(nh)
}

// memSparseIndexTreeUpdateSmallestKey updates the pivot key in ancestors when the first key in a subtree changes.
func memSparseIndexTreeUpdateSmallestKey(since *memSparseIndexTreeNode, key []byte) {
	pIdx := since.parentID
	tnode := since.parent
	for tnode != nil {
		if pIdx == 0 {
			pIdx = tnode.parentID
			tnode = tnode.parent
		} else {
			break
		}
	}
	if tnode != nil {
		tnode.pivots[pIdx-1] = dupBytes(key)
	}
}

func memSparseIndexTreeFindSmallestKey(node *memSparseIndexTreeNode) []byte {
	n := node
	for !n.isLeaf {
		n = n.children[0].node
	}
	if n.count > 0 {
		return n.anchors[0].key
	}
	return nil
}

func memSparseIndexTreeRecycleLinkedList(node *memSparseIndexTreeNode) {
	t := node.tree
	prev := node.prev
	next := node.next
	if prev != nil {
		prev.next = next
	} else {
		t.leafHead = next
	}
	if next != nil {
		next.prev = prev
	}
}

func memSparseIndexTreeNodeShiftApply(node *memSparseIndexTreeNode, shift int64) {
	if node.isLeaf {
		for i := 0; i < node.count; i++ {
			node.anchors[i].loff += shift
		}
	} else {
		for i := 0; i <= node.count; i++ {
			node.children[i].shift += shift
		}
	}
}

func memSparseIndexTreeRecycleNode(node *memSparseIndexTreeNode) {
	t := node.tree
	t.recycleCount++
	parent := node.parent
	if t.root == node {
		panic("flexdb: trying to recycle root node")
	}
	pIdx := node.parentID
	parentExist := parent != nil

	if node.isLeaf {
		memSparseIndexTreeRecycleLinkedList(node)
	}

	if parent.count == 1 {
		sIdx := 1 - pIdx
		sShift := parent.children[sIdx].shift
		sNode := parent.children[sIdx].node
		if t.root == parent {
			memSparseIndexTreeNodeShiftApply(sNode, sShift)
			t.root = sNode
			sNode.parent = nil
			sNode.parentID = 0
		} else {
			gp := parent.parent
			gpIdx := parent.parentID
			gp.children[gpIdx].node = sNode
			gp.children[gpIdx].shift += sShift
			sNode.parent = gp
			sNode.parentID = gpIdx
			if sIdx == 1 {
				np := memSparseIndexTreeFindSmallestKey(sNode)
				if gpIdx == 0 {
					memSparseIndexTreeUpdateSmallestKey(gp, np)
				} else {
					gp.pivots[gpIdx-1] = dupBytes(np)
				}
			}
		}
		parentExist = false
	} else {
		if pIdx == 0 {
			copy(parent.pivots[:], parent.pivots[1:parent.count])
			copy(parent.children[:], parent.children[1:parent.count+1])
			parent.count--
			parent.pivots[parent.count] = nil
			parent.children[parent.count+1] = memSparseIndexTreeChild{}
			for i := 0; i <= parent.count; i++ {
				parent.children[i].node.parentID = i
			}
			np := memSparseIndexTreeFindSmallestKey(parent)
			memSparseIndexTreeUpdateSmallestKey(parent, np)
		} else {
			copy(parent.pivots[pIdx-1:], parent.pivots[pIdx:parent.count])
			copy(parent.children[pIdx:], parent.children[pIdx+1:parent.count+1])
			parent.count--
			parent.pivots[parent.count] = nil
			parent.children[parent.count+1] = memSparseIndexTreeChild{}
			for i := pIdx; i <= parent.count; i++ {
				parent.children[i].node.parentID = i
			}
		}
	}
	if parentExist && parent.count == 0 {
		memSparseIndexTreeRecycleNode(parent)
	}
}
