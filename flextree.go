package yogadb

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/glycerine/vfs"
)

//go:generate greenpack

// Implementing an Arena/Offset system in Go gives
// you the best of both worlds for a B-tree: you
// maintain strict type safety, but you get
// C-like memory contiguity and effectively
// zero Garbage Collection (GC) overhead.
//
// The core trick is twofold:
//
// 1) Use Indices Instead of Pointers: We pack a
// boolean flag (Internal vs. Leaf) and an integer
// index into a single uint32. This mimics a
// C union/pointer pair. Update: we just use a signed int32.
//
// 2) Use Arrays Instead of Slices: By using
// fixed-size arrays inside the node structs,
// we completely eliminate Go's 24-byte slice headers.
// The GC sees the entire Arena as a single block
// of memory and doesn't have to scan inside
// it for pointers.
//
// As a bonus, serialization to and from disk
// becomes easy, since there are no pointers.
// The arenas are ready to be serialized
// directly.

// NodeID acts as our union pointer.
//
// A FlexTree LeafNode will have a NodeID > 0.
// A FlexTree InternalNode will have a NodeID < 0.
//
// A zero NodeID is reserved as IllegalID so we can terminate our linked list
// of NodeIDs properly. It is naturally neither
// positive nor negative, so neither Leaf nor Internal.
type NodeID int32

// IllegalID is used instead of nil pointers to
// represent an uninitialized NodeID.
const IllegalID NodeID = 0

const DefaultFlexTreeMaxExtentSizeLimit = (64 << 20) // 64 MB

// or?
//const DefaultFlexTreeMaxExtentSizeLimit = (128 << 10) // 128 KB

// sentinel values to recognize double frees from our free lists.
var freedLeaf = LeafNode{CommonNode: CommonNode{Freed: true, SlotID: -1}} //, Next: IllegalID, Prev: IllegalID}
var freedInternal = InternalNode{CommonNode: CommonNode{Freed: true, SlotID: -1}}

var initLeafValue LeafNode
var initInternalValue InternalNode

func init() {
	// setup initLeafValue

	// automatic now that IllegalID is 0.
	//initLeafValue.Next = IllegalID
	//initLeafValue.Prev = IllegalID
	initLeafValue.SlotID = -1
	initLeafValue.IsLeaf = true

	// setup initInternalValue
	initInternalValue.SlotID = -1

	// automatic now that IllegalID is 0.
	//for i := range initInternalValue.Children {
	//	initInternalValue.Children[i].NodeID = IllegalID
	//}
	for i := range initInternalValue.ChildSlotIDs {
		initInternalValue.ChildSlotIDs[i] = -1
	}
}

func NewLeafID(index int32) NodeID {
	if index < 0 {
		panic("cannot ask give negative index to NewLeafID")
	}
	// Add 1 so index 0 maps to NodeID(1), strictly > 0, unambiguously an InternalNode.
	return NodeID(index) + 1
}

func NewInternalID(index int32) NodeID {
	if index < 0 {
		panic("cannot ask give negative index to NewInternalID")
	}
	// Add 1 before negating so index 0 maps to NodeID(-1), strictly < 0.
	// Without +1, NewLeafID(0) = -0 = 0, which IsLeaf() (id < 0) would not recognise.
	return -(NodeID(index) + 1)
}

func (id NodeID) IsIllegal() bool  { return id == IllegalID }
func (id NodeID) IsLeaf() bool     { return id > 0 }
func (id NodeID) IsInternal() bool { return id < 0 }
func (id NodeID) Index() int32 {
	if id > 0 {
		return int32(id) - 1
	}
	if id == IllegalID {
		panic("cannot take Index() of IllegalID")
	}
	return int32(-id) - 1
}

// 2. Defining the Nodes and the Arena
//
// To ensure maximum density, we define
// the B-Tree degree with a constant and
// use arrays. This means a node's size
// is known at compile time, and an entire
// slice of nodes will be perfectly contiguous
// in memory—just like a block of memory
// malloc'd in C.

// CommonNode is embedded in both LeafNode and InternalNode.
// 20 bytes at the moment.
type CommonNode struct {

	// SlotID is the on-disk page slot for CoW persistence; -1 = unassigned
	SlotID int64 `zid:"0"`

	// know thyself.
	NodeID NodeID `zid:"1"`

	Count  uint32 `zid:"2"`
	IsLeaf bool   `zid:"3"`
	Dirty  bool   `zid:"4"`
	Freed  bool   `zid:"5"`
	//pad    bool // Go will pad automatically.
}

// InternalNode contains keys and offsets to children. Size is fixed (arrays not slices).
type InternalNode struct {
	CommonNode `zid:"0"` // 20 bytes

	// given n pivots,
	Pivots [FLEXTREE_INTERNAL_CAP]uint64 `zid:"1"` // 8 bytes * 30 = 240 bytes (260 total)
	// we need n+1 children.
	Children [FLEXTREE_INTERNAL_CAP_PLUS_ONE]InternalChild `zid:"2"` // 12 bytes * 31 == 372 bytes (632 total)

	// On-disk slot IDs for CoW persistence: tracks which page slot
	// each child occupies on disk. During CoW sync, dirty nodes are
	// written to freshly allocated slots, and the parent's
	// ChildSlotIDs[idx] records the child's new slot location.
	ChildSlotIDs [FLEXTREE_INTERNAL_CAP_PLUS_ONE]int64 `zid:"3"` // 8 bytes * 31 == 248 bytes (880 total)
}

type InternalChild struct {
	NodeID NodeID `zid:"0"`

	// 64-bit accumulator
	Shift int64 `zid:"1"`
}

// LeafNode contains keys and values. Size is fixed.
type LeafNode struct {
	CommonNode `zid:"0"` //

	Extents [FLEXTREE_LEAF_CAP]FlexTreeExtent `zid:"1"`

	// leaf linked list
	Prev NodeID `zid:"2"`
	Next NodeID `zid:"3"`
}

// FlexTree acts as our Arena allocator and entry point.
type FlexTree struct {
	MaxLoff       uint64 `zid:"0"`
	MaxExtentSize uint32 `zid:"1"`

	Path              string `zid:"2"`
	NodeCount         uint64 `zid:"3"`
	PersistentVersion uint64 `zid:"4"`

	Root NodeID `zid:"5"`

	LeafHead NodeID `zid:"6"` // linked list head

	// the arenas that hold all nodes.
	InternalArena []InternalNode `zid:"7"`
	LeafArena     []LeafNode     `zid:"8"`

	// the two free lists (of indexes into arenas)

	// Stack of available internal node indices
	FreeInternals []int32 `zid:"9"`

	// Stack of available leaf node indices
	FreeLeaves []int32 `zid:"10"`

	// CoW persistence fields (unexported, not greenpack-serialized)
	fs           vfs.FS
	metaFD       vfs.File
	nodeFD       vfs.File
	maxSlotID    int64
	nodesFileCap int64 // pre-allocated slot capacity of FLEXTREE.PAGES file
	freeSlots    []int64
	cowEnabled   bool
	metaNextOff  int64 // next write offset in append-only FLEXTREE.COMMIT file
	metaFileCap  int64 // pre-allocated size of FLEXTREE.COMMIT file in bytes

	// Write-byte counters (accessed atomically)
	FlexTreePagesBytesWritten int64 // FLEXTREE.PAGES + FLEXTREE.COMMIT bytes written to disk

	// Cumulative counters persisted in cowMeta (set by FlexDB before SyncCoW)
	totalLogicalBytesWrit  int64 // cumulative user payload bytes (key+value)
	totalPhysicalBytesWrit int64 // cumulative total bytes written to all files
	MaxHLC                 int64 // highest HLC ever assigned; restored on reopen

	// Live key counters, persisted via cowMeta so OpenFlexDB avoids a full scan.
	liveKeys      int64
	liveBigKeys   int64
	liveSmallKeys int64

	// Per-operation performance counters (unexported, reset before each op)
	nodesVisited   int64 // nodes touched during findLeafNode + shiftUpPropagate
	splitCount     int64 // leaf + internal splits triggered
	shiftPropNodes int64 // internal nodes updated by shiftUpPropagate
}

func (tree *FlexTree) resetOpCounters() {
	tree.nodesVisited = 0
	tree.splitCount = 0
	tree.shiftPropNodes = 0
}

func (tree *FlexTree) opCounters() (visited, splits, shiftNodes int64) {
	return tree.nodesVisited, tree.splitCount, tree.shiftPropNodes
}

// allExtents walks the leaf linked list and returns all extents
// as bruteForceExtent values with effective (global) Loff values,
// suitable for comparison with bruteForce and checkInvariants.
func (tree *FlexTree) allExtents() []bruteForceExtent {
	if tree.MaxLoff == 0 {
		return nil
	}
	var result []bruteForceExtent
	pos := tree.PosGet(0)
	for pos.Valid() {
		ext := &pos.node.Extents[pos.Idx]
		result = append(result, bruteForceExtent{
			Loff: pos.Loff - uint64(pos.Diff),
			Len:  ext.Len,
			Poff: ext.Poff(),
			Tag:  ext.Tag(),
		})
		pos.ForwardExtent()
	}
	return result
}

type FlexTreePath struct {
	Level uint8                       `zid:"0"`
	Path  [FLEXTREE_PATH_DEPTH]uint8  `zid:"1"`
	Nodes [FLEXTREE_PATH_DEPTH]NodeID `zid:"2"`
}

type Offlen struct {
	Poff uint64 `zid:"0"`
	Len  uint64 `zid:"1"`
}

type FlextreeQueryResult struct {
	Loff  uint64   `zid:"0"`
	Len   uint64   `zid:"1"`
	Count uint64   `zid:"2"`
	V     []Offlen `zid:"3"`
}

const FLEXTREE_HOLE = (1 << 47)           // highest bit set to indicate a hole
const FLEXTREE_POFF_MASK = 0xffffffffffff // 48 bits
const FLEXTREE_PATH_DEPTH = 7             // at most 7 levels

// Tunable parameters
const FLEXTREE_LEAF_CAP = 60
const FLEXTREE_INTERNAL_CAP = 30
const FLEXTREE_INTERNAL_CAP_PLUS_ONE = FLEXTREE_INTERNAL_CAP + 1
const FLEXTREE_MAX_EXTENT_SIZE_LIMIT = 64 << 20

// for less allocation on String() generation
var bufPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

func init() {
	// "static assert" like checks

	leafSz := FLEXTREE_MAX_EXTENT_SIZE_LIMIT * FLEXTREE_LEAF_CAP
	if leafSz >= (4 << 30) {
		panicf("leafSz must be < 4GB; was %v", leafSz)
	}

	if FLEXTREE_LEAF_CAP >= 254 {
		panicf("FLEXTREE_LEAF_CAP(%v) must be < 254 for FlexTreePath", FLEXTREE_LEAF_CAP)
	}
	if FLEXTREE_INTERNAL_CAP >= 254 {
		panicf("FLEXTREE_INTERNAL_CAP(%v) must be < 254 for FlexTreePath (we use a uint8 at the moment for FlexTreePath.Path)", FLEXTREE_INTERNAL_CAP)
	}
}

// FlexTreeExtent is a foundation piece.
//
// Q: FlexTreeExtent is defined with the
// logical offset Loff uint32. How it is possible that the small 32-bit
// uint32 is correct here, rather than a larger uint64? It seems that
// the logical address space needs to span 62 or 63 bits, does it not?
// Please help me understand the design here.
//
// A: Great question. Here's the key insight.
//
// The Loff in each FlexTreeExtent is not a global logical
// offset. It's a leaf-local relative offset. The global
// 64-bit logical address is reconstructed on-the-fly during tree
// traversal by accumulating Shift values down the path.
//
// Here's how it works:
//
// 1. Internal nodes carry Shift int64 per child
//
//	type InternalChild struct {
//	    NodeID NodeID
//	    Shift  int64   // <- this is the 64-bit accumulator
//	}
//
// When traversing from root to leaf (line 697), the global loff uint64
// is reduced at each level: loff -= uint64(ie.Children[target].Shift)
//
// By the time you reach a leaf, loff has been transformed into a small,
// leaf-local coordinate. The extents in that leaf use uint32 offsets
// relative to this local frame.
//
// 2. Inserts propagate shifts upward
//
// When data is inserted at a logical offset, FlexTree.shiftUpPropagate()
// (line 752) adds the inserted length to the Shift of sibling
// children at each level -- but only for siblings to the right of the
// insertion point. This means only the path from root to the affected
// leaf is updated; all other subtrees lazily absorb the shift when
// they are next traversed.
//
// 3. rebase() prevents uint32 overflow
//
// Line 736, in FlexTree.rebase():
//
//	if le.Extents[le.Count-1].Loff >= ^uint32(0) - tree.MaxExtentSize*2 {
//	    newBase := le.Extents[0].Loff
//	    parent.Children[pIdx].Shift += int64(newBase)
//	    for i := uint32(0); i < le.Count; i++ {
//	        le.Extents[i].Loff -= newBase
//	    }
//	}
//
// When a leaf's local offsets approach uint32 max (~4 GB),
// rebase() shifts the entire leaf's coordinates back to near zero
// by absorbing the base offset into the parent's Shift.
// This is O(leaf capacity) = O(60) work -- cheap.
//
// Summary: The 64-bit logical address space lives in the tree
// edges (Shift int64 on each InternalChild), not in the leaf
// extents. Each leaf only needs to represent offsets within its
// own local window, which never exceeds ~4 GB thanks to rebase().
//
// This is the paper's key trick -- it keeps the FlexTreeExtent struct
// at exactly 16 bytes (matching a cache line quarter), which is
// critical for the B-tree's I/O efficiency. Storing a uint64 Loff
// would bloat each extent to 20+ bytes and reduce the leaf
// fan-out from 60 to ~48.
// .
type FlexTreeExtent struct {
	Loff uint32 `zid:"0"`
	Len  uint32 `zid:"1"`

	// Combines tag (16 bits) and poff (48 bits)
	TagPoff uint64 `zid:"2"`

	// Poff (48 bits) breakdown:
	// 47 bits for address, 1 bit for hole.
	// The max size of a file is 2^47-1 bytes (~128T, enough)
}

// low level position on leaf nodes
type Pos struct {
	tree *FlexTree // for NodeID -> pointer navigation (unexported, not serialized)
	node *LeafNode

	Loff uint64 `zid:"0"` // logical offset of the position
	Idx  uint32 `zid:"1"` // index of the entry
	Diff uint32 `zid:"2"` // the current position within the entry
}

// --- FlexTreeExtent TagPoff Getters ---

// Tag extracts the lower 16 bits
func (e *FlexTreeExtent) Tag() uint16 {
	return uint16(e.TagPoff & 0xFFFF)
}

// Poff extracts the upper 48 bits
func (e *FlexTreeExtent) Poff() uint64 {
	return e.TagPoff >> 16
}

// --- FlexTreeExtent Setters ---

// SetTag replaces the lower 16 bits while keeping the upper 48 bits intact
func (e *FlexTreeExtent) SetTag(tag uint16) {
	// e.TagPoff &^ 0xFFFF clears the bottom 16 bits
	e.TagPoff = (e.TagPoff &^ 0xFFFF) | uint64(tag)
}

// SetPoff replaces the upper 48 bits while keeping the lower 16 bits intact
func (e *FlexTreeExtent) SetPoff(poff uint64) {
	// Mask poff to ensure it strictly fits in 48 bits, preventing bleed-over
	poff &= FLEXTREE_POFF_MASK

	// e.TagPoff & 0xFFFF keeps ONLY the bottom 16 bits
	e.TagPoff = (e.TagPoff & 0xFFFF) | (poff << 16)
}

// Address extracts the 47-bit physical offset address
func (e *FlexTreeExtent) Address() uint64 {
	// Mask out the 48th bit (hole flag) to get just the lower 47 bits
	return e.Poff() & 0x7FFFFFFFFFFF
}

// IsHole returns true if the 48th bit of poff is set
func (e *FlexTreeExtent) IsHole() bool {
	// Shift the 48-bit value right by 47 spaces.
	// If the hole bit was a 1, the result is 1.
	return (e.Poff() >> 47) == 1
}

// --- Setters ---

// SetAddress sets the 47-bit address without altering the hole flag
func (e *FlexTreeExtent) SetAddress(addr uint64) {
	// 1. Ensure the new address strictly fits in 47 bits
	addr &= 0x7FFFFFFFFFFF

	// 2. Fetch the current 48-bit poff
	currentPoff := e.Poff()

	// 3. Keep ONLY the hole flag from the current poff, and OR in the new address
	newPoff := (currentPoff & 0x800000000000) | addr

	// 4. Save it back
	e.SetPoff(newPoff)
}

// SetHole sets or clears the 1-bit hole flag without altering the address
func (e *FlexTreeExtent) SetHole(isHole bool) {
	currentPoff := e.Poff()
	var newPoff uint64

	if isHole {
		// Set the 48th bit using bitwise OR
		newPoff = currentPoff | 0x800000000000
	} else {
		// Clear the 48th bit using bitwise AND NOT (&^)
		newPoff = currentPoff &^ 0x800000000000
	}

	e.SetPoff(newPoff)
}

func (tree *FlexTree) ParentNode(path *FlexTreePath) (ie *InternalNode, nodeID NodeID, ok bool) {
	if path.Level == 0 {
		return
	}
	ok = true
	nodeID = path.Nodes[path.Level-1]
	ie = tree.GetInternal(nodeID)
	return
}

func (tree *FlexTree) ParentIdx(path *FlexTreePath) (index uint32, ok bool) {
	if path.Level == 0 {
		return ^uint32(0), false
	}
	ok = true
	index = uint32(path.Path[path.Level-1])
	return
}

func (tree *FlexTree) ParentNodeIdx(path *FlexTreePath) (ie *InternalNode, index uint32, ok bool) {
	if path.Level == 0 {
		index = ^uint32(0)
		return
	}
	ok = true
	nodeID := path.Nodes[path.Level-1]
	ie = tree.GetInternal(nodeID)
	index = uint32(path.Path[path.Level-1])
	return
}

func (tree *FlexTree) GrandparentIdx(path *FlexTreePath) (index uint32) {
	if path.Level < 2 {
		index = ^uint32(0)
		return
	}
	index = uint32(path.Path[path.Level-2])
	return
}

func (tree *FlexTree) GrandparentNode(path *FlexTreePath) (ie *InternalNode) {
	if path.Level < 2 {
		return
	}
	nodeID := path.Nodes[path.Level-2]
	ie = tree.GetInternal(nodeID)
	return
}

// 3. Allocation and Access
//
// Instead of calling new(Node), you append to
// the respective slice and return its generated
// NodeID. When you need to read or write to a node,
// you fetch it from the slice using a pointer.

// NewTree initializes the tree and pre-allocates some capacity
// to avoid early slice reallocations.
//
// Warning: after getting a new node with AllocLeaf()
// or AllocInternal() be aware that the Go allocator
// may have just re-sized the underlying arena to
// grow it, which means any existing address (rather
// than index) will need to be re-converted from
// index into pointer before use. We had several
// early bugs from this. You have been warned!
//
// We could and maybe should just allocate the arenas
// with malloc directly from the operating
// system... but meh. It's usually fragile to get into
// the business of creating your own memory manager.
func NewFlexTree(fs vfs.FS) *FlexTree {
	return &FlexTree{
		fs:            fs,
		MaxExtentSize: DefaultFlexTreeMaxExtentSizeLimit,

		//Root:          IllegalID,
		//LeafHead:      IllegalID,
		InternalArena: make([]InternalNode, 0, 128<<10),
		LeafArena:     make([]LeafNode, 0, 128<<10),

		// Pre-allocate some free list capacity
		FreeInternals: make([]int32, 0, 16<<10),
		FreeLeaves:    make([]int32, 0, 16<<10),
		cowEnabled:    true,
	}
}

// AllocLeaf grabs a slot, preferring recycled nodes from the free list.
func (t *FlexTree) AllocLeaf() (le *LeafNode) {
	var idx int32

	if len(t.FreeLeaves) > 0 {
		// 1. Pop from the free list
		last := len(t.FreeLeaves) - 1
		idx = t.FreeLeaves[last]
		t.FreeLeaves = t.FreeLeaves[:last]
	} else {
		// 3. Fallback: expand the arena
		t.LeafArena = append(t.LeafArena, LeafNode{})
		idx = int32(len(t.LeafArena) - 1)
	}

	le = &t.LeafArena[idx]
	*le = initLeafValue // sets next, prev to IllegalID, IsLeaf:true
	le.NodeID = NewLeafID(idx)
	return
}

// AllocInternal mirrors the leaf logic for internal nodes.
func (t *FlexTree) AllocInternal() (ie *InternalNode) {
	var idx int32

	if len(t.FreeInternals) > 0 {
		last := len(t.FreeInternals) - 1
		idx = t.FreeInternals[last]
		t.FreeInternals = t.FreeInternals[:last]

	} else {
		t.InternalArena = append(t.InternalArena, InternalNode{})
		idx = int32(len(t.InternalArena) - 1)
	}

	ie = &t.InternalArena[idx]
	*ie = initInternalValue // starts IllegalID on all Children, -1 on all ChildSlotIDs
	ie.NodeID = NewInternalID(idx)

	return
}

// GetLeaf retrieves a pointer to a leaf node for mutation.
func (t *FlexTree) GetLeaf(id NodeID) *LeafNode {
	if !id.IsLeaf() {
		panicf("GetLeaf called with Internal NodeID %v", id)
	}
	return &t.LeafArena[id.Index()]
}

// GetInternal retrieves a pointer to an internal node.
func (t *FlexTree) GetInternal(id NodeID) *InternalNode {
	if id.IsLeaf() {
		panicf("GetInternal called with Leaf NodeID %v", id)
	}
	return &t.InternalArena[id.Index()]
}

// MarkAllInternalsDirty sets Dirty=true on every allocated internal node.
// This is needed before SyncCoW when leaf nodes have been modified
// (e.g., by VacuumKV) without going through the normal insert/delete
// path that propagates dirty flags up the tree.
func (t *FlexTree) MarkAllInternalsDirty() {
	for i := range t.InternalArena {
		if t.InternalArena[i].NodeID != IllegalID {
			t.InternalArena[i].Dirty = true
		}
	}
}

// NodeSlotID returns the SlotID for the node identified by id.
func (t *FlexTree) NodeSlotID(id NodeID) int64 {
	if id.IsLeaf() {
		return t.LeafArena[id.Index()].SlotID
	}
	return t.InternalArena[id.Index()].SlotID
}

func (t *FlexTree) GetNodeString(id NodeID) string {
	if id.IsLeaf() {
		le := t.GetLeaf(id)
		return le.String()
	}
	ie := t.GetInternal(id)
	return ie.StringOnTree(t)
}

// Why this is blazingly fast in Go
//
// a) No Pointers: Go's GC sweeps memory
// looking for pointers. Because NodeID is
// just a uint32, the GC treats a slice of
// 10,000 nodes as one single object,
// effectively eliminating GC pauses.
//
// b) Cache Locality: In a typical pointer-based
// tree, traversing from parent to child requires
// jumping to random memory locations. Here,
// nodes allocated around the same time sit right
// next to each other in CPU cache.
//
// c) Zero Overhead: By omitting the 24-byte slice
// header (ptr, len, cap), every single byte
// allocated is actually used for your B-Tree logic.
//
// The Trade-off: Memory Reclamation
// The biggest drawback to the Arena approach is
// that deleting nodes becomes your responsibility.
// If you delete a key and a node becomes empty,
// Go will not garbage collect that specific node
// because it is still part of the backing array
// of your LeafArena slice.
//
// To handle deletions in an Arena, you typically
// implement a free list. A free list is a stack of NodeID
// indices that represent empty slots. Before
// appending a new node to the Arena, you check
// if the Free List has any reusable indices.

// FreeNode releases a node's index back to the pool,
// strictly checking for double-free corruption.
func (t *FlexTree) FreeNode(id NodeID) {
	idx := id.Index()

	if id.IsLeaf() {
		node := &t.LeafArena[idx]

		// 1. Check for double free
		if node.Freed {
			panicf("double free detected on leaf node at index %d", idx)
		}

		// 2. Mark as freed. blanks everything except the Freed flag.
		*node = freedLeaf

		// 3. Add to free list
		t.FreeLeaves = append(t.FreeLeaves, idx)

	} else {
		node := &t.InternalArena[idx]

		if node.Freed {
			panicf("double free detected on internal node at index %d", idx)
		}

		*node = freedInternal
		t.FreeInternals = append(t.FreeInternals, idx)
	}
}

///// port over methods from flextree.go

func (le *LeafNode) findPosInLeaf(loff uint64) uint32 {
	hi := le.Count
	lo := uint32(0)

	// binary search
	for lo < hi {
		target := uint32((uint64(lo) + uint64(hi)) >> 1)
		fe := &le.Extents[target]
		if uint64(fe.Loff) <= loff {
			if uint64(fe.Loff)+uint64(fe.Len) > loff {
				return target
			} else {
				lo = target + 1
			}
		} else {
			hi = target
		}
	}
	return lo
}

func (ie *InternalNode) findPosInInternal(loff uint64) uint32 {
	hi := ie.Count
	lo := uint32(0)

	for lo < hi {
		target := uint32((uint64(lo) + uint64(hi)) >> 1)
		base := ie.Pivots[target]
		if base <= loff {
			lo = target + 1
		} else {
			hi = target
		}
	}
	return lo
}

// with a loff, get the leaf node that the loff belongs to..
// if it is larger than any nodes, the rightmost leaf node is returned
func (tree *FlexTree) findLeafNode(path *FlexTreePath, loffIn uint64) (leaf *LeafNode, loffOut uint64) {
	node := tree.Root
	if node.IsLeaf() { // unlikely
		return tree.GetLeaf(node), loffIn
	}
	//vv("tree.Root node = '%v'", node)
	ie := tree.GetInternal(node)

	loff := loffIn
	for {
		tree.nodesVisited++
		target := ie.findPosInInternal(loff)
		loff -= uint64(ie.Children[target].Shift)

		path.Nodes[path.Level] = node

		// The path.Path stores the index within
		// the INTERNAL node's children array, not some global ID.
		// With the cap at 30 (FLEXTREE_INTERNAL_CAP), the maximum
		// valid index is 30, so the uint8 should suffice.
		path.Path[path.Level] = uint8(target) // although it is u32.
		path.Level++
		if path.Level > FLEXTREE_PATH_DEPTH {
			panic("the tree is too high")
		}
		node = ie.Children[target].NodeID
		if node.IsLeaf() {
			return tree.GetLeaf(node), loff
		}
		ie = tree.GetInternal(node)
	}
}

func (n *CommonNode) isFull() bool {
	if n.IsLeaf {
		return n.Count >= FLEXTREE_LEAF_CAP-1
	}
	return n.Count >= FLEXTREE_INTERNAL_CAP-1
}

func (n *CommonNode) isEmpty() bool {
	return n.Count == 0
}

func (tree *FlexTree) rebase(n NodeID, path *FlexTreePath) {
	if !n.IsLeaf() || path.Level == 0 {
		return
	}
	// INVAR: n is a leaf
	le := tree.GetLeaf(n)

	if le.Extents[le.Count-1].Loff >= ^uint32(0)-tree.MaxExtentSize*2 {
		newBase := le.Extents[0].Loff

		parent, pIdx, ok := tree.ParentNodeIdx(path)
		if !ok {
			panicf("jea: TODO: what here?")
		}
		parent.Children[pIdx].Shift += int64(newBase)
		for i := uint32(0); i < le.Count; i++ {
			le.Extents[i].Loff -= newBase
		}
		parent.Dirty = true
		le.Dirty = true
	}
}

func (tree *FlexTree) shiftUpPropagate(path *FlexTreePath, shift int64) {

	// bottom-up traversal

	// make a copy of path we can modify .Level
	// acts as our cursor.
	opath := *path

	parent, _, ok := tree.ParentNode(&opath)
	if !ok {
		return
	}
	var node *InternalNode
	var pIdx uint32

	for ok {
		pIdx, _ = tree.ParentIdx(&opath)

		node = parent
		opath.Level--
		parent, _, ok = tree.ParentNode(&opath)

		tree.shiftPropNodes++
		ie := node
		for i := pIdx; i < node.Count; i++ {
			ie.Pivots[i] += uint64(shift)
			ie.Children[i+1].Shift += shift
		}
		ie.Dirty = true
	}
}

// markPathDirty marks all internal nodes on the root-to-leaf path as dirty.
// This ensures SyncCoW will visit dirty leaves even when shiftUpPropagate
// is not called (e.g., append-at-MaxLoff). Cost: O(depth) ≈ O(log N).
func (tree *FlexTree) markPathDirty(path *FlexTreePath) {
	for i := uint8(0); i < path.Level; i++ {
		ie := tree.GetInternal(path.Nodes[i])
		ie.Dirty = true
	}
}

// propagateDirtyUp does a post-order traversal: if any child is dirty,
// the parent is marked dirty too. Used after GC which marks leaves dirty
// without path context. O(tree nodes) but GC is already expensive.
func (tree *FlexTree) propagateDirtyUp(nodeID NodeID) bool {
	if nodeID.IsIllegal() {
		return false
	}
	if nodeID.IsLeaf() {
		return tree.GetLeaf(nodeID).Dirty
	}
	ie := tree.GetInternal(nodeID)
	childDirty := false
	for i := uint32(0); i <= ie.Count; i++ {
		if tree.propagateDirtyUp(ie.Children[i].NodeID) {
			childDirty = true
		}
	}
	if childDirty {
		ie.Dirty = true
	}
	return ie.Dirty
}

func (tree *FlexTree) splitInternalNode(n *InternalNode, path *FlexTreePath) {
	tree.splitCount++
	node1ID := n.NodeID // save before AllocInternal may reallocate InternalArena

	node2 := tree.AllocInternal()
	node2.Dirty = true

	tree.NodeCount++
	node1 := tree.GetInternal(node1ID) // refetch: AllocInternal may have moved InternalArena
	ie1 := node1
	ie2 := node2

	count := (node1.Count + 1) / 2
	newBase := ie1.Pivots[count]

	node2.Count = node1.Count - count - 1
	copy(ie2.Pivots[:], ie1.Pivots[count+1:count+1+node2.Count])
	copy(ie2.Children[:], ie1.Children[count+1:count+2+node2.Count])
	copy(ie2.ChildSlotIDs[:], ie1.ChildSlotIDs[count+1:count+2+node2.Count])

	node1.Count = count

	parent, _, ok := tree.ParentNode(path)
	if !ok {

		parent = tree.AllocInternal()
		parent.Dirty = true
		tree.NodeCount++
		tree.Root = parent.NodeID
		node1 = tree.GetInternal(node1ID) // refetch: second AllocInternal may have moved InternalArena

		// node2 is not written to below, so no need to refresh it here.
	}

	ie := parent
	if parent.Count == 0 {
		ie.Children[0].NodeID = node1.NodeID
		ie.Children[0].Shift = 0
		ie.Children[1].NodeID = node2.NodeID
		ie.Children[1].Shift = 0
		ie.Pivots[0] = newBase
		ie.ChildSlotIDs[0] = node1.SlotID
		ie.ChildSlotIDs[1] = node2.SlotID
		parent.Count = 1
	} else {
		pIdx := path.Path[path.Level-1]
		origShift := ie.Children[pIdx].Shift

		copy(ie.Pivots[pIdx+1:], ie.Pivots[pIdx:parent.Count])
		copy(ie.Children[pIdx+2:], ie.Children[pIdx+1:parent.Count+1])
		copy(ie.ChildSlotIDs[pIdx+2:], ie.ChildSlotIDs[pIdx+1:parent.Count+1])

		ie.Children[pIdx+1].NodeID = node2.NodeID
		ie.Children[pIdx+1].Shift = origShift
		ie.Pivots[pIdx] = newBase + uint64(origShift)
		ie.ChildSlotIDs[pIdx+1] = node2.SlotID
		parent.Count++
	}

	parent.Dirty = true
	node1.Dirty = true
	// above already did: node2.Dirty = true

	if parent.isFull() {
		ppath := *path
		ppath.Level--
		tree.splitInternalNode(parent, &ppath)
	}
}

func (tree *FlexTree) splitLeafNode(n *LeafNode, path *FlexTreePath) {
	tree.splitCount++

	node1ID := n.NodeID       // save before AllocLeaf may reallocate LeafArena
	node2 := tree.AllocLeaf() // may reallocate LeafArena, making our n *LeafNode stale
	node2.Dirty = true
	tree.NodeCount++

	node1 := tree.GetLeaf(node1ID) // refetch: valid pointer into current LeafArena
	node2.Prev = node1.NodeID
	node2.Next = node1.Next
	node1.Next = node2.NodeID
	if node2.Next != IllegalID {
		e := tree.GetLeaf(node2.Next)
		e.Prev = node2.NodeID
	}

	le1 := node1
	le2 := node2

	count := (node1.Count + 1) / 2
	node2.Count = node1.Count - count
	copy(le2.Extents[:], le1.Extents[count:count+node2.Count])
	node1.Count = count

	parent, _, _ := tree.ParentNode(path)
	if parent == nil {

		parent = tree.AllocInternal()
		parent.Dirty = true
		tree.NodeCount++
		tree.Root = parent.NodeID

		// no refetch of leaves needed because only
		// the InternalNode arena could have changed
		// due to the above AllocInternal(); i.e we do not need:
		//node1 = tree.GetLeaf(node1ID)
		//node2 = tree.GetLeaf(node2.NodeID)
	}
	ie := parent
	if parent.Count == 0 {
		ie.Children[0].NodeID = node1.NodeID
		ie.Children[0].Shift = 0
		ie.Children[1].NodeID = node2.NodeID
		ie.Children[1].Shift = 0
		ie.Pivots[0] = uint64(le2.Extents[0].Loff)
		ie.ChildSlotIDs[0] = node1.SlotID
		ie.ChildSlotIDs[1] = node2.SlotID
		parent.Count = 1
	} else {
		pIdx, ok := tree.ParentIdx(path)
		if !ok {
			panicf("jea what here? !ok for path = '%#v'", path)
		}
		origShift := ie.Children[pIdx].Shift

		copy(ie.Pivots[pIdx+1:], ie.Pivots[pIdx:parent.Count])
		copy(ie.Children[pIdx+2:], ie.Children[pIdx+1:parent.Count+1])
		copy(ie.ChildSlotIDs[pIdx+2:], ie.ChildSlotIDs[pIdx+1:parent.Count+1])

		ie.Children[pIdx+1].NodeID = node2.NodeID
		ie.Children[pIdx+1].Shift = origShift
		ie.Pivots[pIdx] = uint64(le2.Extents[0].Loff) + uint64(origShift)
		ie.ChildSlotIDs[pIdx+1] = node2.SlotID
		parent.Count++
	}

	parent.Dirty = true
	node1.Dirty = true
	node2.Dirty = true

	if path.Level > 0 {
		tree.rebase(node1.NodeID, path)
		spath := *path
		spath.Path[spath.Level-1]++
		tree.rebase(node2.NodeID, &spath)
	}

	if parent.isFull() {
		ppath := *path
		ppath.Level--
		tree.splitInternalNode(parent, &ppath)
	}
}

func (tree *FlexTree) isExtentSequential(ext *FlexTreeExtent, loff, poff uint64, length uint32) bool {
	extPoff := ext.Poff()
	return extPoff+uint64(ext.Len) == poff &&
		uint64(ext.Loff)+uint64(ext.Len) == loff &&
		ext.Len+length <= tree.MaxExtentSize &&
		(extPoff/uint64(tree.MaxExtentSize)) == (poff/uint64(tree.MaxExtentSize))
}

func (tree *FlexTree) insertToLeafNode(le *LeafNode, loff uint32, poff uint64, length uint32, tag uint16) {

	const FLEXTREE_POFF_MASK = 0xffffffffffff
	t := FlexTreeExtent{
		Loff: loff,
		Len:  length,
	}
	t.SetPoff(poff & FLEXTREE_POFF_MASK)
	t.SetTag(tag)
	target := le.findPosInLeaf(uint64(loff))

	shift := uint32(1)
	if target == le.Count {
		if target > 0 && tag == 0 && tree.isExtentSequential(&le.Extents[target-1], uint64(loff), poff, length) {
			le.Extents[target-1].Len += length
			shift = 0
		} else {
			le.Extents[le.Count] = t
			le.Count++
		}
	} else {
		curr := &le.Extents[target]
		if curr.Loff == loff {
			if target > 0 && tag == 0 && tree.isExtentSequential(&le.Extents[target-1], uint64(loff), poff, length) {
				le.Extents[target-1].Len += length
				shift = 0
			} else {
				copy(le.Extents[target+1:], le.Extents[target:le.Count])
				le.Extents[target] = t
				le.Count++
			}
		} else {
			shift = 2
			so := loff - curr.Loff
			left := FlexTreeExtent{
				Loff:    curr.Loff,
				Len:     so,
				TagPoff: curr.TagPoff,
			}
			right := FlexTreeExtent{
				Loff: curr.Loff + so,
				Len:  curr.Len - so,
			}
			right.SetPoff(curr.Poff() + uint64(so))
			right.SetTag(0)
			copy(le.Extents[target+3:], le.Extents[target+1:le.Count])
			le.Extents[target] = left
			le.Extents[target+2] = right
			le.Extents[target+1] = t
			le.Count += 2
		}
	}

	for i := target + shift; i < le.Count; i++ {
		le.Extents[i].Loff += length
	}
	le.Dirty = true
}

func (tree *FlexTree) insertR(loff, poff uint64, length uint32, tag uint16) int {
	if length == 0 {
		return 0
	}
	if length > tree.MaxExtentSize {
		return -1
	}
	if loff > tree.MaxLoff {
		hlen := loff - tree.MaxLoff
		hloff := tree.MaxLoff
		const FLEXTREE_HOLE = 1 << 47
		hpoff := uint64(FLEXTREE_HOLE)
		for hlen != 0 {
			thlen := tree.MaxExtentSize
			if hlen < uint64(tree.MaxExtentSize) {
				thlen = uint32(hlen)
			}
			if r := tree.insertR(hloff, hpoff, thlen, 0); r != 0 {
				return -1
			}
			hlen -= uint64(thlen)
			hloff += uint64(thlen)
			hpoff += uint64(thlen)
		}
	}

	needPropagate := (loff != tree.MaxLoff)
	path := FlexTreePath{}
	node, oloff := tree.findLeafNode(&path, loff)

	tree.insertToLeafNode(node, uint32(oloff), poff, length, tag)

	if path.Level > 0 {
		tree.rebase(node.NodeID, &path)
	}
	node.Dirty = true
	tree.markPathDirty(&path)
	if needPropagate {
		tree.shiftUpPropagate(&path, int64(length))
	}
	if node.isFull() {
		tree.splitLeafNode(node, &path)
	}
	tree.MaxLoff += uint64(length)
	return 0
}

func (tree *FlexTree) rangeCount(loff, length uint64) uint64 {
	if loff+length > tree.MaxLoff {
		return 0
	}
	tlen := length
	tloff := loff
	ret := uint64(0)

	path := FlexTreePath{}
	node, oloff := tree.findLeafNode(&path, tloff)

	idx := node.findPosInLeaf(oloff)
	if idx == node.Count {
		return 0
	}
	ext := &node.Extents[idx]
	diff := uint32(oloff - uint64(ext.Loff))

	for tlen > 0 {
		remain := uint64(ext.Len - diff)
		step := remain
		if step > tlen {
			step = tlen
		}
		tlen -= step
		tloff += step
		ret++

		diff += uint32(step)
		if diff >= ext.Len {
			idx++
			diff = 0
			if idx >= node.Count {
				if node.Next == IllegalID {
					node = nil
					if tlen > 0 {
						return ret
					}
				} else {
					node = tree.GetLeaf(node.Next)
				}
				idx = 0
				if node == nil && tlen > 0 {
					return ret
				}
			}
			if node != nil {
				ext = &node.Extents[idx]
			}
		}
	}
	return ret
}

func (tree *FlexTree) Close() {}

func (tree *FlexTree) Sync() {}

func (tree *FlexTree) Insert(loff, poff uint64, len uint32) int {
	tree.resetOpCounters()
	return tree.insertR(loff, poff, len, 0)
}

func (tree *FlexTree) recycleLinkedList(removeMe *LeafNode) {
	if tree.Root == removeMe.NodeID {
		panicf("should not be root: %v", removeMe)
	}
	prev := removeMe.Prev
	next := removeMe.Next
	if prev != IllegalID {
		prevNode := tree.GetLeaf(prev)
		prevNode.Next = next
	} else {
		tree.LeafHead = next
	}
	if next != IllegalID {
		nextNode := tree.GetLeaf(next)
		nextNode.Prev = prev
	}
}

func (n *LeafNode) shiftApply(shift int64) {
	for i := uint32(0); i < n.Count; i++ {
		n.Extents[i].Loff = uint32(uint64(n.Extents[i].Loff) + uint64(shift))
	}
	n.Dirty = true
}

func (ie *InternalNode) shiftApply(shift int64) {

	for i := uint32(0); i < ie.Count; i++ {
		ie.Pivots[i] += uint64(shift)
	}
	for i := uint32(0); i < ie.Count+1; i++ {
		ie.Children[i].Shift += shift
	}
	ie.Dirty = true
}

// Original C code author's note: "I'm too lazy to implement
// merging.. just recycle empty nodes and
// it is fine in most cases."
func (tree *FlexTree) recycleNode(nodeID NodeID, path *FlexTreePath) {

	if nodeID.IsIllegal() {
		return
	}

	var le *LeafNode
	var ie *InternalNode
	if nodeID.IsLeaf() {
		le = tree.GetLeaf(nodeID)
		if le.Count != 0 {
			panic("only recycle empty nodes")
		}
	} else {
		ie = tree.GetInternal(nodeID)
		if ie.Count != 0 {
			panic("only recycle empty nodes")
		}
	}
	parent, _, parentExist := tree.ParentNode(path)
	pIdx, _ := tree.ParentIdx(path)

	if tree.Root == nodeID {
		// case 1: root

		// just some assertions here..
		if parentExist {
			panic("root should not have parent")
		}
		if !nodeID.IsLeaf() {
			panic("root should be leaf")
		}
		if le.Count != 0 {
			panic("root should be empty")
		}
		if le.Prev != IllegalID {
			panic("root prev should be illegal")
		}
		if le.Next != IllegalID {
			panic("root next should be illegal")
		}

	} else if parent.Count == 1 {
		// case 2: only one pivot in parent
		sIdx := uint32(1)
		if pIdx == 0 {
			sIdx = 1
		} else {
			sIdx = 0
		}
		sShift := parent.Children[sIdx].Shift
		sNodeID := parent.Children[sIdx].NodeID

		var sNodeLeaf *LeafNode
		var sNodeInternal *InternalNode

		if sNodeID.IsLeaf() {
			sNodeLeaf = tree.GetLeaf(sNodeID)
		} else {
			sNodeInternal = tree.GetInternal(sNodeID)
		}

		if le != nil {
			tree.recycleLinkedList(le)
		}
		tree.FreeNode(nodeID)
		// why would we free the parent here? We
		// are going to access parent.NodeID next anyway!
		//tree.FreeNode(parent.NodeID)

		if tree.Root == parent.NodeID {
			if sNodeLeaf != nil {
				sNodeLeaf.shiftApply(sShift)
			} else {
				sNodeInternal.shiftApply(sShift)
			}
			tree.Root = sNodeID
		} else {
			gparent := tree.GrandparentNode(path)
			gpIdx := tree.GrandparentIdx(path)
			gparent.Children[gpIdx].NodeID = sNodeID
			gparent.Children[gpIdx].Shift += sShift
			gparent.ChildSlotIDs[gpIdx] = tree.NodeSlotID(sNodeID)
			gparent.Dirty = true
		}
		parentExist = false
	} else {
		// case 3: remove one pivot from parent
		if le != nil {
			tree.recycleLinkedList(le)
		}
		tree.FreeNode(nodeID)
		ie := parent
		if pIdx == 0 {
			copy(ie.Pivots[:], ie.Pivots[1:parent.Count])
			copy(ie.Children[:], ie.Children[1:parent.Count+1])
			copy(ie.ChildSlotIDs[:], ie.ChildSlotIDs[1:parent.Count+1])
		} else {
			copy(ie.Pivots[pIdx-1:], ie.Pivots[pIdx:parent.Count])
			copy(ie.Children[pIdx:], ie.Children[pIdx+1:parent.Count+1])
			copy(ie.ChildSlotIDs[pIdx:], ie.ChildSlotIDs[pIdx+1:parent.Count+1])
		}
		parent.Count--
		parent.Dirty = true
	}

	if parentExist && parent.isEmpty() {
		ppath := *path
		ppath.Level--
		tree.recycleNode(parent.NodeID, &ppath)
	}
}

func (tree *FlexTree) Delete(loff, length uint64) int {
	tree.resetOpCounters()
	if loff+length > tree.MaxLoff {
		return -1
	}
	olen := length
	for olen > 0 {
		path := FlexTreePath{}
		node, tloff := tree.findLeafNode(&path, loff)

		target := node.findPosInLeaf(tloff)
		le := node
		ext := &le.Extents[target]

		tlen := uint32(ext.Loff + ext.Len - uint32(tloff))
		if uint64(tlen) > olen {
			tlen = uint32(olen)
		}

		shift := uint32(1)
		if uint64(ext.Loff) == tloff {
			ext.Len -= tlen
			ext.SetPoff(ext.Poff() + uint64(tlen))
			ext.SetTag(0)
			if ext.Len == 0 {
				copy(le.Extents[target:], le.Extents[target+1:node.Count])
				node.Count--
				shift = 0
			}
		} else {
			tmp := uint32(tloff - uint64(ext.Loff))
			if ext.Len-tmp == tlen {
				ext.Len -= tlen
			} else {
				right := FlexTreeExtent{
					Loff: uint32(tloff) + tlen,
					Len:  ext.Len - tmp - tlen,
				}
				right.SetPoff(ext.Poff() + uint64(tmp) + uint64(tlen))
				right.SetTag(0)
				copy(le.Extents[target+2:], le.Extents[target+1:node.Count])
				le.Extents[target].Len = tmp
				le.Extents[target+1] = right
				node.Count++
			}
		}
		for i := target + shift; i < node.Count; i++ {
			le.Extents[i].Loff -= tlen
		}

		node.Dirty = true
		tree.shiftUpPropagate(&path, -int64(tlen))

		olen -= uint64(tlen)
		tree.MaxLoff -= uint64(tlen)

		if node.isFull() {
			tree.splitLeafNode(node, &path)
		} else if node.isEmpty() {
			tree.recycleNode(node.NodeID, &path)
		}
	}
	return 0
}

func (tree *FlexTree) Query(loff, length uint64) *FlextreeQueryResult {
	if loff+length > tree.MaxLoff {
		return nil
	}
	count := tree.rangeCount(loff, length)
	if count == 0 {
		return nil
	}
	rr := &FlextreeQueryResult{
		Loff:  loff,
		Len:   length,
		Count: count,
		V:     make([]Offlen, count),
	}

	path := FlexTreePath{}
	le, oloff := tree.findLeafNode(&path, loff)

	idx := le.findPosInLeaf(oloff)
	diff := uint32(oloff - uint64(le.Extents[idx].Loff))

	i := uint64(0)
	tlen := length
	for tlen > 0 {
		ext := &le.Extents[idx]
		remain := uint64(ext.Len - diff)
		step := remain
		if step > tlen {
			step = tlen
		}

		rr.V[i].Poff = ext.Poff() + uint64(diff)
		rr.V[i].Poff = ext.Poff() + uint64(diff)
		rr.V[i].Len = step
		tlen -= step

		diff += uint32(step)
		if diff >= ext.Len {
			idx++
			diff = 0
			if idx >= le.Count {
				//node = node.Leaf.Next
				if le.Next == IllegalID {
					break // right?
				} else {
					le = tree.GetLeaf(le.Next)
					idx = 0
				}
			}
		}
		i++
	}
	return rr
}

func (tree *FlexTree) GetMaxLoff() uint64 {
	return tree.MaxLoff
}

func (tree *FlexTree) SetTag(loff uint64, tag uint16) int {
	tree.resetOpCounters()
	if loff >= tree.MaxLoff {
		return -1
	}
	path := FlexTreePath{}
	node, oloff := tree.findLeafNode(&path, loff)

	target := node.findPosInLeaf(oloff)
	le := node

	if target == node.Count {
		return -1
	}
	ext := &le.Extents[target]
	if uint64(ext.Loff) == oloff {
		ext.SetTag(tag)
	} else {
		so := uint32(oloff - uint64(ext.Loff))
		copy(le.Extents[target+2:], le.Extents[target+1:node.Count])
		left := FlexTreeExtent{
			Loff:    ext.Loff,
			Len:     so,
			TagPoff: ext.TagPoff,
		}
		right := FlexTreeExtent{
			Loff: ext.Loff + so,
			Len:  ext.Len - so,
		}
		right.SetPoff(ext.Poff() + uint64(so))
		right.SetTag(tag)
		le.Extents[target] = left
		le.Extents[target+1] = right
		node.Count++
	}
	node.Dirty = true
	tree.markPathDirty(&path)
	if node.isFull() {
		tree.splitLeafNode(node, &path)
	}
	return 0
}

func (tree *FlexTree) GetTag(loff uint64) (uint16, int) {
	if loff >= tree.MaxLoff {
		return 0, -1
	}
	path := FlexTreePath{}
	node, oloff := tree.findLeafNode(&path, loff)

	target := node.findPosInLeaf(oloff)

	if target == node.Count {
		return 0, -1
	}
	ext := &node.Extents[target]
	if uint64(ext.Loff) == oloff && ext.Tag() != 0 {
		return ext.Tag(), 0
	}
	return 0, -1
}

func (tree *FlexTree) InsertWTag(loff, poff uint64, len uint32, tag uint16) int {
	tree.resetOpCounters()
	return tree.insertR(loff, poff, len, tag)
}

func (tree *FlexTree) PDelete(loff uint64) int {
	tree.resetOpCounters()
	return tree.Delete(loff, 1)
}

func (tree *FlexTree) PQuery(loff uint64) uint64 {
	if loff >= tree.MaxLoff {
		return ^uint64(0)
	}
	path := FlexTreePath{}
	node, oloff := tree.findLeafNode(&path, loff)

	target := node.findPosInLeaf(oloff)
	if target == node.Count {
		return ^uint64(0)
	}
	ext := &node.Extents[target]
	if uint64(ext.Loff) <= oloff && uint64(ext.Loff)+uint64(ext.Len) > oloff {
		return ext.Poff() + oloff - uint64(ext.Loff)
	}
	return ^uint64(0)
}

func (le *LeafNode) String() (r string) {
	buf := bufPool.Get().(*bytes.Buffer)

	fmt.Fprintf(buf, "\n[Leaf NodeID %v]: Count:%d Dirty:%v\n        ", le.NodeID, le.Count, le.Dirty)

	if le.Count > 0 {
		fmt.Fprintf(buf, "    leaf_entries:\n")
		for i := uint32(0); i < le.Count; i++ {
			fe := &le.Extents[i]
			fmt.Fprintf(buf, "        [extent %02d], loff: %d, poff: %d, len: %d, tag: %d\n",
				i, fe.Loff, fe.Poff(), fe.Len, fe.Tag())
		}
	}
	r = buf.String()
	buf.Reset()
	bufPool.Put(buf)
	return
}

func (ie *InternalNode) StringOnTree(tree *FlexTree) (r string) {
	buf := bufPool.Get().(*bytes.Buffer)

	fmt.Fprintf(buf, "\n[Internal NodeID %v]: Count:%d Dirty:%v\n        ", ie.NodeID, ie.Count, ie.Dirty)
	if ie.Count > 0 {
		fmt.Fprintf(buf, "internal_entries:\n")
		for i := uint32(0); i < ie.Count+1; i++ {
			if i != 0 {
				fmt.Fprintf(buf, "  pivot %d\n", ie.Pivots[i-1])
			}
			var typ string
			if ie.Children[i].NodeID.IsIllegal() {
				typ = "Illegal"
			} else if ie.Children[i].NodeID.IsLeaf() {
				typ = "Leaf   "
			} else {
				typ = "Internal"
			}
			fmt.Fprintf(buf, "  [child %02d] %v NodeID %v shift %d id %d\n",
				i, typ, ie.Children[i].NodeID, ie.Children[i].Shift, ie.ChildSlotIDs[i])
		}
		for i := uint32(0); i < ie.Count+1; i++ {
			if ie.Children[i].NodeID != IllegalID {
				fmt.Fprintf(buf, "%v\n", tree.GetNodeString(ie.Children[i].NodeID))
			}
		}
	}
	r = buf.String()
	buf.Reset()
	bufPool.Put(buf)
	return
}

func (tree *FlexTree) Print() {
	fmt.Printf("%v\n", tree.String())
}

// Low-level APIs for Pos (position)
//
// These APIs should be used with care, as they make strong assumptions
// and the low-level access does not have global view.

// PosGet returns a Pos cursor positioned at loff in the tree.
// If loff is not within any extent, returns an invalid (node==nil) Pos.
func (tree *FlexTree) PosGet(loff uint64) Pos {
	var path FlexTreePath
	le, oloff := tree.findLeafNode(&path, loff)
	target := le.findPosInLeaf(oloff)
	if target < le.Count {
		curr := &le.Extents[target]
		if uint64(curr.Loff) <= oloff && uint64(curr.Loff)+uint64(curr.Len) > oloff {
			diff := uint32(oloff - uint64(curr.Loff))
			return Pos{tree: tree, node: le, Loff: loff, Idx: target, Diff: diff}
		}
	}
	// loff is not within any extent (gap or out of range)
	return Pos{tree: tree, node: nil, Loff: loff, Idx: 0, Diff: 0}
}

// Valid returns true if the Pos points to a valid extent.
func (p *Pos) Valid() bool {
	return p.node != nil
}

// GetLoff returns the current logical offset.
func (p *Pos) GetLoff() uint64 {
	return p.Loff
}

// GetPoff returns the physical offset at the current position.
func (p *Pos) GetPoff() uint64 {
	curr := &p.node.Extents[p.Idx]
	return curr.Poff() + uint64(p.Diff)
}

// GetTag returns the tag of the current extent.
// Returns (0, false) if not at the start of an extent.
func (p *Pos) GetTag() (uint16, bool) {
	if p.Diff != 0 {
		return 0, false
	}
	return p.node.Extents[p.Idx].Tag(), true
}

// Forward advances the position by step bytes.
func (p *Pos) Forward(step uint64) {
	ostep := step
	for ostep > 0 && p.node != nil {
		curr := &p.node.Extents[p.Idx]
		remaining := uint64(curr.Len - p.Diff)
		add := remaining
		if remaining > ostep {
			add = ostep
		}
		ostep -= add
		p.Diff += uint32(add)
		if p.Diff == curr.Len {
			if p.Idx+1 < p.node.Count {
				p.Idx++
			} else {
				nextID := p.node.Next
				if nextID.IsIllegal() {
					p.node = nil
				} else {
					p.node = p.tree.GetLeaf(nextID)
					p.Idx = 0
				}
			}
			p.Diff = 0
		}
		p.Loff += add
	}
}

// ForwardExtent advances the position to the end of the current extent.
func (p *Pos) ForwardExtent() {
	// C: asserted p is leaf node.
	remain := uint64(p.node.Extents[p.Idx].Len - p.Diff)
	p.Forward(remain)
}

// Backward moves the position backward by step bytes.
func (p *Pos) Backward(step uint64) {
	// C: asserted p is leaf node.
	ostep := step
	for ostep > 0 && p.node != nil {
		minus := uint64(p.Diff)
		if minus > ostep {
			minus = ostep
		}
		ostep -= minus
		p.Diff -= uint32(minus)
		if ostep > 0 {
			if p.Idx > 0 {
				p.Idx--
			} else {
				prevID := p.node.Prev
				if prevID.IsIllegal() {
					p.node = nil
				} else {
					p.node = p.tree.GetLeaf(prevID)
					if p.node != nil {
						p.Idx = p.node.Count - 1
					}
				}
			}
			if p.node != nil {
				p.Diff = p.node.Extents[p.Idx].Len
			}
		}
		p.Loff -= minus
	}
}

// Rewind moves the position to the start of the current leaf node.
// Bug fix: C code used i < fp->idx-1 which missed the last extent when idx==1.
func (p *Pos) Rewind() {
	var l uint64
	for i := uint32(0); i < p.Idx; i++ {
		l += uint64(p.node.Extents[i].Len)
	}
	l += uint64(p.Diff)
	p.Loff -= l
	p.Idx = 0
	p.Diff = 0
}

func (tree *FlexTree) Diff(b *FlexTree) (diff string) {
	as := tree.String()
	bs := b.String()
	if as == bs {
		return ""
	}
	return fmt.Sprintf(`
=========  begin diff  ========
A:
%v

----------
B:
%v

=========  end diff  ========
`, as, bs)
}

func (tree *FlexTree) String() string {
	if tree.Root == IllegalID {
		return "(empty goflexspace.Tree)"
	}
	r := fmt.Sprintf("NodeCount: %v, maxLoff: %v  Root (NodeID): %v\n", tree.NodeCount, tree.MaxLoff, tree.Root)

	return r + tree.GetNodeString(tree.Root)
}

/*
func (z *FlexTree) String2() (r string) {
	if z.Root == IllegalID {
		return "(empty goflexspace.Tree)"
	}

	r = "&FlexTree{\n"
	r += fmt.Sprintf("          MaxLoff: %v,\n", z.MaxLoff)
	r += fmt.Sprintf("    MaxExtentSize: %v,\n", z.MaxExtentSize)
	r += fmt.Sprintf("             Path: \"%v\",\n", z.Path)
	r += fmt.Sprintf("        NodeCount: %v,\n", z.NodeCount)
	r += fmt.Sprintf("PersistentVersion: %v,\n", z.PersistentVersion)
	r += fmt.Sprintf("             Root: %v,\n", z.Root)
	r += fmt.Sprintf("         LeafHead: %v,\n", z.LeafHead)
	r += fmt.Sprintf("    InternalArena: %#v,\n", z.InternalArena)
	r += fmt.Sprintf("        LeafArena: %#v,\n", z.LeafArena)
	r += fmt.Sprintf("    FreeInternals: %v,\n", z.FreeInternals)
	r += fmt.Sprintf("       FreeLeaves: %v,\n", z.FreeLeaves)
	r += "}\n"
	return
}
*/
