package ramflextree

// flexspace.go - Go port of flexspace/flexfile.c
// Paper: "Building an Efficient Key-Value Store in a Flexible Address Space"
// Chen, Zhong, Wu (EuroSys 2022)
//
// FlexSpace is a RAM-only address space abstraction on top of FlexTree.
// It provides insert-range, collapse-range, read, write, and GC.
// NOT thread-safe.

import (
	"fmt"
	"sync/atomic"
)

// ======================== Constants ========================

const (
	// the address space that flexspace.go manages
	FLEXSPACE_MAX_OFFSET = 800 << 30 // 800 GB logical address space

	// block config
	FLEXSPACE_BLOCK_BITS  = 22                                           // 4 MB blocks
	FLEXSPACE_BLOCK_SIZE  = 1 << FLEXSPACE_BLOCK_BITS                    // 4_194_304 bytes
	FLEXSPACE_BLOCK_COUNT = FLEXSPACE_MAX_OFFSET >> FLEXSPACE_BLOCK_BITS // 204800 blocks

	FLEXSPACE_MAX_EXTENT_BIT  = 5
	FLEXSPACE_MAX_EXTENT_SIZE = FLEXSPACE_BLOCK_SIZE >> FLEXSPACE_MAX_EXTENT_BIT // 131_072 bytes == 128 KB (1/32)

	// garbage collector
	FLEXSPACE_GC_QUEUE_DEPTH = 8192
	FLEXSPACE_GC_THRESHOLD   = 64 // free blocks below this triggers GC

	// block manager
	FLEXSPACE_BM_BLKDIST_BITS = 16
	FLEXSPACE_BM_BLKDIST_SIZE = (FLEXSPACE_BLOCK_SIZE >> FLEXSPACE_BM_BLKDIST_BITS) + 1 // 65 buckets
)

func init() {
	static_assert(FLEXSPACE_MAX_OFFSET >= (4<<30), "dont manage small space")
	static_assert(FLEXSPACE_BLOCK_BITS < 32, "one u32 to track one block")
	static_assert(FLEXSPACE_BLOCK_COUNT < (1<<48), "no more than 2^48 blocks")

	// io_uring, not doing atm:
	// static_assert(FLEXSPACE_BM_DEPTH <= 512, "no more than 512 blocks in a ring")

	static_assert(FLEXSPACE_GC_THRESHOLD >= (1<<FLEXSPACE_MAX_EXTENT_BIT), "need some blocks to do the final gc")
}

// ======================== GC Context ========================

type gcQueueItem struct {
	node *LeafNode // leaf node containing the extent (stable during GC)
	poff uint64    // original physical offset of the extent
	len  uint32    // extent length in bytes
	idx  uint32    // index within node.Extents
	buf  []byte    // data read from this extent
}

type gcCtx struct {
	loff               uint64
	queue              [FLEXSPACE_GC_QUEUE_DEPTH]gcQueueItem
	count              uint32
	writeBetweenStages bool
}

// ======================== Block Manager ========================

type blockManager struct {
	file       *FlexSpace
	blkid      uint64                // current block being written
	blkoff     uint64                // offset within current block (0..FLEXSPACE_BLOCK_SIZE)
	blocks     *omap[uint64, []byte] // all blocks in RAM
	blkusage   []uint32              // [FLEXSPACE_BLOCK_COUNT] bytes used per block
	blkdist    []uint64              // [FLEXSPACE_BM_BLKDIST_SIZE] usage histogram buckets
	freeBlocks uint64                // count of completely empty blocks
}

func (bm *blockManager) offset() uint64 {
	return bm.blkid*FLEXSPACE_BLOCK_SIZE + bm.blkoff
}

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

// findEmptyBlock searches for a block with zero usage.
// Prefers recycled blocks (before fromBlkid) to avoid growing memory.
// If isGC is false, may call GC first to free up blocks.
func (bm *blockManager) findEmptyBlock(fromBlkid uint64, isGC bool) uint64 {
	if !isGC {
		bm.file.GC()
	}
	// Phase 1: prefer recycled blocks - search from block 0 up to fromBlkid
	for i := uint64(0); i < fromBlkid; i++ {
		if bm.blkusage[i] == 0 {
			return i
		}
	}
	// Phase 2: search forward from fromBlkid (extends memory if needed)
	for i := fromBlkid; i < FLEXSPACE_BLOCK_COUNT; i++ {
		if bm.blkusage[i] == 0 {
			return i
		}
	}
	panic("flexspace: cannot find any empty block to write")
}

// blockFit returns true if size bytes fit in the current block without crossing
// the block boundary.
func (bm *blockManager) blockFit(size uint64) bool {
	return (FLEXSPACE_BLOCK_SIZE - bm.blkoff) >= size
}

// write copies up to min(size, FLEXSPACE_MAX_EXTENT_SIZE, blockRemain) bytes from buf
// into the current block's in-memory buffer, updates usage, and moves to the
// next block if the buffer is full.
// Returns the number of bytes actually written.
func (bm *blockManager) write(buf []byte, size uint64, isGC bool) uint64 {
	remain := uint64(FLEXSPACE_BLOCK_SIZE) - bm.blkoff
	osize := size
	if osize > remain {
		osize = remain
	}
	if osize > FLEXSPACE_MAX_EXTENT_SIZE {
		osize = FLEXSPACE_MAX_EXTENT_SIZE
	}
	blk, _ := bm.blocks.get2(bm.blkid)
	copy(blk[bm.blkoff:], buf[:osize])
	bm.blkoff += osize
	bm.updateBlkUsage(bm.blkid, int32(osize))
	if bm.blkoff == FLEXSPACE_BLOCK_SIZE {
		bm.nextBlock(isGC)
	}
	return osize
}

// nextBlock switches to a new empty block when the current block is full.
func (bm *blockManager) nextBlock(isGC bool) {
	oldBlkid := bm.blkid
	newBlkid := bm.findEmptyBlock(oldBlkid, isGC)
	if oldBlkid == newBlkid {
		return // current block is already empty (blkoff==0), nothing to do
	}
	bm.blkid = newBlkid
	bm.blkoff = 0
	// Ensure new block exists in omap
	if _, found := bm.blocks.get2(newBlkid); !found {
		bm.blocks.set(newBlkid, make([]byte, FLEXSPACE_BLOCK_SIZE))
	}
}

// flush is a no-op in RAM-only mode.
func (bm *blockManager) flush(isGC bool) {
	// no-op in RAM-only mode
}

// read reads from the in-memory block at the given physical offset.
// Returns number of bytes served (0 if the block is not allocated).
func (bm *blockManager) read(dst []byte, poff, size uint64) uint64 {
	blkid := poff >> FLEXSPACE_BLOCK_BITS
	blk, found := bm.blocks.get2(blkid)
	if !found {
		return 0
	}
	blkoff := poff & (FLEXSPACE_BLOCK_SIZE - 1)
	remain := uint64(FLEXSPACE_BLOCK_SIZE) - blkoff
	osize := size
	if osize > remain {
		osize = remain
	}
	copy(dst[:osize], blk[blkoff:])
	return osize
}

// bmCreate allocates a new block manager for ff.
func bmCreate(ff *FlexSpace) *blockManager {
	bm := &blockManager{
		file:       ff,
		blocks:     newOmap[uint64, []byte](),
		blkusage:   make([]uint32, FLEXSPACE_BLOCK_COUNT),
		blkdist:    make([]uint64, FLEXSPACE_BM_BLKDIST_SIZE),
		freeBlocks: FLEXSPACE_BLOCK_COUNT,
	}
	bm.blkdist[0] = FLEXSPACE_BLOCK_COUNT // all blocks start in bucket 0 (empty)
	// Allocate block 0
	bm.blocks.set(0, make([]byte, FLEXSPACE_BLOCK_SIZE))
	return bm
}

// bmInit reconstructs the block manager state by scanning the FlexTree extents.
func bmInit(bm *blockManager, tree *FlexTree) {
	bm.blkdist[0] = FLEXSPACE_BLOCK_COUNT
	bm.freeBlocks = FLEXSPACE_BLOCK_COUNT
	maxBlkid := uint64(0)

	// Track the highest physical end offset within each block so we
	// can resume writing at the tail of a partially-filled block
	// instead of always allocating a new empty one.
	var blkHighWater [FLEXSPACE_BLOCK_COUNT]uint64

	nodeID := tree.LeafHead
	for !nodeID.IsIllegal() {
		le := tree.GetLeaf(nodeID)
		for i := uint32(0); i < le.Count; i++ {
			ext := &le.Extents[i]
			if ext.IsHole() {
				continue // holes have no physical storage
			}
			// Handle extents that may span multiple blocks (legacy data).
			poff := ext.Address() // 47-bit physical address (no hole flag)
			remaining := uint64(ext.Len)
			for remaining > 0 {
				blkid := poff >> FLEXSPACE_BLOCK_BITS
				blkEnd := (blkid + 1) << FLEXSPACE_BLOCK_BITS
				inBlock := blkEnd - poff
				if inBlock > remaining {
					inBlock = remaining
				}
				if blkid > maxBlkid {
					maxBlkid = blkid
				}
				bm.updateBlkUsage(blkid, int32(inBlock))
				endInBlock := (poff + inBlock) - (blkid << FLEXSPACE_BLOCK_BITS)
				if endInBlock > blkHighWater[blkid] {
					blkHighWater[blkid] = endInBlock
				}
				poff += inBlock
				remaining -= inBlock
			}
		}
		nodeID = le.Next
	}

	// Find the best block to continue writing in: prefer a partially-filled
	// block with the most remaining space over allocating a new empty one.
	bestBlk := uint64(0)
	bestFree := uint64(0)
	foundPartial := false
	for bid := uint64(0); bid <= maxBlkid; bid++ {
		hw := blkHighWater[bid]
		if hw > 0 && hw < FLEXSPACE_BLOCK_SIZE {
			free := FLEXSPACE_BLOCK_SIZE - hw
			if free > bestFree {
				bestFree = free
				bestBlk = bid
				foundPartial = true
			}
		}
	}

	if foundPartial {
		bm.blkid = bestBlk
		bm.blkoff = blkHighWater[bestBlk]
		// Ensure the block exists in the omap
		if _, found := bm.blocks.get2(bestBlk); !found {
			bm.blocks.set(bestBlk, make([]byte, FLEXSPACE_BLOCK_SIZE))
		}
	} else {
		// isGC=true to avoid recursive GC call during initialization
		bm.blkid = bm.findEmptyBlock(maxBlkid, true)
		bm.blkoff = 0
	}
}

// ======================== FlexSpace ========================

// FlexSpace is a RAM-only address space providing
// insert-range, collapse-range, read, write, and GC operations.
// Corresponds to struct flexfile in flexfile.h/flexfile.c.
//
// FlexSpace itself does no internal locking. Clients must
// guarantee single user at a time; e.g. via FlexDB.ffMu.
//
// Here's how the block management works:
//
// The blocks are stored in an omap as 4 MB byte slices. The block
// manager (blockManager) appends data sequentially:
//
// 1. Writing: When FlexDB flushes a memtable, it calls
// putPassthrough which calls ff.Insert(). Insert copies the
// kv128-encoded bytes into the block manager's current block at
// the current offset. If the current block
// can't fit the data, nextBlock() moves to the next empty block.
//
// 2. Mapping: Each chunk written gets a physical offset
// (poff = blkid * 4MB + blkoff). That poff is inserted into the
// FlexTree, which maps logical offset -> physical offset. So
// the FlexTree is the indirection layer that lets you read
// kv128 intervals by logical position even though they're
// scattered across 4 MB blocks in memory.
//
// 3. GC: The block manager tracks per-block usage (blkusage[]).
// When data is deleted (Collapse), the block's usage count
// decreases. GC reclaims blocks with low utilization by
// rewriting their live extents into the current write
// block, then freeing the old block.
type FlexSpace struct {
	Path string
	tree *FlexTree
	bm   *blockManager
	gc   gcCtx

	// Sequential IO cache (replaces C thread-local seqio_fp/seqio_epoch)
	seqioPos    Pos
	seqioEpoch  uint64
	globalEpoch uint64

	// Write-byte counters (accessed atomically)
	KV128BytesWritten int64 // total bytes written to blocks

	// Debug counters for bloat investigation (accessed atomically)
	updateCount        int64
	updateGarbageBytes int64
	insertCount        int64
	insertBytes        int64
}

// NewFlexSpace creates a new RAM-only FlexSpace.
func NewFlexSpace() *FlexSpace {
	tree := NewFlexTree()
	ff := &FlexSpace{
		tree: tree,
	}
	tree.MaxExtentSize = FLEXSPACE_BLOCK_SIZE // prevent cross-block extent merging
	ff.bm = bmCreate(ff)

	// Initialize GC context
	ff.gc.loff = 0
	ff.gc.count = 0
	ff.gc.writeBetweenStages = false

	ff.globalEpoch = 1
	ff.seqioEpoch = 0

	return ff
}

// Close shuts down the FlexSpace.
func (ff *FlexSpace) Close() int64 {
	return 0
}

// truncateTrailingBlocks is a no-op in RAM-only mode.
// Memory for empty blocks is reclaimed when GC frees them.
func (ff *FlexSpace) truncateTrailingBlocks() {
	// no-op in RAM-only mode
}

var debugTruncate = false

// syncR is a no-op in RAM-only mode.
func (ff *FlexSpace) syncR(isGC bool) {
	// no-op in RAM-only mode
}

// Sync is a no-op in RAM-only mode.
func (ff *FlexSpace) Sync() {
	// no-op in RAM-only mode
}

// Size returns the current logical size of the FlexSpace.
func (ff *FlexSpace) Size() uint64 {
	return ff.tree.MaxLoff
}

// ======================== Read ========================

// Read reads len bytes from loff into buf.
// Returns bytes read or -1 on error.
func (ff *FlexSpace) Read(buf []byte, loff, length uint64) (int, error) {
	return ff.readR(buf, loff, length, nil)
}

// ReadFragmentation reads and also returns the number of physical extents (frag).
func (ff *FlexSpace) ReadFragmentation(buf []byte, loff, length uint64) (int, uint64, error) {
	var frag uint64
	n, err := ff.readR(buf, loff, length, &frag)
	return n, frag, err
}

func (ff *FlexSpace) readR(buf []byte, loff, length uint64, frag *uint64) (int, error) {
	if loff+length > ff.tree.MaxLoff {
		return -1, fmt.Errorf("flexspace: read out of range loff=%d len=%d maxloff=%d", loff, length, ff.tree.MaxLoff)
	}

	// Sequential IO cache: reuse pos if epoch matches and loff matches
	var fp *Pos
	if ff.globalEpoch != ff.seqioEpoch || loff != ff.seqioPos.GetLoff() {
		ff.seqioEpoch = ff.globalEpoch
		ff.seqioPos = ff.tree.PosGet(loff)
	}
	fp = &ff.seqioPos

	if !fp.Valid() {
		return -1, fmt.Errorf("flexspace: read at loff=%d: no extent (maxloff=%d)", loff, ff.tree.MaxLoff)
	}

	b := buf
	tlen := length
	count := uint64(0)
	for tlen > 0 {
		count++
		if fp.node == nil {
			return -1, fmt.Errorf("flexspace: read underflow at loff=%d remaining=%d of %d", loff, tlen, length)
		}
		ext := &fp.node.Extents[fp.Idx]
		slen := uint64(ext.Len - fp.Diff)
		if slen > tlen {
			slen = tlen
		}
		poff := ext.Address() + uint64(fp.Diff)

		// Read from in-memory block
		r := ff.bm.read(b, poff, slen)
		if r == 0 {
			return -1, fmt.Errorf("flexspace: block not found for poff=%d", poff)
		}
		fp.Forward(slen)
		b = b[slen:]
		tlen -= slen
	}
	if frag != nil {
		*frag = count
	}
	return int(length), nil
}

// ======================== Insert ========================

// insertR writes len bytes at loff, shifting subsequent data.
func (ff *FlexSpace) insertR(buf []byte, loff, length uint64, commit bool) (int, error) {
	if loff > ff.tree.MaxLoff {
		return -1, fmt.Errorf("flexspace: insert loff=%d > maxloff=%d (no holes)", loff, ff.tree.MaxLoff)
	}
	ff.gc.writeBetweenStages = true
	ff.globalEpoch++

	b := buf
	olen := length
	oloff := loff

	// Ensure the first write fits within a block
	if !ff.bm.blockFit(olen) {
		ff.bm.nextBlock(false)
	}

	for olen > 0 {
		poff := ff.bm.offset()
		tlen := ff.bm.write(b, olen, false)
		ff.tree.Insert(oloff, poff, uint32(tlen))
		oloff += tlen
		olen -= tlen
		b = b[tlen:]
	}
	return int(length), nil
}

// Insert inserts len bytes at loff, shifting all subsequent extents right.
func (ff *FlexSpace) Insert(buf []byte, loff, length uint64) (int, error) {
	atomic.AddInt64(&ff.insertCount, 1)
	atomic.AddInt64(&ff.insertBytes, int64(length))
	return ff.insertR(buf, loff, length, true)
}

// ======================== Collapse ========================

// collapseR deletes len bytes starting at loff, shifting subsequent data left.
func (ff *FlexSpace) collapseR(loff, length uint64, commit bool) error {
	if loff+length > ff.tree.MaxLoff {
		return fmt.Errorf("flexspace: collapse loff=%d len=%d > maxloff=%d", loff, length, ff.tree.MaxLoff)
	}
	ff.gc.writeBetweenStages = true
	ff.globalEpoch++

	// Query extents being collapsed to update block usage.
	// Extents may span multiple blocks (FlexTree merges sequential extents),
	// so split the usage decrement across block boundaries.
	rr := ff.tree.Query(loff, length)
	if rr != nil {
		for i := uint64(0); i < rr.Count; i++ {
			poff := rr.V[i].Poff
			remaining := uint64(rr.V[i].Len)
			for remaining > 0 {
				blkid := poff >> FLEXSPACE_BLOCK_BITS
				blkEnd := (blkid + 1) << FLEXSPACE_BLOCK_BITS
				inBlock := blkEnd - poff
				if inBlock > remaining {
					inBlock = remaining
				}
				ff.bm.updateBlkUsage(blkid, -int32(inBlock))
				poff += inBlock
				remaining -= inBlock
			}
		}
	}
	ff.tree.Delete(loff, length)
	return nil
}

// Collapse deletes len bytes at loff, shifting subsequent extents left.
func (ff *FlexSpace) Collapse(loff, length uint64) error {
	return ff.collapseR(loff, length, true)
}

// ======================== Write ========================

// Write overwrites len bytes at loff. Semantics:
//   - if loff == size: equivalent to Insert (append)
//   - if loff + len > size: truncate tail then Insert
//   - otherwise: Update (collapse old + insert new, same size)
func (ff *FlexSpace) Write(buf []byte, loff, length uint64) (int, error) {
	size := ff.Size()
	if loff > size {
		return -1, fmt.Errorf("flexspace: write loff=%d > size=%d", loff, size)
	}
	if loff == size {
		return ff.Insert(buf, loff, length)
	}
	if loff+length > size {
		if err := ff.Collapse(loff, size-loff); err != nil {
			return -1, err
		}
		return ff.Insert(buf, loff, length)
	}
	return ff.Update(buf, loff, length, length)
}

// ======================== SetTag / GetTag ========================

func (ff *FlexSpace) setTagR(loff uint64, tag uint16, commit bool) error {
	ff.gc.writeBetweenStages = true
	ff.globalEpoch++
	r := ff.tree.SetTag(loff, tag)
	if r != 0 {
		return fmt.Errorf("flexspace: SetTag loff=%d: not found", loff)
	}
	return nil
}

// SetTag sets the 16-bit tag on the extent at loff.
func (ff *FlexSpace) SetTag(loff uint64, tag uint16) error {
	return ff.setTagR(loff, tag, true)
}

// GetTag returns the tag at loff, or an error if not found.
func (ff *FlexSpace) GetTag(loff uint64) (uint16, error) {
	tag, r := ff.tree.GetTag(loff)
	if r != 0 {
		return 0, fmt.Errorf("flexspace: GetTag loff=%d: not found", loff)
	}
	return tag, nil
}

// ======================== Update ========================

// Update atomically replaces olen bytes at loff with len bytes from buf.
// The tag (if any) is preserved.
func (ff *FlexSpace) Update(buf []byte, loff, length, olen uint64) (int, error) {
	if loff+olen > ff.tree.MaxLoff {
		return -1, fmt.Errorf("flexspace: update out of range")
	}
	atomic.AddInt64(&ff.updateCount, 1)
	atomic.AddInt64(&ff.updateGarbageBytes, int64(olen))
	// Preserve tag
	tag, _ := ff.GetTag(loff)

	if err := ff.collapseR(loff, olen, false); err != nil {
		return -1, err
	}
	n, err := ff.insertR(buf, loff, length, false)
	if err != nil {
		return -1, err
	}
	if tag != 0 {
		_ = ff.setTagR(loff, tag, false)
	}
	return n, nil
}

// ======================== Overwrite ========================

// Overwrite writes buf directly to the physical location backing the extent
// at loff, without mutating the FlexTree.
// The extent at loff must already exist and have length == len(buf).
// This creates zero garbage - the same physical blocks are reused in-place.
func (ff *FlexSpace) Overwrite(buf []byte, loff uint64, length uint64) error {
	if length == 0 {
		return nil
	}
	if loff+length > ff.tree.MaxLoff {
		return fmt.Errorf("flexspace: overwrite out of range loff=%d len=%d maxloff=%d", loff, length, ff.tree.MaxLoff)
	}

	fp := ff.tree.PosGet(loff)
	if !fp.Valid() {
		return fmt.Errorf("flexspace: overwrite at loff=%d len=%d: no extent (maxloff=%d)", loff, length, ff.tree.MaxLoff)
	}

	b := buf
	remain := length
	for remain > 0 {
		if fp.node == nil {
			return fmt.Errorf("flexspace: overwrite underflow at loff=%d remaining=%d of %d", loff, remain, length)
		}
		ext := &fp.node.Extents[fp.Idx]
		slen := uint64(ext.Len - fp.Diff)
		if slen > remain {
			slen = remain
		}
		poff := ext.Address() + uint64(fp.Diff)

		// Write directly to the in-memory block
		blkid := poff >> FLEXSPACE_BLOCK_BITS
		blk, found := ff.bm.blocks.get2(blkid)
		if !found {
			return fmt.Errorf("flexspace: overwrite block not found for poff=%d", poff)
		}
		blkoff := poff & (FLEXSPACE_BLOCK_SIZE - 1)
		copy(blk[blkoff:], b[:slen])

		atomic.AddInt64(&ff.KV128BytesWritten, int64(slen))
		fp.Forward(slen)
		b = b[slen:]
		remain -= slen
	}
	return nil
}

// ======================== Defrag ========================

// Defrag rewrites the len bytes at loff as a fresh contiguous physical extent.
func (ff *FlexSpace) Defrag(buf []byte, loff, length uint64) error {
	n, err := ff.Update(buf, loff, length, length)
	if err != nil {
		return err
	}
	if uint64(n) != length {
		return fmt.Errorf("flexspace: defrag partial write")
	}
	return nil
}

// ======================== Fallocate / Ftruncate ========================

// Fallocate pre-allocates size bytes starting at loff by inserting zero data.
func (ff *FlexSpace) Fallocate(loff, size uint64) error {
	remain := size
	off := uint64(0)
	buf := make([]byte, FLEXSPACE_MAX_EXTENT_SIZE)
	for remain > 0 {
		tsize := uint64(FLEXSPACE_MAX_EXTENT_SIZE)
		if tsize > remain {
			tsize = remain
		}
		n, err := ff.Insert(buf[:tsize], loff+off, tsize)
		if err != nil {
			return err
		}
		off += uint64(n)
		remain -= uint64(n)
	}
	return nil
}

// Ftruncate truncates the FlexSpace to size bytes by collapsing the tail.
func (ff *FlexSpace) Ftruncate(size uint64) error {
	fsize := ff.Size()
	if fsize <= size {
		return nil
	}
	return ff.Collapse(size, fsize-size)
}

// ======================== Handler API ========================

// FlexSpaceHandler is a stateful read-only cursor over a FlexSpace.
// Corresponds to struct flexfile_handler.
type FlexSpaceHandler struct {
	file *FlexSpace
	fp   Pos // cached FlexTree position
}

// GetHandler returns a handler positioned at loff.
func (ff *FlexSpace) GetHandler(loff uint64) FlexSpaceHandler {
	return FlexSpaceHandler{
		file: ff,
		fp:   ff.tree.PosGet(loff),
	}
}

// Read reads len bytes from the handler's current position into buf.
func (fh *FlexSpaceHandler) Read(buf []byte, length uint64) (int, error) {
	tlen := length
	b := buf
	tfh := *fh // local copy for advancing
	for tlen > 0 {
		if !tfh.fp.Valid() {
			return -1, fmt.Errorf("flexspace: handler read past end")
		}
		ext := &tfh.fp.node.Extents[tfh.fp.Idx]
		slen := uint64(ext.Len - tfh.fp.Diff)
		if slen > tlen {
			slen = tlen
		}
		poff := ext.Address() + uint64(tfh.fp.Diff)
		ff := tfh.file

		r := ff.bm.read(b, poff, slen)
		if r == 0 {
			return -1, fmt.Errorf("flexspace: handler read block not found for poff=%d", poff)
		}
		b = b[slen:]
		tlen -= slen
		tfh.fp.Forward(slen)
	}
	return int(length), nil
}

// Forward advances the handler's position by step bytes.
func (fh *FlexSpaceHandler) Forward(step uint64) {
	fh.fp.Forward(step)
}

// ForwardExtent advances the handler to the start of the next extent.
func (fh *FlexSpaceHandler) ForwardExtent() {
	fh.fp.ForwardExtent()
}

// Backward moves the handler's position backward by step bytes.
func (fh *FlexSpaceHandler) Backward(step uint64) {
	fh.fp.Backward(step)
}

// Valid returns true if the handler points to a valid extent.
func (fh *FlexSpaceHandler) Valid() bool {
	return fh.fp.Valid()
}

// Loff returns the current logical offset.
func (fh *FlexSpaceHandler) Loff() uint64 {
	return fh.fp.GetLoff()
}

// Poff returns the current physical offset.
func (fh *FlexSpaceHandler) Poff() uint64 {
	return fh.fp.GetPoff()
}

// GetTag returns the tag at the handler's current position.
func (fh *FlexSpaceHandler) GetTag() (uint16, error) {
	tag, ok := fh.fp.GetTag()
	if !ok {
		return 0, fmt.Errorf("flexspace: handler GetTag: not at extent start")
	}
	return tag, nil
}

// ======================== GC ========================

// garbageMetrics scans the block usage array and returns:
//   - totalLiveBytes: sum of live bytes across all non-empty blocks
//   - totalGarbageBytes: sum of dead bytes across all non-empty blocks
//   - blocksInUse: count of non-empty blocks
//   - lowUtilBlocks: count of non-empty blocks with utilization below threshold
//
// lowUtilPct is a fraction in (0,1]; e.g. 0.25 means blocks with < 25% utilization.
func (ff *FlexSpace) garbageMetrics(lowUtilPct float64) (totalLiveBytes, totalGarbageBytes, blocksInUse, lowUtilBlocks int64) {
	threshold := uint32(float64(FLEXSPACE_BLOCK_SIZE) * lowUtilPct)
	for i := uint64(0); i < FLEXSPACE_BLOCK_COUNT; i++ {
		usage := ff.bm.blkusage[i]
		if usage == 0 {
			continue // empty block, already free
		}
		blocksInUse++
		totalLiveBytes += int64(usage)
		totalGarbageBytes += int64(FLEXSPACE_BLOCK_SIZE) - int64(usage)
		if usage < threshold {
			lowUtilBlocks++
		}
	}
	return
}

// gcNeeded returns true if free blocks are below the threshold.
func (ff *FlexSpace) gcNeeded() bool {
	return ff.bm.freeBlocks < FLEXSPACE_GC_THRESHOLD
}

// gcFindTargets marks blocks in bitmap whose usage is <= threshold.
// Returns the count of blocks marked.
func (ff *FlexSpace) gcFindTargets(bitmap []bool, histBitmap []bool, round int, nfblks uint64) uint64 {
	// Clear bitmap
	for i := range bitmap {
		bitmap[i] = false
	}
	var threshold uint64
	if round == 0 {
		threshold = FLEXSPACE_BLOCK_SIZE - 2*FLEXSPACE_MAX_EXTENT_SIZE
	} else {
		threshold = uint64(FLEXSPACE_BLOCK_SIZE) >> round
	}
	var onfblks uint64
	if round == 0 {
		onfblks = 1 << FLEXSPACE_MAX_EXTENT_BIT // 32
	} else {
		onfblks = (nfblks * uint64((1<<round)-1)) >> round
	}

	count := uint64(0)
	for i := uint64(0); i < FLEXSPACE_BLOCK_COUNT && count < onfblks; i++ {
		usage := uint64(ff.bm.blkusage[i])
		if usage != 0 && usage <= threshold && !bitmap[i] && !histBitmap[i] {
			bitmap[i] = true
			count++
		}
	}
	if round == 0 && count != onfblks {
		// The final round must find exactly 32 blocks or we log and continue
		// (C code panics here; we continue to avoid crashing)
		vv("flexspace: gc final round found %v / %v target blocks", count, onfblks)
	}
	return count
}

// gcAsyncPrepare scans one leaf node's extents and buffers those in target blocks.
func (ff *FlexSpace) gcAsyncPrepare(bitmap []bool) {
	if ff.gc.count >= FLEXSPACE_GC_QUEUE_DEPTH {
		return
	}
	if ff.gc.loff >= ff.Size() {
		ff.gc.loff = 0
	}
	// If writes happened between stages, discard the stale queue
	if ff.gc.writeBetweenStages && ff.gc.count > 0 {
		for i := uint32(0); i < ff.gc.count; i++ {
			ff.gc.queue[i].buf = nil
		}
		ff.gc.count = 0
		ff.gc.loff = 0
	}
	ff.gc.writeBetweenStages = false

	fp := ff.tree.PosGet(ff.gc.loff)
	if !fp.Valid() {
		return
	}
	// Rewind to start of current leaf node
	fp.Rewind()
	ff.gc.loff = fp.GetLoff()

	le := fp.node
	for i := uint32(0); i < le.Count; i++ {
		ext := &le.Extents[i]
		poff := ext.Address()
		length := uint32(ext.Len)
		blkid := poff >> FLEXSPACE_BLOCK_BITS

		ff.gc.loff += uint64(length)

		// Skip extents that cross block boundaries (legacy merged extents).
		// They'll be cleaned up naturally as their individual blocks are GC'd.
		if blkid != (poff+uint64(length)-1)>>FLEXSPACE_BLOCK_BITS {
			continue
		}
		if !bitmap[blkid] {
			continue // not a GC target block
		}

		idx := ff.gc.count
		ff.gc.count++
		ff.gc.queue[idx].node = le
		ff.gc.queue[idx].poff = poff
		ff.gc.queue[idx].len = length
		ff.gc.queue[idx].idx = i

		// Read the data now (before we move it)
		buf := make([]byte, length)
		r := ff.bm.read(buf, poff, uint64(length))
		if r == 0 {
			panic(fmt.Sprintf("flexspace: gc read block not found for poff=%d", poff))
		}
		ff.gc.queue[idx].buf = buf

		if ff.gc.count >= FLEXSPACE_GC_QUEUE_DEPTH {
			break
		}
	}
	if ff.gc.loff >= ff.Size() {
		ff.gc.loff = 0 // wrap: we've scanned the whole address space
	}
}

// gcAsyncExecute writes buffered GC items to new locations.
// Returns the count of blocks that became completely free.
func (ff *FlexSpace) gcAsyncExecute(histBitmap []bool, commit bool) int {
	if ff.gc.count == 0 {
		return 0
	}
	ff.gc.writeBetweenStages = false
	rblocks := 0

	for i := uint32(0); i < ff.gc.count; i++ {
		item := &ff.gc.queue[i]
		opoff := item.poff
		length := item.len

		// Write to a new block location
		if !ff.bm.blockFit(uint64(length)) {
			ff.bm.nextBlock(true /* isGC */)
		}
		newPoff := ff.bm.offset()
		newBlkid := ff.bm.blkid
		ff.bm.write(item.buf, uint64(length), true /* isGC */)
		histBitmap[newBlkid] = true // blacklist new GC block from future rounds

		// Decrement old block usage (handle cross-block extents from legacy data)
		{
			p := opoff
			rem := uint64(length)
			for rem > 0 {
				oblkid := p >> FLEXSPACE_BLOCK_BITS
				blkEnd := (oblkid + 1) << FLEXSPACE_BLOCK_BITS
				inBlock := blkEnd - p
				if inBlock > rem {
					inBlock = rem
				}
				newUsage := ff.bm.updateBlkUsage(oblkid, -int32(inBlock))
				if newUsage == 0 {
					rblocks++
				}
				p += inBlock
				rem -= inBlock
			}
		}

		// Update the leaf node in-place
		item.node.Extents[item.idx].SetPoff(newPoff)
		item.node.Dirty = true

		item.buf = nil
	}

	// Propagate dirty flags up: if any child is dirty, mark the parent dirty.
	// This is O(tree nodes) but GC is already an expensive operation.
	ff.tree.propagateDirtyUp(ff.tree.Root)

	ff.gc.count = 0

	if commit {
		ff.syncR(true)
	}
	return rblocks
}

// gcAsyncQueueFull returns true if the GC queue is full.
func (ff *FlexSpace) gcAsyncQueueFull() bool {
	return ff.gc.count >= FLEXSPACE_GC_QUEUE_DEPTH
}

// GC runs garbage collection if free blocks are below the threshold.
// Iterates up to 4 rounds of decreasing aggressiveness.
func (ff *FlexSpace) GC() {
	if !ff.gcNeeded() {
		return
	}
	vv("FlexSpace.GC running")

	// Discard any pending GC queue (we're starting fresh)
	for i := uint32(0); i < ff.gc.count; i++ {
		ff.gc.queue[i].buf = nil
	}
	ff.gc.count = 0
	ff.gc.loff = 0

	bitmap := make([]bool, FLEXSPACE_BLOCK_COUNT)
	histBitmap := make([]bool, FLEXSPACE_BLOCK_COUNT)

	// Four rounds from most conservative (round=3) to most aggressive (round=0).
	// Use int to avoid unsigned-wrap infinite loop (C had u8 i=3; i>=0 bug).
	for _, round := range []int{3, 2, 1, 0} {
		for ff.gcNeeded() &&
			ff.gcFindTargets(bitmap, histBitmap, round, ff.bm.freeBlocks) > 1 {

			ff.gcAsyncPrepare(bitmap)
			for ff.gc.loff != 0 {
				ff.gcAsyncPrepare(bitmap)
				if ff.gcAsyncQueueFull() {
					ff.gcAsyncExecute(histBitmap, false)
				}
			}
			ff.gcAsyncExecute(histBitmap, true)
		}
	}

	if ff.gcNeeded() {
		vv("flexspace: GC failed to reclaim enough blocks (free=%d threshold=%d)",
			ff.bm.freeBlocks, uint64(FLEXSPACE_GC_THRESHOLD))
	}
}
