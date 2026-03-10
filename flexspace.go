package yogadb

// flexspace.go — Go port of flexspace/flexfile.c
// Paper: "Building an Efficient Key-Value Store in a Flexible Address Space"
// Chen, Zhong, Wu (EuroSys 2022)
//
// FlexSpace is a log-structured file abstraction on top of FlexTree.
// It provides insert-range, collapse-range, read, write, sync, and GC.
// NOT thread-safe.

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	//"os"
	"path/filepath"
	"sync/atomic"

	"github.com/glycerine/vfs"
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

	// logical logging
	FLEXSPACE_LOG_MEM_CAP  = 8 << 20 // 8 MB in-memory log buffer
	FLEXSPACE_LOG_MAX_SIZE = 2 << 30 // 2 GB max on-disk log

	// garbage collector
	FLEXSPACE_GC_QUEUE_DEPTH = 8192
	FLEXSPACE_GC_THRESHOLD   = 64 // free blocks below this triggers GC

	// block manager
	FLEXSPACE_BM_BLKDIST_BITS = 16
	FLEXSPACE_BM_BLKDIST_SIZE = (FLEXSPACE_BLOCK_SIZE >> FLEXSPACE_BM_BLKDIST_BITS) + 1 // 65 buckets

	flexLogVersionSize = 12 // 8-byte version + 4-byte CRC32C
	flexLogEntrySize   = 20 // 16-byte packed entry + 4-byte CRC32C
)

func init() {
	static_assert(FLEXSPACE_MAX_OFFSET >= (4<<30), "dont manage small space")
	static_assert(FLEXSPACE_BLOCK_BITS < 32, "one u32 to track one block")
	static_assert(FLEXSPACE_BLOCK_COUNT < (1<<48), "no more than 2^48 blocks")

	// io_uring, not doing atm:
	// static_assert(FLEXSPACE_BM_DEPTH <= 512, "no more than 512 blocks in a ring")

	static_assert(FLEXSPACE_GC_THRESHOLD >= (1<<FLEXSPACE_MAX_EXTENT_BIT), "need some blocks to do the final gc")

	static_assert(memtableWalBufCap >= 4*(MaxKeySize+256), "a fresh memtable buffer must have room for a couple of new keys, even of MaxKeySize")
}

// ======================== Log Entry ========================

type flexOp uint8

const (
	flexOpTreeInsert    flexOp = 0
	flexOpTreeCollapseN flexOp = 1
	flexOpGC            flexOp = 2
	flexOpSetTag        flexOp = 3
)

// encodeLogEntry encodes a 20-byte packed log entry into dst.
// Layout: [8-byte word0][8-byte word1][4-byte CRC32C]
// Packed little-endian bitfields in words: op:2 | p1:48 | p2:48 | p3:30
func encodeLogEntry(dst []byte, op flexOp, p1, p2, p3 uint64) {
	p1 &= 0xffffffffffff
	p2 &= 0xffffffffffff
	p3 &= 0x3fffffff
	word0 := uint64(op) | (p1 << 2) | ((p2 & 0x3fff) << 50)
	word1 := (p2 >> 14) | (p3 << 34)
	binary.LittleEndian.PutUint64(dst[0:8], word0)
	binary.LittleEndian.PutUint64(dst[8:16], word1)
	binary.LittleEndian.PutUint32(dst[16:20], crc32.Checksum(dst[0:16], crc32cTable))
}

// decodeLogEntry decodes a 20-byte packed log entry from src, verifying CRC32C.
func decodeLogEntry(src []byte) (op flexOp, p1, p2, p3 uint64, ok bool) {
	if crc32.Checksum(src[0:16], crc32cTable) != binary.LittleEndian.Uint32(src[16:20]) {
		return
	}
	word0 := binary.LittleEndian.Uint64(src[0:8])
	word1 := binary.LittleEndian.Uint64(src[8:16])
	op = flexOp(word0 & 0x3)
	p1 = (word0 >> 2) & 0xffffffffffff
	p2 = ((word0 >> 50) & 0x3fff) | ((word1 & 0x3ffffffff) << 14)
	p3 = word1 >> 34
	ok = true
	return
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
	blkid      uint64   // current block being written
	blkoff     uint64   // offset within current block (0..FLEXSPACE_BLOCK_SIZE)
	buf        []byte   // single 4 MB write buffer
	blkusage   []uint32 // [FLEXSPACE_BLOCK_COUNT] bytes used per block
	blkdist    []uint64 // [FLEXSPACE_BM_BLKDIST_SIZE] usage histogram buckets
	freeBlocks uint64   // count of completely empty blocks
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
// Prefers recycled blocks (before fromBlkid) to avoid growing the file.
// If isGC is false, may call GC first to free up blocks.
func (bm *blockManager) findEmptyBlock(fromBlkid uint64, isGC bool) uint64 {
	if !isGC {
		bm.file.GC()
	}
	// Phase 1: prefer recycled blocks — search from block 0 up to fromBlkid
	for i := uint64(0); i < fromBlkid; i++ {
		if bm.blkusage[i] == 0 {
			return i
		}
	}
	// Phase 2: search forward from fromBlkid (extends file if needed)
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
	copy(bm.buf[bm.blkoff:], buf[:osize])
	bm.blkoff += osize
	bm.updateBlkUsage(bm.blkid, int32(osize))
	if bm.blkoff == FLEXSPACE_BLOCK_SIZE {
		bm.nextBlock(isGC)
	}
	return osize
}

// nextBlock flushes the current block to disk and switches to a new empty block.
func (bm *blockManager) nextBlock(isGC bool) {
	oldBlkid := bm.blkid
	newBlkid := bm.findEmptyBlock(oldBlkid, isGC)
	if oldBlkid == newBlkid {
		return // current block is already empty (blkoff==0), nothing to do
	}
	// Write the filled portion of the current block to disk
	off := int64(oldBlkid * FLEXSPACE_BLOCK_SIZE)
	_, err := bm.file.fdKV128blocks.WriteAt(bm.buf[:bm.blkoff], off)
	panicOn(err)
	atomic.AddInt64(&bm.file.KV128BytesWritten, int64(bm.blkoff))
	bm.blkid = newBlkid
	bm.blkoff = 0
}

// flush persists the current in-memory block to disk and syncs the data file.
func (bm *blockManager) flush(isGC bool) {
	bm.nextBlock(isGC)
	panicOn(bm.file.fdKV128blocks.Sync())
	//vv("we did direct fsync on '%v'; stack= \n %v", bm.file.fdKV128blocks.Name(), stack())
}

// read attempts to satisfy a read from the in-memory buffer.
// Returns number of bytes served (0 if the block is not the current in-memory block).
func (bm *blockManager) read(dst []byte, poff, size uint64) uint64 {
	blkid := poff >> FLEXSPACE_BLOCK_BITS
	if blkid != bm.blkid {
		return 0
	}
	blkoff := poff & (FLEXSPACE_BLOCK_SIZE - 1)
	remain := uint64(FLEXSPACE_BLOCK_SIZE) - blkoff
	osize := size
	if osize > remain {
		osize = remain
	}
	copy(dst[:osize], bm.buf[blkoff:])
	return osize
}

// bmCreate allocates a new block manager for ff.
func bmCreate(ff *FlexSpace) *blockManager {
	bm := &blockManager{
		file:       ff,
		blkusage:   make([]uint32, FLEXSPACE_BLOCK_COUNT),
		blkdist:    make([]uint64, FLEXSPACE_BM_BLKDIST_SIZE),
		buf:        make([]byte, FLEXSPACE_BLOCK_SIZE),
		freeBlocks: FLEXSPACE_BLOCK_COUNT,
	}
	bm.blkdist[0] = FLEXSPACE_BLOCK_COUNT // all blocks start in bucket 0 (empty)
	return bm
}

// bmInit reconstructs the block manager state by scanning the FlexTree extents.
func bmInit(bm *blockManager, tree *FlexTree) {
	bm.blkdist[0] = FLEXSPACE_BLOCK_COUNT
	bm.freeBlocks = FLEXSPACE_BLOCK_COUNT
	maxBlkid := uint64(0)

	nodeID := tree.LeafHead
	for !nodeID.IsIllegal() {
		le := tree.GetLeaf(nodeID)
		for i := uint32(0); i < le.Count; i++ {
			ext := &le.Extents[i]
			if ext.IsHole() {
				continue // holes have no physical storage
			}
			poff := ext.Address() // 47-bit physical address (no hole flag)
			length := uint64(ext.Len)
			blkid := poff >> FLEXSPACE_BLOCK_BITS
			if blkid > maxBlkid {
				maxBlkid = blkid
			}
			// extents are guaranteed not to cross block boundaries
			bm.updateBlkUsage(blkid, int32(length))
		}
		nodeID = le.Next
	}
	// isGC=true to avoid recursive GC call during initialization
	bm.blkid = bm.findEmptyBlock(maxBlkid, true)
	bm.blkoff = 0
}

// ======================== FlexSpace ========================

// FlexSpace is a log-structured file-like address space providing
// insert-range, collapse-range, read, write, and GC operations.
// Corresponds to struct flexfile in flexfile.h/flexfile.c.
//
// FlexSpace itself does no internal locking. Clients must
// guarantee single user at a time; e.g. via FlexDB.ffMu.
//
// The main data file within the FLEXSPACE directory, the file
// that holds all the keys + small values is "FLEXSPACE.KV128_BLOCKS".
//
// Here's how the block management works:
//
// The FLEXSPACE.KV128_BLOCKS file is divided into 4 MB blocks. The block
// manager (blockManager) keeps one 4 MB in-memory write buffer and
// appends data into it sequentially:
//
// 1. Writing: When FlexDB flushes a memtable, it calls
// putPassthrough which calls ff.Insert(). Insert copies the
// kv128-encoded bytes into the block manager's buffer at
// the current offset (bm.buf[blkoff:]). If the current block
// can't fit the data, nextBlock() writes the filled buffer to
// disk at blkid × 4MB and moves to the next empty block.
//
// 2. Mapping: Each chunk written gets a physical offset
// (poff = blkid × 4MB + blkoff). That poff is inserted into the
// FlexTree, which maps logical offset → physical offset. So
// the FlexTree is the indirection layer that lets you read
// kv128 intervals by logical position even though they're
// scattered across 4 MB blocks on disk.
//
// 3. GC: The block manager tracks per-block usage (blkusage[]).
// When data is deleted (Collapse), the block's usage count
// decreases. GC reclaims blocks with low utilization by
// rewriting their live extents into the current write
// block, then freeing the old block.
//
// So "block-managed" means the FLEXSPACE.KV128_BLOCKS file is a pool of 4 MB blocks
// with an append-only write cursor, a FlexTree for logical -> physical
// mapping, and a GC that reclaims fragmented blocks. It's
// essentially a log-structured store at the block level — writes
// always go to the current block, never overwrite in place.
type FlexSpace struct {
	Path          string
	vfs           vfs.FS
	tree          *FlexTree
	fdKV128blocks vfs.File // data file, "FLEXSPACE.KV128_BLOCKS"
	redoLogFD     vfs.File // redo log file, "FLEXSPACE.REDO.LOG".
	logBuf        []byte   // in-memory log buffer (cap = FLEXSPACE_LOG_MEM_CAP)
	logBufSize    uint32
	logTotalSize  uint32 // bytes persisted to the log file (including header)
	bm            *blockManager
	gc            gcCtx

	// Sequential IO cache (replaces C thread-local seqio_fp/seqio_epoch)
	seqioPos    Pos
	seqioEpoch  uint64
	globalEpoch uint64

	omitRedoLog bool // when true, skip redo log writes and SyncCoW on every Sync

	// Write-byte counters (accessed atomically)
	KV128BytesWritten   int64 // FLEXSPACE.KV128_BLOCKS file bytes written
	REDOLogBytesWritten int64 // redo LOG bytes written
}

// ======================== Log helpers ========================

// logFull returns true if the in-memory log buffer is full.
func (ff *FlexSpace) logFull() bool {
	return ff.logBufSize >= FLEXSPACE_LOG_MEM_CAP
}

// logWrite appends a 20-byte log entry to the in-memory buffer.
// Caller guarantees the buffer is not full.
func (ff *FlexSpace) logWrite(op flexOp, p1, p2, p3 uint64) {
	encodeLogEntry(ff.logBuf[ff.logBufSize:], op, p1, p2, p3)
	ff.logBufSize += flexLogEntrySize
}

// logSync writes the in-memory buffer to the log file and fdatasyncs.
func (ff *FlexSpace) redoLogFlushAndSync() {
	if ff.logBufSize == 0 {
		return
	}
	_, err := ff.redoLogFD.WriteAt(ff.logBuf[:ff.logBufSize], int64(ff.logTotalSize))
	panicOn(err)
	atomic.AddInt64(&ff.REDOLogBytesWritten, int64(ff.logBufSize))
	ff.logTotalSize += ff.logBufSize
	ff.logBufSize = 0
	panicOn(ff.redoLogFD.Sync())
}

// logTruncate resets the log file to zero length.
func (ff *FlexSpace) logTruncate() {
	panicOn(ff.redoLogFD.Truncate(0))
}

// writeLogVersion writes the current tree version as the 12-byte log header
// (8-byte version + 4-byte CRC32C).
func (ff *FlexSpace) writeLogVersion() {
	var buf [12]byte
	binary.LittleEndian.PutUint64(buf[:8], ff.tree.PersistentVersion)
	binary.LittleEndian.PutUint32(buf[8:12], crc32.Checksum(buf[:8], crc32cTable))
	_, err := ff.redoLogFD.WriteAt(buf[:], 0)
	panicOn(err)
	atomic.AddInt64(&ff.REDOLogBytesWritten, flexLogVersionSize)
	ff.logTotalSize = flexLogVersionSize
	ff.logBufSize = 0
}

// logRedo replays log entries when the log version matches the tree checkpoint.
// After replay it saves the flextree checkpoint.
func (ff *FlexSpace) logRedo() {
	entryBuf := make([]byte, flexLogEntrySize)
	i := uint64(0)
	// Leaf list cursor for GC redo
	gcNodeID := ff.tree.LeafHead
	gcLeaf := (*LeafNode)(nil)
	gcIdx := uint32(0)
	if !gcNodeID.IsIllegal() {
		gcLeaf = ff.tree.GetLeaf(gcNodeID)
	}

	for {
		off := int64(uint64(flexLogVersionSize) + i*uint64(flexLogEntrySize))
		n, err := ff.redoLogFD.ReadAt(entryBuf, off)
		if n != flexLogEntrySize || err != nil {
			break
		}
		op, p1, p2, p3, ok := decodeLogEntry(entryBuf)
		if !ok {
			break // CRC32C mismatch — stop replay
		}
		switch op {
		case flexOpTreeInsert:
			ff.tree.Insert(p1, p2, uint32(p3))
		case flexOpTreeCollapseN:
			ff.tree.Delete(p1, p2)
		case flexOpGC:
			// GC redo: find the extent with poff==p1 (old poff), len==p3,
			// then update its poff to p2 (new poff) in-place.
			if gcLeaf == nil {
				panic("flexspace: invalid GC log — no leaf nodes")
			}
		gcRedoSearch:
			for {
				if gcIdx >= gcLeaf.Count {
					if gcLeaf.Next.IsIllegal() {
						gcLeaf = ff.tree.GetLeaf(ff.tree.LeafHead)
						gcIdx = 0
					} else {
						gcLeaf = ff.tree.GetLeaf(gcLeaf.Next)
						gcIdx = 0
					}
				}
				ext := &gcLeaf.Extents[gcIdx]
				if ext.Poff() == p1 {
					if uint64(ext.Len) != p3 {
						panic("flexspace: GC log inconsistency: len mismatch")
					}
					ext.SetPoff(p2)
					gcLeaf.Dirty = true
					gcIdx++
					break gcRedoSearch
				}
				gcIdx++
			}
		case flexOpSetTag:
			ff.tree.SetTag(p1, uint16(p2))
		default:
			panic("flexspace: log corrupted — unknown op")
		}
		i++
	}
	// Save the replayed state as a new checkpoint
	panicOn(ff.tree.SyncCoW())
}

// OpenFlexSpaceCoW opens or creates a FlexSpace using CoW page-based persistence
// for the FlexTree instead of greenpack full-tree serialization.
// The directory will contain: FLEXSPACE.KV128_BLOCKS, FLEXTREE.COMMIT, FLEXTREE.PAGES, FLEXSPACE.REDO.LOG.
// When omitRedoLog is true, redo log writes are skipped and SyncCoW is called on every Sync.
func OpenFlexSpaceCoW(path string, omitRedoLog bool, fs vfs.FS) (*FlexSpace, error) {
	//vv("OpenFlexSpaceCoW with fs = '%#v'", fs)

	if path == "" {
		return nil, fmt.Errorf("flexspace: null path")
	}

	if err := fs.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("flexspace: mkdir %s: %w", path, err)
	}

	ff := &FlexSpace{
		Path:        path,
		vfs:         fs,
		omitRedoLog: omitRedoLog,
	}

	// Clean up stale vacuum file from a previous interrupted VacuumKV.
	fs.Remove(filepath.Join(path, "FLEXSPACE.KV128_BLOCKS.vacuum"))

	// Open the data file
	dataPath := filepath.Join(path, "FLEXSPACE.KV128_BLOCKS")
	//fd, err := fs.OpenFile(dataPath, os.O_RDWR|os.O_CREATE, 0644)
	fd, err := fs.OpenReadWrite(dataPath, vfs.WriteCategoryUnspecified)

	if err != nil {
		return nil, fmt.Errorf("flexspace: open FLEXSPACE.KV128_BLOCKS: %w", err)
	}
	fd.Sync()
	ff.fdKV128blocks = fd

	// Open or create the FlexTree with CoW persistence
	tree, err := OpenFlexTreeCoW(path, fs)
	if err != nil {
		fd.Close()
		return nil, fmt.Errorf("flexspace: open CoW tree: %w", err)
	}
	ff.tree = tree

	// Open the redo-log file
	logPath := filepath.Join(path, "FLEXSPACE.REDO.LOG")
	//redoLogFD, err := fs.OpenFile(logPath, os.O_RDWR|os.O_CREATE, 0644)
	// pebble/v2/vfs docs: "OpenReadWrite opens the named file for reading and
	// writing. If the file does not exist, it is created."
	redoLogFD, err := fs.OpenReadWrite(logPath, vfs.WriteCategoryUnspecified)

	if err != nil {
		fd.Close()
		tree.CloseCoW()
		return nil, fmt.Errorf("flexspace: open FLEXSPACE.REDO.LOG: %w", err)
	}
	ff.redoLogFD = redoLogFD

	// Crash recovery: replay log if version matches the checkpoint.
	// When omitRedoLog=true there are no log entries to replay.
	if !omitRedoLog {
		logStat, err := redoLogFD.Stat()
		if err != nil {
			fd.Close()
			redoLogFD.Close()
			tree.CloseCoW()
			return nil, fmt.Errorf("flexspace: stat FLEXSPACE.REDO.LOG: %w", err)
		}
		if logStat.Size() > flexLogVersionSize {
			var versionBuf [12]byte
			if _, err := redoLogFD.ReadAt(versionBuf[:], 0); err == nil {
				logVersion := binary.LittleEndian.Uint64(versionBuf[:8])
				logCRC := binary.LittleEndian.Uint32(versionBuf[8:12])
				if logCRC == crc32.Checksum(versionBuf[:8], crc32cTable) && logVersion == tree.PersistentVersion {
					ff.logRedo()
				}
			}
		}
	}

	// Truncate log and write fresh version header
	ff.logTruncate()
	ff.writeLogVersion()
	ff.redoLogFlushAndSync()

	// Allocate in-memory log buffer
	ff.logBuf = make([]byte, FLEXSPACE_LOG_MEM_CAP*8)
	ff.logBufSize = 0
	ff.logTotalSize = flexLogVersionSize

	// Initialize block manager
	ff.bm = bmCreate(ff)
	bmInit(ff.bm, ff.tree)

	// Initialize GC context
	ff.gc.loff = 0
	ff.gc.count = 0
	ff.gc.writeBetweenStages = false

	ff.globalEpoch = 1
	ff.seqioEpoch = 0

	return ff, nil
}

// Close syncs and cleanly shuts down the FlexSpace.
func (ff *FlexSpace) Close() {
	ff.Sync() // does syncR which does ff.bm.flush which does fsync on FLEXSPACE.KV128.BLOCKS

	// Save the tree checkpoint
	panicOn(ff.tree.SyncCoW())

	// Truncate log (all changes now in checkpoint).
	// When omitRedoLog=true, SyncCoW in Sync() already committed — skip truncate.
	if !ff.omitRedoLog {
		ff.logTruncate()
	}

	// Destroy block manager (flush remaining data)
	ff.bm.flush(false) // fsyncs FLEXSPACE.KV128.BLOCKS again! do we need to?

	panicOn(ff.fdKV128blocks.Close())
	panicOn(ff.redoLogFD.Close())

	// Close CoW file descriptors (SyncCoW already done above)
	ff.tree.nodeFD.Close()
	ff.tree.metaFD.Close()
	ff.tree.cowEnabled = false
}

// truncateTrailingBlocks shrinks FLEXSPACE.KV128_BLOCKS by removing empty blocks at the end.
func (ff *FlexSpace) truncateTrailingBlocks() {
	// Find the highest block with non-zero usage
	highBlock := int64(-1)
	for i := int64(FLEXSPACE_BLOCK_COUNT) - 1; i >= 0; i-- {
		if ff.bm.blkusage[i] > 0 {
			highBlock = i
			break
		}
	}
	// Also account for the current write block (it may have buffered data)
	if ff.bm.blkoff > 0 && int64(ff.bm.blkid) > highBlock {
		highBlock = int64(ff.bm.blkid)
	}

	newSize := (highBlock + 1) * FLEXSPACE_BLOCK_SIZE
	if newSize < 0 {
		newSize = 0
	}

	fi, err := ff.fdKV128blocks.Stat()
	if debugTruncate {
		fmt.Printf("DEBUG truncateTrailingBlocks: highBlock=%d newSize=%d fileSize=%d blkid=%d blkoff=%d truncate=%v\n",
			highBlock, newSize, fi.Size(), ff.bm.blkid, ff.bm.blkoff, fi.Size() > newSize)
	}
	if err != nil || fi.Size() <= newSize {
		return // nothing to truncate
	}
	ff.fdKV128blocks.Truncate(newSize)
}

var debugTruncate = false

// syncR flushes data and log. If the log file exceeds the max size,
// it checkpoints the tree and resets the log.
// isGC=true means we're called from within GC.
func (ff *FlexSpace) syncR(isGC bool) {
	ff.bm.flush(isGC) // does fsync on FLEXSPACE.KV128.BLOCKS
	// so this is redundant
	//panicOn(ff.fdKV128blocks.Sync())

	if ff.omitRedoLog {
		// No redo log — always commit tree via CoW
		panicOn(ff.tree.SyncCoW())
	} else {
		// Original path: sync redo log, checkpoint only when log is large
		ff.redoLogFlushAndSync()
		if uint64(ff.logTotalSize) >= FLEXSPACE_LOG_MAX_SIZE {
			panicOn(ff.tree.SyncCoW())

			ff.logTruncate()
			ff.writeLogVersion()
			ff.redoLogFlushAndSync()
		}
	}
	ff.truncateTrailingBlocks()
}

// Sync flushes all in-memory state to disk.
func (ff *FlexSpace) Sync() {
	ff.syncR(false)
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

		// Try in-memory buffer first, then disk
		r := ff.bm.read(b, poff, slen)
		if r == 0 {
			n, err := ff.fdKV128blocks.ReadAt(b[:slen], int64(poff))
			if err != nil || uint64(n) != slen {
				return -1, fmt.Errorf("flexspace: pread at poff=%d len=%d: %w", poff, slen, err)
			}
			r = slen
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
// If commit is true, syncs when the log buffer is full.
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
		if !ff.omitRedoLog {
			ff.logWrite(flexOpTreeInsert, oloff, poff, tlen)
		}
		oloff += tlen
		olen -= tlen
		b = b[tlen:]
	}
	if !ff.omitRedoLog && commit && ff.logFull() {
		ff.Sync()
	}
	return int(length), nil
}

// Insert inserts len bytes at loff, shifting all subsequent extents right.
func (ff *FlexSpace) Insert(buf []byte, loff, length uint64) (int, error) {
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

	// Query extents being collapsed to update block usage
	rr := ff.tree.Query(loff, length)
	if rr != nil {
		for i := uint64(0); i < rr.Count; i++ {
			blkid := rr.V[i].Poff >> FLEXSPACE_BLOCK_BITS
			ff.bm.updateBlkUsage(blkid, -int32(rr.V[i].Len))
		}
	}
	ff.tree.Delete(loff, length)
	if !ff.omitRedoLog {
		ff.logWrite(flexOpTreeCollapseN, loff, length, 0)
	}

	if !ff.omitRedoLog && commit && ff.logFull() {
		ff.Sync()
	}
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
	if !ff.omitRedoLog {
		ff.logWrite(flexOpSetTag, loff, uint64(tag), 0)
	}
	if !ff.omitRedoLog && commit && ff.logFull() {
		ff.Sync()
	}
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
	if !ff.omitRedoLog && ff.logFull() {
		ff.Sync()
	}
	return n, nil
}

// ======================== Overwrite ========================

// Overwrite writes buf directly to the physical location backing the extent
// at loff, without mutating the FlexTree or writing redo log entries.
// The extent at loff must already exist and have length == len(buf).
// This creates zero garbage — the same physical blocks are reused in-place.
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

		// Write to in-memory block buffer if it's the current block,
		// otherwise write directly to disk.
		blkid := poff >> FLEXSPACE_BLOCK_BITS
		if blkid == ff.bm.blkid {
			blkoff := poff & (FLEXSPACE_BLOCK_SIZE - 1)
			copy(ff.bm.buf[blkoff:], b[:slen])
		} else {
			_, err := ff.fdKV128blocks.WriteAt(b[:slen], int64(poff))
			if err != nil {
				return fmt.Errorf("flexspace: overwrite pwrite at poff=%d len=%d: %w", poff, slen, err)
			}
		}
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
// Bug fix: C code used FLEXSPACE_MAX_EXTENT_BIT (5) instead of FLEXSPACE_MAX_EXTENT_SIZE (131072).
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
			n, err := ff.fdKV128blocks.ReadAt(b[:slen], int64(poff))
			if err != nil || uint64(n) != slen {
				return -1, fmt.Errorf("flexspace: handler pread: %w", err)
			}
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

		// Sanity: extent must not cross block boundary
		if blkid != (poff+uint64(length)-1)>>FLEXSPACE_BLOCK_BITS {
			panic("flexspace: GC found extent crossing block boundary")
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
			n, err := ff.fdKV128blocks.ReadAt(buf, int64(poff))
			panicOn(err)
			_ = n
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

// gcAsyncExecute writes buffered GC items to new locations, logging each move.
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

		// Decrement old block usage
		oblkid := opoff >> FLEXSPACE_BLOCK_BITS
		newUsage := ff.bm.updateBlkUsage(oblkid, -int32(length))
		if newUsage == 0 {
			rblocks++
		}

		// Log the move: p1=old_poff, p2=new_poff, p3=len
		// Use the CURRENT poff from the leaf (which equals opoff since no writes between stages)
		if !ff.omitRedoLog {
			currPoff := item.node.Extents[item.idx].Poff()
			ff.logWrite(flexOpGC, currPoff, newPoff, uint64(length))
		}

		// Update the leaf node in-place
		item.node.Extents[item.idx].SetPoff(newPoff)
		item.node.Dirty = true

		item.buf = nil
	}

	// Propagate dirty flags up: if any child is dirty, mark the parent dirty
	// so SyncCoW can reach all dirty leaves. This is O(tree nodes) but GC
	// is already an expensive operation so the cost is negligible.
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
