package yogadb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	iofs "io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	"github.com/glycerine/vfs"
)

// Page-based Copy-on-Write persistence for FlexTree.
//
// On-disk layout:
//   {dir}/FLEXTREE.COMMIT  - 80-byte binary header (the commit point)
//   {dir}/FLEXTREE.PAGES - flat array of 1024-byte pages, one per node

const (
	cowPageSize = 1024

	pageTypeLeaf     = 0x01
	pageTypeInternal = 0x02

	cowMetaSize      = 136
	cowMetaMagic     = "FTCOW004"
	cowMetaAllocSize = 64 << 10 // 64 KB pre-allocation for FLEXTREE.COMMIT file
)

// ======================== FLEXTREE.COMMIT ========================

type cowMeta struct {
	FormatMajor uint64 // for future format evolution (currently 3)
	FormatMinor uint64 // (currently 0)

	Version      uint64
	RootSlotID   int64
	MaxSlotID    int64
	NodeCount    uint64
	MaxLoff      uint64
	MaxExtentSz  uint32
	NodesFileCap int64 // pre-allocated slot capacity of FLEXTREE.PAGES file

	totalLogicalBytesWrit  int64 // cumulative user payload bytes (key+value)
	totalPhysicalBytesWrit int64 // cumulative total bytes written to all files
	MaxHLC                 int64 // highest HLC ever assigned; used to seed clock on reopen

	LiveKeys      int64 // total live (non-tombstone) keys
	LiveBigKeys   int64 // keys with values in VLOG (HasVPtr=true)
	LiveSmallKeys int64 // keys with inline values

	totalCompressSavedBytes int64 // cumulative bytes saved by VLOG s2 compression
}

func encodeCowMeta(buf *[cowMetaSize]byte, m *cowMeta) {
	copy(buf[0:8], cowMetaMagic)
	binary.LittleEndian.PutUint64(buf[8:16], m.FormatMajor)
	binary.LittleEndian.PutUint64(buf[16:24], m.FormatMinor)
	binary.LittleEndian.PutUint64(buf[24:32], m.Version)
	binary.LittleEndian.PutUint64(buf[32:40], uint64(m.RootSlotID))
	binary.LittleEndian.PutUint64(buf[40:48], uint64(m.MaxSlotID))
	binary.LittleEndian.PutUint64(buf[48:56], m.NodeCount)
	binary.LittleEndian.PutUint64(buf[56:64], m.MaxLoff)
	binary.LittleEndian.PutUint32(buf[64:68], m.MaxExtentSz)
	binary.LittleEndian.PutUint64(buf[68:76], uint64(m.NodesFileCap))
	binary.LittleEndian.PutUint64(buf[76:84], uint64(m.totalLogicalBytesWrit))
	binary.LittleEndian.PutUint64(buf[84:92], uint64(m.totalPhysicalBytesWrit))
	binary.LittleEndian.PutUint64(buf[92:100], uint64(m.MaxHLC))
	binary.LittleEndian.PutUint64(buf[100:108], uint64(m.LiveKeys))
	binary.LittleEndian.PutUint64(buf[108:116], uint64(m.LiveBigKeys))
	binary.LittleEndian.PutUint64(buf[116:124], uint64(m.LiveSmallKeys))
	binary.LittleEndian.PutUint64(buf[124:132], uint64(m.totalCompressSavedBytes))
	crc := crc32.Checksum(buf[:132], crc32cTable)
	binary.LittleEndian.PutUint32(buf[132:136], crc)
}

func decodeCowMeta(buf *[cowMetaSize]byte) (*cowMeta, error) {
	if string(buf[0:8]) != cowMetaMagic {
		return nil, fmt.Errorf("cow: bad FLEXTREE.COMMIT magic")
	}
	crc := crc32.Checksum(buf[:132], crc32cTable)
	stored := binary.LittleEndian.Uint32(buf[132:136])
	if crc != stored {
		return nil, fmt.Errorf("cow: FLEXTREE.COMMIT CRC mismatch: computed %08x, stored %08x", crc, stored)
	}
	m := &cowMeta{
		FormatMajor:             binary.LittleEndian.Uint64(buf[8:16]),
		FormatMinor:             binary.LittleEndian.Uint64(buf[16:24]),
		Version:                 binary.LittleEndian.Uint64(buf[24:32]),
		RootSlotID:              int64(binary.LittleEndian.Uint64(buf[32:40])),
		MaxSlotID:               int64(binary.LittleEndian.Uint64(buf[40:48])),
		NodeCount:               binary.LittleEndian.Uint64(buf[48:56]),
		MaxLoff:                 binary.LittleEndian.Uint64(buf[56:64]),
		MaxExtentSz:             binary.LittleEndian.Uint32(buf[64:68]),
		NodesFileCap:            int64(binary.LittleEndian.Uint64(buf[68:76])),
		totalLogicalBytesWrit:   int64(binary.LittleEndian.Uint64(buf[76:84])),
		totalPhysicalBytesWrit:  int64(binary.LittleEndian.Uint64(buf[84:92])),
		MaxHLC:                  int64(binary.LittleEndian.Uint64(buf[92:100])),
		LiveKeys:                int64(binary.LittleEndian.Uint64(buf[100:108])),
		LiveBigKeys:             int64(binary.LittleEndian.Uint64(buf[108:116])),
		LiveSmallKeys:           int64(binary.LittleEndian.Uint64(buf[116:124])),
		totalCompressSavedBytes: int64(binary.LittleEndian.Uint64(buf[124:132])),
	}
	return m, nil
}

// scanMetaLatest scans all cowMetaSize-byte slots in the FLEXTREE.COMMIT file and returns
// the record with the highest Version that has a valid CRC. This is the
// recovery path: the FLEXTREE.COMMIT file is append-only, so a torn write at the
// tail leaves all previous records intact.
func scanMetaLatest(f vfs.File, fileSize int64) (*cowMeta, int64, error) {
	nSlots := fileSize / cowMetaSize
	if nSlots == 0 {
		return nil, 0, fmt.Errorf("cow: FLEXTREE.COMMIT file is empty")
	}

	var best *cowMeta
	var bestOff int64
	var buf [cowMetaSize]byte

	for i := int64(0); i < nSlots; i++ {
		off := i * cowMetaSize
		n, err := f.ReadAt(buf[:], off)
		if n != cowMetaSize || err != nil {
			continue
		}
		m, err := decodeCowMeta(&buf)
		if err != nil {
			continue // bad CRC or magic - skip (torn write or unwritten slot)
		}
		if best == nil || m.Version > best.Version {
			best = m
			bestOff = off
		}
	}

	if best == nil {
		return nil, 0, fmt.Errorf("cow: no valid FLEXTREE.COMMIT record found in %d slots", nSlots)
	}
	return best, bestOff, nil
}

// ======================== Page encode/decode ========================

// encodeLeafPage encodes a LeafNode into a 1024-byte page.
func encodeLeafPage(buf *[cowPageSize]byte, n *LeafNode) {
	*buf = [cowPageSize]byte{} // zero
	buf[0] = pageTypeLeaf
	// buf[1] reserved
	binary.LittleEndian.PutUint16(buf[2:4], uint16(n.Count))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(n.NodeID))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(n.SlotID))

	// Extents: 60 × 16 bytes starting at offset 16
	off := 16
	for i := uint32(0); i < FLEXTREE_LEAF_CAP; i++ {
		e := &n.Extents[i]
		binary.LittleEndian.PutUint32(buf[off:off+4], e.Loff)
		binary.LittleEndian.PutUint32(buf[off+4:off+8], e.Len)
		binary.LittleEndian.PutUint64(buf[off+8:off+16], e.TagPoff)
		off += 16
	}
	// off = 16 + 60*16 = 976
	crc := crc32.Checksum(buf[:976], crc32cTable)
	binary.LittleEndian.PutUint32(buf[976:980], crc)
	// bytes 980..1023 padding (already zero)
}

// decodeLeafPage decodes a 1024-byte page into a LeafNode.
// The caller must have already allocated the node in the arena.
func decodeLeafPage(buf *[cowPageSize]byte, n *LeafNode) error {
	if buf[0] != pageTypeLeaf {
		return fmt.Errorf("cow: expected leaf page type 0x%02x, got 0x%02x", pageTypeLeaf, buf[0])
	}
	crc := crc32.Checksum(buf[:976], crc32cTable)
	stored := binary.LittleEndian.Uint32(buf[976:980])
	if crc != stored {
		return fmt.Errorf("cow: leaf page CRC mismatch: computed %08x, stored %08x", crc, stored)
	}

	n.Count = uint32(binary.LittleEndian.Uint16(buf[2:4]))
	// NodeID is set by the arena allocator, but we store it for verification
	storedNodeID := NodeID(int32(binary.LittleEndian.Uint32(buf[4:8])))
	_ = storedNodeID // debug verification only
	n.SlotID = int64(binary.LittleEndian.Uint64(buf[8:16]))

	off := 16
	for i := uint32(0); i < FLEXTREE_LEAF_CAP; i++ {
		e := &n.Extents[i]
		e.Loff = binary.LittleEndian.Uint32(buf[off : off+4])
		e.Len = binary.LittleEndian.Uint32(buf[off+4 : off+8])
		e.TagPoff = binary.LittleEndian.Uint64(buf[off+8 : off+16])
		off += 16
	}

	n.Dirty = false
	n.IsLeaf = true
	return nil
}

// encodeInternalPage encodes an InternalNode into a 1024-byte page.
func encodeInternalPage(buf *[cowPageSize]byte, n *InternalNode) {
	*buf = [cowPageSize]byte{} // zero
	buf[0] = pageTypeInternal
	// buf[1] reserved
	binary.LittleEndian.PutUint16(buf[2:4], uint16(n.Count))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(n.NodeID))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(n.SlotID))

	// Pivots: 30 × uint64 starting at offset 16
	off := 16
	for i := 0; i < FLEXTREE_INTERNAL_CAP; i++ {
		binary.LittleEndian.PutUint64(buf[off:off+8], n.Pivots[i])
		off += 8
	}
	// off = 16 + 30*8 = 256

	// ChildShifts: 31 × int64
	for i := 0; i < FLEXTREE_INTERNAL_CAP_PLUS_ONE; i++ {
		binary.LittleEndian.PutUint64(buf[off:off+8], uint64(n.Children[i].Shift))
		off += 8
	}
	// off = 256 + 31*8 = 504

	// ChildSlotIDs: 31 × int64
	for i := 0; i < FLEXTREE_INTERNAL_CAP_PLUS_ONE; i++ {
		binary.LittleEndian.PutUint64(buf[off:off+8], uint64(n.ChildSlotIDs[i]))
		off += 8
	}
	// off = 504 + 31*8 = 752

	crc := crc32.Checksum(buf[:752], crc32cTable)
	binary.LittleEndian.PutUint32(buf[752:756], crc)
	// bytes 756..1023 padding (already zero)
}

// decodeInternalPage decodes a 1024-byte page into an InternalNode.
func decodeInternalPage(buf *[cowPageSize]byte, n *InternalNode) error {
	if buf[0] != pageTypeInternal {
		return fmt.Errorf("cow: expected internal page type 0x%02x, got 0x%02x", pageTypeInternal, buf[0])
	}
	crc := crc32.Checksum(buf[:752], crc32cTable)
	stored := binary.LittleEndian.Uint32(buf[752:756])
	if crc != stored {
		return fmt.Errorf("cow: internal page CRC mismatch: computed %08x, stored %08x", crc, stored)
	}

	n.Count = uint32(binary.LittleEndian.Uint16(buf[2:4]))
	storedNodeID := NodeID(int32(binary.LittleEndian.Uint32(buf[4:8])))
	_ = storedNodeID
	n.SlotID = int64(binary.LittleEndian.Uint64(buf[8:16]))

	off := 16
	for i := 0; i < FLEXTREE_INTERNAL_CAP; i++ {
		n.Pivots[i] = binary.LittleEndian.Uint64(buf[off : off+8])
		off += 8
	}
	for i := 0; i < FLEXTREE_INTERNAL_CAP_PLUS_ONE; i++ {
		n.Children[i].Shift = int64(binary.LittleEndian.Uint64(buf[off : off+8]))
		off += 8
	}
	for i := 0; i < FLEXTREE_INTERNAL_CAP_PLUS_ONE; i++ {
		n.ChildSlotIDs[i] = int64(binary.LittleEndian.Uint64(buf[off : off+8]))
		off += 8
	}

	n.Dirty = false
	n.IsLeaf = false
	return nil
}

// ======================== Slot allocation ========================

// Initial and minimum FLEXTREE.PAGES file capacity in slots.
// 256 slots = 256 KB; grown by doubling.
const cowInitialSlotCap = 256

func (t *FlexTree) allocSlot() int64 {
	if len(t.freeSlots) > 0 {
		last := len(t.freeSlots) - 1
		s := t.freeSlots[last]
		t.freeSlots = t.freeSlots[:last]
		return s
	}
	s := t.maxSlotID
	t.maxSlotID++
	// Grow FLEXTREE.PAGES file if we've exceeded pre-allocated capacity.
	// growNodesFile uses Truncate + fsync (not fdatasync) since
	// the file size metadata is changing.
	if t.maxSlotID > t.nodesFileCap {
		t.growNodesFile()
	}
	return s
}

// growNodesFile doubles the FLEXTREE.PAGES file capacity using fallocate and
// fsyncs the size change. Subsequent writes use fdatasync until the
// next growth event.
func (t *FlexTree) growNodesFile() {
	newCap := t.nodesFileCap * 2
	if newCap < cowInitialSlotCap {
		newCap = cowInitialSlotCap
	}
	for newCap < t.maxSlotID {
		newCap *= 2
	}
	if err := t.nodeFD.Preallocate(0, newCap*cowPageSize); err != nil {
		panicf("cow: growNodesFile fallocate to %d slots: %v", newCap, err)
	}
	// Full fsync here because the file size and block allocation changed.
	if err := t.nodeFD.Sync(); err != nil {
		panicf("cow: growNodesFile fsync: %v", err)
	}
	t.nodesFileCap = newCap
}

// ======================== CoW Sync ========================

// SyncCoW writes only dirty nodes to disk using copy-on-write.
// The FLEXTREE.COMMIT file is the commit point: written only after FLEXTREE.PAGES is synced.
func (t *FlexTree) SyncCoW() error {
	if !t.cowEnabled {
		return fmt.Errorf("cow: not enabled - use OpenFlexTreeCoW")
	}
	if t.Root.IsIllegal() {
		return nil // empty tree, nothing to sync
	}

	t.PersistentVersion++
	var freedSlots []int64

	// Post-order recursive walk: write dirty nodes
	if err := t.syncCowRec(t.Root, &freedSlots); err != nil {
		return err
	}

	// SyncData FLEXTREE.PAGES - file size is pre-allocated and stable,
	// so fdatasync only flushes data pages, not size metadata.
	if err := t.nodeFD.SyncData(); err != nil {
		return fmt.Errorf("cow: fdatasync FLEXTREE.PAGES: %w", err)
	}

	// Append FLEXTREE.COMMIT record. The FLEXTREE.COMMIT file is an append-only ring-
	// buffer-in-a-file, so that a torn write never destroys a
	// previous recent commit - on recovery we scan
	// for the record with the highest version and a valid CRC.
	rootSlot := t.NodeSlotID(t.Root)
	meta := cowMeta{
		FormatMajor:            3,
		FormatMinor:            0,
		Version:                t.PersistentVersion,
		RootSlotID:             rootSlot,
		MaxSlotID:              t.maxSlotID,
		NodeCount:              t.NodeCount,
		MaxLoff:                t.MaxLoff,
		MaxExtentSz:            t.MaxExtentSize,
		NodesFileCap:           t.nodesFileCap,
		totalLogicalBytesWrit:   t.totalLogicalBytesWrit,
		totalPhysicalBytesWrit:  t.totalPhysicalBytesWrit,
		totalCompressSavedBytes: t.totalCompressSavedBytes,
		MaxHLC:                  t.MaxHLC,
		LiveKeys:                t.liveKeys,
		LiveBigKeys:             t.liveBigKeys,
		LiveSmallKeys:           t.liveSmallKeys,
	}
	var metaBuf [cowMetaSize]byte
	encodeCowMeta(&metaBuf, &meta)

	// Wrap to beginning when we've reached the end of the pre-allocated file.
	// Previous records behind us are still intact for crash recovery.
	if t.metaNextOff+cowMetaSize > t.metaFileCap {
		t.metaNextOff = 0
	}
	if _, err := t.metaFD.WriteAt(metaBuf[:], t.metaNextOff); err != nil {
		return fmt.Errorf("cow: write FLEXTREE.COMMIT at offset %d: %w", t.metaNextOff, err)
	}
	atomic.AddInt64(&t.FlexTreePagesBytesWritten, cowMetaSize)
	if err := t.metaFD.SyncData(); err != nil {
		return fmt.Errorf("cow: fdatasync FLEXTREE.COMMIT: %w", err)
	}
	t.metaNextOff += cowMetaSize

	// Now safe to reclaim old slots
	t.freeSlots = append(t.freeSlots, freedSlots...)

	return nil
}

// syncCowRec does a post-order walk, writing dirty nodes.
func (t *FlexTree) syncCowRec(nodeID NodeID, freedSlots *[]int64) error {
	if nodeID.IsIllegal() {
		return nil
	}

	var pageBuf [cowPageSize]byte

	if nodeID.IsLeaf() {
		le := t.GetLeaf(nodeID)
		if !le.Dirty {
			return nil
		}

		// Reclaim old slot
		oldSlot := le.SlotID
		if oldSlot >= 0 {
			*freedSlots = append(*freedSlots, oldSlot)
		}

		// Allocate new slot
		newSlot := t.allocSlot()
		le.SlotID = newSlot

		// Encode and write
		encodeLeafPage(&pageBuf, le)
		off := int64(newSlot) * cowPageSize
		if _, err := t.nodeFD.WriteAt(pageBuf[:], off); err != nil {
			return fmt.Errorf("cow: write leaf page at slot %d: %w", newSlot, err)
		}
		atomic.AddInt64(&t.FlexTreePagesBytesWritten, cowPageSize)
		le.Dirty = false
		return nil
	}

	// Internal node
	ie := t.GetInternal(nodeID)
	if !ie.Dirty {
		return nil
	}

	// Recurse into children first (post-order)
	for i := uint32(0); i <= ie.Count; i++ {
		childID := ie.Children[i].NodeID
		if !childID.IsIllegal() {
			if err := t.syncCowRec(childID, freedSlots); err != nil {
				return err
			}
			// Update ChildSlotIDs after child has been written
			ie.ChildSlotIDs[i] = t.NodeSlotID(childID)
		}
	}

	// Reclaim old slot
	oldSlot := ie.SlotID
	if oldSlot >= 0 {
		*freedSlots = append(*freedSlots, oldSlot)
	}

	// Allocate new slot
	newSlot := t.allocSlot()
	ie.SlotID = newSlot

	// Encode and write
	encodeInternalPage(&pageBuf, ie)
	off := int64(newSlot) * cowPageSize
	if _, err := t.nodeFD.WriteAt(pageBuf[:], off); err != nil {
		return fmt.Errorf("cow: write internal page at slot %d: %w", newSlot, err)
	}
	atomic.AddInt64(&t.FlexTreePagesBytesWritten, cowPageSize)
	ie.Dirty = false
	return nil
}

// ======================== Load ========================

// OpenFlexTreeCoW opens an existing CoW FlexTree from dirPath, or creates
// a new one if the directory does not contain FLEXTREE.COMMIT/FLEXTREE.PAGES files.
func OpenFlexTreeCoW(dirPath string, fs vfs.FS) (*FlexTree, error) {
	//vv("OpenFlexTreeCoW has fs = '%#v'", fs)

	// what OpenFlexTree/openOrCreateFlexTree does for empty dirPath
	// strings (in many tests atm)

	// back from OpenFlexTree/openOrCreateFlexTree

	if dirPath == "" {
		return nil, fmt.Errorf("cow: empty path")
	}
	if err := fs.MkdirAll(dirPath, 0755); err != nil {
		return nil, fmt.Errorf("cow: mkdir %s: %w", dirPath, err)
	}

	metaPath := filepath.Join(dirPath, "FLEXTREE.COMMIT")
	nodePath := filepath.Join(dirPath, "FLEXTREE.PAGES")

	// Try to open existing FLEXTREE.COMMIT
	_, statErr := fs.Stat(metaPath)
	if statErr != nil {
		//vv("statErr was not os.IsNotExist: '%v'", statErr)

		errs := statErr.Error()

		if os.IsNotExist(statErr) ||
			errors.Is(statErr, iofs.ErrNotExist) ||
			strings.Contains(errs, "no such file or directory") {

			// Create fresh tree
			return createFreshCoW(dirPath, metaPath, nodePath, fs)
		}
		panicOn(statErr)
	}

	// Open existing
	//metaFD, err := fs.OpenFile(metaPath, os.O_RDWR, 0644)
	metaFD, err := fs.OpenReadWrite(metaPath, vfs.WriteCategoryUnspecified)

	if err != nil {
		return nil, fmt.Errorf("cow: open FLEXTREE.COMMIT: %w", err)
	}
	//nodeFD, err := fs.OpenFile(nodePath, os.O_RDWR, 0644)
	nodeFD, err := fs.OpenReadWrite(nodePath, vfs.WriteCategoryUnspecified)

	if err != nil {
		metaFD.Close()
		return nil, fmt.Errorf("cow: open FLEXTREE.PAGES: %w", err)
	}

	// Scan FLEXTREE.COMMIT for the record with the highest version and valid CRC.
	// FLEXTREE.COMMIT is append-only: each commit writes an 80-byte record at the
	// next offset. On crash, the last write may be torn, but the
	// previous record is intact.
	metaStat, err := metaFD.Stat()
	if err != nil {
		metaFD.Close()
		nodeFD.Close()
		return nil, fmt.Errorf("cow: stat FLEXTREE.COMMIT: %w", err)
	}
	metaFileSize := metaStat.Size()

	meta, metaOff, err := scanMetaLatest(metaFD, metaFileSize)
	if err != nil {
		metaFD.Close()
		nodeFD.Close()
		return nil, err
	}

	t := NewFlexTree(fs)
	t.Path = dirPath
	t.PersistentVersion = meta.Version
	t.NodeCount = meta.NodeCount
	t.MaxLoff = meta.MaxLoff
	t.MaxExtentSize = meta.MaxExtentSz
	t.metaFD = metaFD
	t.nodeFD = nodeFD
	t.maxSlotID = meta.MaxSlotID
	t.nodesFileCap = meta.NodesFileCap
	t.cowEnabled = true
	t.metaFileCap = metaFileSize
	t.metaNextOff = metaOff + cowMetaSize // resume appending after the latest record
	t.totalLogicalBytesWrit = meta.totalLogicalBytesWrit
	t.totalPhysicalBytesWrit = meta.totalPhysicalBytesWrit
	t.totalCompressSavedBytes = meta.totalCompressSavedBytes
	t.MaxHLC = meta.MaxHLC
	t.liveKeys = meta.LiveKeys
	t.liveBigKeys = meta.LiveBigKeys
	t.liveSmallKeys = meta.LiveSmallKeys

	// If NodesFileCap is 0 (old format without pre-allocation),
	// derive it from the actual file size and re-establish the invariant.
	if t.nodesFileCap <= 0 {
		fi, err := nodeFD.Stat()
		if err != nil {
			metaFD.Close()
			nodeFD.Close()
			return nil, fmt.Errorf("cow: stat FLEXTREE.PAGES: %w", err)
		}
		t.nodesFileCap = fi.Size() / cowPageSize
		if t.nodesFileCap < t.maxSlotID {
			t.nodesFileCap = t.maxSlotID
		}
		if t.nodesFileCap < cowInitialSlotCap {
			t.nodesFileCap = cowInitialSlotCap
		}
		// Grow to the computed capacity and fsync the new size.
		if err := nodeFD.Preallocate(0, t.nodesFileCap*cowPageSize); err != nil {
			metaFD.Close()
			nodeFD.Close()
			return nil, fmt.Errorf("cow: preallocate FLEXTREE.PAGES on upgrade: %w", err)
		}
		if err := nodeFD.Sync(); err != nil {
			metaFD.Close()
			nodeFD.Close()
			return nil, fmt.Errorf("cow: fsync FLEXTREE.PAGES on upgrade: %w", err)
		}
	}

	// Recursive load from root
	rootNodeID, err := t.loadNodeRec(nodeFD, meta.RootSlotID)
	if err != nil {
		metaFD.Close()
		nodeFD.Close()
		return nil, fmt.Errorf("cow: load tree: %w", err)
	}
	t.Root = rootNodeID

	// Rebuild leaf linked list
	var lastLeafID NodeID
	t.LeafHead = IllegalID
	t.rebuildLinkedList(t.Root, &lastLeafID)

	// Rebuild free slot set
	t.rebuildFreeSlots()

	return t, nil
}

func createFreshCoW(dirPath, metaPath, nodePath string, fs vfs.FS) (*FlexTree, error) {
	//metaFD, err := fs.OpenFile(metaPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	metaFD, err := fs.Create(metaPath, vfs.WriteCategoryUnspecified)

	if err != nil {
		return nil, fmt.Errorf("cow: create FLEXTREE.COMMIT: %w", err)
	}
	//nodeFD, err := fs.OpenFile(nodePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	nodeFD, err := fs.Create(nodePath, vfs.WriteCategoryUnspecified)

	if err != nil {
		metaFD.Close()
		return nil, fmt.Errorf("cow: create FLEXTREE.PAGES: %w", err)
	}

	//vv("calling metaFD.Preallocate with: cowMetaAllocSize = %v", cowMetaAllocSize)

	// Pre-allocate FLEXTREE.COMMIT using Preallocate (real blocks, not sparse).
	// FLEXTREE.COMMIT is append-only: each commit writes a 64-byte record at the next
	// offset. The pre-allocation ensures the file size never changes during normal
	// operation, making SyncData safe and fast.
	if err := metaFD.Preallocate(0, cowMetaAllocSize); err != nil {
		metaFD.Close()
		nodeFD.Close()
		return nil, fmt.Errorf("cow: preallocate FLEXTREE.COMMIT: %w", err)
	}

	// Pre-allocate FLEXTREE.PAGES to initial capacity so pwrite
	// stays within the file and SyncData never needs to flush size metadata.
	initCap := int64(cowInitialSlotCap)
	//vv("calling nodeFD.Preallocate with: initCap*cowPageSize = %v", initCap*cowPageSize)

	//var err error
	//if fs.IsReal() {
	//	err = fallocateFile(metaFD, cowMetaAllocSize)
	// used to do:
	// f.Truncate(size)
	//}
	if err := nodeFD.Preallocate(0, initCap*cowPageSize); err != nil {
		metaFD.Close()
		nodeFD.Close()
		return nil, fmt.Errorf("cow: preallocate FLEXTREE.PAGES: %w", err)
	}

	// Sync both files to persist the pre-allocated sizes and blocks.
	if err := metaFD.Sync(); err != nil {
		metaFD.Close()
		nodeFD.Close()
		return nil, fmt.Errorf("cow: fsync FLEXTREE.COMMIT: %w", err)
	}
	if err := nodeFD.Sync(); err != nil {
		metaFD.Close()
		nodeFD.Close()
		return nil, fmt.Errorf("cow: fsync FLEXTREE.PAGES: %w", err)
	}

	t := NewFlexTree(fs)
	t.Path = dirPath
	t.metaFD = metaFD
	t.nodeFD = nodeFD
	t.maxSlotID = 0
	t.nodesFileCap = initCap
	t.cowEnabled = true
	t.metaFileCap = cowMetaAllocSize
	t.metaNextOff = 0 // first record written at offset 0

	// Create initial root leaf
	root := t.AllocLeaf()
	root.Dirty = true
	t.NodeCount++
	t.Root = root.NodeID
	t.LeafHead = root.NodeID

	// Write initial state (uses fdatasync - sizes are already stable)
	if err := t.SyncCoW(); err != nil {
		metaFD.Close()
		nodeFD.Close()
		return nil, fmt.Errorf("cow: initial sync: %w", err)
	}

	return t, nil
}

// loadNodeRec recursively loads a node and its children from the FLEXTREE.PAGES file.
func (t *FlexTree) loadNodeRec(nodeFD vfs.File, slotID int64) (NodeID, error) {
	if slotID < 0 {
		return IllegalID, nil
	}

	var pageBuf [cowPageSize]byte
	off := slotID * cowPageSize
	if _, err := nodeFD.ReadAt(pageBuf[:], off); err != nil {
		return IllegalID, fmt.Errorf("cow: read page at slot %d: %w", slotID, err)
	}

	switch pageBuf[0] {
	case pageTypeLeaf:
		le := t.AllocLeaf()
		if err := decodeLeafPage(&pageBuf, le); err != nil {
			return IllegalID, err
		}
		return le.NodeID, nil

	case pageTypeInternal:
		ie := t.AllocInternal()
		if err := decodeInternalPage(&pageBuf, ie); err != nil {
			return IllegalID, err
		}
		ieNodeID := ie.NodeID
		ieCount := ie.Count
		// Collect child slots and shifts before recursion may invalidate ie
		var childSlots [FLEXTREE_INTERNAL_CAP_PLUS_ONE]int64
		copy(childSlots[:], ie.ChildSlotIDs[:])

		// Recursively load children (each may reallocate arenas)
		for i := uint32(0); i <= ieCount; i++ {
			if childSlots[i] < 0 {
				continue
			}
			childNodeID, err := t.loadNodeRec(nodeFD, childSlots[i])
			if err != nil {
				return IllegalID, err
			}
			// Re-fetch ie after potential reallocation
			ie = t.GetInternal(ieNodeID)
			ie.Children[i].NodeID = childNodeID
		}
		return ieNodeID, nil

	default:
		return IllegalID, fmt.Errorf("cow: unknown page type 0x%02x at slot %d", pageBuf[0], slotID)
	}
}

// rebuildLinkedList does an in-order traversal to rebuild leaf prev/next pointers.
func (t *FlexTree) rebuildLinkedList(nodeID NodeID, lastLeafID *NodeID) {
	if nodeID.IsIllegal() {
		return
	}

	if nodeID.IsLeaf() {
		le := t.GetLeaf(nodeID)
		le.Prev = *lastLeafID
		le.Next = IllegalID
		if !lastLeafID.IsIllegal() {
			prev := t.GetLeaf(*lastLeafID)
			prev.Next = nodeID
		} else {
			t.LeafHead = nodeID
		}
		*lastLeafID = nodeID
		return
	}

	ie := t.GetInternal(nodeID)
	for i := uint32(0); i <= ie.Count; i++ {
		t.rebuildLinkedList(ie.Children[i].NodeID, lastLeafID)
	}
}

// rebuildFreeSlots scans all slots from 0..maxSlotID-1 and marks any not
// currently referenced by a live node as free.
func (t *FlexTree) rebuildFreeSlots() {
	if t.maxSlotID == 0 {
		return
	}
	used := make(map[int64]bool, len(t.LeafArena)+len(t.InternalArena))
	for i := range t.LeafArena {
		if !t.LeafArena[i].Freed && t.LeafArena[i].SlotID >= 0 {
			used[t.LeafArena[i].SlotID] = true
		}
	}
	for i := range t.InternalArena {
		if !t.InternalArena[i].Freed && t.InternalArena[i].SlotID >= 0 {
			used[t.InternalArena[i].SlotID] = true
		}
	}
	t.freeSlots = t.freeSlots[:0]
	for s := int64(0); s < t.maxSlotID; s++ {
		if !used[s] {
			t.freeSlots = append(t.freeSlots, s)
		}
	}
}

// CloseCoW syncs the tree and closes the file descriptors.
func (t *FlexTree) CloseCoW() error {
	if !t.cowEnabled {
		return nil
	}
	if err := t.SyncCoW(); err != nil {
		return err
	}
	err1 := t.nodeFD.Close()
	err2 := t.metaFD.Close()
	t.cowEnabled = false
	t.nodeFD = nil
	t.metaFD = nil
	if err1 != nil {
		return err1
	}
	return err2
}

// markAllDirty marks every node in the tree as dirty, useful for
// migrating a greenpack-loaded tree to CoW persistence.
func (t *FlexTree) markAllDirty() {
	for i := range t.LeafArena {
		if !t.LeafArena[i].Freed {
			t.LeafArena[i].Dirty = true
		}
	}
	for i := range t.InternalArena {
		if !t.InternalArena[i].Freed {
			t.InternalArena[i].Dirty = true
		}
	}
}
