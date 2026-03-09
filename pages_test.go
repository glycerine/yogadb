package yogadb

import (
	"fmt"
	//"os"
	"path/filepath"
	"testing"

	"github.com/glycerine/vfs"
)

// TestCoW_PageRoundTrip tests leaf and internal page encode/decode.
func TestCoW_PageRoundTrip(t *testing.T) {
	// Leaf page round-trip
	le := initLeafValue
	le.NodeID = NewLeafID(42)
	le.SlotID = 7
	le.Count = 3
	le.Extents[0] = FlexTreeExtent{Loff: 0, Len: 100}
	le.Extents[0].SetPoff(1000)
	le.Extents[0].SetTag(5)
	le.Extents[1] = FlexTreeExtent{Loff: 100, Len: 200}
	le.Extents[1].SetPoff(2000)
	le.Extents[2] = FlexTreeExtent{Loff: 300, Len: 50}
	le.Extents[2].SetPoff(3000)
	le.Extents[2].SetTag(99)

	var pageBuf [cowPageSize]byte
	encodeLeafPage(&pageBuf, &le)

	var le2 LeafNode
	le2.NodeID = NewLeafID(42) // NodeID set by arena, not page
	if err := decodeLeafPage(&pageBuf, &le2); err != nil {
		t.Fatalf("decodeLeafPage: %v", err)
	}
	if le2.Count != le.Count {
		t.Fatalf("Count: got %d, want %d", le2.Count, le.Count)
	}
	if le2.SlotID != le.SlotID {
		t.Fatalf("SlotID: got %d, want %d", le2.SlotID, le.SlotID)
	}
	for i := uint32(0); i < le.Count; i++ {
		if le2.Extents[i] != le.Extents[i] {
			t.Fatalf("Extent[%d]: got %+v, want %+v", i, le2.Extents[i], le.Extents[i])
		}
	}

	// Internal page round-trip
	ie := initInternalValue
	ie.NodeID = NewInternalID(10)
	ie.SlotID = 3
	ie.Count = 2
	ie.Pivots[0] = 1000
	ie.Pivots[1] = 2000
	ie.Children[0].Shift = 0
	ie.Children[1].Shift = -50
	ie.Children[2].Shift = 100
	ie.ChildSlotIDs[0] = 0
	ie.ChildSlotIDs[1] = 1
	ie.ChildSlotIDs[2] = 2

	encodeInternalPage(&pageBuf, &ie)

	var ie2 InternalNode
	ie2.NodeID = NewInternalID(10)
	if err := decodeInternalPage(&pageBuf, &ie2); err != nil {
		t.Fatalf("decodeInternalPage: %v", err)
	}
	if ie2.Count != ie.Count {
		t.Fatalf("Count: got %d, want %d", ie2.Count, ie.Count)
	}
	if ie2.SlotID != ie.SlotID {
		t.Fatalf("SlotID: got %d, want %d", ie2.SlotID, ie.SlotID)
	}
	for i := 0; i < FLEXTREE_INTERNAL_CAP; i++ {
		if ie2.Pivots[i] != ie.Pivots[i] {
			t.Fatalf("Pivots[%d]: got %d, want %d", i, ie2.Pivots[i], ie.Pivots[i])
		}
	}
	for i := 0; i < FLEXTREE_INTERNAL_CAP_PLUS_ONE; i++ {
		if ie2.Children[i].Shift != ie.Children[i].Shift {
			t.Fatalf("Shift[%d]: got %d, want %d", i, ie2.Children[i].Shift, ie.Children[i].Shift)
		}
		if ie2.ChildSlotIDs[i] != ie.ChildSlotIDs[i] {
			t.Fatalf("ChildSlotIDs[%d]: got %d, want %d", i, ie2.ChildSlotIDs[i], ie.ChildSlotIDs[i])
		}
	}
}

// TestCoW_MetaRoundTrip tests FLEXTREE.COMMIT header encode/decode.
func TestCoW_MetaRoundTrip(t *testing.T) {
	m := cowMeta{
		FormatMajor:   3,
		FormatMinor:   0,
		Version:       42,
		RootSlotID:    7,
		MaxSlotID:     100,
		NodeCount:     50,
		MaxLoff:       999999,
		MaxExtentSz:   128 << 10,
		NodesFileCap:  512,
		LiveKeys:      1234,
		LiveBigKeys:   100,
		LiveSmallKeys: 1134,
	}
	var buf [cowMetaSize]byte
	encodeCowMeta(&buf, &m)

	m2, err := decodeCowMeta(&buf)
	if err != nil {
		t.Fatalf("decodeCowMeta: %v", err)
	}
	if *m2 != m {
		t.Fatalf("got %+v, want %+v", *m2, m)
	}

	// Corrupt a byte and verify CRC detects it
	buf[20] ^= 0xff
	_, err = decodeCowMeta(&buf)
	if err == nil {
		t.Fatal("expected CRC error on corrupted FLEXTREE.COMMIT")
	}
}

// TestCoW_PageCRCDetectsCorruption verifies CRC catches corruption.
func TestCoW_PageCRCDetectsCorruption(t *testing.T) {
	le := initLeafValue
	le.NodeID = NewLeafID(0)
	le.SlotID = 0
	le.Count = 1
	le.Extents[0] = FlexTreeExtent{Loff: 0, Len: 10}
	le.Extents[0].SetPoff(100)

	var buf [cowPageSize]byte
	encodeLeafPage(&buf, &le)

	// Corrupt data area
	buf[20] ^= 0xff
	var le2 LeafNode
	err := decodeLeafPage(&buf, &le2)
	if err == nil {
		t.Fatal("expected CRC error on corrupted leaf page")
	}
}

// TestCoW_FreshTreeCreateAndReopen tests creating and reopening a CoW tree.
func TestCoW_FreshTreeCreateAndReopen(t *testing.T) {

	fs, dir := newTestFS(t)

	// Create fresh tree, insert data, sync, close
	tree, err := OpenFlexTreeCoW(dir, fs)
	if err != nil {
		t.Fatalf("OpenFlexTreeCoW: %v", err)
	}

	tree.Insert(0, 1000, 100)
	tree.Insert(100, 2000, 200)
	tree.Insert(300, 3000, 50)

	if err := tree.SyncCoW(); err != nil {
		t.Fatalf("SyncCoW: %v", err)
	}

	expectedMaxLoff := tree.MaxLoff

	if err := tree.CloseCoW(); err != nil {
		t.Fatalf("CloseCoW: %v", err)
	}
	// CloseCoW calls SyncCoW which bumps version again
	expectedVersion := tree.PersistentVersion

	// Verify files exist
	if _, err := fs.Stat(filepath.Join(dir, "FLEXTREE.COMMIT")); err != nil {
		t.Fatalf("FLEXTREE.COMMIT file missing: %v", err)
	}
	if _, err := fs.Stat(filepath.Join(dir, "FLEXTREE.PAGES")); err != nil {
		t.Fatalf("FLEXTREE.PAGES file missing: %v", err)
	}

	// Reopen and verify
	tree2, err := OpenFlexTreeCoW(dir, fs)
	if err != nil {
		t.Fatalf("reopen OpenFlexTreeCoW: %v", err)
	}
	defer tree2.CloseCoW()

	if tree2.MaxLoff != expectedMaxLoff {
		t.Fatalf("MaxLoff: got %d, want %d", tree2.MaxLoff, expectedMaxLoff)
	}
	if tree2.PersistentVersion != expectedVersion {
		t.Fatalf("Version: got %d, want %d", tree2.PersistentVersion, expectedVersion)
	}

	// Verify point queries
	p1 := tree2.PQuery(0)
	if p1 != 1000 {
		t.Fatalf("PQuery(0): got %d, want 1000", p1)
	}
	p2 := tree2.PQuery(100)
	if p2 != 2000 {
		t.Fatalf("PQuery(100): got %d, want 2000", p2)
	}
	p3 := tree2.PQuery(300)
	if p3 != 3000 {
		t.Fatalf("PQuery(300): got %d, want 3000", p3)
	}
}

// TestCoW_InsertDeleteReopen tests insert+delete then reopen.
func TestCoW_InsertDeleteReopen(t *testing.T) {

	fs, dir := newTestFS(t)

	tree, err := OpenFlexTreeCoW(dir, fs)
	if err != nil {
		t.Fatalf("OpenFlexTreeCoW: %v", err)
	}

	// Insert several extents
	for i := uint64(0); i < 100; i++ {
		tree.Insert(i*10, i*1000+500, 10)
	}
	tree.SyncCoW()

	// Delete some
	tree.Delete(50, 30)
	tree.SyncCoW()

	maxLoff := tree.MaxLoff
	tree.CloseCoW()

	// Reopen and verify
	tree2, err := OpenFlexTreeCoW(dir, fs)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer tree2.CloseCoW()

	if tree2.MaxLoff != maxLoff {
		t.Fatalf("MaxLoff: got %d, want %d", tree2.MaxLoff, maxLoff)
	}

	// First extent should still be at poff 500
	p := tree2.PQuery(0)
	if p != 500 {
		t.Fatalf("PQuery(0): got %d, want 500", p)
	}
}

// TestCoW_DirtyOnlyWrite verifies that SyncCoW only writes dirty nodes.
func TestCoW_DirtyOnlyWrite(t *testing.T) {
	fs, dir := newTestFS(t)

	tree, err := OpenFlexTreeCoW(dir, fs)
	if err != nil {
		t.Fatalf("OpenFlexTreeCoW: %v", err)
	}

	// Insert enough to create multiple leaves (cap=60)
	for i := uint64(0); i < 200; i++ {
		tree.Insert(i*10, i*1000+500, 10)
	}
	if err := tree.SyncCoW(); err != nil {
		t.Fatalf("SyncCoW: %v", err)
	}

	// Record FLEXTREE.PAGES file size
	nodesPath := filepath.Join(dir, "FLEXTREE.PAGES")
	info1, _ := fs.Stat(nodesPath)
	size1 := info1.Size()

	// Verify no nodes are dirty after sync
	for i := range tree.LeafArena {
		if !tree.LeafArena[i].Freed && tree.LeafArena[i].Dirty {
			t.Fatalf("leaf %d still dirty after sync", i)
		}
	}
	for i := range tree.InternalArena {
		if !tree.InternalArena[i].Freed && tree.InternalArena[i].Dirty {
			t.Fatalf("internal %d still dirty after sync", i)
		}
	}

	// Insert one more extent (dirties only the path to one leaf)
	tree.Insert(2000, 99999, 10)
	if err := tree.SyncCoW(); err != nil {
		t.Fatalf("SyncCoW after single insert: %v", err)
	}

	// FLEXTREE.PAGES file should have grown by a small number of pages, not all
	info2, _ := fs.Stat(nodesPath)
	size2 := info2.Size()

	// At most we should have written root path (depth) + 1 new pages
	// which adds at most a few KB to the file
	growth := size2 - size1
	maxExpectedGrowth := int64(FLEXTREE_PATH_DEPTH+1) * cowPageSize
	if growth > maxExpectedGrowth {
		t.Fatalf("FLEXTREE.PAGES grew by %d bytes (max expected %d), suggesting too many pages written",
			growth, maxExpectedGrowth)
	}

	tree.CloseCoW()
}

// TestCoW_ManyInsertsVsBruteForce does a randomized test like the FlexTree tests.
func TestCoW_ManyInsertsVsBruteForce(t *testing.T) {
	fs, dir := newTestFS(t)

	tree, err := OpenFlexTreeCoW(dir, fs)
	if err != nil {
		t.Fatalf("OpenFlexTreeCoW: %v", err)
	}

	bf := openBruteForce(tree.MaxExtentSize)

	resetRand()
	n := 5000
	for i := 0; i < n; i++ {
		loff := uint64(randInt()) % (tree.GetMaxLoff() + 1)
		poff := uint64(randInt())
		length := uint32(randInt()%1000 + 1)

		tree.Insert(loff, poff, length)
		bf.Insert(loff, poff, length)
	}

	// Sync and close
	tree.SyncCoW()
	tree.CloseCoW()

	// Reopen
	tree2, err := OpenFlexTreeCoW(dir, fs)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer tree2.CloseCoW()

	// Verify all point queries match BruteForce
	resetRand()
	for i := 0; i < 1000; i++ {
		loff := uint64(randInt()) % tree2.GetMaxLoff()
		got := tree2.PQuery(loff)
		want := bf.PQuery(loff)
		if got != want {
			t.Fatalf("PQuery(%d): got %d, want %d (iter %d)", loff, got, want, i)
		}
	}
}

// TestCoW_MultipleReopenCycles tests multiple sync/close/reopen cycles.
func TestCoW_MultipleReopenCycles(t *testing.T) {
	fs, dir := newTestFS(t)

	bf := openBruteForce(DefaultFlexTreeMaxExtentSizeLimit)

	for cycle := 0; cycle < 5; cycle++ {
		tree, err := OpenFlexTreeCoW(dir, fs)
		if err != nil {
			t.Fatalf("cycle %d open: %v", cycle, err)
		}

		// Insert some data each cycle
		for i := 0; i < 100; i++ {
			loff := tree.GetMaxLoff()
			poff := uint64(cycle*10000 + i*100)
			tree.Insert(loff, poff, 10)
			bf.Insert(loff, poff, 10)
		}

		if err := tree.CloseCoW(); err != nil {
			t.Fatalf("cycle %d close: %v", cycle, err)
		}
	}

	// Final open and verify
	tree, err := OpenFlexTreeCoW(dir, fs)
	if err != nil {
		t.Fatalf("final open: %v", err)
	}
	defer tree.CloseCoW()

	if tree.MaxLoff != bf.GetMaxLoff() {
		t.Fatalf("MaxLoff: got %d, want %d", tree.MaxLoff, bf.GetMaxLoff())
	}

	// Check some point queries
	for loff := uint64(0); loff < tree.MaxLoff && loff < 2000; loff += 5 {
		got := tree.PQuery(loff)
		want := bf.PQuery(loff)
		if got != want {
			t.Fatalf("PQuery(%d): got %d, want %d", loff, got, want)
		}
	}
}

// TestCoW_EmptyTree tests that an empty tree can be synced and reopened.
func TestCoW_EmptyTree(t *testing.T) {
	fs, dir := newTestFS(t)

	tree, err := OpenFlexTreeCoW(dir, fs)
	if err != nil {
		t.Fatalf("OpenFlexTreeCoW: %v", err)
	}

	if err := tree.CloseCoW(); err != nil {
		t.Fatalf("CloseCoW: %v", err)
	}

	// Reopen
	tree2, err := OpenFlexTreeCoW(dir, fs)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer tree2.CloseCoW()

	if tree2.MaxLoff != 0 {
		t.Fatalf("MaxLoff: got %d, want 0", tree2.MaxLoff)
	}
}

// TestCoW_LargeTreeSplit tests that trees requiring splits work correctly.
func TestCoW_LargeTreeSplit(t *testing.T) {
	fs, dir := newTestFS(t)

	tree, err := OpenFlexTreeCoW(dir, fs)
	if err != nil {
		t.Fatalf("OpenFlexTreeCoW: %v", err)
	}

	bf := openBruteForce(tree.MaxExtentSize)

	// Insert enough to cause multiple splits (>60 unique extents)
	for i := uint64(0); i < 300; i++ {
		tree.Insert(i*100, i*1000+1, 100)
		bf.Insert(i*100, i*1000+1, 100)
	}

	tree.SyncCoW()
	tree.CloseCoW()

	// Reopen
	tree2, err := OpenFlexTreeCoW(dir, fs)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer tree2.CloseCoW()

	// Verify every extent
	for i := uint64(0); i < 300; i++ {
		loff := i * 100
		got := tree2.PQuery(loff)
		want := bf.PQuery(loff)
		if got != want {
			t.Fatalf("PQuery(%d): got %d, want %d", loff, got, want)
		}
	}
}

// TestCoW_WithTags tests that tags survive persistence.
func TestCoW_WithTags(t *testing.T) {
	fs, dir := newTestFS(t)

	tree, err := OpenFlexTreeCoW(dir, fs)
	if err != nil {
		t.Fatalf("OpenFlexTreeCoW: %v", err)
	}

	tree.InsertWTag(0, 1000, 100, 42)
	tree.InsertWTag(100, 2000, 100, 99)
	tree.SetTag(50, 7)

	tree.CloseCoW()

	// Reopen
	tree2, err := OpenFlexTreeCoW(dir, fs)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer tree2.CloseCoW()

	tag1, _ := tree2.GetTag(0)
	if tag1 != 42 {
		t.Fatalf("GetTag(0): got %d, want 42", tag1)
	}

	tag2, _ := tree2.GetTag(50)
	if tag2 != 7 {
		t.Fatalf("GetTag(50): got %d, want 7", tag2)
	}

	tag3, _ := tree2.GetTag(100)
	if tag3 != 99 {
		t.Fatalf("GetTag(100): got %d, want 99", tag3)
	}
}

// TestCoW_PreAllocation verifies that FLEXTREE.PAGES and FLEXTREE.COMMIT are pre-allocated
// and that file sizes remain stable across syncs (enabling fdatasync).
func TestCoW_PreAllocation(t *testing.T) {
	fs, dir := newTestFS(t)

	tree, err := OpenFlexTreeCoW(dir, fs)
	if err != nil {
		t.Fatalf("OpenFlexTreeCoW: %v", err)
	}

	nodesPath := filepath.Join(dir, "FLEXTREE.PAGES")
	metaPath := filepath.Join(dir, "FLEXTREE.COMMIT")

	// FLEXTREE.COMMIT must be pre-allocated to 16 MB
	metaInfo, _ := fs.Stat(metaPath)
	if metaInfo.Size() != cowMetaAllocSize {
		t.Fatalf("FLEXTREE.COMMIT size: got %d, want %d", metaInfo.Size(), cowMetaAllocSize)
	}

	// FLEXTREE.PAGES must be pre-allocated to initial capacity
	nodesInfo, _ := fs.Stat(nodesPath)
	expectedNodesSize := int64(cowInitialSlotCap) * cowPageSize
	if nodesInfo.Size() != expectedNodesSize {
		t.Fatalf("FLEXTREE.PAGES initial size: got %d, want %d", nodesInfo.Size(), expectedNodesSize)
	}

	// Insert data and sync several times — FLEXTREE.PAGES size should not change
	// (256 slots is more than enough for a few hundred nodes)
	for round := 0; round < 5; round++ {
		for i := 0; i < 20; i++ {
			tree.Insert(tree.GetMaxLoff(), uint64(round*1000+i), 10)
		}
		tree.SyncCoW()
	}

	nodesInfo2, _ := fs.Stat(nodesPath)
	if nodesInfo2.Size() != expectedNodesSize {
		t.Fatalf("FLEXTREE.PAGES size changed after inserts: got %d, want %d",
			nodesInfo2.Size(), expectedNodesSize)
	}

	// FLEXTREE.COMMIT size must remain 16 MB (pre-allocated, never grows)
	metaInfo2, _ := fs.Stat(metaPath)
	if metaInfo2.Size() != cowMetaAllocSize {
		t.Fatalf("FLEXTREE.COMMIT size changed: got %d, want %d", metaInfo2.Size(), cowMetaAllocSize)
	}

	// nodesFileCap should be preserved through close/reopen
	capBefore := tree.nodesFileCap
	tree.CloseCoW()

	tree2, err := OpenFlexTreeCoW(dir, fs)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer tree2.CloseCoW()

	if tree2.nodesFileCap != capBefore {
		t.Fatalf("nodesFileCap: got %d, want %d", tree2.nodesFileCap, capBefore)
	}
}

// TestCoW_GrowNodesFile verifies that the FLEXTREE.PAGES file grows correctly
// when more slots are needed than the current capacity.
func TestCoW_GrowNodesFile(t *testing.T) {
	fs, dir := newTestFS(t)

	tree, err := OpenFlexTreeCoW(dir, fs)
	if err != nil {
		t.Fatalf("OpenFlexTreeCoW: %v", err)
	}

	initialCap := tree.nodesFileCap
	if initialCap != cowInitialSlotCap {
		t.Fatalf("initial cap: got %d, want %d", initialCap, cowInitialSlotCap)
	}

	// Insert enough unique extents to exceed cowInitialSlotCap nodes.
	// Each leaf holds 60 extents; with 256 initial slots, we need
	// more than 256 leaf+internal nodes. ~260 leaves × 60 = 15600 extents.
	// Use unique poffs so extents don't merge.
	for i := uint64(0); i < 16000; i++ {
		tree.Insert(i*10, i*1000+1, 10)
	}
	if err := tree.SyncCoW(); err != nil {
		t.Fatalf("SyncCoW: %v", err)
	}

	if tree.nodesFileCap <= initialCap {
		t.Fatalf("capacity should have grown: got %d, initial was %d",
			tree.nodesFileCap, initialCap)
	}

	nodesPath := filepath.Join(dir, "FLEXTREE.PAGES")
	info, _ := fs.Stat(nodesPath)
	expectedSize := tree.nodesFileCap * cowPageSize
	if info.Size() != expectedSize {
		t.Fatalf("FLEXTREE.PAGES file size: got %d, want %d", info.Size(), expectedSize)
	}

	// Close and reopen — verify data survived the growth
	tree.CloseCoW()

	tree2, err := OpenFlexTreeCoW(dir, fs)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer tree2.CloseCoW()

	p := tree2.PQuery(0)
	if p != 1 {
		t.Fatalf("PQuery(0): got %d, want 1", p)
	}
	p = tree2.PQuery(100)
	if p != 10001 {
		t.Fatalf("PQuery(100): got %d, want 10001", p)
	}
}

// TestCoW_MetaAppendOnly verifies the append-only FLEXTREE.COMMIT behavior:
// multiple syncs produce multiple records, and recovery finds the latest.
func TestCoW_MetaAppendOnly(t *testing.T) {
	fs, dir := newTestFS(t)

	tree, err := OpenFlexTreeCoW(dir, fs)
	if err != nil {
		t.Fatalf("OpenFlexTreeCoW: %v", err)
	}

	// The initial SyncCoW in createFreshCoW wrote the first record.
	// Do several more syncs.
	for i := 0; i < 10; i++ {
		tree.Insert(tree.GetMaxLoff(), uint64(i*1000+1), 10)
		if err := tree.SyncCoW(); err != nil {
			t.Fatalf("SyncCoW #%d: %v", i, err)
		}
	}

	// metaNextOff should have advanced: 1 initial + 10 syncs = 11 records
	expectedOff := int64(11) * cowMetaSize
	if tree.metaNextOff != expectedOff {
		t.Fatalf("metaNextOff: got %d, want %d", tree.metaNextOff, expectedOff)
	}

	// CloseCoW calls SyncCoW, which appends one more record
	tree.CloseCoW()
	latestVersion := tree.PersistentVersion

	// Reopen — should find the latest record via scan
	tree2, err := OpenFlexTreeCoW(dir, fs)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer tree2.CloseCoW()

	if tree2.PersistentVersion != latestVersion {
		t.Fatalf("Version after reopen: got %d, want %d",
			tree2.PersistentVersion, latestVersion)
	}
}

// TestCoW_MetaTornWriteRecovery simulates a torn FLEXTREE.COMMIT write and verifies
// that recovery falls back to the previous valid record.
func TestCoW_MetaTornWriteRecovery(t *testing.T) {

	fs, dir := newTestFS(t)

	tree, err := OpenFlexTreeCoW(dir, fs)
	if err != nil {
		t.Fatalf("OpenFlexTreeCoW: %v", err)
	}

	// Insert some data and sync a few times to build up FLEXTREE.COMMIT records
	for i := 0; i < 5; i++ {
		tree.Insert(tree.GetMaxLoff(), uint64(i*1000+1), 10)
		tree.SyncCoW()
	}

	// Close writes one more record
	tree.CloseCoW()
	// After close: createFreshCoW initial sync (v1) + 5 syncs + close sync = 7 records
	// Version is 7, last record is at offset 6*64 = 384

	// Corrupt the last two FLEXTREE.COMMIT records (the close + the last explicit sync)
	metaPath := filepath.Join(dir, "FLEXTREE.COMMIT")
	//f, err := os.OpenFile(metaPath, os.O_RDWR, 0644)
	f, err := fs.OpenReadWrite(metaPath, vfs.WriteCategoryUnspecified)

	if err != nil {
		t.Fatalf("open FLEXTREE.COMMIT for corruption: %v", err)
	}
	garbage := make([]byte, cowMetaSize)
	for i := range garbage {
		garbage[i] = 0xDE
	}
	// Corrupt records at slot 5 (version 6) and slot 6 (version 7)
	f.WriteAt(garbage, 5*cowMetaSize)
	f.WriteAt(garbage, 6*cowMetaSize)
	f.Sync()
	f.Close()

	// Reopen — should recover to version 5 (the last uncorrupted record)
	tree2, err := OpenFlexTreeCoW(dir, fs)
	if err != nil {
		t.Fatalf("reopen after corruption: %v", err)
	}
	defer tree2.CloseCoW()

	if tree2.PersistentVersion != 5 {
		t.Fatalf("recovered version: got %d, want 5", tree2.PersistentVersion)
	}
}

// ======================== FlexSpace + CoW integration tests ========================

func mustOpenCoW(t *testing.T, path string, fs vfs.FS) *FlexSpace {
	t.Helper()
	ff, err := OpenFlexSpaceCoW(path, false, fs)
	if err != nil {
		t.Fatalf("OpenFlexSpaceCoW(%q): %v", path, err)
	}
	return ff
}

// TestCoW_FlexSpacePersistence tests insert + sync + close + reopen via CoW.
func TestCoW_FlexSpacePersistence(t *testing.T) {
	fs, dir := newTestFS(t)

	ff := mustOpenCoW(t, dir, fs)
	mustInsert(t, ff, "hello cow", 0)
	ff.Sync()
	ff.Close()

	ff2 := mustOpenCoW(t, dir, fs)
	defer ff2.Close()
	checkContent(t, ff2, "hello cow")
}

// TestCoW_FlexSpaceMultipleCycles tests multiple open/close cycles with CoW.
func TestCoW_FlexSpaceMultipleCycles(t *testing.T) {
	fs, dir := newTestFS(t)

	// Cycle 1
	ff := mustOpenCoW(t, dir, fs)
	mustInsert(t, ff, "aaa", 0)
	ff.Close()

	// Cycle 2
	ff2 := mustOpenCoW(t, dir, fs)
	mustInsert(t, ff2, "bbb", ff2.Size())
	ff2.Close()

	// Cycle 3: verify
	ff3 := mustOpenCoW(t, dir, fs)
	defer ff3.Close()
	checkContent(t, ff3, "aaabbb")
}

// TestCoW_FlexSpaceCrashRecovery tests log replay with CoW persistence.
func TestCoW_FlexSpaceCrashRecovery(t *testing.T) {
	fs, dir := newTestFS(t)

	// Phase 1: write and close cleanly
	ff := mustOpenCoW(t, dir, fs)
	mustInsert(t, ff, "checkpoint data", 0)
	ff.Sync()
	ff.Close()

	// Phase 2: reopen, write more, sync log, but don't close cleanly
	ff2 := mustOpenCoW(t, dir, fs)
	mustInsert(t, ff2, " plus more", ff2.Size())
	ff2.Sync() // flush log but not tree checkpoint

	// Phase 3: reopen — log should be replayed
	ff3 := mustOpenCoW(t, dir, fs)
	defer ff3.Close()
	checkContent(t, ff3, "checkpoint data plus more")
}

// TestCoW_FlexSpaceLargeData tests writing more data than a single extent.
func TestCoW_FlexSpaceLargeData(t *testing.T) {
	fs, dir := newTestFS(t)

	ff := mustOpenCoW(t, dir, fs)

	// Write 1MB of data
	data := make([]byte, 1<<20)
	for i := range data {
		data[i] = byte(i % 256)
	}
	n, err := ff.Insert(data, 0, uint64(len(data)))
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if n != len(data) {
		t.Fatalf("Insert returned %d, want %d", n, len(data))
	}
	ff.Sync()
	ff.Close()

	// Reopen and verify
	ff2 := mustOpenCoW(t, dir, fs)
	defer ff2.Close()

	if ff2.Size() != uint64(len(data)) {
		t.Fatalf("Size: got %d, want %d", ff2.Size(), len(data))
	}

	buf := make([]byte, len(data))
	rn, err := ff2.Read(buf, 0, uint64(len(data)))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if rn != len(data) {
		t.Fatalf("Read returned %d, want %d", rn, len(data))
	}
	for i := range data {
		if buf[i] != data[i] {
			t.Fatalf("byte %d: got %d, want %d", i, buf[i], data[i])
		}
	}
}

// ======================== Benchmarks: CoW vs Greenpack ========================

// countDirtyNodes counts how many nodes have Dirty==true in the tree.
func countDirtyNodes(tree *FlexTree) int {
	n := 0
	for i := range tree.LeafArena {
		if !tree.LeafArena[i].Freed && tree.LeafArena[i].Dirty {
			n++
		}
	}
	for i := range tree.InternalArena {
		if !tree.InternalArena[i].Freed && tree.InternalArena[i].Dirty {
			n++
		}
	}
	return n
}

// benchInsertRandom populates a tree using the same random-insert pattern as
// flextree_test.go (inserts at random positions within the first 1000 logical
// offsets). This creates many non-mergeable extents and forces node splits,
// resulting in a tree with many leaf and internal nodes.
func benchInsertRandom(tree *FlexTree, count uint64) {
	resetRand()
	tree.Insert(0, 0, 4)
	var maxLoff uint64 = 4
	for i := uint64(0); i < count; i++ {
		length := uint32(randInt()%1000 + 1)
		tree.InsertWTag(maxLoff%1000, maxLoff, length, uint16(i%0xffff))
		maxLoff += uint64(length)
	}
}

// BenchmarkSyncCoW measures the cost of CoW persistence after a single
// modification to a large tree. CoW only writes dirty nodes (the root-to-leaf
// path), so it should be O(log N) pages regardless of tree size.
//
// Reported metrics:
//   - ns/op:               wall-clock time per sync
//   - bytes_written/op:    bytes of node data actually written to disk
//   - dirty_nodes/op:      number of dirty nodes written per sync
//   - total_nodes/op:      total nodes in the tree (for context)
func BenchmarkSyncCoW(b *testing.B) {
	for _, insertCount := range []uint64{1000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("inserts_%d", insertCount), func(b *testing.B) {
			dir := b.TempDir()

			// Build the tree with random inserts via CoW.
			cowTree, err := OpenFlexTreeCoW(dir, vfs.Default)
			if err != nil {
				b.Fatalf("OpenFlexTreeCoW: %v", err)
			}
			benchInsertRandom(cowTree, insertCount)

			// Initial full sync (all nodes dirty).
			if err := cowTree.SyncCoW(); err != nil {
				b.Fatalf("initial SyncCoW: %v", err)
			}
			totalNodes := int(cowTree.NodeCount)

			b.ResetTimer()
			b.ReportAllocs()

			var totalBytesWritten int64
			var totalDirty int64

			for i := 0; i < b.N; i++ {
				// Single-point modification: insert a small extent at a
				// random position. This dirties only the root-to-leaf path.
				loff := uint64(i%500) * 2
				cowTree.Insert(loff, uint64(i*1000+1), 1)

				dirty := countDirtyNodes(cowTree)

				if err := cowTree.SyncCoW(); err != nil {
					b.Fatalf("SyncCoW iter %d: %v", i, err)
				}

				bytesWritten := int64(dirty)*cowPageSize + cowMetaSize
				totalBytesWritten += bytesWritten
				totalDirty += int64(dirty)
			}

			b.StopTimer()

			b.ReportMetric(float64(totalBytesWritten)/float64(b.N), "bytes_written/op")
			b.ReportMetric(float64(totalDirty)/float64(b.N), "dirty_nodes/op")
			b.ReportMetric(float64(totalNodes), "total_nodes/op")

			cowTree.CloseCoW()
		})
	}
}

/*
// BenchmarkSyncGreenpack measures the cost of greenpack (full-tree)
// persistence after a single modification to a large tree. Greenpack
// always serializes every node, so cost is O(N) regardless of how
// many nodes changed.
//
// Reported metrics:
//   - ns/op:               wall-clock time per sync
//   - bytes_written/op:    bytes written to disk (entire serialized tree)
//   - total_nodes/op:      total nodes in the tree (for context)
func BenchmarkSyncGreenpack(b *testing.B) {
	for _, insertCount := range []uint64{1000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("inserts_%d", insertCount), func(b *testing.B) {
			dir := b.TempDir()
			fn := filepath.Join(dir, "tree.dat")

			// Build the tree with same random-insert pattern.
			tree := OpenFlexTree(vfs.Default, "")
			benchInsertRandom(tree, insertCount)

			// Initial save.
			if err := saveFlexTree(tree, fn); err != nil {
				b.Fatalf("initial saveFlexTree: %v", err)
			}
			totalNodes := int(tree.NodeCount)

			b.ResetTimer()
			b.ReportAllocs()

			var totalBytesWritten int64

			for i := 0; i < b.N; i++ {
				// Same single-point modification as the CoW benchmark.
				loff := uint64(i%500) * 2
				tree.Insert(loff, uint64(i*1000+1), 1)

				if err := saveFlexTree(tree, fn); err != nil {
					b.Fatalf("saveFlexTree iter %d: %v", i, err)
				}

				fi, err := os.Stat(fn)
				if err != nil {
					b.Fatalf("stat: %v", err)
				}
				totalBytesWritten += fi.Size()
			}

			b.StopTimer()

			b.ReportMetric(float64(totalBytesWritten)/float64(b.N), "bytes_written/op")
			b.ReportMetric(float64(totalNodes), "total_nodes/op")
		})
	}
}
*/
