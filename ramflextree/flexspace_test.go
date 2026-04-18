package ramflextree

// flexspace_test.go - tests for the FlexSpace implementation (RAM-only).

import (
	"testing"
)

// mustOpenRAM creates a new in-memory FlexSpace or fails the test.
func mustOpenRAM(t *testing.T) *FlexSpace {
	t.Helper()
	return NewFlexSpace()
}

// mustRead reads exactly len bytes at loff and returns the result.
func mustRead(t *testing.T, ff *FlexSpace, loff, length uint64) []byte {
	t.Helper()
	buf := make([]byte, length)
	n, err := ff.Read(buf, loff, length)
	if err != nil {
		t.Fatalf("Read(loff=%d, len=%d): %v", loff, length, err)
	}
	if n != int(length) {
		t.Fatalf("Read: got %d bytes, want %d", n, length)
	}
	return buf
}

// mustInsert inserts data at loff.
func mustInsert(t *testing.T, ff *FlexSpace, data string, loff uint64) {
	t.Helper()
	n, err := ff.Insert([]byte(data), loff, uint64(len(data)))
	if err != nil {
		t.Fatalf("Insert(%q, loff=%d): %v", data, loff, err)
	}
	if n != len(data) {
		t.Fatalf("Insert: got %d, want %d", n, len(data))
	}
}

// mustWrite writes data at loff (overwrite/extend semantics).
func mustWrite(t *testing.T, ff *FlexSpace, data string, loff uint64) {
	t.Helper()
	n, err := ff.Write([]byte(data), loff, uint64(len(data)))
	if err != nil {
		t.Fatalf("Write(%q, loff=%d): %v", data, loff, err)
	}
	if n != len(data) {
		t.Fatalf("Write: got %d, want %d", n, len(data))
	}
}

// checkContent reads and asserts the content of the FlexSpace.
func checkContent(t *testing.T, ff *FlexSpace, want string) {
	t.Helper()
	size := ff.Size()
	if size != uint64(len(want)) {
		t.Errorf("size: got %d, want %d", size, len(want))
		return
	}
	if size == 0 {
		return
	}
	got := mustRead(t, ff, 0, size)
	if string(got) != want {
		t.Errorf("content: got %q, want %q", got, want)
	}
}

// ======================== Basic Insert / Read ========================

func TestFlexspace_InsertRead(t *testing.T) {
	ff := mustOpenRAM(t)
	defer ff.Close()

	// Insert "hello" at offset 0
	mustInsert(t, ff, "hello", 0)
	checkContent(t, ff, "hello")

	// Insert "XY" at offset 2 (shifts "llo" right)
	mustInsert(t, ff, "XY", 2)
	checkContent(t, ff, "heXYllo")

	// Insert "!" at the end (= append)
	mustInsert(t, ff, "!", uint64(len("heXYllo")))
	checkContent(t, ff, "heXYllo!")
}

// ======================== Collapse ========================

func TestFlexspace_Collapse(t *testing.T) {
	ff := mustOpenRAM(t)
	defer ff.Close()

	mustInsert(t, ff, "abcdefgh", 0)
	checkContent(t, ff, "abcdefgh")

	// Collapse bytes [2..5) = "cde"
	if err := ff.Collapse(2, 3); err != nil {
		t.Fatalf("Collapse: %v", err)
	}
	checkContent(t, ff, "abfgh")

	// Collapse from start
	if err := ff.Collapse(0, 2); err != nil {
		t.Fatalf("Collapse: %v", err)
	}
	checkContent(t, ff, "fgh")

	// Collapse to empty
	if err := ff.Collapse(0, 3); err != nil {
		t.Fatalf("Collapse: %v", err)
	}
	if ff.Size() != 0 {
		t.Errorf("size after full collapse: got %d, want 0", ff.Size())
	}
}

// ======================== Write (POSIX-like) ========================

// TestFlexspace_WriteOverwrite verifies that Write(buf, loff, len) when
// loff+len <= size behaves like an in-place update (collapse+insert of same len).
func TestFlexspace_WriteOverwrite(t *testing.T) {
	ff := mustOpenRAM(t)
	defer ff.Close()

	// Set up "abcdefgh" via inserts
	mustInsert(t, ff, "abcdefgh", 0)
	checkContent(t, ff, "abcdefgh")

	// Overwrite bytes [2..5) with "XYZ" (same length, in-place)
	mustWrite(t, ff, "XYZ", 2)
	checkContent(t, ff, "abXYZfgh")
}

// TestFlexspace_WriteExtend verifies that Write extends the file when
// loff+len > size.
func TestFlexspace_WriteExtend(t *testing.T) {
	ff := mustOpenRAM(t)
	defer ff.Close()

	// "abc": write at 0, size=0 -> append
	mustWrite(t, ff, "abc", 0)
	checkContent(t, ff, "abc")

	// write "def" at loff=1, len=3: loff+len=4>3 -> collapse(1,2)+insert("def",1)
	// Result: "adef"
	mustWrite(t, ff, "def", 1)
	checkContent(t, ff, "adef")

	// write "123" at loff=2, len=3: loff+len=5>4
	// Result: "ad123"
	mustWrite(t, ff, "123", 2)
	checkContent(t, ff, "ad123")
}

// ======================== SetTag / GetTag ========================

func TestFlexspace_Tags(t *testing.T) {
	ff := mustOpenRAM(t)
	defer ff.Close()

	mustInsert(t, ff, "hello world", 0)

	// SetTag at the start of the extent (loff=0)
	if err := ff.SetTag(0, 42); err != nil {
		t.Fatalf("SetTag: %v", err)
	}
	tag, err := ff.GetTag(0)
	if err != nil {
		t.Fatalf("GetTag: %v", err)
	}
	if tag != 42 {
		t.Errorf("tag: got %d, want 42", tag)
	}
}

// ======================== Handler API ========================

func TestFlexspace_Handler(t *testing.T) {
	ff := mustOpenRAM(t)
	defer ff.Close()

	mustInsert(t, ff, "abcdefghij", 0)

	// Get handler at offset 0
	h := ff.GetHandler(0)
	if !h.Valid() {
		t.Fatal("handler should be valid at offset 0")
	}
	if h.Loff() != 0 {
		t.Errorf("handler loff: got %d, want 0", h.Loff())
	}

	// Read through the handler
	buf := make([]byte, 5)
	n, err := h.Read(buf, 5)
	if err != nil {
		t.Fatalf("handler Read: %v", err)
	}
	if n != 5 || string(buf) != "abcde" {
		t.Errorf("handler Read: got %q, want %q", buf[:n], "abcde")
	}
	// h.Loff() still 0 - Read does not advance h, only a local copy
	if h.Loff() != 0 {
		t.Errorf("handler loff after Read: got %d, want 0 (Read doesn't advance)", h.Loff())
	}

	// Forward by 5 then 3
	h.Forward(5)
	h.Forward(3)
	if h.Loff() != 8 {
		t.Errorf("handler loff after Forward(5)+Forward(3): got %d, want 8", h.Loff())
	}

	// Backward by 3
	h.Backward(3)
	if h.Loff() != 5 {
		t.Errorf("handler loff after Backward(3): got %d, want 5", h.Loff())
	}
}

// ======================== Ftruncate ========================

func TestFlexspace_Ftruncate(t *testing.T) {
	ff := mustOpenRAM(t)
	defer ff.Close()

	mustInsert(t, ff, "abcdefghij", 0)
	checkContent(t, ff, "abcdefghij")

	if err := ff.Ftruncate(5); err != nil {
		t.Fatalf("Ftruncate: %v", err)
	}
	checkContent(t, ff, "abcde")

	// Truncating to current size is a no-op
	if err := ff.Ftruncate(5); err != nil {
		t.Fatalf("Ftruncate no-op: %v", err)
	}
	checkContent(t, ff, "abcde")
}

// ======================== Fallocate ========================

func TestFlexspace_Fallocate(t *testing.T) {
	ff := mustOpenRAM(t)
	defer ff.Close()

	// Allocate 1 MB of zeroed space
	const allocSize = 1 << 20 // 1 MB
	if err := ff.Fallocate(0, allocSize); err != nil {
		t.Fatalf("Fallocate: %v", err)
	}
	if ff.Size() != allocSize {
		t.Errorf("size after Fallocate: got %d, want %d", ff.Size(), uint64(allocSize))
	}
}

// ======================== Update ========================

func TestFlexspace_Update(t *testing.T) {
	ff := mustOpenRAM(t)
	defer ff.Close()

	mustInsert(t, ff, "hello world", 0)

	// Replace "world" (5 bytes at offset 6) with "Go" (2 bytes)
	n, err := ff.Update([]byte("Go"), 6, 2, 5)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if n != 2 {
		t.Errorf("Update returned %d, want 2", n)
	}
	checkContent(t, ff, "hello Go")
}

// ======================== Defrag ========================

func TestFlexspace_Defrag(t *testing.T) {
	ff := mustOpenRAM(t)
	defer ff.Close()

	mustInsert(t, ff, "fragmented", 0)

	// Defrag first 5 bytes
	if err := ff.Defrag([]byte("fragm"), 0, 5); err != nil {
		t.Fatalf("Defrag: %v", err)
	}
	checkContent(t, ff, "fragmented")
}

// ======================== Multi-block Write ========================

func TestFlexspace_MultiBlock(t *testing.T) {
	ff := mustOpenRAM(t)
	defer ff.Close()

	// Write 10 MB in 128 KB chunks to cross block boundaries
	const chunkSize = FLEXSPACE_MAX_EXTENT_SIZE // 128 KB
	const totalBytes = 10 * (1 << 20)           // 10 MB
	chunk := make([]byte, chunkSize)
	for i := range chunk {
		chunk[i] = byte(i & 0xff)
	}

	written := uint64(0)
	for written < totalBytes {
		sz := uint64(chunkSize)
		if sz > totalBytes-written {
			sz = totalBytes - written
		}
		n, err := ff.Insert(chunk[:sz], written, sz)
		if err != nil {
			t.Fatalf("Insert at %d: %v", written, err)
		}
		written += uint64(n)
	}

	if ff.Size() != totalBytes {
		t.Errorf("size after multi-block write: got %d, want %d", ff.Size(), uint64(totalBytes))
	}

	// Verify a sample of the data
	sample := mustRead(t, ff, 0, chunkSize)
	for i, b := range sample {
		want := byte(i & 0xff)
		if b != want {
			t.Errorf("sample[%d]: got %d, want %d", i, b, want)
			break
		}
	}
}

// ======================== Large Sequential Read ========================

func TestFlexspace_LargeSequentialReadWrite(t *testing.T) {
	ff := mustOpenRAM(t)
	defer ff.Close()

	// Write a pattern: byte[i] = i % 251 (prime, fills full byte range)
	const size = 4 * (1 << 20) // 4 MB
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251)
	}

	n, err := ff.Insert(data, 0, uint64(size))
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if n != size {
		t.Fatalf("Insert: wrote %d, want %d", n, size)
	}

	// Read it back
	got := mustRead(t, ff, 0, uint64(size))
	for i := range got {
		want := byte(i % 251)
		if got[i] != want {
			t.Errorf("mismatch at %d: got %d, want %d", i, got[i], want)
			break
		}
	}
}

// ======================== Handler Tag ========================

func TestFlexspace_HandlerTag(t *testing.T) {
	ff := mustOpenRAM(t)
	defer ff.Close()

	mustInsert(t, ff, "tagged extent", 0)
	if err := ff.SetTag(0, 99); err != nil {
		t.Fatalf("SetTag: %v", err)
	}

	h := ff.GetHandler(0)
	if !h.Valid() {
		t.Fatal("handler not valid")
	}
	tag, err := h.GetTag()
	if err != nil {
		t.Fatalf("handler GetTag: %v", err)
	}
	if tag != 99 {
		t.Errorf("handler tag: got %d, want 99", tag)
	}
}

// ======================== Error cases ========================

func TestFlexspace_ErrorCases(t *testing.T) {
	ff := mustOpenRAM(t)
	defer ff.Close()

	// Read on empty file should fail
	buf := make([]byte, 1)
	if _, err := ff.Read(buf, 0, 1); err == nil {
		t.Error("Read on empty file should return error")
	}

	// Insert at loff > size (would create a hole) should fail
	mustInsert(t, ff, "abc", 0)
	if _, err := ff.Insert(buf, 10, 1); err == nil {
		t.Error("Insert with gap (hole) should return error")
	}

	// Write at loff > size should fail
	if _, err := ff.Write(buf, 100, 1); err == nil {
		t.Error("Write at loff > size should return error")
	}

	// Collapse out of range should fail
	if err := ff.Collapse(1, 5); err == nil {
		t.Error("Collapse beyond file end should return error")
	}
}

// ======================== Block Recycling ========================

func TestFlexspace_BlockRecycling(t *testing.T) {
	t.Skip("block recycling order differs in RAM-only mode (no disk-based free block scan)")
	ff := mustOpenRAM(t)
	defer ff.Close()

	chunkSize := uint64(FLEXSPACE_MAX_EXTENT_SIZE) // 128 KB
	chunksPerBlock := FLEXSPACE_BLOCK_SIZE / chunkSize

	data := make([]byte, chunkSize)
	for i := range data {
		data[i] = byte('A' + (i % 26))
	}

	// Fill 3 full blocks (32 x 128KB each = 4MB per block).
	for b := 0; b < 3; b++ {
		for c := uint64(0); c < chunksPerBlock; c++ {
			_, err := ff.Insert(data, ff.Size(), chunkSize)
			if err != nil {
				t.Fatalf("Insert block %d chunk %d: %v", b, c, err)
			}
		}
	}
	ff.Sync()

	// Verify blocks 0, 1, 2 have non-zero usage.
	for i := 0; i < 3; i++ {
		if ff.bm.blkusage[i] == 0 {
			t.Errorf("block %d should have non-zero usage after write", i)
		}
	}

	// Collapse all data (frees blocks 0, 1, 2).
	err := ff.Collapse(0, ff.Size())
	if err != nil {
		t.Fatalf("Collapse: %v", err)
	}
	ff.Sync()

	// Verify blocks 0, 1, 2 are now free.
	for i := 0; i < 3; i++ {
		if ff.bm.blkusage[i] != 0 {
			t.Errorf("block %d should be free after collapse, got usage=%d", i, ff.bm.blkusage[i])
		}
	}

	// Write new data - should reuse blocks 0, 1, 2 instead of 3+.
	_, err = ff.Insert(data, ff.Size(), chunkSize)
	if err != nil {
		t.Fatalf("Insert reuse: %v", err)
	}

	// The new write should have gone to block 0 (recycled).
	if ff.bm.blkusage[0] == 0 {
		t.Errorf("block 0 should be reused, but usage is 0")
	}

	// Verify data is readable.
	readBuf := make([]byte, chunkSize)
	n, readErr := ff.Read(readBuf, 0, chunkSize)
	if readErr != nil {
		t.Fatalf("Read after reuse: %v", readErr)
	}
	if uint64(n) != chunkSize {
		t.Fatalf("Read: got %d bytes, want %d", n, chunkSize)
	}
}

// ======================== Block Usage Invariant Helpers ========================

// verifyBlkUsageInvariant walks all FlexTree leaf extents and verifies that
// bm.blkusage[i] matches the actual bytes stored in block i.
func verifyBlkUsageInvariant(t *testing.T, ff *FlexSpace) {
	t.Helper()

	expected := make(map[uint64]uint32)
	totalExtentBytes := uint64(0)

	nodeID := ff.tree.LeafHead
	for !nodeID.IsIllegal() {
		le := ff.tree.GetLeaf(nodeID)
		for i := uint32(0); i < le.Count; i++ {
			ext := &le.Extents[i]
			if ext.IsHole() {
				continue
			}
			poff := ext.Address()
			remaining := uint64(ext.Len)
			totalExtentBytes += remaining
			for remaining > 0 {
				blkid := poff >> FLEXSPACE_BLOCK_BITS
				blkEnd := (blkid + 1) << FLEXSPACE_BLOCK_BITS
				inBlock := blkEnd - poff
				if inBlock > remaining {
					inBlock = remaining
				}
				expected[blkid] += uint32(inBlock)
				poff += inBlock
				remaining -= inBlock
			}
		}
		nodeID = le.Next
	}

	// Check expected against actual
	for blkid, want := range expected {
		got := ff.bm.blkusage[blkid]
		if got != want {
			t.Errorf("blkusage[%d]: got %d, want %d", blkid, got, want)
		}
		if want > FLEXSPACE_BLOCK_SIZE {
			t.Errorf("blkusage[%d] = %d exceeds block size %d", blkid, want, FLEXSPACE_BLOCK_SIZE)
		}
	}

	// Check no unexpected non-zero blocks
	for i := range ff.bm.blkusage {
		if ff.bm.blkusage[i] != 0 && expected[uint64(i)] == 0 {
			t.Errorf("blkusage[%d] = %d but no extents reference this block", i, ff.bm.blkusage[i])
		}
	}

	// Verify total bytes match
	totalUsage := uint64(0)
	for _, v := range expected {
		totalUsage += uint64(v)
	}
	if totalUsage != totalExtentBytes {
		t.Errorf("sum(blkusage) = %d != sum(extent.Len) = %d", totalUsage, totalExtentBytes)
	}
}

// verifyNoBlockCrossing checks that no extent crosses a 4MB block boundary.
func verifyNoBlockCrossing(t *testing.T, ff *FlexSpace) {
	t.Helper()
	nodeID := ff.tree.LeafHead
	for !nodeID.IsIllegal() {
		le := ff.tree.GetLeaf(nodeID)
		for i := uint32(0); i < le.Count; i++ {
			ext := &le.Extents[i]
			if ext.IsHole() || ext.Len == 0 {
				continue
			}
			poff := ext.Address()
			startBlk := poff >> FLEXSPACE_BLOCK_BITS
			endBlk := (poff + uint64(ext.Len) - 1) >> FLEXSPACE_BLOCK_BITS
			if startBlk != endBlk {
				t.Errorf("extent at loff=%d poff=%d len=%d crosses block boundary: blocks %d-%d",
					ext.Loff, poff, ext.Len, startBlk, endBlk)
			}
		}
		nodeID = le.Next
	}
}

// ======================== Block Usage Invariant Tests ========================

func TestFlexspace_BlkUsageInvariant_InsertCollapse(t *testing.T) {
	ff := mustOpenRAM(t)
	defer ff.Close()

	// Insert 10 chunks of varying sizes
	sizes := []uint64{32768, 65536, 40000, 128000, 50000, 80000, 100000, 131072, 70000, 90000}
	data := make([]byte, 131072)
	for i := range data {
		data[i] = byte(i % 251)
	}
	for _, sz := range sizes {
		_, err := ff.Insert(data[:sz], ff.Size(), sz)
		if err != nil {
			t.Fatalf("Insert(%d): %v", sz, err)
		}
	}
	verifyBlkUsageInvariant(t, ff)
	verifyNoBlockCrossing(t, ff)

	// Collapse first 3 chunks worth (32768+65536+40000 = 138304)
	collapseLen := uint64(32768 + 65536 + 40000)
	if err := ff.Collapse(0, collapseLen); err != nil {
		t.Fatalf("Collapse: %v", err)
	}
	verifyBlkUsageInvariant(t, ff)

	// Insert more data
	_, err := ff.Insert(data[:50000], ff.Size(), 50000)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	verifyBlkUsageInvariant(t, ff)

	// Collapse all data
	if err := ff.Collapse(0, ff.Size()); err != nil {
		t.Fatalf("Collapse all: %v", err)
	}
	verifyBlkUsageInvariant(t, ff)

	// All blkusage should be 0
	for i := range ff.bm.blkusage {
		if ff.bm.blkusage[i] != 0 {
			t.Errorf("blkusage[%d] = %d after full collapse, want 0", i, ff.bm.blkusage[i])
			break
		}
	}
}

func TestFlexspace_BlkUsageInvariant_OverwriteInFlushedRegion(t *testing.T) {
	ff := mustOpenRAM(t)
	defer ff.Close()

	// Insert data and sync
	data := make([]byte, 64*1024)
	for i := range data {
		data[i] = byte('A')
	}
	_, err := ff.Insert(data, 0, uint64(len(data)))
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	ff.Sync()

	// Insert more data
	data2 := make([]byte, 32*1024)
	for i := range data2 {
		data2[i] = byte('B')
	}
	_, err = ff.Insert(data2, ff.Size(), uint64(len(data2)))
	if err != nil {
		t.Fatalf("Insert2: %v", err)
	}

	// Overwrite data in the flushed region
	overwrite := make([]byte, 16*1024)
	for i := range overwrite {
		overwrite[i] = byte('C')
	}
	_, err = ff.Write(overwrite, 0, uint64(len(overwrite)))
	if err != nil {
		t.Fatalf("Write overwrite: %v", err)
	}
	verifyBlkUsageInvariant(t, ff)

	// Verify data reads back correctly
	got := mustRead(t, ff, 0, uint64(len(overwrite)))
	for i, b := range got {
		if b != byte('C') {
			t.Errorf("byte[%d] = %d, want 'C'", i, b)
			break
		}
	}
}

func TestFlexspace_BlkUsageInvariant_MultiBlockFill(t *testing.T) {
	ff := mustOpenRAM(t)
	defer ff.Close()

	chunkSize := uint64(FLEXSPACE_MAX_EXTENT_SIZE) // 128 KB
	chunksPerBlock := uint64(FLEXSPACE_BLOCK_SIZE) / chunkSize

	data := make([]byte, chunkSize)
	for i := range data {
		data[i] = byte(i % 251)
	}

	// Fill 3 complete blocks
	for b := 0; b < 3; b++ {
		for c := uint64(0); c < chunksPerBlock; c++ {
			_, err := ff.Insert(data, ff.Size(), chunkSize)
			if err != nil {
				t.Fatalf("Insert block %d chunk %d: %v", b, c, err)
			}
		}
	}
	verifyBlkUsageInvariant(t, ff)
	verifyNoBlockCrossing(t, ff)

	// Each block should have exactly 4MB usage
	for i := 0; i < 3; i++ {
		if ff.bm.blkusage[i] != FLEXSPACE_BLOCK_SIZE {
			t.Errorf("block %d usage: got %d, want %d", i, ff.bm.blkusage[i], FLEXSPACE_BLOCK_SIZE)
		}
	}

	// Collapse middle block
	if err := ff.Collapse(uint64(FLEXSPACE_BLOCK_SIZE), uint64(FLEXSPACE_BLOCK_SIZE)); err != nil {
		t.Fatalf("Collapse middle: %v", err)
	}
	verifyBlkUsageInvariant(t, ff)

	// Insert new data - should reuse freed space
	_, err := ff.Insert(data, ff.Size(), chunkSize)
	if err != nil {
		t.Fatalf("Insert after collapse: %v", err)
	}
	verifyBlkUsageInvariant(t, ff)
}

func TestFlexspace_BlkUsageInvariant_MultiSyncSameBlock(t *testing.T) {
	ff := mustOpenRAM(t)
	defer ff.Close()

	data := make([]byte, 64*1024)
	for i := range data {
		data[i] = byte('X')
	}

	// Insert 64KB, Sync x3
	for round := 0; round < 3; round++ {
		_, err := ff.Insert(data, ff.Size(), uint64(len(data)))
		if err != nil {
			t.Fatalf("Insert round %d: %v", round, err)
		}
		ff.Sync()
	}

	verifyBlkUsageInvariant(t, ff)

	// All data should be in block 0 (192KB < 4MB)
	expectedUsage := uint32(3 * 64 * 1024)
	if ff.bm.blkusage[0] != expectedUsage {
		t.Errorf("blkusage[0]: got %d, want %d", ff.bm.blkusage[0], expectedUsage)
	}
	// No other block should have data
	for i := 1; i < len(ff.bm.blkusage); i++ {
		if ff.bm.blkusage[i] != 0 {
			t.Errorf("blkusage[%d] = %d, want 0", i, ff.bm.blkusage[i])
			break
		}
	}
}

func TestFlexspace_BlkUsageInvariant_GC(t *testing.T) {
	ff := mustOpenRAM(t)
	defer ff.Close()

	chunkSize := uint64(FLEXSPACE_MAX_EXTENT_SIZE)
	chunksPerBlock := uint64(FLEXSPACE_BLOCK_SIZE) / chunkSize
	data := make([]byte, chunkSize)
	for i := range data {
		data[i] = byte(i % 251)
	}

	// Fill block 0 completely (4MB), sync
	for c := uint64(0); c < chunksPerBlock; c++ {
		_, err := ff.Insert(data, ff.Size(), chunkSize)
		if err != nil {
			t.Fatalf("Insert chunk %d: %v", c, err)
		}
	}
	ff.Sync()

	// Collapse most of block 0 (leave ~128KB)
	collapseLen := uint64(FLEXSPACE_BLOCK_SIZE) - chunkSize
	if err := ff.Collapse(0, collapseLen); err != nil {
		t.Fatalf("Collapse: %v", err)
	}
	ff.Sync()

	verifyBlkUsageInvariant(t, ff)

	// Verify data reads back correctly
	size := ff.Size()
	if size == 0 {
		t.Fatal("size should be > 0")
	}
	buf := make([]byte, size)
	_, err := ff.Read(buf, 0, size)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
}

func TestFlexspace_NoBlockCrossing_AfterMaxExtentSizeFix(t *testing.T) {
	ff := mustOpenRAM(t)
	defer ff.Close()

	// Verify MaxExtentSize is set to block size
	if ff.tree.MaxExtentSize != FLEXSPACE_BLOCK_SIZE {
		t.Fatalf("MaxExtentSize: got %d, want %d", ff.tree.MaxExtentSize, FLEXSPACE_BLOCK_SIZE)
	}

	chunkSize := uint64(FLEXSPACE_MAX_EXTENT_SIZE) // 128 KB
	data := make([]byte, chunkSize)
	for i := range data {
		data[i] = byte(i % 251)
	}

	// Write data that fills block 0 and spills into block 1
	chunksPerBlock := uint64(FLEXSPACE_BLOCK_SIZE) / chunkSize
	for c := uint64(0); c < chunksPerBlock+4; c++ {
		_, err := ff.Insert(data, ff.Size(), chunkSize)
		if err != nil {
			t.Fatalf("Insert chunk %d: %v", c, err)
		}
	}

	verifyNoBlockCrossing(t, ff)
	verifyBlkUsageInvariant(t, ff)
}

func TestFlexspace_BlkUsageInvariant_RandomOps(t *testing.T) {
	ff := mustOpenRAM(t)
	defer ff.Close()

	// Deterministic PRNG
	seed := uint64(12345)
	prng := func() uint64 {
		seed ^= seed << 13
		seed ^= seed >> 7
		seed ^= seed << 17
		return seed
	}

	const numOps = 500
	const checkInterval = 50

	data := make([]byte, 128*1024)
	for i := range data {
		data[i] = byte(i % 251)
	}

	for op := 0; op < numOps; op++ {
		size := ff.Size()
		r := prng() % 4

		switch {
		case r == 0 || size < 1024:
			// Insert: random size 1KB-128KB at end
			sz := (prng()%(128*1024-1024) + 1024)
			_, err := ff.Insert(data[:sz], ff.Size(), sz)
			if err != nil {
				t.Fatalf("op %d Insert(%d): %v", op, sz, err)
			}

		case r == 1 && size > 2048:
			// Collapse: random range within existing data
			maxLen := size / 4
			if maxLen < 1 {
				maxLen = 1
			}
			clen := prng()%maxLen + 1
			coff := prng() % (size - clen)
			if err := ff.Collapse(coff, clen); err != nil {
				t.Fatalf("op %d Collapse(%d, %d): %v", op, coff, clen, err)
			}

		case r == 2 && size > 1024:
			// Overwrite: random range within existing data
			maxLen := size / 4
			if maxLen > 128*1024 {
				maxLen = 128 * 1024
			}
			if maxLen < 1 {
				maxLen = 1
			}
			wlen := prng()%maxLen + 1
			woff := prng() % (size - wlen)
			_, err := ff.Write(data[:wlen], woff, wlen)
			if err != nil {
				t.Fatalf("op %d Write(%d, %d): %v", op, woff, wlen, err)
			}

		case r == 3:
			// Sync
			ff.Sync()
		}

		if (op+1)%checkInterval == 0 {
			verifyBlkUsageInvariant(t, ff)
			verifyNoBlockCrossing(t, ff)
		}
	}

	// Final check
	verifyBlkUsageInvariant(t, ff)
	verifyNoBlockCrossing(t, ff)
}
