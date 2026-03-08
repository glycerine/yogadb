package yogadb

import (
	"fmt"
	"time"
	"unsafe"

	//"github.com/glycerine/vfs"
	"testing"
)

var _ = time.Now

const testFlexTreeMaxExtentSizeLimit = (64 << 20)

// Helpers for generating pseudo random test data
// in a reproducible fashion.
var globalTestPRNG *prng

var globalR uint64

func resetRand() {
	var seed [32]byte
	seed[0] = 42
	globalTestPRNG = newPRNG(seed)
}

func init() {
	resetRand()
}

func randInt() uint64 {
	return globalTestPRNG.Uint64()
}

func queryResultEqual(rr1, rr2 *FlextreeQueryResult) bool {
	if rr1 == nil && rr2 == nil {
		return true
	}
	if rr1 != rr2 && (rr1 == nil || rr2 == nil) {
		return false
	}
	if rr1.Count != rr2.Count {
		return false
	}
	for i := uint64(0); i < rr1.Count; i++ {
		if rr1.V[i].Poff != rr2.V[i].Poff || rr1.V[i].Len != rr2.V[i].Len {
			return false
		}
	}
	return true
}

func randomInsert(t *testing.T, ft *FlexTree, bf *BruteForce, count uint64) {
	if ft != nil {
		resetRand()
		//t0 := time.Now()
		ft.Insert(0, 0, 4)
		var maxLoff uint64 = 4
		for i := uint64(0); i < count; i++ {
			len := uint32(randInt()%1000 + 1)
			ft.InsertWTag(maxLoff%1000, maxLoff, len, uint16(i%0xffff))
			maxLoff += uint64(len)
		}
		//vv("insert to flextree %v elapsed\n", time.Since(t0))
	}

	if bf != nil {
		resetRand()
		bf.Insert(0, 0, 4)
		var maxLoff uint64 = 4
		for i := uint64(0); i < count; i++ {
			len := uint32(randInt()%1000 + 1)
			bf.InsertWTag(maxLoff%1000, maxLoff, len, uint16(i%0xffff))
			maxLoff += uint64(len)
		}
	}
}

func sequentialQuery(t *testing.T, ft *FlexTree, bf *BruteForce, totalSize uint64) {
	if ft == nil || bf == nil {
		return
	}
	for i := uint64(0); i < totalSize; i++ {
		globalR += ft.PQuery(i)
	}
	for i := uint64(0); i < totalSize; i++ {
		globalR += bf.PQuery(i)
	}

	for i := int64(totalSize); i >= 0; i-- {
		fr := ft.PQuery(uint64(i))
		br := bf.PQuery(uint64(i))
		if fr != br {
			t.Fatalf("Error encountered on %d %d %d", i, fr, br)
		}
		if i == 0 {
			break
		}
	}
}

func randomDelete(t *testing.T, ft *FlexTree, bf *BruteForce, totalSize uint64, count uint64) {
	if ft == nil || bf == nil {
		return
	}
	osize := totalSize
	if ft != nil {
		resetRand()
		for i := uint64(0); i < count; i++ {
			tmp := randInt() % totalSize
			tmp2 := randInt()%10 + 1
			ft.Delete(tmp, tmp2)
			totalSize -= tmp2
		}
	}

	totalSize = osize
	if bf != nil {
		resetRand()
		for i := uint64(0); i < count; i++ {
			tmp := randInt() % totalSize
			tmp2 := randInt()%10 + 1
			bf.Delete(tmp, tmp2)
			totalSize -= tmp2
		}
	}
}

// checkFlexTreeContiguity verifies FlexTree extent integrity independent of any oracle.
// Walks the leaf linked list and checks:
//   - INV-FT-1: Within each leaf, extents are sorted and contiguous (no gaps, no overlaps)
//   - INV-FT-2: No zero-length extents
//   - INV-FT-3: Extent count matches node.Count
//   - INV-FT-4: PQuery succeeds for every loff in [0, MaxLoff) (full coverage, no holes)
//   - INV-FT-5: MaxLoff == sum of all extent lengths across all leaves
func checkFlexTreeContiguity(t testing.TB, ft *FlexTree, opDesc string) {
	t.Helper()
	if ft.MaxLoff == 0 {
		return
	}

	totalExtentLen := uint64(0)
	leafCount := 0
	nodeID := ft.LeafHead

	for nodeID != IllegalID {
		leaf := ft.GetLeaf(nodeID)
		leafCount++

		// INV-FT-3: Count is sane
		if leaf.Count == 0 && leafCount > 1 {
			// Only the very first leaf in an empty tree can have count 0
			t.Fatalf("[INV-FT-3] %s: leaf %v has Count=0", opDesc, nodeID)
		}

		le := leaf
		for i := uint32(0); i < leaf.Count; i++ {
			ext := &le.Extents[i]

			// INV-FT-2: No zero-length extents
			if ext.Len == 0 {
				t.Fatalf("[INV-FT-2] %s: leaf %v extent[%d] has Len=0 (Loff=%d)",
					opDesc, nodeID, i, ext.Loff)
			}

			totalExtentLen += uint64(ext.Len)

			// INV-FT-1: Intra-leaf contiguity
			if i+1 < leaf.Count {
				next := &le.Extents[i+1]
				end := ext.Loff + ext.Len
				if end != next.Loff {
					t.Fatalf("[INV-FT-1] %s: leaf %v extent[%d] ends at %d but extent[%d] starts at %d (gap/overlap of %d)",
						opDesc, nodeID, i, end, i+1, next.Loff, int64(next.Loff)-int64(end))
				}
			}
		}

		nodeID = leaf.Next
	}

	// INV-FT-5: Total extent length == MaxLoff
	if totalExtentLen != ft.MaxLoff {
		t.Fatalf("[INV-FT-5] %s: sum of extent lengths = %d but MaxLoff = %d",
			opDesc, totalExtentLen, ft.MaxLoff)
	}

	// INV-FT-4: Full PQuery coverage — every loff maps to a valid poff.
	// (Spot-check: test 1024 evenly-spaced positions plus boundaries)
	step := ft.MaxLoff / 1024
	if step == 0 {
		step = 1
	}
	for loff := uint64(0); loff < ft.MaxLoff; loff += step {
		poff := ft.PQuery(loff)
		if poff == ^uint64(0) {
			t.Fatalf("[INV-FT-4] %s: PQuery(%d) returned not-found (MaxLoff=%d)",
				opDesc, loff, ft.MaxLoff)
		}
	}
	// Check last valid position
	if ft.MaxLoff > 0 {
		poff := ft.PQuery(ft.MaxLoff - 1)
		if poff == ^uint64(0) {
			t.Fatalf("[INV-FT-4] %s: PQuery(%d) returned not-found (MaxLoff=%d)",
				opDesc, ft.MaxLoff-1, ft.MaxLoff)
		}
	}
}

func TestFlextree_Test0(t *testing.T) {

	count := uint64(50000) // Lower count for faster general tests initially

	vv("---test0 insertion and point lookup %v ---", count)

	fs, dir := newTestFS(t)
	ft, err := OpenFlexTreeCoW(dir, fs)
	panicOn(err)

	randomInsert(t, ft, nil, count)
	checkFlexTreeContiguity(t, ft, "Test0-afterInsert")
	randomDelete(t, ft, nil, ft.GetMaxLoff(), count)
	checkFlexTreeContiguity(t, ft, "Test0-afterDelete")
	sequentialQuery(t, ft, nil, ft.GetMaxLoff())

	loff := uint64(0)
	for ft.GetMaxLoff() > 100 {
		loff = (loff + 0xabcd12) % (ft.GetMaxLoff() - 100)
		ft.Delete(loff, 100)
	}
	ft.Delete(0, ft.GetMaxLoff()-10)
	ft.Print()
	ft.Delete(0, ft.GetMaxLoff())
	ft.Print()

	ft.Close()
}

func TestFlextree_Test1(t *testing.T) {
	count := uint64(500)

	fs, dir := newTestFS(t)

	ft, err := OpenFlexTreeCoW(dir, fs)
	panicOn(err)

	bf := OpenBruteForce(DefaultFlexTreeMaxExtentSizeLimit)

	randomInsert(t, ft, bf, count)

	if ft.GetMaxLoff() != bf.GetMaxLoff() {
		t.Fatalf("MaxLoff mismatch: %d != %d", ft.GetMaxLoff(), bf.GetMaxLoff())
	}
	// ft.GetMaxLoff() is currently 0 in the stub, causing sequentialQuery to do nothing. Let's force it to fail if it's 0 after inserts.
	if ft.GetMaxLoff() == 0 {
		t.Fatalf("MaxLoff should not be zero after inserts")
	}

	sequentialQuery(t, ft, bf, ft.GetMaxLoff())
	bf.Close()
	ft.Close()
}

func randomPDelete(t *testing.T, ft *FlexTree, bf *BruteForce, totalSize uint64, count uint64) {
	if ft != nil {
		resetRand()
		for i := uint64(0); i < count; i++ {
			tmp := randInt() % totalSize
			ft.PDelete(tmp)
		}
	}
	if bf != nil {
		resetRand()
		for i := uint64(0); i < count; i++ {
			tmp := randInt() % totalSize
			bf.PDelete(tmp)
		}
	}
}

func TestFlextree_Test2(t *testing.T) {
	count := uint64(500)
	fs, dir := newTestFS(t)
	_ = dir
	ft, err := OpenFlexTreeCoW(dir, fs)
	panicOn(err)

	bf := OpenBruteForce(DefaultFlexTreeMaxExtentSizeLimit)

	randomInsert(t, ft, bf, count)
	checkFlexTreeContiguity(t, ft, "Test2-afterInsert")
	randomPDelete(t, ft, bf, ft.GetMaxLoff(), count)
	checkFlexTreeContiguity(t, ft, "Test2-afterPDelete")

	if ft.GetMaxLoff() != bf.GetMaxLoff() {
		t.Fatalf("MaxLoff mismatch: %d != %d", ft.GetMaxLoff(), bf.GetMaxLoff())
	}
	sequentialQuery(t, ft, bf, ft.GetMaxLoff())
	bf.Close()
	ft.Close()
}

func TestFlextree_Test3(t *testing.T) {
	count := uint64(500)
	fs, dir := newTestFS(t)
	_ = dir
	ft, err := OpenFlexTreeCoW(dir, fs)
	panicOn(err)

	bf := OpenBruteForce(DefaultFlexTreeMaxExtentSizeLimit)

	randomInsert(t, ft, bf, count)
	checkFlexTreeContiguity(t, ft, "Test3-afterInsert")
	randomDelete(t, ft, bf, ft.GetMaxLoff(), count)
	checkFlexTreeContiguity(t, ft, "Test3-afterDelete")

	if ft.GetMaxLoff() != bf.GetMaxLoff() {
		t.Fatalf("MaxLoff mismatch: %d != %d", ft.GetMaxLoff(), bf.GetMaxLoff())
	}
	sequentialQuery(t, ft, bf, ft.GetMaxLoff())
	bf.Close()
	ft.Close()
}

func randomRangeQuery(t *testing.T, ft *FlexTree, bf *BruteForce, totalSize uint64, count uint64) {
	if ft == nil || bf == nil {
		return
	}
	resetRand()
	for i := uint64(0); i < count; i++ {
		loff := randInt() % totalSize
		len := randInt() % 100
		fr := ft.Query(loff, len)
		br := bf.Query(loff, len)
		if !queryResultEqual(fr, br) {
			t.Errorf("Range query mismatch for loff %d len %d", loff, len)
			if fr != nil {
				t.Errorf("FR: %+v, V: %+v", fr, fr.V)
			} else {
				t.Errorf("FR is nil")
			}
			if br != nil {
				t.Errorf("BR: %+v, V: %+v", br, br.V)
			} else {
				t.Errorf("BR is nil")
			}
			t.FailNow()
		}
	}
}

func TestFlextree_Test4(t *testing.T) {
	count := uint64(500)
	fs, dir := newTestFS(t)

	ft, err := OpenFlexTreeCoW(dir, fs)
	panicOn(err)

	bf := OpenBruteForce(DefaultFlexTreeMaxExtentSizeLimit)

	randomInsert(t, ft, bf, count)
	randomDelete(t, ft, bf, ft.GetMaxLoff(), count)

	if ft.GetMaxLoff() != bf.GetMaxLoff() {
		t.Fatalf("MaxLoff mismatch: %d != %d", ft.GetMaxLoff(), bf.GetMaxLoff())
	}
	randomRangeQuery(t, ft, bf, ft.GetMaxLoff(), count)
	bf.Close()
	ft.Close()
}

func TestFlextree_Test6(t *testing.T) {
	count := uint64(500)
	fs, dir := newTestFS(t)

	ft, err := OpenFlexTreeCoW(dir, fs)
	panicOn(err)

	bf := OpenBruteForce(DefaultFlexTreeMaxExtentSizeLimit)

	randomInsert(t, ft, bf, count)

	r1 := ft.Insert(1<<34, 1<<40, 50)
	if r1 != 0 {
		t.Fatalf("ft insert hole failed")
	}
	r2 := bf.Insert(1<<34, 1<<40, 50)
	if r2 != 0 {
		t.Fatalf("bf insert hole failed")
	}

	randomRangeQuery(t, ft, bf, ft.GetMaxLoff(), count)
	bf.Close()
	ft.Close()
}

func TestFlextree_Test7(t *testing.T) {
	count := uint64(500)
	fs, dir := newTestFS(t)

	ft, err := OpenFlexTreeCoW(dir, fs)
	panicOn(err)

	bf := OpenBruteForce(DefaultFlexTreeMaxExtentSizeLimit)

	randomInsert(t, ft, bf, count)

	ft.Delete(ft.GetMaxLoff()/4, ft.GetMaxLoff()/4*3)
	bf.Delete(bf.GetMaxLoff()/4, bf.GetMaxLoff()/4*3)

	if ft.GetMaxLoff() != bf.GetMaxLoff() {
		t.Fatalf("MaxLoff mismatch: %d != %d", ft.GetMaxLoff(), bf.GetMaxLoff())
	}
	sequentialQuery(t, ft, bf, ft.GetMaxLoff())
	bf.Close()
	ft.Close()
}

func sequentialTagQuery(t *testing.T, ft *FlexTree, bf *BruteForce, totalSize uint64) {
	if ft == nil || bf == nil {
		return
	}
	ft.SetTag(ft.GetMaxLoff()-1, 0xffff)
	bf.SetTag(bf.GetMaxLoff()-1, 0xffff)

	for i := uint64(0); i < totalSize; i++ {
		ftTag, fr := ft.GetTag(i)
		bfTag, br := bf.GetTag(i)
		if fr != br || ftTag != bfTag {
			t.Fatalf("Tag mismatch on loff %d: fr %d br %d ftTag %d bfTag %d", i, fr, br, ftTag, bfTag)
		}
	}
}

func TestFlextree_Test8(t *testing.T) {
	count := uint64(500)
	fs, dir := newTestFS(t)

	ft, err := OpenFlexTreeCoW(dir, fs)
	panicOn(err)
	bf := OpenBruteForce(DefaultFlexTreeMaxExtentSizeLimit)

	randomInsert(t, ft, bf, count)
	randomDelete(t, ft, bf, ft.GetMaxLoff(), count)

	if ft.GetMaxLoff() != bf.GetMaxLoff() {
		t.Fatalf("MaxLoff mismatch: %d != %d", ft.GetMaxLoff(), bf.GetMaxLoff())
	}
	sequentialTagQuery(t, ft, bf, ft.GetMaxLoff())
	bf.Close()
	ft.Close()
}

// randomAppend inserts sequential extents at the end of the tree
// (append pattern, not random-offset insert). Mirrors C's random_append().
func randomAppend(t *testing.T, ft *FlexTree, bf *BruteForce, count uint64) {
	if ft != nil {
		resetRand()
		ft.Insert(0, 0, 4)
		var maxLoff uint64 = 4
		for i := uint64(0); i < count; i++ {
			len := uint32(randInt()%1000 + 1)
			ft.InsertWTag(maxLoff, maxLoff, len, uint16(i%0xffff))
			maxLoff += uint64(len)
		}
	}
	if bf != nil {
		resetRand()
		bf.Insert(0, 0, 4)
		var maxLoff uint64 = 4
		for i := uint64(0); i < count; i++ {
			len := uint32(randInt()%1000 + 1)
			bf.InsertWTag(maxLoff, maxLoff, len, uint16(i%0xffff))
			maxLoff += uint64(len)
		}
	}
}

// countLeafNodes recursively counts leaf nodes reachable from root.
// Mirrors C's count_leaf_nodes().
func countLeafNodes(tree *FlexTree, id NodeID) uint64 {
	if id.IsLeaf() {
		return 1
	}
	ie := tree.GetInternal(id)
	var c uint64
	for i := uint32(0); i < ie.Count+1; i++ {
		c += countLeafNodes(tree, ie.Children[i].NodeID)
	}
	return c
}

// countLeafNodesLL counts leaf nodes by walking the linked list from LeafHead.
func countLeafNodesLL(tree *FlexTree) uint64 {
	var c uint64
	id := tree.LeafHead
	for !id.IsIllegal() {
		le := tree.GetLeaf(id)
		c++
		id = le.Next
	}
	return c
}

// flextreeCheck validates tree integrity by summing extent lengths via Pos API.
// Mirrors C's flextree_check().
func flextreeCheck(t *testing.T, tree *FlexTree) {
	t.Helper()
	var totalLen uint64
	pos := tree.PosGet(0)
	for pos.Valid() {
		ext := &pos.node.Extents[pos.Idx]
		totalLen += uint64(ext.Len)
		pos.ForwardExtent()
	}
	if tree.GetMaxLoff() != totalLen {
		t.Fatalf("flextreeCheck: max_loff=%d but total extent len=%d", tree.GetMaxLoff(), totalLen)
	}
}

// TestFlextree_Test5_CoW is the same as Test5 but uses CoW page-based
// persistence instead of greenpack serialization.
func TestFlextree_Test5_CoW(t *testing.T) {
	count := uint64(5000)

	fs, dir := newTestFS(t)

	ft, err := OpenFlexTreeCoW(dir, fs)
	panicOn(err)
	bf := OpenBruteForce(DefaultFlexTreeMaxExtentSizeLimit)

	randomInsert(t, ft, bf, count)

	// Count leaves via linked list and via tree recursion.
	c1 := countLeafNodesLL(ft)
	c2 := countLeafNodes(ft, ft.Root)
	if c1 != c2 {
		t.Fatalf("linked list count %d != tree walk count %d", c1, c2)
	}

	// Sync and reopen via CoW.
	panicOn(ft.SyncCoW())
	panicOn(ft.CloseCoW())
	ft, err = OpenFlexTreeCoW(dir, fs)
	panicOn(err)

	// Verify linked list count survives CoW persistence.
	c1r := countLeafNodesLL(ft)
	if c1r != c2 {
		t.Fatalf("after CoW reload: linked list count %d != original tree walk count %d", c1r, c2)
	}

	// Delete 3/4 of data.
	ft.Delete(ft.GetMaxLoff()/4, ft.GetMaxLoff()/4*3)
	bf.Delete(bf.GetMaxLoff()/4, bf.GetMaxLoff()/4*3)
	c2 = countLeafNodes(ft, ft.Root)

	// Sync and reopen via CoW.
	panicOn(ft.SyncCoW())
	panicOn(ft.CloseCoW())
	ft, err = OpenFlexTreeCoW(dir, fs)
	panicOn(err)

	// Verify MaxLoff matches oracle.
	if ft.GetMaxLoff() != bf.GetMaxLoff() {
		t.Fatalf("MaxLoff mismatch: ft=%d bf=%d", ft.GetMaxLoff(), bf.GetMaxLoff())
	}
	sequentialQuery(t, ft, bf, ft.GetMaxLoff())

	c1 = countLeafNodesLL(ft)
	if c1 != c2 {
		t.Fatalf("after delete+CoW reload: linked list count %d != tree walk count %d", c1, c2)
	}

	// Delete everything.
	ft.Delete(0, ft.GetMaxLoff())
	bf.Delete(0, bf.GetMaxLoff())
	c2 = countLeafNodes(ft, ft.Root)

	// Sync and reopen via CoW.
	panicOn(ft.SyncCoW())
	panicOn(ft.CloseCoW())
	ft, err = OpenFlexTreeCoW(dir, fs)
	panicOn(err)

	if ft.GetMaxLoff() != bf.GetMaxLoff() {
		t.Fatalf("final MaxLoff mismatch: ft=%d bf=%d", ft.GetMaxLoff(), bf.GetMaxLoff())
	}

	c1 = countLeafNodesLL(ft)
	if c1 != c2 {
		t.Fatalf("final: linked list count %d != tree walk count %d", c1, c2)
	}

	sequentialQuery(t, ft, bf, ft.GetMaxLoff())
	bf.Close()
	panicOn(ft.CloseCoW())
}

// TestFlextree_Test9 tests large persistent tree with repeated append/insert
// cycles and integrity checks via Pos API.
// Mirrors C test9 at reduced scale (C does 2.6B inserts + 10K rounds).
func TestFlextree_Test9(t *testing.T) {
	count := uint64(500)
	fs, dir := newTestFS(t)

	// Phase 1: sequential inserts with 128KB max extent size.
	ft, err := OpenFlexTreeCoW(dir, fs)
	panicOn(err)
	ft.MaxExtentSize = 128 << 10

	fillCount := uint64(50000)
	for i := uint64(0); i < fillCount; i++ {
		ft.Insert(i*156, i*156, 156)
	}
	//panicOn(saveFlexTree(ft, fn))
	ft.SyncCoW()

	// Phase 2: repeated rounds of append, close/reopen, insert, close/reopen, check.
	ft, err = OpenFlexTreeCoW(dir, fs)
	panicOn(err)
	rounds := 10
	for r := 0; r < rounds; r++ {
		randomAppend(t, ft, nil, count)
		//panicOn(saveFlexTree(ft, fn))
		ft.SyncCoW()
		ft, err = OpenFlexTreeCoW(dir, fs)
		panicOn(err)

		randomInsert(t, ft, nil, count)
		ft.SyncCoW()
		//panicOn(saveFlexTree(ft, fn))

		ft, err = OpenFlexTreeCoW(dir, fs)
		panicOn(err)

		flextreeCheck(t, ft)
		checkFlexTreeContiguity(t, ft, fmt.Sprintf("Test9-round%d", r))
	}

	ft.Close()
}

func TestEndianMatchesC(t *testing.T) {
	ext := FlexTreeExtent{
		Loff: 0x11223344,
		Len:  0x55667788,
	}

	ext.SetTag(0x99AA)
	ext.SetAddress(0xBEEFCAFE)
	ext.SetHole(true)

	// Cast the struct's memory pointer to a raw byte slice of size 16
	extSize := unsafe.Sizeof(ext)
	extBytes := unsafe.Slice((*byte)(unsafe.Pointer(&ext)), extSize)

	expect := `44 33 22 11 88 77 66 55 AA 99 FE CA EF BE 00 80 `

	var obs string
	for _, b := range extBytes {
		obs += fmt.Sprintf("%02X ", b)
	}
	//fmt.Printf("Go Output: ")
	//fmt.Println(obs)
	if obs != expect {
		panicf("mismatch! obs(%v) != expected(%v)", obs, expect)
	}
}

/* the C to check that we emulate the tag/poff bit packing correctly.
#include <stdio.h>
#include <stdint.h>

struct flextree_extent {
    uint32_t loff;
    uint32_t len;
    uint64_t tag  :16;
    uint64_t poff :48; // 47 bits for address, 1 bit for hole
} __attribute__((packed));

int main() {
    struct flextree_extent ext = {0};

    // Assign recognizable test values
    ext.loff = 0x11223344;
    ext.len  = 0x55667788;
    ext.tag  = 0x99AA;

    // Address: 0xBEEFCAFE (Fits well within 47 bits)
    uint64_t address = 0xBEEFCAFE;
    // Shift a 1 into the 48th position (bit 47) for the hole flag
    uint64_t hole_bit = 1ULL << 47;

    ext.poff = hole_bit | address;

    // Treat the struct as an array of 16 bytes and print them
    uint8_t *bytes = (uint8_t *)&ext;
    printf("C Output:  ");
    for (int i = 0; i < 16; i++) {
        printf("%02X ", bytes[i]);
    }
    printf("\n");

    return 0;
}
*/
