package yogadb

import (
	"os"

	"testing"
)

var _ = os.Remove

func TestSaver(t *testing.T) {

	fs, dir := newTestFS(t)

	fn := dir + sep + "saver_test_" + cryRand15B() + ".out"
	defer os.Remove(fn)

	tree, err := openOrCreateFlexTree(fs, "")
	panicOn(err)

	//count := uint64(5_000_000) // green, but 15 seconds to run.
	count := uint64(5_000)

	randomInsert(t, tree, nil, count)
	//vv("done with randomInsert of count %v", count)

	//vv("tree = %v", tree)

	// 2356 (2.3 KB) or 2050 without ChildSlotIDs on disk. int64 NodeID.
	// 2104 with int32 NodeID.
	//vv("size of first Internal Node: %v", tree.InternalArena[0].Msgsize())

	// 4213 (4.114 KB) for int64 NodeID.
	// 4201 for int32 NodeID.
	//vv("size of first Leaf Node: %v", tree.LeafArena[0].Msgsize())

	err = saveFlexTree(tree, fn)
	panicOn(err)

	tree2, err := openOrCreateFlexTree(fs, fn)
	panicOn(err)

	//vv("tree2 = %v", tree2)

	diff := tree.Diff(tree2)
	if diff != "" {
		panicf("original(A) differs from restored tree(B): '%v'", diff)
	}
	//vv("no diff seen between saved and restored trees.")
}
