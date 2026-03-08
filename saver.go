package yogadb

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
	//"time"

	"github.com/glycerine/greenpack/msgp"
	"github.com/glycerine/vfs"
	//"github.com/klauspost/compress/s2"
)

// gain from compression: 13MB -> 1.6MB for the linux paths (8x).
//const globalFullSkipS2 = false

var FlexTreeSaverMagic string = "flexspace.FlexTreeSaverMagic:"

type FlexTreeSaver struct {
	w *msgp.Writer
	//ws2         *s2.Writer
	mut         sync.Mutex
	numLeafWrit int64
	tree        *FlexTree

	//skipS2 bool // compression or not
}

// ======================== FlexTree persistence helpers ========================

// openOrCreateFlexTree loads the FlexTree from treePath if it exists,
// or creates a fresh tree if not.
func openOrCreateFlexTree(fs vfs.FS, treePath string) (*FlexTree, error) {
	needNew := false
	if treePath == "" {
		needNew = true
	}
	var f vfs.File
	var err error
	if !needNew {
		f, err = fs.Open(treePath)
		if err != nil && os.IsNotExist(err) {
			needNew = true
		}
	}
	if needNew {
		// Brand-new tree
		tree := NewFlexTree(fs)
		tree.Path = treePath
		root := tree.AllocLeaf()
		root.Dirty = true
		tree.NodeCount++
		tree.Root = root.NodeID
		tree.LeafHead = root.NodeID
		return tree, nil
	}
	if err != nil {
		return nil, err
	}
	f.Sync() // make sure we are reading what is on disk, not spurious.
	defer f.Close()
	loader, err := NewFlexTreeLoader(fs, f)
	if err != nil {
		return nil, err
	}
	tree, err := loader.Load()
	if err != nil {
		return nil, err
	}
	tree.Path = treePath
	return tree, nil
}

// saveFlexTree increments PersistentVersion and atomically writes
// the tree checkpoint to treePath using a tmp-then-rename.
func saveFlexTree(tree *FlexTree, treePath string) error {
	tree.PersistentVersion++
	tmpPath := treePath + ".tmp"
	f, err := tree.fs.Create(tmpPath, vfs.WriteCategoryUnspecified)
	if err != nil {
		return fmt.Errorf("flexspace: saveFlexTree create tmp: %w", err)
	}
	saver, err := tree.NewTreeSaver(f)
	if err != nil {
		f.Close()
		tree.fs.Remove(tmpPath)
		return fmt.Errorf("flexspace: saveFlexTree saver: %w", err)
	}
	if err := saver.Save(); err != nil {
		f.Close()
		tree.fs.Remove(tmpPath)
		return fmt.Errorf("flexspace: saveFlexTree save: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		tree.fs.Remove(tmpPath)
		return fmt.Errorf("flexspace: saveFlexTree fsync: %w", err)
	}
	f.Close()
	if err := tree.fs.Rename(tmpPath, treePath); err != nil {
		tree.fs.Remove(tmpPath)
		return fmt.Errorf("flexspace: saveFlexTree rename: %w", err)
	}
	return nil
}

func (tree *FlexTree) NewTreeSaver(w io.Writer) (*FlexTreeSaver, error) {
	r := &FlexTreeSaver{
		tree: tree,
		//skipS2: globalFullSkipS2,
	}

	// write magic uncompressed.
	_, err := w.Write([]byte(FlexTreeSaverMagic))
	if err != nil {
		return nil, fmt.Errorf("error writing FlexTreeSaverMagic: '%v'", err)
	}
	//if r.skipS2 {
	r.w = msgp.NewWriter(w)
	//} else {
	//	r.ws2 = s2.NewWriter(w)
	//	r.w = msgp.NewWriter(r.ws2)
	//}

	return r, nil
}

func (s *FlexTreeSaver) NumLeafWrit() (r int64) {
	s.mut.Lock()
	r = s.numLeafWrit
	s.mut.Unlock()
	return
}

func (s *FlexTreeSaver) Save() error {
	s.mut.Lock()
	defer s.mut.Unlock()

	// not ByteSlice framed. okay.
	err := s.tree.EncodeMsg(s.w)
	if err != nil {
		return fmt.Errorf("error on EncodeMsg "+
			"of tree: '%v'", err)
	}

	err = s.w.Flush()
	var err2 error
	//if !s.skipS2 {
	//	err2 = s.ws2.Close() // does Flush first.
	//}
	if err != nil {
		return err
	}
	return err2
}

func (s *FlexTreeSaver) Flush() (err error) {
	s.mut.Lock()
	defer s.mut.Unlock()
	err = s.w.Flush()
	var err2 error
	//if !s.skipS2 {
	//	err2 = s.ws2.Flush()
	//}
	if err != nil {
		return err
	}
	return err2
}

type FlexTreeLoader struct {
	r *msgp.Reader
	//rs2  *s2.Reader
	tree *FlexTree

	//skipS2 bool
}

func NewFlexTreeLoader(fs vfs.FS, r io.Reader) (*FlexTreeLoader, error) {
	s := &FlexTreeLoader{
		tree: NewFlexTree(fs),
		//skipS2: globalFullSkipS2,
	}

	// read magic uncompressed.
	buf := make([]byte, len(FlexTreeSaverMagic))
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return nil, fmt.Errorf("error reading FlexTreeSaverMagic: '%v'", err)
	}
	if !bytes.Equal(buf, []byte(FlexTreeSaverMagic)) {
		return nil, fmt.Errorf("error did not find '%v' at start", FlexTreeSaverMagic)
	}

	//if s.skipS2 {
	s.r = msgp.NewReader(r)
	//} else {
	//	s.rs2 = s2.NewReader(r)
	//	s.r = msgp.NewReader(s.rs2)
	//}
	return s, nil
}

func (s *FlexTreeLoader) Load() (tree *FlexTree, err error) {

	tree = s.tree

	err = tree.DecodeMsg(s.r)
	if err != nil {
		return nil, fmt.Errorf("error on "+
			"tree DecodeMsg: '%v'", err)
	}

	return tree, nil
}
