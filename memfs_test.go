//go:build memfs

package yogadb

import (
	"testing"

	"github.com/glycerine/vfs"
)

const memFiles = true

// newTestFS returns a fresh, isolated in-memory file system.
func newTestFS(t *testing.T) (fs vfs.FS, freshlyMadeTempDir string) {
	fs = vfs.NewMem()
	freshlyMadeTempDir = "yogadb_test_root"
	panicOn(fs.MkdirAll(freshlyMadeTempDir, 0755))
	return
}
