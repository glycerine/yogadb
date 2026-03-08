//go:build !memfs

package yogadb

import (
	"testing"

	"github.com/glycerine/vfs"
)

const memFiles = false

// newTestFS returns Pebble's wrapper for the real OS file system.
func newTestFS(t *testing.T) (fs vfs.FS, freshlyMadeTempDir string) {
	return vfs.Default, t.TempDir()
}
