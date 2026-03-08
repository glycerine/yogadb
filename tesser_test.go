package yogadb

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/glycerine/vfs"
)

// CopyToMemFS copies a file (or recursively a directory) from the real FS into a vfs.MemFS.
func CopyToMemFS(memDestFS vfs.FS, realSrcPath, memDestPath string) error {
	return filepath.Walk(realSrcPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Compute the relative path, then join with memDestPath
		rel, err := filepath.Rel(realSrcPath, path)
		if err != nil {
			return err
		}
		destPath := vfs.Default.PathJoin(memDestPath, rel)

		if info.IsDir() {
			return memDestFS.MkdirAll(destPath, 0755)
		} else {
			// automatically make target parent directories.
			dir := filepath.Dir(destPath)
			if dir != "." && dir != "/" {
				panicOn(memDestFS.MkdirAll(dir, 0755))
			}
		}

		return copyFileToMemFS(memDestFS, path, destPath)
	})
}

func copyFileToMemFS(memDestFS vfs.FS, realSrc, memDest string) error {
	// Open the source file from the real FS
	src, err := os.Open(realSrc)
	if err != nil {
		return err
	}
	defer src.Close()

	// Create the destination file in MemFS
	dst, err := memDestFS.Create(memDest, vfs.WriteCategoryUnspecified)
	if err != nil {
		return err
	}
	defer dst.Close()

	_, err = io.Copy(dst, src)
	return err
}

func Test104_CopyAssetsFromRealFilesystemIntoMemFS(t *testing.T) {
	mem := vfs.NewMem()

	// Copy your real test assets into the MemFS
	if err := CopyToMemFS(mem, "./assets", "assets"); err != nil {
		t.Fatalf("failed to copy assets into MemFS: %v", err)
	}

}
