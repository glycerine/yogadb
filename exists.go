package yogadb

import (
	"fmt"
	"io"
	iofs "io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/glycerine/vfs"
)

func fileExists(fs vfs.FS, name string) bool {
	fi, err := fs.Stat(name)
	if err != nil {
		return false
	}
	if fi.IsDir() {
		return false
	}
	return true
}

func dirExists(fs vfs.FS, name string) bool {
	fi, err := fs.Stat(name)
	if err != nil {
		return false
	}
	if fi.IsDir() {
		return true
	}
	return false
}

func fileSize(fs vfs.FS, name string) (int64, error) {
	fi, err := fs.Stat(name)
	if err != nil {
		return -1, err
	}
	return fi.Size(), nil
}

// IsWritable returns true if the file
// does not exist. Otherwise it checks
// the write bits. If any write bits
// (owner, group, others) are set, then
// we return true. Otherwise false.
func isWritable(fs vfs.FS, path string) bool {
	if !fileExists(fs, path) {
		return true
	}
	fileInfo, err := fs.Stat(path)
	panicOn(err)

	// Get the file's mode (permission bits)
	mode := fileInfo.Mode()

	// Check write permission for owner, group, and others
	return mode&0222 != 0 // Write permission for any?
}

func copyFileDestSrc(fs vfs.FS, topath, frompath string) (int64, error) {
	if !fileExists(fs, frompath) {
		return 0, os.ErrNotExist
	}

	src, err := fs.Open(frompath)
	if err != nil {
		return 0, err
	}
	defer src.Close()

	dest, err := fs.Create(topath, vfs.WriteCategoryUnspecified)
	if err != nil {
		return 0, err
	}
	defer dest.Close()

	return io.Copy(dest, src)
}

func truncateFileToZero(fs vfs.FS, path string) error {
	//var perm os.FileMode
	//f, err := os.OpenFile(path, os.O_TRUNC, perm)
	//f, err := fs.Create(path)
	f, err := fs.Create(path, vfs.WriteCategoryUnspecified)

	if err != nil {
		return fmt.Errorf("could not open file %q for truncation: %v", path, err)
	}
	if err = f.Close(); err != nil {
		return fmt.Errorf("could not close file handler for %q after truncation: %v", path, err)
	}
	return nil
}

func fileSizeModTime(fs vfs.FS, name string) (sz int64, modTime time.Time, err error) {
	var fi os.FileInfo
	fi, err = fs.Stat(name)
	if err != nil {
		return
	}
	return fi.Size(), fi.ModTime(), nil
}

func homed(path string) string {
	home := os.Getenv("HOME")
	return strings.Replace(path, "~", home, 1)
}

func removeAllFilesWithPrefix(prefixPath string) {
	matches, err := filepath.Glob(prefixPath + "*")
	if err != nil {
		return
	}
	//vv("matches = '%#v'", matches)
	for _, m := range matches {
		//vv("would delete '%v'", m)
		os.Remove(m)
	}
}

// DirSize calculates the total size of a directory in bytes.
// For symlinks, this will count the size of the target path
// string itself but not the size of the pointed-to-file.
func dirSize(fs vfs.FS, path string) (totalSize int64, err0 error) {

	err0 = fs.WalkDir(path, func(filePath string, d iofs.DirEntry, err error) error {
		// Handle permission errors or missing files gracefully
		if err != nil {
			return err
		}

		// We only want to add the size of actual files, not the directories themselves
		if !d.IsDir() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			totalSize += info.Size()
		}
		return nil
	})

	return
}

// MustDirSize panics on any error encountered
// when trying to return the number of bytes
// used by the path directory/file.
func mustDirSize(fs vfs.FS, path string) int64 {
	totalSize, err := dirSize(fs, path)
	panicOn(err)
	return totalSize
}

func mustListDir(fs vfs.FS, path string) (r string) {

	err0 := fs.WalkDir(path, func(filePath string, d iofs.DirEntry, err error) error {
		// Handle permission errors or missing files gracefully
		if err != nil {
			return err
		}
		r += filePath + "\n"
		return nil
	})
	panicOn(err0)
	return
}

func mustStatFileSize(fd vfs.File) int64 {
	fi, err := fd.Stat()
	panicOn(err)
	return fi.Size()
}
