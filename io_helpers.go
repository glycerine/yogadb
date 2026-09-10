package yogadb

import (
	"fmt"
	"io"

	"github.com/glycerine/vfs"
)

func writeAtFull(f vfs.File, p []byte, off int64, context string) error {
	n, err := f.WriteAt(p, off)
	if err != nil {
		return fmt.Errorf("%s: write at %d len %d: %w", context, off, len(p), err)
	}
	if n != len(p) {
		return fmt.Errorf("%s: short write at %d: wrote %d bytes, want %d: %w",
			context, off, n, len(p), io.ErrShortWrite)
	}
	return nil
}

func readAtFull(f vfs.File, p []byte, off int64, context string) error {
	n, err := f.ReadAt(p, off)
	if err != nil {
		return fmt.Errorf("%s: read at %d len %d: n=%d: %w", context, off, len(p), n, err)
	}
	if n != len(p) {
		return fmt.Errorf("%s: short read at %d: read %d bytes, want %d: %w",
			context, off, n, len(p), io.ErrUnexpectedEOF)
	}
	return nil
}
