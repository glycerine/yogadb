package main

import (
	"fmt"
	"os"
	"time"

	"github.com/glycerine/yogadb"
)

const cmd = "ymerge_into"

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "%s use: ymerge_into <destination-db> <source-db>\n", cmd)
		os.Exit(1)
	}
	srcPath := os.Args[2]
	dstPath := os.Args[1]
	if !dirExists(srcPath) {
		fmt.Fprintf(os.Stderr, "%s error: source database path does not exist: %q\n", cmd, srcPath)
		os.Exit(1)
	}
	if !dirExists(dstPath) {
		fmt.Fprintf(os.Stderr, "%s error: destination database path does not exist: %q\n", cmd, dstPath)
		os.Exit(1)
	}
	if srcPath == dstPath {
		fmt.Fprintf(os.Stderr, "%s error: source and destination must be different databases\n", cmd)
		os.Exit(1)
	}

	cfg := &yogadb.Config{
		OmitFlexSpaceOpsRedoLog: true,
		OmitMemWalFsync:         true,
	}

	src, err := yogadb.OpenFlexDB(srcPath, cfg)
	panicOn(err)
	defer src.Close()
	src.AllowReads()

	dst, err := yogadb.OpenFlexDB(dstPath, cfg)
	panicOn(err)
	defer dst.Close()
	dst.AllowReads()

	t0 := time.Now()
	stats, err := dst.MergeFrom(src)
	panicOn(err)
	panicOn(dst.Sync())
	fmt.Fprintf(os.Stderr, "%s merged %q into %q in %v\n", cmd, srcPath, dstPath, time.Since(t0))
	fmt.Fprintf(os.Stderr, "stats: %+v\n", *stats)
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

func panicOn(err error) {
	if err != nil {
		panic(err)
	}
}
