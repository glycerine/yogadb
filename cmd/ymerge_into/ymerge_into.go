package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/glycerine/yogadb"
)

const cmd = "ymerge_into"

func main() {
	os.Exit(runYMergeInto(os.Args[1:], os.Stderr))
}

type ymergeArgs struct {
	dstPath           string
	srcPath           string
	tiesToDestination bool
}

func parseYMergeArgs(args []string, stderr io.Writer) (ymergeArgs, int) {
	var out ymergeArgs
	flags := flag.NewFlagSet(cmd, flag.ContinueOnError)
	flags.SetOutput(stderr)
	tiesToDest := flags.Bool("ties-to-dest", false, "resolve equal-HLC conflicts in favor of the destination database")
	if err := flags.Parse(args); err != nil {
		return out, 2
	}
	if flags.NArg() != 2 {
		fmt.Fprintf(stderr, "%s use: ymerge_into [-ties-to-dest] <destination-db> <source-db>\n", cmd)
		return out, 2
	}
	out.dstPath = flags.Arg(0)
	out.srcPath = flags.Arg(1)
	out.tiesToDestination = *tiesToDest
	if out.srcPath == out.dstPath {
		fmt.Fprintf(stderr, "%s error: source and destination must be different databases\n", cmd)
		return out, 2
	}
	return out, 0
}

func runYMergeInto(args []string, stderr io.Writer) int {
	parsed, code := parseYMergeArgs(args, stderr)
	if code != 0 {
		return code
	}
	if !dirExists(parsed.dstPath) {
		fmt.Fprintf(stderr, "%s error: destination database path does not exist: %q\n", cmd, parsed.dstPath)
		return 1
	}
	if !dirExists(parsed.srcPath) {
		fmt.Fprintf(stderr, "%s error: source database path does not exist: %q\n", cmd, parsed.srcPath)
		return 1
	}

	cfg := &yogadb.Config{
		OmitFlexSpaceOpsRedoLog: true,
		OmitMemWalFsync:         true,
	}

	dst, err := yogadb.OpenFlexDB(parsed.dstPath, cfg)
	if err != nil {
		fmt.Fprintf(stderr, "%s error: open destination: %v\n", cmd, err)
		return 1
	}
	defer dst.Close()
	dst.AllowReads()

	src, err := yogadb.OpenFlexDB(parsed.srcPath, cfg)
	if err != nil {
		fmt.Fprintf(stderr, "%s error: open source: %v\n", cmd, err)
		return 1
	}
	defer src.Close()
	src.AllowReads()

	t0 := time.Now()
	stats, err := dst.MergeFromWithOptions(src, yogadb.MergeOptions{
		TiesToDestination: parsed.tiesToDestination,
	})
	if err != nil {
		fmt.Fprintf(stderr, "%s error: merge: %v\n", cmd, err)
		return 1
	}
	if err := dst.Sync(); err != nil {
		fmt.Fprintf(stderr, "%s error: sync destination: %v\n", cmd, err)
		return 1
	}
	fmt.Fprintf(stderr, "%s merged %q into %q in %v\n", cmd, parsed.srcPath, parsed.dstPath, time.Since(t0))
	fmt.Fprintf(stderr, "stats: %+v\n", *stats)
	return 0
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}
