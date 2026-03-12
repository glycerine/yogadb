package yogadb

import (
	"bufio"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/glycerine/vfs"
)

// ====================== Batch tests ======================

// TestFlexDB_BatchBasic verifies Batch.Commit writes are visible via Get.
func TestFlexDB_BatchBasic(t *testing.T) {
	db, fs := openTestDB(t, nil)
	_ = fs
	//vv("fs = '%#v'", fs)

	batch := db.NewBatch()
	batch.Set("k1", []byte("v1"))
	batch.Set("k2", []byte("v2"))
	batch.Set("k3", []byte("v3"))
	if _, err := batch.Commit(false); err != nil {
		t.Fatal(err)
	}
	batch.Close()

	mustGet(t, db, "k1", "v1")
	mustGet(t, db, "k2", "v2")
	mustGet(t, db, "k3", "v3")
	mustMiss(t, db, "k4")
}

// TestFlexDB_BatchOverwrite verifies last write wins within a batch.
func TestFlexDB_BatchOverwrite(t *testing.T) {
	db, _ := openTestDB(t, nil)

	mustPut(t, db, "k1", "original")

	batch := db.NewBatch()
	batch.Set("k1", []byte("updated"))
	batch.Set("k1", []byte("final"))
	if _, err := batch.Commit(false); err != nil {
		t.Fatal(err)
	}
	batch.Close()

	mustGet(t, db, "k1", "final")
}

// TestFlexDB_BatchEmpty verifies an empty batch commit is a no-op.
func TestFlexDB_BatchEmpty(t *testing.T) {
	db, _ := openTestDB(t, nil)
	mustPut(t, db, "k1", "v1")

	batch := db.NewBatch()
	if _, err := batch.Commit(false); err != nil {
		t.Fatal(err)
	}
	batch.Close()

	mustGet(t, db, "k1", "v1")
}

// TestFlexDB_BatchPersistence verifies batch writes survive Sync + reopen.
func TestFlexDB_BatchPersistence(t *testing.T) {
	fs, dir := newTestFS(t)
	cfg := &Config{
		FS: fs,
	}
	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}

	batch := db.NewBatch()
	for i := 0; i < 100; i++ {
		batch.Set(fmt.Sprintf("key%03d", i), []byte(fmt.Sprintf("val%03d", i)))
	}
	if _, err := batch.Commit(false); err != nil {
		t.Fatal(err)
	}
	batch.Close()
	db.Sync()
	db.Close()

	// Reopen and verify.
	db2 := openTestDBAt(fs, t, dir, nil)
	for i := 0; i < 100; i++ {
		mustGet(t, db2, fmt.Sprintf("key%03d", i), fmt.Sprintf("val%03d", i))
	}
}

// verifies a large batch (10k keys).
func Test630_FlexDB_BatchMany(t *testing.T) {
	//dir := "test_batchmany630.outdir"
	//panicOn(os.RemoveAll(dir))
	//db, fs := openTestDBAt(fs, t,  dir, nil)

	db, fs := openTestDB(t, nil)
	_ = fs
	const n = 10000
	batch := db.NewBatch()
	nWrit := 0
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%06d", i)
		val := []byte(fmt.Sprintf("val%06d", i))
		batch.Set(key, val)
		nWrit += len(key) + len(val)
	}
	if _, err := batch.Commit(false); err != nil {
		t.Fatal(err)
	}
	batch.Close()

	db.Sync()

	for i := 0; i < n; i++ {
		mustGet(t, db, fmt.Sprintf("key%06d", i), fmt.Sprintf("val%06d", i))
	}
	//dirSz := MustDirSize(dir)
	//wa := float64(dirSz) / float64(nWrit)
	//vv("end _BatchMany. db.Path = '%v', dir size = %v; nWrit = %v, WA = %v", db.Path, dirSz, nWrit, wa)
}

// was seeing some data loss on bench inject then output.
//
// Root cause: kv128 codec couldn't distinguish nil from empty
//
// The kv128Encode/kv128Decode pair used vlen=0 for
// both tombstones (Value == nil) and empty values (Value == []byte{}).
// After a round-trip through FlexSpace (flush + read-back),
// empty values were silently converted to tombstones,
// causing data loss.
//
// This was a pre-existing bug that affects db.Put(key, []byte{})
// too - not just Batch. Any key-as-set-member pattern
// (storing keys with empty values, as Pebble benchmarks commonly do)
// would lose data after a memtable flush.
//
// Fix: shifted value length encoding
//
// The encoded rawVlen field now uses:
// - rawVlen == 0 -> tombstone (Value = nil), no value bytes follow
// - rawVlen == len(Value) + 1 -> value of length len(Value), followed by that many bytes
//
// So empty values encode as rawVlen=1 (with 0 data bytes), cleanly
// distinguishing them from tombstones (rawVlen=0). All
// four codec functions were updated: kv128Encode, kv128Decode,
// kv128EncodedSize, kv128SizePrefix.
func Test640_FlexDB_Batch_NoDataLoss(t *testing.T) {
	fs, dir := newTestFS(t)
	if !fs.IsReal() {
		panicOn(fs.MountReadOnlyRealDir("assets", "assets"))
	}
	db := openTestDBAt(fs, t, dir, nil)

	panicOn(batchLoadAndReadOut(fs, t, db, "assets/one.txt"))

	// Note: In FlexSpace.syncR(), SyncCoW() is only called
	// when the log exceeds FLEXFILE_LOG_MAX_SIZE (2GB). For small datasets,
	// the FlexTree is never synced to disk until Close().

	//metrics := db.SessionMetrics()
	//if metrics.TreeBytesWritten != 0 {
	//	panicf("why was metrics.TreeBytesWritten != 0 in: \n %v\n", metrics)
	//}
	//if metrics.VLOGBytesWritten != 0 {
	//	panicf("why was metrics.VLOGBytesWritten != 0 in: \n %v\n", metrics)
	//}
}

// Test641 exercises batch ingest+readback on each asset file in a fresh DB.
// note when comparing with sort on the command line:
// export LC_ALL=C
// first to get byte.Compare equivalent sorting,
// instead of locale sorting. (for hsk_words.txt).
func Test641_FlexDB_Batch_AllAssets(t *testing.T) {

	for _, f := range []string{
		"assets/one.txt",
		"assets/alphabet.txt",
		"assets/medium.txt",
		"assets/hsk_words.txt",
		"assets/linux.txt",
		"assets/uuid.txt",
		"assets/words.txt",
	} {
		t.Run(f, func(t *testing.T) {
			db, fs := openTestDB(t, nil)
			if !fs.IsReal() {
				panicOn(fs.MountReadOnlyRealDir("assets", "assets"))
			}
			panicOn(batchLoadAndReadOut(fs, t, db, f))
		})
	}
}

func batchLoadAndReadOut(fs vfs.FS, t *testing.T, db *FlexDB, dataPath string) error {

	if !fileExists(fs, dataPath) {
		panicf("could not locate dataPath '%v'", dataPath)
	}

	file, err := fs.Open(dataPath)
	panicOn(err)
	defer file.Close()

	verify := make(map[string]bool)

	scanner := bufio.NewScanner(file)

	// Configuration for batching
	const maxBatchCount = 10000
	batch := db.NewBatch()
	defer batch.Close()
	currentBatchCount := 0

	//fmt.Fprintf(os.Stderr, "Starting Batched YogaDB ingestion...\n")
	t0 := time.Now()

	duplicates := 0
	lines := 0
	for scanner.Scan() {
		lines++
		key := scanner.Bytes()

		skey := string(key)
		dup := verify[skey]
		if dup {
			duplicates++
		}
		verify[skey] = true

		// Add the key to the batch.
		// nil is passed for WriteOptions because options are applied at Commit.
		if err := batch.Set(string(key), []byte{}); err != nil {
			panicOn(err)
		}
		currentBatchCount++

		// When the batch hits our threshold, commit and reset.
		if currentBatchCount >= maxBatchCount {
			if _, err := batch.Commit(false); err != nil {
				panicOn(err)
			}
			currentBatchCount = 0
		}
	}

	if err := scanner.Err(); err != nil {
		panicOn(err)
	}

	// Commit any remaining keys that didn't fill up the final batch
	if currentBatchCount > 0 {
		if _, err := batch.Commit(false); err != nil {
			panicOn(err)
		}
	}
	db.Sync()

	//vv("lines = %v; duplicates = %v; unique seen = %v", lines, duplicates, len(verify))

	t1 := time.Now()
	elapsed := t1.Sub(t0)
	_ = elapsed
	//fmt.Fprintf(os.Stderr, "YogaDB ingestion finished in: %s\n", elapsed)
	//fmt.Fprintf(os.Stderr, "--------------------------------------------------\n")

	// write back out, to check completeness (no data is lost) and sorted-ness (proper order).
	saw := 0
	db.View(func(roDB *ReadOnlyTx) error {
		roDB.Ascend("", func(key string, value []byte) bool {
			saw++
			_, ok := verify[key]
			if !ok {
				panicf("extra key seen, was not in original input: '%v'", key)
			}
			delete(verify, key)
			return true
		})
		return nil
	})
	os.Stdout.Sync()
	//fmt.Fprintf(os.Stderr, "YogaDB saw %v, output in sorted order took: %v\n", saw, time.Since(t1))

	nLeft := len(verify)
	if nLeft > 0 {
		alwaysPrintf("bad! nLeft = %v that were not gotten back out after being put in!", nLeft)
		i := 0
		for k := range verify {
			_, inDB, _ := db.Get(k) // looks like none of them are in the db.

			fmt.Printf("in but not out [%02d]: '%v'  (inDB: %v)\n", i, k, inDB)
			i++
		}
		t.Fatalf("should have gotten back out everything we put in!")
	}
	return nil
}
