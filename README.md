YogaDB: a flexible-address-space key-value store in Go (golang)
===========

Bottom line up front: with minor modifications, and some
sophisticated system design, B-trees can beat LSM-trees 
on both write throughput and latency. (And B-trees have always 
stomped LSM-trees on reads).

# benchmarks up front: 

Let's compare three embedded, persistent, ordered key-value stores in Go.
Each of these can support more data than can fit in main memory,
persists its data on disk to survive reboots,
and each supports range queries and reading keys back in sorted order.

* random writes
~~~
Pebble (github.com/cockroachdb/pebble): Log Structured Merge (LSM) tree
-------------------------
1.2 seconds to write 100K random 21 byte keys and values.

BoltDB (go.etcd.io/bbolt): LMDB based memory-mapped B-tree
-------------------------
9 seconds to write 100K random 21 byte keys and values.

YogaDB: FlexSpace architecture
-----------------
400 msec to write 100K random 21 byte keys and values.
~~~

Here YogaDB is 2x faster than Pebble and 20x faster than BoltDB at random writes.

* read (iterate sequentially through all keys)

~~~
Pebble      114.9 iter_ns/key
Bolt         16.7 iter_ns/key
YogaDB       12.6 iter_ns/key
~~~

YogaDB is on par with BoltDB for read performance, and 10x faster than Pebble.

# what is YogaDB?

YogaDB is a Go port of the https://github.com/flexible-address-space/flexspace FlexSpace C project. YogaDB is an embedded key-value engine-as-a-library, similar in form to BoltDB(bbolt), Sqlite3, RocksDB, and LevelDB. YogaDB offers sorted-order key-range iteration methods Ascend/Descend alongside simple Put, Get, and Delete. Write batching and transactions are available. Large values are stored in a separate VLOG. Small values are stored inline with their keys.

Originally called FlexSpaceKV, we changed the name to YogaDB because 
(a) yoga keeps you flexible, and (b) there were too many other 
things with similar names[4]. Below you may see both terms; YogaDB
in Go is the port of FlexDB in C. FlexDB is the top layer, calling
down into the FlexSpace, which in turn calls down to the FlexTree.

<img width="1230" height="373" alt="image" src="https://github.com/user-attachments/assets/9519abbb-b1fe-4c82-9064-859591b0d341" />

Figure 2: the fundamentally new data structure is the FlexTree.

The FlexSpace architecture is heavily inspired by Log-structured File Systems (LFS),
and how filesystems in general handle extents (contiguous spans of
disk space). In addition, FlexSpace introduces a new data structure
called the FlexTree; a B-tree variant that has asymptodically better
big-O performance for Insert-Range extent operations. The third
idea is to separate logical from physical address space, and to sort
in logical space to minimize physical data movement.

* Why YogaDB: faster reads and writes than LSM trees. About 2x faster writes.

Part of the speed is due to defering compaction and on-disk garbage
collection (often called vacuuming, cf PostgreSQL). So YogaDB
becomes a space-time tradeoff. You can (temporarily) use more disk space to go faster.
Then call VacuumKV() and VacuumVLOG() only when you are ready.

~~~
Quick benchmark loading the lines in assets/ as both key and value:

I. Pebble (LSM-tree based engine under CockroachDB; in Go): 6.45 seconds.

jaten@jbook ~/yogadb/load_pebble (master) $ make
rm -rf bench_pebble.db
rm -f load_pebble
go build
./load_pebble ~/all
2026/03/05 20:56:02 Found 0 WALs

pebble.go:75 [goID 1] 2026-03-05 20:56:02.667426000 +0000 UTC note: duplicating key as value too to see VLOG fill. This will double our logical write!
Starting Batched Pebble ingestion to dbPath: bench_pebble.db ...
Starting Batched Pebble ingestion...
Pebble ingestion finished in: 6.447456109s
--------------------------------------------------
Total Bytes Ingested (Logical):  24129150 bytes
Total Bytes Flushed (Physical):  34065912 bytes
Total Bytes Compacted:           21021586 bytes
Total Write Amplification (WA):  2.28
du -sh bench_pebble.db
 14M	bench_pebble.db
jaten@jbook ~/yogadb/load_pebble (master) $ 

II. YogaDB (herein; the Flexspace design in Go): 3.57 seconds.

jaten@jbook ~/yogadb/load_yogadb (master) $ make
rm -rf bench.db
rm -f load_yogadb
go build
./load_yogadb ~/all
using REDO.LOG: true

yoga.go:120 [goID 1] 2026-03-05 20:55:44.415275000 +0000 UTC note: duplicating key as value too to see VLOG fill. This will double our logical write!
Starting Batched YogaDB ingestion to dbPath: bench.db ...
YogaDB ingestion finished in: 3.565796103s
--------------------------------------------------
finMetrics = Metrics{
      (just this) Session: true
        KV128 BytesWritten: 92434432
       MemWAL BytesWritten: 27422057
      REDOLog BytesWritten: 1354032
FlexTreePages BytesWritten: 823472
   LARGE.VLOG BytesWritten: 2910390
      Logical BytesWritten: 22816132
        Total BytesWritten: 124944383
                 WriteAmp: 5.476

   -------- lifetime totals over all sessions  --------  
    TotalLogical BytesWrit: 22816132
   TotalPhysical BytesWrit: 124944383
       CumulativeWriteAmp: 5.476

   -------- free space / block utilization --------  
            TotalLiveBytes: 46217216 (44.08 MB)
    TotalFreeBytesInBlocks: 4114432 (3.92 MB)
      FLEXSPACE_BLOCK_SIZE: 4.00 MB
               BlocksInUse: 12  (48.00 MB)
  BlocksWithLowUtilization: 1

   -------- based on parameters used --------  
    LowBlockUtilizationPct: 50.0 %
}

du -sh bench.db
 49M	bench.db
 
III. Run the batch_bench_test.go random write benchmarks
(versus the assets/ which are kind of sorted already)

YogaDB   no redo log: 3427 ns/random write
YogaDB with redo log: 2955 ns/random write
Pebble:               6011 ns/random write

These benchmarks write in batches, and defer the one fsync
until the end of all writes for maximum performance.

YogaDB is 2x faster if the Redo log is used, a tiny bit
slower without the redo log (omitting the redo log
has the advantage of reducing the write amplification
a little; again it is a space-time trade-off).

go test -v -run=xxx -bench BigRandomRWBatch
goos: darwin
goarch: amd64
pkg: github.com/glycerine/yogadb
cpu: Intel(R) Core(TM) i7-1068NG7 CPU @ 2.30GHz

BenchmarkYogaDB_BigRandomRWBatch
BenchmarkYogaDB_BigRandomRWBatch/batch_1000/redo=0/write=1_
BenchmarkYogaDB_BigRandomRWBatch/batch_1000/redo=0/write=1_-8         	  313790	      3427 ns/op	      1000 batch_size	  41277492 disk_bytes	  17572240 logical_bytes	      3427 our_PUT_ns/op	         2.349 write_amp	 103005932 yoga_total_physical_bytes_written	         5.862 yoga_write_amp

BenchmarkYogaDB_BigRandomRWBatch/batch_1000/redo=1/write=1_
BenchmarkYogaDB_BigRandomRWBatch/batch_1000/redo=1/write=1_-8         	  369402	      2955 ns/op	      1000 batch_size	  48995128 disk_bytes	  20686512 logical_bytes	      2955 our_PUT_ns/op	         2.368 write_amp	 121808896 yoga_total_physical_bytes_written	         5.888 yoga_write_amp

BenchmarkPebble_BigRandomRWBatch
BenchmarkPebble_BigRandomRWBatch/batch_1000_write=1
BenchmarkPebble_BigRandomRWBatch/batch_1000_write=1-8                 	  168066	      6011 ns/op	      1000 batch_size	  14579319 disk_bytes	   9411696 logical_bytes	      6011 our_PUT_ns/op	         1.926 pebble_write_amp	         1.549 write_amp

finished at Thu Mar  5 17:43:01
~~~

* How: Data is write-ahead-log appended Bitcask/Haystack 
style to disk once, and then (almost) never re-written
until manual vacuuming is invoked for on-disk GC.
Keys are sorted in memory in a FlexSpace, which is 
optimized for logical sorting. Persistence of the sorted structure is 
done with Copy-on-write. YogaDB's FlexSpace can 
handle more keys than can fit in RAM.

* The result: the C gets 2x the throughput of RocksDB (on both writes and reads). See pages 10-14 of the paper. Also, much better latency.

* Bottom line: Your SSD and NVMe drives are your friends. They want to 
help you write quickly while not sacrificing
read performance. Let them! (This design is somewhat optimized for them
rather than traditional spinning platters).

<img width="1258" height="785" alt="image" src="https://github.com/user-attachments/assets/0b467c28-4dbc-463b-94f7-53baaa6fd620" />

A note about Table 2: the Write-Amplification circa 1.03 only applies 
to the middle layer, FlexSpace, alone. Write Amplification is 
much greater (3-6x, not atypical for comparable systems) once 
the entire system is assembled.

[2] https://arxiv.org/abs/2510.05518 https://splinterdb.org/

[3] https://arxiv.org/abs/2509.10714

[4] The FlexSpace paper should not be confused with "Flex-KV: Enabling High-performance and Flexible KV Systems" 2012 by Phanishayee et al. https://www.microsoft.com/en-us/research/wp-content/uploads/2016/10/flexkv_mbds12.pdf which is from a completely different research group.

# Reference C code repository

[5] https://github.com/flexible-address-space/flexspace

# theory of operation (the paper)

[6] "Building an Efficient Key-Value Store in a Flexible Address Space", 
by C. Chen, W. Zhong, X. Wu, EuroSys ’22, April 5–8, 2022, RENNES, France.

[[ACM DL](https://dl.acm.org/doi/10.1145/3492321.3519555)]
[[Paper PDF](https://www.roychan.org/assets/publications/eurosys22chen.pdf)]
[[Slides](https://www.roychan.org/assets/publications/eurosys22chen-slides.pdf)]

# design summary

In his Recap slide #56, Dr. Chen says,

>> "FlexDB manages sorted data without using extra persistent indirections."

They map keys to physical locations on disk without 
maintaining a massive, constantly updating on-disk index.

The approach is to split the problem into two in-memory structures.

The Logical Illusion (FlexDB): FlexDB uses the insert-range API to shift logical addresses. The keys then appear perfectly sorted in a contiguous logical space. FlexDB only needs a tiny, lightweight, sparse index (e.g., "Keys A through M are in Logical Block 1"), which easily fits entirely in RAM.

The Extent Map (FlexSpace/FlexTree): The FlexTree maps those logical blocks to physical SSD blocks. Because it tracks large extents (megabytes of data at a time) rather than individual keys, it is also highly compressed and also fits entirely in RAM.

# comment on the API / goroutine safety

* API notes: all Go API calls are goroutine safe, except 
for iterator use (deliberately). Ascend and Descend iterators 
are designed to allow concurrent mutation while scanning through
the database, and hence by design they hold no locks. This allows the user
to delete keys on the fly, rather than collect a separate long list of things
to delete (a hazardous approach since that list could overflow available memory).

In short: users must bring their own synchronization during iteration. 
In other words, do not allow other goroutines to operate on the database
while you are iterating it.

# getting started

go get github.com/glycerine/yogadb

import "github.com/glycerine/yogadb"

The small example programs that have a single aim are probably the easiest way
to understand how to embed yogadb. See cmd/yload to load a bunch of key/value
pairs. See cmd/yview to dump them back out. See cmd/yvac to vacuum your
database. load_yogadb does all of the above and was used for running
some benchmarks; it is another example.

# api documentation

https://pkg.go.dev/github.com/glycerine/yogadb

will be the most up to date; but it case you are offline, run

go doc -all 

in the directory to see the latest.

~~~
package yogadb // import "github.com/glycerine/yogadb"

YogaDB is an embedded persistent ordered key-value store built on the FlexSpace
log-structured storage engine architecture. A three-layer architecture is
deployed: at the lowest level is the FlexTree (a B-tree extent index with
shift-propagation); at the middle level is the FlexSpace (log-structured file
layer with GC); and at the top sits the FlexDB (the KV store with memtables,
WAL, sparse index, and interval cache).

CONSTANTS

const (
	MaxKeySize = 4096
)
const (
	// the address space that flexspace.go manages
	FLEXSPACE_MAX_OFFSET = 800 << 30 // 800 GB logical address space

	// block config
	FLEXSPACE_BLOCK_BITS  = 22                                           // 4 MB blocks
	FLEXSPACE_BLOCK_SIZE  = 1 << FLEXSPACE_BLOCK_BITS                    // 4_194_304 bytes
	FLEXSPACE_BLOCK_COUNT = FLEXSPACE_MAX_OFFSET >> FLEXSPACE_BLOCK_BITS // 204800 blocks

	FLEXSPACE_MAX_EXTENT_BIT  = 5
	FLEXSPACE_MAX_EXTENT_SIZE = FLEXSPACE_BLOCK_SIZE >> FLEXSPACE_MAX_EXTENT_BIT // 131_072 bytes == 128 KB (1/32)

	// logical logging
	FLEXSPACE_LOG_MEM_CAP  = 8 << 20 // 8 MB in-memory log buffer
	FLEXSPACE_LOG_MAX_SIZE = 2 << 30 // 2 GB max on-disk log

	// garbage collector
	FLEXSPACE_GC_QUEUE_DEPTH = 8192
	FLEXSPACE_GC_THRESHOLD   = 64 // free blocks below this triggers GC

	// block manager
	FLEXSPACE_BM_BLKDIST_BITS = 16
	FLEXSPACE_BM_BLKDIST_SIZE = (FLEXSPACE_BLOCK_SIZE >> FLEXSPACE_BM_BLKDIST_BITS) + 1 // 65 buckets

)
const (
	// SLOTTED_PAGE_KB is the target page size for slotted page intervals.
	// Tune this for different workloads. Larger pages amortize overhead
	// better but increase rewrite cost.
	SLOTTED_PAGE_KB = 2

	// MAX_KEY_BYTES is the maximum key size in bytes that a slotted page
	// can hold. Derived from SLOTTED_PAGE_KB to ensure at least one key
	// always fits in a page.
	MAX_KEY_BYTES = SLOTTED_PAGE_KB * 512 // 32768 bytes

)
const DefaultFlexTreeMaxExtentSizeLimit = (64 << 20) // 64 MB
const FLEXTREE_HOLE = (1 << 47) // highest bit set to indicate a hole
const FLEXTREE_INTERNAL_CAP = 30
const FLEXTREE_INTERNAL_CAP_PLUS_ONE = FLEXTREE_INTERNAL_CAP + 1
const FLEXTREE_LEAF_CAP = 60
    Tunable parameters

const FLEXTREE_MAX_EXTENT_SIZE_LIMIT = 64 << 20
const FLEXTREE_PATH_DEPTH = 7 // at most 7 levels
const FLEXTREE_POFF_MASK = 0xffffffffffff // 48 bits
const MaxKeys = 7 // Example degree

VARIABLES

var (
	// ErrTxDone is returned when Commit is called on an already-cancelled transaction.
	ErrTxDone = errors.New("flexdb: transaction already finished")
	// ErrTxNotWritable is returned when Put or Delete is called on a read-only transaction.
	ErrTxNotWritable = errors.New("flexdb: cannot write in a read-only transaction")
)

TYPES

type Batch struct {
	// Has unexported fields.
}
    Batch submits a set of writes all together at once
    or load efficiency and/or atomic change to the database.

func (s *Batch) Close()
    Close forgets any existing queued up puts, and frees any other resources
    associated with the Batch.

func (s *Batch) Commit(doFsync bool) (interv HLCInterval, err error)
    Commit flushes the batch atomically all the way to disk but does not fsync
    unless set doFsync true.

    After Commit the batch is empty and can be re-used immediately.

    Returns the half-open HLC interval [Begin, Endx) assigned to this batch.

    Again, we do not wait for the data to be fdatasynced to disk. Call db.Sync()
    if you need durability across power restarts. Usually if performance is
    required this is done once after all your batches are loaded.

    Metrics are useful, but relatively expensive as we must scan all of the
    FlexSpace blocks linearly; use CommitGetMetrics() to view them. Commit()
    itself now skips them for speed.

func (s *Batch) CommitGetMetrics(doFsync bool) (HLCInterval, *Metrics, error)
    CommitGetMetrics does Commit, and then returns metrics on the flex space
    for garbage collection and write-amplification study purposes; hence it is
    slower. It does a linear scan through all the FLEXSPACE.KV128_BLOCKS to see
    how much free space could be reclaimed.

func (s *Batch) Reset()
    Reset forgets any existing queued up puts.

func (s *Batch) Set(key, value []byte) (err error)
    Set copies key and value internally, so the original memory is safe to be
    re-used by the caller immediately after Set returns.

type Config struct {
	CacheMB uint64 // default 32 (for 32 MB)

	// NoDisk runs the entire database in-memory using MemVFS.
	// No files are created on disk. Useful for testing.
	NoDisk bool

	// FS overrides the filesystem implementation. When nil, RealVFS{}
	// is used (or MemVFS if NoDisk is true). Allows injecting a
	// custom VFS for testing (e.g. fault injection).
	FS vfs.FS

	// DisableVLOG disables the value log. When true, all values are stored
	// inline in FlexSpace regardless of size (original behavior).
	DisableVLOG bool

	// OmitFlexSpaceOpsRedoLog skips FlexSpace redo-log writes and instead
	// calls SyncCoW() on every Sync(). This eliminates ~0.86x write
	// amplification from the redo log at the cost of slightly more CoW
	// tree page writes. The net effect is lower total write amp (~3.2x
	// vs ~4.1x). Requires useCoW=true (which is always the case now).
	// Safe either way.
	OmitFlexSpaceOpsRedoLog bool

	// LowBlockUtilizationPct sets the threshold (0.0–1.0) for counting
	// blocks as "low utilization" in Metrics.BlocksWithLowUtilization.
	// A block whose live bytes / FLEXSPACE_BLOCK_SIZE is below this
	// fraction is counted. Default 0.25 (25%) when zero.
	LowBlockUtilizationPct float64

	// OmitMemWalFsync true means we do not durably fdatasync the MEMWAL1/2 files.
	// This is useful for batch loading alot of data quickly, and then doing
	// one fsync at the end for durability. The proviso of course is that
	// if your process crashes you have no intermediate state and have to
	// start again at the beginning; which may be fine.
	OmitMemWalFsync bool

	// PiggybackGC_on_SyncOrFlush enables automatic GC at the end of
	// every Sync() and flush operation. GC runs only if the garbage
	// fraction (wasted bytes / total bytes in used blocks) exceeds
	// GCGarbagePct. Default false (disabled).
	PiggybackGC_on_SyncOrFlush bool

	// GCGarbagePct is the minimum fraction of wasted bytes in used
	// blocks (garbage / (garbage + live)) required to trigger piggyback GC.
	// Value between 0.0 and 1.0. Default 0.50 (50%) when zero and
	// PiggybackGC_on_SyncOrFlush is true.
	GCGarbagePct float64
}
    Config allow configuration of a FlexDB.

type FlexDB struct {
	Path string

	// Write-byte counters (accessed atomically)
	MemWALBytesWritten  int64 // WAL (FLEXDB.MEMWAL1 + FLEXDB.MEMWAL2) bytes written
	LogicalBytesWritten int64 // user payload bytes (key+value)

	// Has unexported fields.
}
    FlexDB is a persistent ordered key-value store backed by FlexSpace. It is
    thread-safe, except for iteration via Ascend/Descend--which allows deletions
    and updates on the fly.

func OpenFlexDB(path string, pCfg *Config) (*FlexDB, error)
    OpenFlexDB opens or creates a FlexDB at the given directory path. cacheMB is
    the cache capacity in megabytes.

func (db *FlexDB) Ascend(pivot []byte, iter func(key, value []byte) bool)
    Ascend iterates key-value pairs in ascending order from the first key >=
    pivot. If pivot is nil, starts from the beginning. Stops when iter returns
    false. The callback may safely call db.Put/db.Delete.

func (db *FlexDB) AscendRange(greaterOrEqual, lessThan []byte, iter func(key, value []byte) bool)
    AscendRange iterates key-value pairs where key is in [greaterOrEqual,
    lessThan) ascending. Either bound may be nil. Stops when iter returns false.
    The callback may safely call db.Put/db.Delete.

func (db *FlexDB) CheckIntegrity() []IntegrityError
    CheckIntegrity performs a read-only consistency check of the FlexDB.
    It flushes memtables first, then acquires a read lock on FlexSpace.

    Checks performed:
     1. FlexTree leaf linked list: no cycles, prev/next consistency
     2. Extent validity: every non-hole extent has poff + len within file bounds
     3. Extent readability: data at every extent can be read from disk
     4. Block usage: recomputed from FlexTree matches the block manager's state
     5. Sparse index: every anchor interval is readable and kv128-decodable
     6. Sorted keys: keys within each decoded interval are in sorted order
     7. Anchor coverage: anchor loff+psize spans tile the FlexSpace without
        gaps/overlaps

    Returns nil if no errors found.

func (db *FlexDB) Clear(includeLarge bool) (allGone bool, err error)
    Clear deletes all keys in the database.

    When includeLarge is true, the entire database is wiped and re-initialized
    (fast path). When false, only keys with inline (small) values are deleted;
    keys with large values stored in the VLOG survive.

    Returns allGone=true when the database was re-initialized. In that case,
    ALL previously held iterators, cursors, and pointers into the database are
    invalid and must be re-acquired.

    Goroutine safe. Acquires the database write lock for the duration of the
    call, serializing against all other operations.

func (db *FlexDB) Close() *Metrics
    Close syncs and shuts down the FlexDB.

func (db *FlexDB) CumulativeMetrics() *Metrics
    CumulativeMetrics reports file sizes on disk, reflecting the cumulative
    history of all sessions. Each physical metric is the current file size
    (via Stat), so it captures bytes written by previous sessions as well.
    LogicalBytesWritten uses FlexSpace's MaxLoff as an approximation of total
    user payload stored (it includes kv128 encoding overhead of ~10-20 bytes per
    entry; values separated to VLOG are represented by 16-byte VPtrs).

func (db *FlexDB) Delete(key []byte) error
    Delete removes key from the store.

func (db *FlexDB) DeleteRange(includeLarge bool, begKey, endKey []byte, begInclusive, endInclusive bool) (n int64, allGone bool, err error)
    DeleteRange deletes all keys in the range [begKey, endKey] with configurable
    inclusivity on each bound.

    Returns:
      - n: number of tombstones written (0 when allGone is true)
      - allGone: true if the entire database was wiped and re-initialized.
        When true, ALL previously held iterators, cursors, and pointers into the
        database are invalid and must be re-acquired.
      - err: non-nil on failure

    When includeLarge is false, keys whose values are stored in the VLOG (large
    values, > 64 bytes) are skipped and survive the deletion.

    The begInclusive and endInclusive parameters control whether the bounds are
    inclusive or exclusive:

        DeleteRange(true,  a, z, true,  true)   // [a, z]  — both inclusive, include large values
        DeleteRange(true,  a, z, true,  false)  // [a, z)  — half-open, include large values
        DeleteRange(false, a, z, true,  true)   // [a, z]  — both inclusive, skip large values

    Goroutine safe. Concurrent reads and writes are serialized via the database
    write lock. However, when allGone is returned true, all previously held
    iterators, cursors, and references are invalidated.

func (db *FlexDB) Descend(pivot []byte, iter func(key, value []byte) bool)
    Descend iterates key-value pairs in descending order from the last key <=
    pivot. If pivot is nil, starts from the end. Stops when iter returns false.
    The callback may safely call db.Put/db.Delete.

func (db *FlexDB) DescendRange(lessOrEqual, greaterThan []byte, iter func(key, value []byte) bool)
    DescendRange iterates key-value pairs where key is in (greaterThan,
    lessOrEqual] descending. Either bound may be nil. Stops when iter returns
    false. The callback may safely call db.Put/db.Delete.

func (db *FlexDB) Find(smod SearchModifier, key []byte, fetchLarge bool) (value []byte, found, exact, large bool)
    Find is a convenience wrapper around FindIt that closes the iterator before
    returning so that callers don't need to manage `it` if they don't need `it`.

    If you want to traverse the key space after Find-ing your key (or the next
    nearest), use FindIt to get an Iter back instead of Find.

func (db *FlexDB) FindIt(smod SearchModifier, key []byte, fetchLarge bool) (value []byte, found, exact, large bool, it *Iter)
    FindIt allows GTE, GT, LTE, LT, and Exact searches.

    GTE: find a leaf greater-than-or-equal to key; the smallest such key.

    GT: find a leaf strictly greater-than key; the smallest such key.

    LTE: find a leaf less-than-or-equal to key; the largest such key.

    LT: find a leaf less-than key; the largest such key.

    Exact: find a matching key exactly. A key can only be stored once in the
    tree. (It is not a multi-map in the C++ STL sense).

    If key is nil, then GTE and GT return the first leaf in the tree, while LTE
    and LT return the last leaf in the tree.

    Other returned flags: found means some key was found. exact means an exact
    matching key to the query was found. large means the value is large and only
    returned in value if fetchLarge was true.

    The returned iterator, it, is positioned at the found key and can be used
    to scan beyond it (Next/Prev). The caller must call it.Close() when done.
    If found is false, the iterator is not valid but must still be closed.

func (db *FlexDB) Get(key []byte) ([]byte, bool)
    Get retrieves the value for key. Returns nil, false if not found.

func (db *FlexDB) Len() int64
    Len returns the total number of live (non-tombstone) keys in the database.
    O(1) — reads a pre-maintained counter. Goroutine safe.

func (db *FlexDB) LenBigSmall() (big int64, small int64)
    LenBigSmall returns the live key count partitioned by storage location.
    big: keys whose values are stored in the VLOG (> 64 bytes). small:
    keys whose values are stored inline. O(1) — reads pre-maintained counters.
    Goroutine safe.

func (db *FlexDB) Merge(key []byte, fn func(oldVal []byte, exists bool) (newVal []byte, write bool)) error
    Merge performs an atomic read-modify-write on key. The merge function fn
    receives the old value (nil if not found) and whether the key existed.
    It returns the new value to write and whether to perform the write.
    If write is false, the merge is a no-op. If newVal is nil and write is true,
    a tombstone (delete) is written.

    This mirrors C's flexdb_merge: it looks up the old value across all layers
    (active memtable, inactive memtable, FlexSpace), applies the user function,
    and writes the result atomically.

func (db *FlexDB) NewBatch() (b *Batch)

func (db *FlexDB) NewIter() *Iter
    NewIter creates an iterator. Call Seek, SeekToFirst, or SeekToLast
    to position it. The iterator holds no locks; it is safe to call
    db.Put/db.Delete between iterator calls.

func (db *FlexDB) Put(key, value []byte) error
    Put writes key -> value. value==nil deletes the key. Values of any size
    are accepted. Values > vlogInlineThreshold (64 bytes) are stored in the
    VLOG file; smaller values are stored inline in the FLEXSPACE.KV128_BLOCKS
    file with the keys. Large values are written exactly once: to the VLOG.
    The WAL stores only the VPtr (16 bytes), not the full value.

    Puts are not durably on disk until after the user has also completed a
    db.Sync() call. This allows the user to control the rate of fsyncs and trade
    that against their durability requirements.

func (db *FlexDB) SessionMetrics() *Metrics
    Metrics returns a snapshot of write-byte counters aggregated from all
    layers.

func (db *FlexDB) Sync() error
    Sync flushes all in-memory data in the active memtable to disk in
    FLEXSPACE.KV128.BLOCKS and fsyncs it. Users must call Sync after Puts for
    them to be durable.

func (db *FlexDB) Update(fn func(tx *Tx) error) error
    Update executes fn within a serializable read-write transaction. Only one
    Update transaction runs at a time (serialized by db.topMutRW). If fn returns
    without calling Commit or Cancel, the transaction is auto-cancelled.

func (db *FlexDB) VacuumKV() (*VacuumKVStats, error)
    VacuumKV reclaims dead FLEXSPACE.KV128_BLOCKS space by rewriting all live
    extents sequentially to a new file and replacing the old file. This is an
    exclusive operation that acquires topMutRW.

    Crash safety: if the process crashes before the rename completes, the old
    FLEXSPACE.KV128_BLOCKS and old FlexTree remain intact. The stale .vacuum
    file (if present) is harmless and will be overwritten on the next vacuum.

    VacuumKV does a one-time compaction of already-bloated databases. Algorithm:
    1. Flush memtables, acquire exclusive locks 2. Walk FlexTree leaf linked
    list, read/rewrite all live extents sequentially to a .vacuum file 3. Close
    old fd, rename .vacuum -> FLEXSPACE.KV128_BLOCKS, reopen 4. Rebuild block
    manager, checkpoint FlexTree, invalidate caches 5. Clean up stale .vacuum
    files on OpenFlexSpaceCoW

    See the tests: TestFlexDB_VacuumKV_Basic — overwrites 200 keys, vacuums,
    verifies data integrity across reopen TestFlexDB_VacuumKV_WithDeletes —
    deletes half of 100 keys, vacuums, verifies correct keys survive .

func (db *FlexDB) VacuumVLOG() (*VacuumStats, error)
    VacuumVLOG reclaims dead LARGE.VLOG space by copying live values to a new
    VLOG file and rewriting their VPtrs in FlexSpace. This is an exclusive
    operation that acquires topMutRW.

    Crash safety: if the process crashes before the rename completes, the old
    VLOG and old intervals remain intact. The stale VLOG.new file (if present)
    is harmless and will be overwritten on the next vacuum.

func (db *FlexDB) View(fn func(tx *Tx) error) error
    View executes fn within a read-only transaction. Multiple View transactions
    can run concurrently. The transaction is auto-released when fn returns.

type Iter struct {
	// Has unexported fields.
}
    ====================== Lock-Free Re-Seeking Iterator ======================

    Iter is a stateful, prefetching iterator over the merged view of:
      - Active memtable (highest priority)
      - Inactive memtable
      - FlexSpace via sparse index (lowest priority)

    The iterator holds NO locks between calls. It maintains a stateful
    flexCursor into FlexSpace's sparse index tree, an HLC snapshot for version
    detection, and a span-based prefetch buffer. On the fast path (both
    memtables empty, no concurrent mutations), Next() serves from prefetched
    spans with only an atomic HLC check — no lock, no seek. When the HLC has
    changed (mutation occurred) or memtables are non-empty, the iterator falls
    back to a merged re-seek under topMutRW.RLock().

    Mutations (Put/Delete) are safe during iteration — the HLC check detects
    them and triggers a re-seek. No snapshot consistency guarantee; this is a
    design tradeoff we made for this embedded single-user store because we want
    deletion and udpate _during_ iteration.

    Large values (stored in VLOG) are not fetched by default. The Value() method
    returns the inline value or nil if the value is large. Use Large() to check,
    and FetchV() to fetch on demand.

    In short:

    Iter is a lock-free re-seeking merge iterator over memtables and FlexSpace.

func (it *Iter) Close()
    Close marks the iterator as closed and releases cursor state.

func (it *Iter) FetchV() ([]byte, error)
    FetchV fetches the current iterator value from the VLOG if the iterator
    points to a large value. For inline values, this is equivalent to Vin().
    Returns the value bytes and any error from the VLOG read. If the value is
    inline and not large, it will stiill be returned (and the error will be
    nil).

func (it *Iter) Hlc() HLC
    Hlc returns the HLC timestamp of the current key-value pair.

func (it *Iter) Key() []byte
    Key returns the current key. On the fast path this is a direct pointer into
    cache memory (zero-copy). Call dupBytes(it.Key) if you need to keep a copy
    beyond the next Next()/Prev() call.

func (it *Iter) Large() bool
    Large returns false if the current value is stored inline (small value).
    When true, the value is stored in the VLOG and must be fetched via FetchV().
    Iteration remains cheap until you really need to see the large value.

func (it *Iter) Next()
    Next advances to the next key in ascending order.

func (it *Iter) Prev()
    Prev moves to the previous key in descending order.

func (it *Iter) Seek(target []byte)
    Seek positions the iterator at the first key >= target.

func (it *Iter) SeekToFirst()
    SeekToFirst positions the iterator at the first key.

func (it *Iter) SeekToLast()
    SeekToLast positions the iterator at the last key.

func (it *Iter) Valid() bool
    Valid returns true if the iterator is positioned at a valid key.

func (it *Iter) Vel() (val []byte, empty, large bool)
    Vel can be more ergonomic than Vin. Vel returns the current inline Value
    as well as the two important flags to tell you how to interpret the zero
    len(val) situation: whether the value is truly empty, or just large and
    sitting in the VLOG waiting for a separate FetchV call.

    Vel is a mnemonic for the returns: value, empty, large.

    Plus it's fun to say with your best Young Frankenstein style faux German
    accept (Gene Wilder, Mel Brooks 1974)

    "Vel, vel, vel... vhat have ve here? A large value??"

func (it *Iter) Vin() (val []byte)
    Vin returns the current value for _inline_ values. For large values stored
    in VLOG, Vin returns nil. If you get back nil, you must use Large() to check
    if the value is actually large, and then FetchV() to fetch it on demand if
    so. Or just use Vel() below to find out what you've got in one call.

    On the fast path (empty memtables), the first call copies from the cache
    reference into valBuf. Subsequent calls return valBuf directly.


type Metrics struct {
	Session                   bool
	KV128BytesWritten         int64 // FlexSpace FLEXSPACE.KV128_BLOCKS file
	MemWALBytesWritten        int64 // FlexDB WAL (FLEXDB.MEMWAL1 + FLEXDB.MEMWAL2)
	REDOLogBytesWritten       int64 // FLEXSPACE.REDO.LOG
	FlexTreePagesBytesWritten int64 // CoW FLEXTREE.PAGES + FLEXTREE.COMMIT
	VLOGBytesWritten          int64 // VLOG value log
	LogicalBytesWritten       int64 // user payload (key + value)
	TotalBytesWritten         int64 // sum of all physical writes

	// WriteAmp returns the write amplification factor (total physical / logical).
	// Returns 0 if no logical bytes have been written.
	WriteAmp float64 //  TotalBytesWritten / LogicalBytesWritten

	CumulativeWriteAmp float64 // totalPhysicalBytesWrit / totalLogicalBytesWrit

	// Garbage metrics computed from FlexSpace block usage tracking.
	// TotalFreeBytesInBlocks is the sum of dead (unused) bytes across all
	// non-empty blocks. A block with 1000 live bytes out of 4 MB has
	// 4_193_304 garbage bytes. Completely empty blocks are not counted
	// (they are already free for reuse).
	TotalFreeBytesInBlocks int64

	// BlocksInUse shows how many 4MB blocks FLEXSPACE.KV128_BLOCKS is using.
	BlocksInUse int64

	// BlocksWithLowUtilization is the count of non-empty blocks whose
	// utilization (live bytes / block size) is below the configured
	// LowBlockUtilizationPct threshold (default 25%).
	BlocksWithLowUtilization int64

	// TotalLiveBytes is the sum of live (used) bytes across all blocks,
	// as tracked by the block manager.
	TotalLiveBytes int64

	// LowBlockUtilizationPct we used; copied from Config
	// or what default we used if not set.
	LowBlockUtilizationPct float64

	// PiggybackGCRuns is the number of piggyback GC runs during this session.
	PiggybackGCRuns int64

	// PiggybackGCLastDurMs is the duration of the last piggyback GC run in milliseconds.
	PiggybackGCLastDurMs int64
	// Has unexported fields.
}
    Metrics holds byte-level write counters for computing write amplification.

type SearchModifier int
    SearchModifier controls the matching behavior of Find.

const (
	// Exact matches only; like a hash table.
	Exact SearchModifier = 0
    
	// GTE finds the smallest key greater-than-or-equal to the query.
	GTE SearchModifier = 1
    
	// LTE finds the largest key less-than-or-equal to the query.
	LTE SearchModifier = 2
    
	// GT finds the smallest key strictly greater-than the query.
	GT SearchModifier = 3
    
	// LT finds the largest key strictly less-than the query.
	LT SearchModifier = 4
)

type Tx struct {
	// Has unexported fields.
}
    Tx represents a database transaction.

func (tx *Tx) Cancel() error
    Cancel discards all buffered writes. Uses first-wins semantics: if Commit
    was called first, returns nil. Double Cancel returns nil (idempotent).

func (tx *Tx) Commit() error
    Commit applies all buffered writes to the database. Uses first-wins
    semantics: if Cancel was called first, returns ErrTxDone. Double Commit
    returns nil (idempotent).

func (tx *Tx) Delete(key []byte) error
    Delete marks a key for deletion in the transaction's write buffer.
    The deletion is only applied to the database when Commit is called. Returns
    ErrTxNotWritable for read-only transactions.

func (tx *Tx) Get(key []byte) ([]byte, bool)
    Get retrieves the value for key within the transaction's snapshot. Returns
    nil, false if not found or if the key is a tombstone.

func (tx *Tx) Put(key, value []byte) error
    Put writes a key-value pair into the transaction's write buffer.
    The write is only applied to the database when Commit is called. Returns
    ErrTxNotWritable for read-only transactions.

type VacuumKVStats struct {
	OldFileSize      int64
	NewFileSize      int64
	BytesReclaimed   int64
	ExtentsRewritten int64
}
    VacuumKVStats reports the results of a VacuumKV operation.

type VacuumVLOGStats struct {
	OldVLOGSize        int64
	NewVLOGSize        int64
	BytesReclaimed     int64
	EntriesCopied      int64
	IntervalsRewritten int64
}
    VacuumVLOGStats reports the results of a VacuumVLOG operation.

~~~

# grab bag of implementation notes 

in no particular order -- and mostly for my reference. Users/casual readers can
ignore the rest of this file(!)


~~~
Current Architecture

FlexDB has three layers:
1. FlexTree (flextree.go, pages.go) - B-tree extent index with CoW page persistence
2. FlexSpace (flexspace.go) - Log-structured file layer, stores data in 4MB blocks
3. FlexDB (db.go) - Full KV store with memtable, sparse index, interval cache

Current read path (FlexDB.Get):

1. Check memtables (in-memory btree) - no disk I/O
2. Check FlexSpace via sparse index -> getPassthrough() -> loadInterval():
  - Allocates itvbuf := make([]byte, anchor.psize)
  - Calls db.ff.ReadFragmentation(itvbuf, ...) which does fd.ReadAt() (pread)
  - Calls kv128Decode(src) which allocates new slices for key and value (make([]byte, klen), copy)
  - Cache entries hold the decoded KVs (heap-allocated copies)
  - Get() then copies the value again before returning to caller

Current write path:

- FlexSpace.fd is opened O_RDWR
- Block manager has a 4MB in-memory write buffer
- Writes go to buffer first, then fd.WriteAt() when buffer is full or on sync
- Reads check buffer first, fall back to fd.ReadAt() for committed data

File layout:

FlexDB dir/
├── FLEXSPACE.KV128_BLOCKS <- All KV data (random pread/pwrite, grows)
├── FLEXSPACE.REDO.LOG     <- Redo log (append-only)
├── FLEXTREE.COMMIT        <- CoW commit records (append-only, 64KB)
├── FLEXTREE.PAGES         <- CoW pages (random read/write, 1024-byte pages)
├── FLEXDB.MEMWAL1         <- WAL for memtable 0
├── FLEXDB.MEMWAL2         <- WAL for memtable 1
└── LARGE.VLOG             <- values > 64 bytes are stored separately here.

notes:

- kv128Decode already copies keys and values into new allocations - NOT zero-copy
- FlexTree nodes are decoded from 1024-byte pages into Go structs - NOT zero-copy
- Get() copies the value before returning - caller always gets an independent copy
- The block manager buffer handles recent writes; pread handles committed data
~~~

# VLOG: the Value Log (big values (those > 64 bytes) are kept here).

~~~
VLOG Properties

Not all values go to VLOG. Only values > vlogInlineThreshold (64 bytes) 
are stored in VLOG. Smaller values are stored inline in 
the kv128-encoded entry within FlexSpace. The split is:

┌──────────────┬─────────────────────────────────┬───────────────────────────────────────────────┐
│  Value size  │            Stored in            │                 WAL contains                  │
├──────────────┼─────────────────────────────────┼───────────────────────────────────────────────┤
│nil(tombstone)│ nothing                         │ kv128 tombstone marker                        │
├──────────────┼─────────────────────────────────┼───────────────────────────────────────────────┤
│<= 64 bytes   │ inline in FlexSpace kv128 entry │ full inline kv128 entry                       │
├──────────────┼─────────────────────────────────┼───────────────────────────────────────────────┤
│> 64 bytes    │ VLOG file                       │ kv128 entry with 12-byte VPtr (offset+length) │
└──────────────┴─────────────────────────────────┴───────────────────────────────────────────────┘

VLOG can be disabled entirely with Config{DisableVLOG: true}, in 
which case all values are inline regardless of size.

VLOG entry format (post-HLC) has 5 fields (HLC = hybrid logical/physical clock; BE = big endian)

field 1: [header CRC: 4-byte CRC32C]  bytes [0:4)
field 2: [8-byte HLC BE]              bytes [4:12)
field 3: [8-byte length: N]           bytes [12:20)
field 4: [value CRC: 4-byte CRC32C]   bytes [20:24)
field 5: [N-byte value]               bytes [24:Z) where Z=24+N

legend:
[header CRC: 4-byte CRC32C] = covering bytes [4:24) also known as fields 2, 3, and 4: 
from the HLC through the value CRC inclusive.

[ value CRC: 4-byte CRC32C] = covering only the N-byte value that follows it.

The length field N stores only the value length (not including HLC). 
So the total on-disk entry size is 4 + 8 + 8 + 4 + len(value) == 24 + len(value).

VLOG's role in crash recovery

The VLOG plays no active role in the recovery procedure. 
Recovery (db.go:2282) does two things:

1. Rebuilds the sparse index by scanning FlexSpace tags
2. Replays WAL logs - decoding kv128 entries and 
applying them via putPassthrough/deletePassthrough

The WAL entries for large values contain VPtrs (not the actual values). 
The actual values already reside durably in the VLOG file because 
the VLOG is always fsynced before the WAL entry referencing it is
written:

- Put(): calls vlog.appendAndSync() (fsync), then writes VPtr to WAL
- Batch.Commit(): calls vlog.appendBatchAndSync() (single fsync 
for all large values), then writes VPtr kv128 entries to WAL

So after a crash, the WAL replay reconstructs VPtrs in FlexSpace 
that point to already-durable VLOG entries. The VLOG file itself 
is never scanned or replayed - it's just a passive store that
resolveVPtr() reads from at query time.

About VLOG garbage collection:

Dead VLOG entries (from overwrites/deletes) accumulate until 
VacuumVLOG() is called explicitly. 
VacuumVLOG() walks all live intervals, copies live VPtr values
to a new VLOG file (preserving HLCs), rewrites VPtrs
in FlexSpace, then atomically renames new -> old.
~~~

# Transactions (Tx)

YogaDB offers linearizable data access. A sync.RWMutex 
allows one writer at a time, and multiple readers can read
simultaneously (when there is no writer).

Read-only (View) and read-write (Update) transactions are available.

~~~

  In db.go:
   - topMutRW sync.RWMutex in FlexDB for serializing write transactions
   - Tx struct with atomic state, COW btree snapshots, write buffer
   - db.Update(fn) - serializable read-write transaction (single-writer via txMu)
   - db.View(fn) - concurrent read-only transaction
   - tx.Get() - reads write buffer > memtable snapshots > FlexSpace
   - tx.Put() / tx.Delete() - buffers writes locally (Update only)
   - tx.Commit() / tx.Cancel() - first-wins semantics
          
 Tests of Tx: flexdb_tx_test.go (11 tests):
   - Double Commit, Double Cancel (both idempotent)
   - Cancel->Commit (returns ErrTxDone), Commit->Cancel (Cancel is no-op)
   - Update basic, View basic, Update sees own writes
   - Cancel discards writes, Auto-cancel on return without Commit/Cancel
   - View rejects Put/Delete with ErrTxNotWritable
   - Serialized concurrent Updates (10 goroutines increment a counter to exactly 10)

~~~

# Q & A

Q1: What is the system architecture? I see
mention of FlexTree, FlexSpace, and FlexDB. How are they related?

A1: 

## System Architecture (Three Layers)

Note that these three layers are the same in the Go version
and the C version. The C/paper calls the top layer FlexDB.
For the Go version, we call the top layer YogaDB (the 
main/top-level file is db.go).

~~~

FlexDB (kv store)               <- db.go
    └── FlexSpace (file layer)  <- flexspace.go
            └── FlexTree (B-tree extent index)  <- flextree.go
                    └── CoW persistence         <- pages.go

~~~

### (bottom) Layer 1: FlexTree (`flextree.h/.c` -> `flextree.go`)

A B-tree that maps **logical offsets** (loff) to **physical offsets** (poff). Each entry is an **Extent**. Gentle reader, this is a metaphor, inspired by the filesystem extent idea. It is not actually referring to actual filesystem extents(!) Insertions shift all subsequent logical offsets, so the tree supports true insert-range semantics (not just overwrite).

**Key insight:** Each internal node child carries a `shift` accumulator. When data is inserted at a logical offset, only the path from root to that leaf is updated; other subtrees lazily absorb the shift when traversed. This gives ~O(log n) insert cost and write amplification near 1.

### (middle) Layer 2: FlexSpace (`flexfile.h/.c` -> `flexspace.go`)

A log-structured file abstraction on top of FlexTree providing:
- Log-structured writes with 8 MB in-memory buffer, 2 GB max on-disk log
- 4 MB blocks with a block manager
- Garbage collection (GC threshold: 64 blocks)
- Crash recovery by replaying the logical log
- Operations: `Read`, `Write`, `Insert`, `Collapse` (delete range), `Sync`
- Handler API (`FlexSpaceHandler`) for stateful cursors with sequential reads

**C vs Go I/O:** The C version uses `io_uring` for async I/O. The Go port uses synchronous `pread`/`pwrite` via `os.File.ReadAt`/`WriteAt`. This is a known difference; Go's goroutine scheduler provides concurrency at a different level.

### (top) Layer 3: FlexDB (`flexdb.h/.c` -> `db.go`) a.k.a YogaDB

A full persistent ordered key-value store on top of FlexSpace providing:
- **MemTable:** `tidwall/btree` B-tree (replaces C's Wormhole skip list); 1 GB cap, dual-memtable for concurrent flush
- **Sparse KV Index:** Separate B-tree of anchors with configurable interval (default 32 KV pairs per anchor)
- **Interval Cache:** 1024-partition LRU cache of decoded KV intervals from FlexSpace
- **WAL:** Write-ahead log for crash recovery with CRC32 checksums
- **Flush Worker:** Background goroutine that flushes MemTable to FlexSpace + updates sparse index
- Operations: `Put`, `Get`, `Del`, `Scan`



Q2: We use the very fast in-memory B-tree https://github.com/tidwall/btree as the 
top level memtable for the top YogaDB layer. This is nice since it has Copy-on-Write snapshots. But why are there two memtables?

A2: The dual-memtable design enables non-blocking writes during flush.

With a single memtable, when it's time to flush to FlexSpace, there are two (bad options:

a) Block all writes while the flush completes (the memtable is being 
read by the flush worker and can't accept new writes simultaneously)

b) Copy the entire memtable before flushing. This is expensive for a 1 GB 
memtable

With two memtables, the flush is just a pointer _swap_.

The active memtable (say `mt[0]`) fills up or a timer fires

`doFlush()` atomically swaps: `mt[1]` becomes the new active 
(accepting writes immediately), `mt[0]` becomes inactive. 

The flush worker drains `mt[0]` to FlexSpace in the background. Writes to `mt[1]` proceed concurrently with zero blocking. When the flush finishes, `mt[0]` is cleared and ready for the next swap. The `mtMu.Lock()` is held only for the swap itself (a pointer swap - nanoseconds), not for the entire flush duration. This is the same pattern used by LSM-tree stores like LevelDB/RocksDB (they call it the "immutable memtable").

# write-amplification measurement 

We note when bytes are written to disk along the write path, to
accurately measure write amplification.

A. planning:

~~~
File: flexspace.go
  --------------------------------------------------------

  | Line | Operation | File Desc | Size | Type | Context |
  |------|-----------|-----------|------|------|---------|
  | 198  | WriteAt   | fd (KV128_BLOCKS) | VAR  | KV128_BLOCKS | nextBlock() flush 4MB block |
  | 207  | Sync      | fd (KV128_BLOCKS) | -    | SYNC | blockMgr.flush() |
  | 313  | WriteAt   | logFD     | VAR  | KV128_BLOCKS | logSync() append log buffer |
  | 317  | Sync      | logFD     | -    | SYNC | logSync() fsync log |
  | 322  | Truncate  | logFD     | 0    | TRUNC| logTruncate() reset |
  | 329  | WriteAt   | logFD     | 8B   | FLEXTREE.COMMIT | writeLogVersion() header |

  Key Constants (flexspace.go):
  - FLEXSPACE_BLOCK_SIZE = 4 MB (1 << 22)
  - FLEXSPACE_LOG_MEM_CAP = 8 MB
  - FLEXSPACE_LOG_MAX_SIZE = 2 GB
  - flexLogVersionSize = 8 bytes

  File: db.go
  --------------------------------------------------------

  | Line | Operation | File Desc | Size | Type | Context |
  |------|-----------|-----------|------|------|---------|
  | 1095 | Write     | logFD     | VAR  | KV128_BLOCKS | logFlushLocked() WAL append |
  | 1108 | Truncate  | logFD     | 0    | TRUNC| logTruncate() reset WAL |
  | 1111 | WriteAt   | logFD     | 8B   | FLEXTREE.COMMIT | logTruncate() timestamp |

  Key Constants (db.go):
  - flexdbMemtableLogBufCap = 4 MB
  - flexdbSparseInterval = 32 KVs
  - KV128 encoded: varint(klen) + varint(vlen) + key + value

  File: saver.go
  -------------------------------------------

  | Line | Operation | File Desc | Size | Type | Context |
  |------|-----------|-----------|------|------|---------|
  | 95   | Sync      | f (TREE)  | -    | SYNC | saveFlexTree() fsync checkpoint |

  Note: Actual data writes are in msgp.Writer.Write/Flush
  - Full FlexTree serialization (greenpack msgpack format)
  - Variable size, potentially 100s of MB for large trees

  File: fdatasync_linux.go
  -------------------------------------------

  | Line | Function | Syscall | Notes |
  |------|----------|---------|-------|
  | 21   | fdatasync| Fdatasync| Linux-specific, data-only flush |
  | 30   | fallocate| Fallocate| Pre-allocate blocks |

  | Line | Function | Syscall | Notes |
  |------|----------|---------|-------|
  | 21   | fdatasync| Fdatasync| Linux-specific, data-only flush |
  | 30   | fallocate| Fallocate| Pre-allocate blocks |

  File: fdatasync_other.go
  -------------------------------------------

  | Line | Function | Fallback | Notes |
  |------|----------|----------|-------|
  | 18   | fdatasync| f.Sync() | macOS uses F_FULLFSYNC |

  ================================================================
  WRITE CALL GRAPH
  ================================================================

  User Operation: Put(key, value)
    └─> FlexDB.Put()
        └─> dbMemtable.put()
            └─> [memtable btree operation - no disk write]
        └─> dbMemtable.logAppend() [OPTIONAL: only if logBufSize full]
            └─> db.go:1095 Write(logFD) [WAL WRITE 1]
                └─> [appends to memtable WAL: MEMWAL1 or MEMWAL2]

  Periodic: Flush worker detects memtable full
    └─> FlexDB.flush()
        └─> FlexSpace.Write(flattened_memtable_kvs)
            └─> flexspace.go:719 blockManager.write()
                └─> flexspace.go:198 WriteAt(fd, KV128_BLOCKS) [KV128_BLOCKS WRITE 2]
            └─> flexspace.go:721 logWrite(flexOpTreeInsert)
                └─> [appends 16-byte entry to logBuf]
        └─> FlexSpace.Sync() [if logFull]
            └─> flexspace.go:313 WriteAt(logFD, REDO.LOG) [REDO.LOG WRITE 3]
            └─> flexspace.go:207 Sync(fd, KV128_BLOCKS) [SYNC 1]
            └─> flexspace.go:317 Sync(logFD) [SYNC 2]

  Checkpoint: FlexSpace.Sync() when log exceeds 2 GB
    └─> saveFlexTree(tree, FLEXTREE)
        └─> msgp.Writer.Write() [TREE WRITE 4]
        └─> saver.go:95 Sync(f) [SYNC 3]
        OR
    └─> FlexTree.SyncCoW()
        └─> pages.go:373 WriteAt(nodeFD, FLEXTREE.PAGES) [NODE WRITE 4]
        └─> pages.go:411 WriteAt(nodeFD, FLEXTREE.PAGES) [NODE WRITE 4]
        └─> pages.go:306 fdatasync(nodeFD) [SYNC 3]
        └─> pages.go:332 WriteAt(metaFD, FLEXTREE.COMMIT) [FLEXTREE.COMMIT WRITE 5]
        └─> pages.go:335 fdatasync(metaFD) [SYNC 4]

  Growth events:
    └─> pages.go:274 fallocateFile(nodeFD) [PRE-ALLOC]
    └─> pages.go:278 Sync(nodeFD) [GROWTH SYNC]

  ================================================================
  WRITE AMPLIFICATION ANALYSIS
  ================================================================

  Example: Insert 1 KB of KV data

  Path 1: Memtable -> FlexSpace -> Greenpack checkpoint
  -------------------------------------------------
  1. Memtable WAL (MEMWAL1):
     - KV128 encode: ~20 bytes (varint headers) + 1000 bytes = ~1020 bytes
     - Buffered; flushed when buffer >= 4 MB
     - WRITE COUNT: ~1020 bytes (amortized across batch)

  2. FlexSpace -> KV128_BLOCKS block:
     - Data write: 1024 bytes (to 4 MB block)
     - LogOp entry: 16 bytes
     - WRITE COUNT: 1024 bytes data + 16 bytes log

  3. FlexSpace REDO.LOG:
     - Flush when >= 8 MB or explicit Sync
     - Contains 16-byte entries per operation
     - WRITE COUNT: amortized 16 bytes per operation

  4. FlexTree Checkpoint (greenpack):
     - Full tree serialization when log exceeds 2 GB
     - Variable size; compressed msgpack
     - Estimate: ~30-50 bytes per leaf extent node state
     - WRITE COUNT: depends on tree size (sparse data structure)

  Total for one 1 KB operation:
  - WAL: 1020 bytes
  - KV128_BLOCKS: 1024 bytes
  - REDO.LOG: 16 bytes
  - Checkpoint: amortized 30-50 bytes (infrequent)
  - ≈ 2060+ bytes per 1000 bytes data = 2x+ amplification minimum

  Path 2: Memtable -> FlexSpace -> CoW checkpoint
  -------------------------------------------------
  Same as Path 1, but checkpoint is:

  4. FlexTree Checkpoint (CoW):
     - Write dirty nodes only: 1024 bytes per node
     - Memtable write affects leaf node (holds extents)
     - FLEXTREE.COMMIT record: 64 bytes per checkpoint
     - WRITE COUNT: 1024 bytes (node) + 64 bytes (FLEXTREE.COMMIT)

  Total for one 1 KB operation:
  - WAL: 1020 bytes
  - KV128_BLOCKS: 1024 bytes
  - REDO.LOG: 16 bytes
  - CoW: 1024 bytes (node) + 64 bytes (FLEXTREE.COMMIT) if dirty
  - ≈ 3148+ bytes per 1000 bytes data = 3x+ amplification

  ================================================================
  INSTRUMENTATION OPPORTUNITIES
  ================================================================

  Current Write Sites:

  1. pages.go:373 - Leaf page write
     Counter: bytes += 1024 (or len(pageBuf[:]))

  2. pages.go:411 - Internal page write
     Counter: bytes += 1024

  3. flexspace.go:198 - KV128_BLOCKS block write
     Counter: bytes += bm.blkoff

  4. flexspace.go:313 - REDO.LOG buffer write
     Counter: bytes += ff.logBufSize

  5. db.go:1095 - Memtable WAL write
     Counter: bytes += len(m.logBuf[:m.logBufSize])

  6. saver.go - Greenpack write (via msgp.Writer)
     Counter: Wrap msgp.Writer to count bytes

  7. pages.go:332 - FLEXTREE.COMMIT record write
     Counter: bytes += 64

  Total formula:
  WA = (CoW_pages + REDO.LOG_buffer + KV128_BLOCKS + WAL_buffer) / original_kv_size

  ================================================================

summary document with the complete mapping:

Summary

I have completed a thorough inventory of every disk write operation in the yogadb codebase. Here are the findings:

Complete Write Operations Inventory

Total Unique Write Sites: 20 operations across 6 files

Layer 1: FlexTree CoW Persistence (pages.go)

┌──────┬───────────┬───────────────┬─────────────┬───────────┬─────────────────────┐
│ Line │ Operation │       FD      │    Size     │   Type    │       Context       │
├──────┼───────────┼───────────────┼─────────────┼───────────┼─────────────────────┤
│ 373  │ WriteAt   │FLEXTREE.PAGES │ 1024B       │ Data      │ Leaf page write     │
├──────┼───────────┼───────────────┼─────────────┼───────────┼─────────────────────┤
│ 411  │ WriteAt   │FLEXTREE.PAGES │ 1024B       │ Data      │ Internal page write │
├──────┼───────────┼───────────────┼─────────────┼───────────┼─────────────────────┤
│ 332  │ WriteAt   │FLEXTREE.COMMIT│ 64B         │ Meta      │ Commit record       │
├──────┼───────────┼───────────────┼─────────────┼───────────┼─────────────────────┤
│ 306  │ fdatasync │FLEXTREE.PAGES │ -           │ Sync      │ Data flush          │
├──────┼───────────┼───────────────┼─────────────┼───────────┼─────────────────────┤
│ 335  │ fdatasync │FLEXTREE.COMMIT│ -           │ Sync      │ Data flush          │
├──────┼───────────┼───────────────┼─────────────┼───────────┼─────────────────────┤
│ 274  │ fallocate │FLEXTREE.PAGES │ 2x previous │ Pre-alloc │ Doubling growth     │
├──────┼───────────┼───────────────┼─────────────┼───────────┼─────────────────────┤
│ 278  │ Sync      │FLEXTREE.PAGES │ -           │ Sync      │ Growth fsync        │
└──────┴───────────┴───────────────┴─────────────┴───────────┴─────────────────────┘

Layer 2: FlexSpace Log-Structured (flexspace.go)

┌──────┬───────────┬─────────────┬──────────┬───────┬───────────────────┐
│ Line │ Operation │     FD      │   Size   │ Type  │      Context      │
├──────┼───────────┼─────────────┼──────────┼───────┼───────────────────┤
│ 198  │ WriteAt   │ fd (KV128_BLOCKS)   │ variable │ Data  │ 4MB block flush   │
├──────┼───────────┼─────────────┼──────────┼───────┼───────────────────┤
│ 313  │ WriteAt   │ logFD (REDO.LOG) │ variable │ Data  │ Log buffer append │
├──────┼───────────┼─────────────┼──────────┼───────┼───────────────────┤
│ 329  │ WriteAt   │ logFD (REDO.LOG) │ 8B       │ Meta  │ Version header    │
├──────┼───────────┼─────────────┼──────────┼───────┼───────────────────┤
│ 207  │ Sync      │ fd (KV128_BLOCKS)   │ -        │ Sync  │ Block flush sync  │
├──────┼───────────┼─────────────┼──────────┼───────┼───────────────────┤
│ 317  │ Sync      │ logFD (REDO.LOG) │ -        │ Sync  │ Log flush sync    │
├──────┼───────────┼─────────────┼──────────┼───────┼───────────────────┤
│ 322  │ Truncate  │ logFD (REDO.LOG) │ 0        │ Reset │ Log reset         │
└──────┴───────────┴─────────────┴──────────┴───────┴───────────────────┘

Layer 3: FlexDB Memtable (db.go)

┌──────┬───────────┬────────────────┬──────────┬───────┬──────────────────┐
│ Line │ Operation │       FD       │   Size   │ Type  │     Context      │
├──────┼───────────┼────────────────┼──────────┼───────┼──────────────────┤
│ 1095 │ Write     │ logFD (MEMWAL1/2) │ variable │ Data  │ WAL append       │
├──────┼───────────┼────────────────┼──────────┼───────┼──────────────────┤
│ 1111 │ WriteAt   │ logFD (MEMWAL1/2) │ 8B       │ Meta  │ Timestamp header │
├──────┼───────────┼────────────────┼──────────┼───────┼──────────────────┤
│ 1108 │ Truncate  │ logFD (MEMWAL1/2) │ 0        │ Reset │ WAL reset        │
└──────┴───────────┴────────────────┴──────────┴───────┴──────────────────┘

Files Created on Disk

CoW Mode:
- FLEXTREE.PAGES - 1024-byte pages (one per tree node)
- FLEXTREE.COMMIT - 64-byte commit records (append-only)
- REDO.LOG - 8-byte header + 16-byte redo entries
- KV128_BLOCKS - 4 MB blocks

FlexDB:
- MEMWAL1, MEMWAL2 - memtable WALs (one per memtable)
- FLEXSPACE/ - complete FlexSpace directory

Write Amplification Paths

For a single 1 KB write:

Put(key, value)
  ├─ Memtable WAL write: ~1020 bytes (KV128 encoded)
  ├─ FlexSpace KV128_BLOCKS block: 1024 bytes (to 4 MB block)
  ├─ FlexSpace REDO.LOG entry: 16 bytes
  ├─ Tree checkpoint (CoW): 1024 bytes (node) + 64 bytes (FLEXTREE.COMMIT)
  └─ Tree checkpoint (greenpack): 30-50 bytes (amortized)

Total: 2.5x-3.2x write amplification minimum

Key Files and Line Numbers

Complete mapping for instrumentation:

1. /Users/me/go/src/github.com/glycerine/yogadb/pages.go:373 - CoW leaf write
2. /Users/me/go/src/github.com/glycerine/yogadb/pages.go:411 - CoW internal write
3. /Users/me/go/src/github.com/glycerine/yogadb/pages.go:332 - FLEXTREE.COMMIT commit
4. /Users/me/go/src/github.com/glycerine/yogadb/flexspace.go:198 - KV128_BLOCKS block write
5. /Users/me/go/src/github.com/glycerine/yogadb/flexspace.go:313 - REDO.LOG buffer write
6. /Users/me/go/src/github.com/glycerine/yogadb/db.go:1095 - Memtable WAL write

These 6 sites account for all data writes; wrapping with 
atomic counters at these points will give complete write 
amplification visibility.
~~~
B. final write amplification work, summary of changes

~~~
  Changes Summary

  flextree.go

  - Added TreeBytesWritten int64 counter field to FlexTree struct

  pages.go

  - Added sync/atomic import
  - Instrumented 3 write sites with atomic.AddInt64(&t.TreeBytesWritten, ...):
    - Leaf page write (cowPageSize = 1024 bytes)
    - Internal page write (cowPageSize = 1024 bytes)
    - FLEXTREE.COMMIT record write (cowMetaSize = 64 bytes)

  flexspace.go

  - Added sync/atomic import
  - Added DataBytesWritten int64 and LogBytesWritten int64 counter fields to FlexSpace struct
  - Instrumented 3 write sites:
    - KV128_BLOCKS block flush (variable size up to 4MB)
    - Redo REDO.LOG write (variable size)
    - Log version header write (8 bytes)

  db.go

  - Added WALBytesWritten int64 and LogicalBytesWritten int64 counter fields to FlexDB struct
  - Added walCounter *int64 field to dbMemtable struct (pointer back to FlexDB.WALBytesWritten)
  - Wired up walCounter in OpenFlexDB
  - Instrumented 2 WAL write sites via the walCounter pointer
  - Added logical byte tracking in Put() and Batch.Commit()
  - Added Metrics struct with WriteAmp() method
  - Added FlexDB.Metrics() method that aggregates counters from all three layers

  batch_bench_test.go

  - Updated BenchmarkBatchYogaDB and BenchmarkPutYogaDB to report yoga_write_amp from db.Metrics().WriteAmp()
  
~~~

# Plan for more aggressive Garbage Collection: Piggyback GC on Sync/Flush for YogaDB

The flush-worker goroutine aleady tries to flush every 5 seconds in the background.
The user can choose to have that flush also do GC at the end of its flush, or
to manually run Sync themselves with the Config flags set to indicate that a GC
should be done at the end of a Sync; or as a third very manual option, simply
call VacuumKV and/or VacuumVLOG when they wish.

~~~
## Context

YogaDB's FlexSpace GC is currently synchronous and demand-driven: it only runs when a write needs a new block and free blocks < 64. For workloads with heavy overwrites/deletes, fragmentation accumulates in `FLEXSPACE.KV128_BLOCKS` without being reclaimed until critically low on blocks. Adding a simple opt-in flag to run GC at every Sync/flush boundary when garbage exceeds a threshold.

## Approach: Piggyback on Sync/Flush (Mode A only)

Run GC at the end of `writeLockHeldSync()` and `doFlush()` when the write lock is already held. No new goroutines. No timer/cooldown — just check garbage fraction on every sync.

## Config Fields (db.go ~line 533)

```go
// PiggybackGC_on_SyncOrFlush enables automatic GC at the end of
// every Sync() and flush operation. GC runs only if the garbage
// fraction (wasted bytes / total bytes in used blocks) exceeds
// GCGarbagePct. Default false (disabled).
PiggybackGC_on_SyncOrFlush bool

// GCGarbagePct is the minimum fraction of wasted bytes in used
// blocks (garbage / (garbage + live)) required to trigger piggyback GC.
// Value between 0.0 and 1.0. Default 0.50 (50%) when zero and
// PiggybackGC_on_SyncOrFlush is true.
GCGarbagePct float64
```

## FlexDB Fields (db.go ~line 541)

```go
piggyGCStats PiggybackGCStats
```

## New Types

```go
type PiggybackGCStats struct {
    LastGCTime     time.Time
    LastGCDuration time.Duration
    TotalGCRuns    int64
}
```

## New Method: `maybePiggybackGC()` (db.go)

```go
func (db *FlexDB) maybePiggybackGC() {
    if !db.cfg.PiggybackGC_on_SyncOrFlush {
        return
    }
    threshold := db.cfg.GCGarbagePct
    if threshold <= 0 {
        threshold = 0.50
    }
    live, garbage, _, _ := db.ff.garbageMetrics(db.cfg.LowBlockUtilizationPct)
    total := live + garbage
    if total == 0 || float64(garbage)/float64(total) < threshold {
        return
    }
    start := time.Now()
    db.ff.GC()
    db.piggyGCStats.LastGCTime = time.Now()
    db.piggyGCStats.LastGCDuration = time.Since(start)
    db.piggyGCStats.TotalGCRuns++
}
```

## Call Sites

1. `writeLockHeldSync()` (db.go:1881) — after `db.ff.Sync()`:
   ```go
   db.ff.Sync()
   db.maybePiggybackGC()  // <-- insert here
   db.verifyAnchorTags()
   ```

2. `doFlush()` (db.go:3461) — after `db.ff.Sync()`:
   ```go
   db.ff.Sync()
   db.maybePiggybackGC()  // <-- insert here
   ```

## Metrics (db.go Metrics struct ~line 945)

Add to `Metrics`:
```go
PiggybackGCRuns      int64
PiggybackGCLastDurMs int64
```

Populate in `SessionMetrics()`, `CumulativeMetrics()`, `finalMetrics()`.

## Defaults
- `PiggybackGC_on_SyncOrFlush = false` → disabled (opt-in)
- `GCGarbagePct = 0` → default 0.50 when PiggybackGC_on_SyncOrFlush is true

## Files to Modify

| File | Changes |
|------|---------|
| `db.go` | Config fields, FlexDB field, `maybePiggybackGC()`, `PiggybackGCStats` type, Metrics fields, call sites in `writeLockHeldSync` and `doFlush` |
| `gc_test.go` | Add piggyback GC tests |

## Verification

1. Unit tests in `gc_test.go`:
   - `TestPiggybackGC_TriggersOnSync` — enable flag, create fragmentation, Sync, verify `TotalGCRuns > 0`
   - `TestPiggybackGC_RespectsGarbageThreshold` — set threshold=0.99, verify GC does NOT run
   - `TestPiggybackGC_DisabledByDefault` — default config, verify no piggyback GC
   - `TestPiggybackGC_ReclaimsSpace` — write, delete half, enable, Sync, check free blocks increased

2. Run full test suite: `go test ./...` — ensure no regressions

## Critical Files

- `db.go` — Config (line 498), FlexDB struct (line 541), `writeLockHeldSync` (line 1857), `doFlush` (line 3440), Metrics (line 945)
- `flexspace.go` — `FlexSpace.GC()` (line 1272), `garbageMetrics()` (line 1081)
- `gc_test.go` — existing GC test patterns, test helpers (`openTestDB`, etc.)


  db.go

  1. Config fields (after line 533): Added PiggybackGC_on_SyncOrFlush bool and GCGarbagePct float64
  2. PiggybackGCStats type (before FlexDB struct): Tracks LastGCTime, LastGCDuration, TotalGCRuns
  3. FlexDB field: Added piggyGCStats PiggybackGCStats
  4. maybePiggybackGC() method: Checks if piggyback GC is enabled, computes garbage fraction via garbageMetrics(), runs
   ff.GC() if above threshold (default 50%)
  5. Call sites: Inserted db.maybePiggybackGC() after db.ff.Sync() in both writeLockHeldSync() and doFlush()
  6. Metrics fields: Added PiggybackGCRuns int64 and PiggybackGCLastDurMs int64
  7. Metrics population: Added piggyback stats to writeLockHeldSessionMetrics(), finalMetrics(), and
  CumulativeMetrics()
  8. String(): Conditionally prints piggyback GC section when PiggybackGCRuns > 0

  gc_test.go

  Added 4 tests:
  - TestPiggybackGC_TriggersOnSync — enables flag, creates fragmentation, verifies GC runs
  - TestPiggybackGC_RespectsGarbageThreshold — threshold=1.0, verifies GC does NOT run
  - TestPiggybackGC_DisabledByDefault — default config, verifies no piggyback GC
  - TestPiggybackGC_ReclaimsSpace — write, delete half, sync, verify data correctness

~~~

---------
Author: Jason E. Aten, Ph.D.

YogaDB is Copyright (C) 2026, Jason E. Aten, Ph.D. All rights reserved.

LICENSE: MIT. See LICENSE file.
