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

# details

* API notes: all Go API calls are goroutine safe, except for iterator use (deliberately).
Iterators are designed to allow concurrent mutation while scanning through
the database, and hence by design they hold no locks. This allows the user
to delete on the fly, rather than collect a separate long list of things
to delete (which may overflow available memory).

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


# grab bag of implementation notes

in no particular order


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
field 5: [N-byte value]

where
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

# why we implemented COW page based rather than one-shot repeated serialization (for the FlexTree serialization part)
~~~
  Summary of BenchmarkSyncCoW versus BenchmarkSyncGreenpack

  Both benchmarks follow the same pattern: build a tree 
  with N random inserts (1K, 10K, 100K), then on each 
  iteration perform a single-point insert and sync to disk. 
  
  This measures the incremental persistence cost - the dominant 
  real-world workload.

  Key results (100K-insert tree, 5,475 nodes):

  ┌───────────────┬────────────┬─────────────────┬──────────────────┐
  │               │    CoW     │    Greenpack    │  CoW advantage   │
  ├───────────────┼────────────┼─────────────────┼──────────────────┤
  │ Time          │ 42.6 ms/op │ 114.8 ms/op     │ 2.7× faster      │
  ├───────────────┼────────────┼─────────────────┼──────────────────┤
  │ Bytes written │ 4,236 B/op │ 16,149,047 B/op │ 3,812× less I/O  │
  ├───────────────┼────────────┼─────────────────┼──────────────────┤
  │ Dirty nodes   │ 4.1/op     │ 5,475 (all)     │ O(log N) vs O(N) │
  ├───────────────┼────────────┼─────────────────┼──────────────────┤
  │ Heap allocs   │ 78/op      │ 328,269/op      │ 4,208× fewer     │
  └───────────────┴────────────┴─────────────────┴──────────────────┘

  The CoW bytes_written/op stays nearly constant (~4 KB) across all 
  tree sizes because only the root-to-leaf dirty path (3–5 nodes × 
  1024 bytes + 64 byte FLEXTREE.COMMIT record) is written. Greenpack writes 
  scale linearly with tree size: 180 KB -> 1.6 MB -> 16 MB.
~~~

raw output of another run:
~~~

BenchmarkSyncCoW
BenchmarkSyncCoW/inserts_1000
BenchmarkSyncCoW/inserts_1000-8            	      26	  44_670_466 ns/op	      3215 bytes_written/op	         3.077 dirty_nodes/op	        54.00 total_nodes/op	   27462 B/op	      31 allocs/op

BenchmarkSyncCoW/inserts_10000
BenchmarkSyncCoW/inserts_10000-8           	      25	  44_430_850 ns/op	      4242 bytes_written/op	         4.080 dirty_nodes/op	       548.0 total_nodes/op	   45921 B/op	      49 allocs/op

BenchmarkSyncCoW/inserts_100000
BenchmarkSyncCoW/inserts_100000-8          	      27	  44_110_721 ns/op	      4236 bytes_written/op	         4.074 dirty_nodes/op	      5475 total_nodes/op	   74049 B/op	      77 allocs/op

BenchmarkSyncGreenpack
BenchmarkSyncGreenpack/inserts_1000
BenchmarkSyncGreenpack/inserts_1000-8      	      48	  26_240_206 ns/op	    164_549 bytes_written/op	        54.00 total_nodes/op	   56246 B/op	    3339 allocs/op

BenchmarkSyncGreenpack/inserts_10000
BenchmarkSyncGreenpack/inserts_10000-8     	      27	  45_412_641 ns/op	   1_619_307 bytes_written/op	       548.0 total_nodes/op	  525733 B/op	   32911 allocs/op

BenchmarkSyncGreenpack/inserts_100000
BenchmarkSyncGreenpack/inserts_100000-8    	       9	 118_718_165 ns/op	  16_146_466 bytes_written/op	      5475 total_nodes/op	 5214116 B/op	  328221 allocs/op


transcript with the other benchmarks

started at Sun Mar  1 10:03:22

go test -v -run=xxx -bench=. -benchmem
goos: darwin
goarch: amd64
pkg: github.com/glycerine/goflexspace
cpu: Intel(R) Core(TM) i7-1068NG7 CPU @ 2.30GHz
BenchmarkFlexspace_Insert
BenchmarkFlexspace_Insert-8                	  428605	      2408 ns/op	1701.22 MB/s	      48 B/op	       1 allocs/op
BenchmarkFlexspace_SequentialRead
BenchmarkFlexspace_SequentialRead-8        	  812606	      1402 ns/op	2921.21 MB/s	       0 B/op	       0 allocs/op
BenchmarkMarshalMsgBruteForce
BenchmarkMarshalMsgBruteForce-8            	37094476	        32.01 ns/op	      80 B/op	       1 allocs/op
BenchmarkAppendMsgBruteForce
BenchmarkAppendMsgBruteForce-8             	209661574	         5.494 ns/op	 182.03 MB/s	       0 B/op	       0 allocs/op
BenchmarkUnmarshalBruteForce
BenchmarkUnmarshalBruteForce-8             	38643217	        29.39 ns/op	  34.03 MB/s	       0 B/op	       0 allocs/op
BenchmarkEncodeBruteForce
BenchmarkEncodeBruteForce-8                	35948038	        33.26 ns/op	 420.98 MB/s	      16 B/op	       1 allocs/op
BenchmarkDecodeBruteForce
BenchmarkDecodeBruteForce-8                	18890314	        62.59 ns/op	 223.66 MB/s	       0 B/op	       0 allocs/op
BenchmarkMarshalMsgBruteForceExtent
BenchmarkMarshalMsgBruteForceExtent-8      	36012325	        34.56 ns/op	      96 B/op	       1 allocs/op
BenchmarkAppendMsgBruteForceExtent
BenchmarkAppendMsgBruteForceExtent-8       	204466639	         5.938 ns/op	 168.41 MB/s	       0 B/op	       0 allocs/op
BenchmarkUnmarshalBruteForceExtent
BenchmarkUnmarshalBruteForceExtent-8       	37906490	        31.82 ns/op	  31.43 MB/s	       0 B/op	       0 allocs/op
BenchmarkEncodeBruteForceExtent
BenchmarkEncodeBruteForceExtent-8          	33239271	        36.90 ns/op	 542.07 MB/s	      16 B/op	       1 allocs/op
BenchmarkDecodeBruteForceExtent
BenchmarkDecodeBruteForceExtent-8          	17372379	        69.15 ns/op	 289.21 MB/s	       0 B/op	       0 allocs/op
BenchmarkMarshalMsgCommonNode
BenchmarkMarshalMsgCommonNode-8            	26384380	        41.63 ns/op	     128 B/op	       1 allocs/op
BenchmarkAppendMsgCommonNode
BenchmarkAppendMsgCommonNode-8             	147017896	         8.041 ns/op	 124.36 MB/s	       0 B/op	       0 allocs/op
BenchmarkUnmarshalCommonNode
BenchmarkUnmarshalCommonNode-8             	25666387	        45.99 ns/op	  21.74 MB/s	       0 B/op	       0 allocs/op
BenchmarkEncodeCommonNode
BenchmarkEncodeCommonNode-8                	32362620	        36.23 ns/op	 386.38 MB/s	      16 B/op	       1 allocs/op
BenchmarkDecodeCommonNode
BenchmarkDecodeCommonNode-8                	15251576	        78.50 ns/op	 178.35 MB/s	       0 B/op	       0 allocs/op
BenchmarkMarshalMsgFlexTree
BenchmarkMarshalMsgFlexTree-8              	16231210	        77.51 ns/op	     320 B/op	       1 allocs/op
BenchmarkAppendMsgFlexTree
BenchmarkAppendMsgFlexTree-8               	93178617	        11.82 ns/op	  84.62 MB/s	       0 B/op	       0 allocs/op
BenchmarkUnmarshalFlexTree
BenchmarkUnmarshalFlexTree-8               	14383398	        86.60 ns/op	  11.55 MB/s	       0 B/op	       0 allocs/op
BenchmarkEncodeFlexTree
BenchmarkEncodeFlexTree-8                  	32788125	        36.78 ns/op	 326.26 MB/s	       8 B/op	       1 allocs/op
BenchmarkDecodeFlexTree
BenchmarkDecodeFlexTree-8                  	 8346694	       145.4 ns/op	  82.52 MB/s	       0 B/op	       0 allocs/op
BenchmarkMarshalMsgFlexTreeExtent
BenchmarkMarshalMsgFlexTreeExtent-8        	38440520	        31.83 ns/op	      80 B/op	       1 allocs/op
BenchmarkAppendMsgFlexTreeExtent
BenchmarkAppendMsgFlexTreeExtent-8         	225433383	         5.415 ns/op	 184.66 MB/s	       0 B/op	       0 allocs/op
BenchmarkUnmarshalFlexTreeExtent
BenchmarkUnmarshalFlexTreeExtent-8         	40938327	        27.24 ns/op	  36.71 MB/s	       0 B/op	       0 allocs/op
BenchmarkEncodeFlexTreeExtent
BenchmarkEncodeFlexTreeExtent-8            	35557196	        33.38 ns/op	 539.18 MB/s	      16 B/op	       1 allocs/op
BenchmarkDecodeFlexTreeExtent
BenchmarkDecodeFlexTreeExtent-8            	19737956	        60.68 ns/op	 296.65 MB/s	       0 B/op	       0 allocs/op
BenchmarkMarshalMsgFlexTreePath
BenchmarkMarshalMsgFlexTreePath-8          	17741598	        67.49 ns/op	     112 B/op	       1 allocs/op
BenchmarkAppendMsgFlexTreePath
BenchmarkAppendMsgFlexTreePath-8           	36284377	        32.73 ns/op	1466.50 MB/s	       0 B/op	       0 allocs/op
BenchmarkUnmarshalFlexTreePath
BenchmarkUnmarshalFlexTreePath-8           	12120326	        91.93 ns/op	 522.16 MB/s	       0 B/op	       0 allocs/op
BenchmarkEncodeFlexTreePath
BenchmarkEncodeFlexTreePath-8              	14387042	        84.34 ns/op	 758.86 MB/s	      16 B/op	       1 allocs/op
BenchmarkDecodeFlexTreePath
BenchmarkDecodeFlexTreePath-8              	 4826005	       248.0 ns/op	 258.09 MB/s	       0 B/op	       0 allocs/op
BenchmarkMarshalMsgFlextreeQueryResult
BenchmarkMarshalMsgFlextreeQueryResult-8   	36304290	        34.84 ns/op	      96 B/op	       1 allocs/op
BenchmarkAppendMsgFlextreeQueryResult
BenchmarkAppendMsgFlextreeQueryResult-8    	204586672	         5.900 ns/op	 169.50 MB/s	       0 B/op	       0 allocs/op
BenchmarkUnmarshalFlextreeQueryResult
BenchmarkUnmarshalFlextreeQueryResult-8    	38515906	        33.14 ns/op	  30.17 MB/s	       0 B/op	       0 allocs/op
BenchmarkEncodeFlextreeQueryResult
BenchmarkEncodeFlextreeQueryResult-8       	31000598	        40.07 ns/op	 574.03 MB/s	      24 B/op	       1 allocs/op
BenchmarkDecodeFlextreeQueryResult
BenchmarkDecodeFlextreeQueryResult-8       	16894274	        71.90 ns/op	 319.90 MB/s	       0 B/op	       0 allocs/op
BenchmarkMarshalMsgInternalChild
BenchmarkMarshalMsgInternalChild-8         	44264338	        26.83 ns/op	      48 B/op	       1 allocs/op
BenchmarkAppendMsgInternalChild
BenchmarkAppendMsgInternalChild-8          	242837253	         4.961 ns/op	 201.57 MB/s	       0 B/op	       0 allocs/op
BenchmarkUnmarshalInternalChild
BenchmarkUnmarshalInternalChild-8          	52324213	        23.01 ns/op	  43.47 MB/s	       0 B/op	       0 allocs/op
BenchmarkEncodeInternalChild
BenchmarkEncodeInternalChild-8             	36125029	        33.20 ns/op	 511.98 MB/s	      16 B/op	       1 allocs/op
BenchmarkDecodeInternalChild
BenchmarkDecodeInternalChild-8             	22389864	        53.27 ns/op	 319.14 MB/s	       0 B/op	       0 allocs/op
BenchmarkMarshalMsgInternalNode
BenchmarkMarshalMsgInternalNode-8          	 2136025	       559.4 ns/op	    2304 B/op	       1 allocs/op
BenchmarkAppendMsgInternalNode
BenchmarkAppendMsgInternalNode-8           	 5315955	       220.3 ns/op	 830.56 MB/s	       0 B/op	       0 allocs/op
BenchmarkUnmarshalInternalNode
BenchmarkUnmarshalInternalNode-8           	 1470295	       821.7 ns/op	 222.72 MB/s	       0 B/op	       0 allocs/op
BenchmarkEncodeInternalNode
BenchmarkEncodeInternalNode-8              	  883243	      1374 ns/op	 514.55 MB/s	     528 B/op	      33 allocs/op
BenchmarkDecodeInternalNode
BenchmarkDecodeInternalNode-8              	  471559	      2580 ns/op	 274.06 MB/s	       0 B/op	       0 allocs/op
BenchmarkMarshalMsgLeafNode
BenchmarkMarshalMsgLeafNode-8              	 1445840	       816.3 ns/op	    4864 B/op	       1 allocs/op
BenchmarkAppendMsgLeafNode
BenchmarkAppendMsgLeafNode-8               	 5237744	       229.1 ns/op	 454.01 MB/s	       0 B/op	       0 allocs/op
BenchmarkUnmarshalLeafNode
BenchmarkUnmarshalLeafNode-8               	  764460	      1545 ns/op	  67.30 MB/s	       0 B/op	       0 allocs/op
BenchmarkEncodeLeafNode
BenchmarkEncodeLeafNode-8                  	  555592	      2098 ns/op	 547.28 MB/s	     984 B/op	      62 allocs/op
BenchmarkDecodeLeafNode
BenchmarkDecodeLeafNode-8                  	  312878	      3628 ns/op	 316.42 MB/s	       0 B/op	       0 allocs/op
BenchmarkMarshalMsgOfflen
BenchmarkMarshalMsgOfflen-8                	43877218	        27.35 ns/op	      48 B/op	       1 allocs/op
BenchmarkAppendMsgOfflen
BenchmarkAppendMsgOfflen-8                 	243821780	         4.893 ns/op	 204.37 MB/s	       0 B/op	       0 allocs/op
BenchmarkUnmarshalOfflen
BenchmarkUnmarshalOfflen-8                 	51542290	        22.55 ns/op	  44.35 MB/s	       0 B/op	       0 allocs/op
BenchmarkEncodeOfflen
BenchmarkEncodeOfflen-8                    	38312234	        29.53 ns/op	 338.62 MB/s	       8 B/op	       1 allocs/op
BenchmarkDecodeOfflen
BenchmarkDecodeOfflen-8                    	22825026	        52.94 ns/op	 188.88 MB/s	       0 B/op	       0 allocs/op
BenchmarkMarshalMsgPos
BenchmarkMarshalMsgPos-8                   	40332393	        29.57 ns/op	      64 B/op	       1 allocs/op
BenchmarkAppendMsgPos
BenchmarkAppendMsgPos-8                    	223899483	         5.135 ns/op	 194.75 MB/s	       0 B/op	       0 allocs/op
BenchmarkUnmarshalPos
BenchmarkUnmarshalPos-8                    	44891947	        28.20 ns/op	  35.47 MB/s	       0 B/op	       0 allocs/op
BenchmarkEncodePos
BenchmarkEncodePos-8                       	42495799	        30.64 ns/op	 228.48 MB/s	       3 B/op	       1 allocs/op
BenchmarkDecodePos
BenchmarkDecodePos-8                       	20502928	        58.09 ns/op	 120.50 MB/s	       0 B/op	       0 allocs/op

PASS
ok  	github.com/glycerine/goflexspace	184.844s

finished at Sun Mar  1 10:06:27
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

```
FlexDB (kv store)               <- db.go
    └── FlexSpace (file layer)  <- flexspace.go
            └── FlexTree (B-tree extent index)  <- flextree.go
                    └── CoW persistence         <- pages.go
```

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

With a single memtable, when it's time to flush to FlexSpace, you have two bad options:

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
  EOF
  cat /tmp/detailed_write_ops.txt
  )
  ⎿  Response:
Perfect! Now let me create one final summary document with the complete mapping:

Summary

I have completed a thorough inventory of every disk write operation in the yogadb codebase. Here are the findings:

Complete Write Operations Inventory

Total Unique Write Sites: 20 operations across 6 files

Layer 1: FlexTree CoW Persistence (pages.go)

┌──────┬───────────┬────────────────┬─────────────┬───────────┬─────────────────────┐
│ Line │ Operation │       FD       │    Size     │   Type    │       Context       │
├──────┼───────────┼────────────────┼─────────────┼───────────┼─────────────────────┤
│ 373  │ WriteAt   │ nodeFD (FLEXTREE.PAGES) │ 1024B       │ Data      │ Leaf page write     │
├──────┼───────────┼────────────────┼─────────────┼───────────┼─────────────────────┤
│ 411  │ WriteAt   │ nodeFD (FLEXTREE.PAGES) │ 1024B       │ Data      │ Internal page write │
├──────┼───────────┼────────────────┼─────────────┼───────────┼─────────────────────┤
│ 332  │ WriteAt   │ metaFD (FLEXTREE.COMMIT)  │ 64B         │ Meta      │ Commit record       │
├──────┼───────────┼────────────────┼─────────────┼───────────┼─────────────────────┤
│ 306  │ fdatasync │ nodeFD (FLEXTREE.PAGES) │ -           │ Sync      │ Data flush          │
├──────┼───────────┼────────────────┼─────────────┼───────────┼─────────────────────┤
│ 335  │ fdatasync │ metaFD (FLEXTREE.COMMIT)  │ -           │ Sync      │ Data flush          │
├──────┼───────────┼────────────────┼─────────────┼───────────┼─────────────────────┤
│ 274  │ fallocate │ nodeFD (FLEXTREE.PAGES) │ 2x previous │ Pre-alloc │ Doubling growth     │
├──────┼───────────┼────────────────┼─────────────┼───────────┼─────────────────────┤
│ 278  │ Sync      │ nodeFD (FLEXTREE.PAGES) │ -           │ Sync      │ Growth fsync        │
└──────┴───────────┴────────────────┴─────────────┴───────────┴─────────────────────┘

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

Layer 4: Checkpoint Persistence (saver.go)

┌──────┬───────────┬──────────────┬──────┬──────┬─────────────────┐
│ Line │ Operation │      FD      │ Size │ Type │     Context     │
├──────┼───────────┼──────────────┼──────┼──────┼─────────────────┤
│ 95   │ Sync      │ f (FLEXTREE) │ -    │ Sync │ Greenpack fsync │
└──────┴───────────┴──────────────┴──────┴──────┴─────────────────┘

Files Created on Disk

CoW Mode:
- FLEXTREE.PAGES - 1024-byte pages (one per tree node)
- FLEXTREE.COMMIT - 64-byte commit records (append-only)
- REDO.LOG - 8-byte header + 16-byte redo entries
- KV128_BLOCKS - 4 MB blocks

Greenpack Mode:
- FLEXTREE.greenpack - msgpack serialized tree
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


---------
Author: Jason E. Aten, Ph.D.

YogaDB is Copyright (C) 2026, Jason E. Aten, Ph.D. All rights reserved.

LICENSE: MIT. See LICENSE file.
