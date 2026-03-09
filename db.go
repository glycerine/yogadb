package yogadb

// yogadb/db.go — Go port of flexspace/flexdb.c
// FlexDB: a persistent ordered key-value store backed by FlexSpace.
// Uses github.com/tidwall/btree for the in-memory write buffer (memtable).
//
// Architecture:
//   Active Memtable (btree + WAL) -> (flush) -> FlexSpace
//   Reads: check active memtable -> check inactive memtable -> check FlexSpace via sparse index
//   Crash recovery: rebuild sparse index from FlexSpace tags, replay WAL logs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math/bits"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/glycerine/vfs"
)

// ====================== Constants ======================

const (
	MaxKeySize                        = 4096
	flexMemSparseIndexTreeLeafCap     = 122
	flexMemSparseIndexTreeInternalCap = 40
	flexdbSparseIntervalCount         = 2000
	flexdbSparseIntervalSize          = SLOTTED_PAGE_KB << 10 // 64 KB
	memtableCap                       = 1 << 30               // 1 GB
	memtableWalBufCap                 = 4 << 20               // 4 MB log buffer (must be at least 2x than MaxKeySize + space for a KV struct) so we don't deadlock trying to flush the memtable and write a new large key.
	memtableFlushBatch                = 1024
	flexdbUnsortedWriteQuota          = 200
	// sparseInterval = sortedCount + unsortedQuota + 1 = 32
	flexdbSparseInterval        = flexdbSparseIntervalCount + flexdbUnsortedWriteQuota + 1
	intervalCachePartitionCount = 1024
	intervalCachePartitionMask  = intervalCachePartitionCount - 1

	// see intervalcache.go for intervalCacheEntry
	intervalCacheEntryChance = 2

	// see memtable.go for memtable
	memtableFlushTime = 5 * time.Second

	flexdbWALHeaderSize = 12 // 8-byte timestamp + 4-byte CRC32C
)

var sep = string(os.PathSeparator)

// Batch submits a set of writes all together at once
// for load efficiency and/or atomic change to the database.
type Batch struct {
	db   *FlexDB
	puts []*KV
}

// NewBatch returns an empty new Batch.
func (db *FlexDB) NewBatch() (b *Batch) {
	b = &Batch{
		db: db,
	}
	return
}

// Set copies key and value internally, so the
// original memory is safe to be re-used by the
// caller immediately after Set returns.
func (s *Batch) Set(key, value []byte) (err error) {

	// We must make copies here. Even after the Batch
	// is Commit()-ed, these copies can remain unflushed
	// in the memtables and interval caches, as they
	// try and amortize the cost of disk writes.
	s.puts = append(s.puts, &KV{
		Key:   append([]byte{}, key...),
		Value: append([]byte{}, value...),
	})
	return nil
}

// Commit flushes the batch atomically all the way to disk
// but does not fsync unless set doFsync true.
//
// After Commit the batch is empty and can be re-used immediately.
//
// Returns the half-open HLC interval [Begin, Endx) assigned to this batch.
//
// Again, we do not wait for the data to be fdatasynced to disk. Call db.Sync()
// if you need durability across power restarts. Usually if performance
// is required this is done once after all your batches are loaded.
//
// Metrics are useful, but relatively expensive as we must
// scan all of the FlexSpace blocks linearly; use CommitGetMetrics()
// to view them. Commit() itself now skips them for speed.
func (s *Batch) Commit(doFsync bool) (interv HLCInterval, err error) {
	interv, _, err = s.commitMaybeMetrics(doFsync, false)
	return
}

// CommitGetMetrics does Commit, and then returns metrics on the
// flex space for garbage collection and write-amplification study purposes;
// hence it is slower. It does a linear scan through all the
// FLEXSPACE.KV128_BLOCKS to see how much free space could be reclaimed.
func (s *Batch) CommitGetMetrics(doFsync bool) (HLCInterval, *Metrics, error) {
	return s.commitMaybeMetrics(doFsync, true)
}

func (s *Batch) commitMaybeMetrics(doFsync bool, wantMetrics bool) (interv HLCInterval, metrics *Metrics, err error) {
	db := s.db

	db.topMutRW.Lock()
	defer db.topMutRW.Unlock()

	if len(s.puts) == 0 {
		if wantMetrics {
			return HLCInterval{}, db.writeLockHeldSessionMetrics(), nil
		}
		return HLCInterval{}, nil, nil
	}
	if doFsync && db.cfg.OmitMemWalFsync {
		return HLCInterval{}, nil, fmt.Errorf("cannot request doFsync on a database opened with Config.OmitMemWalFsync")
	}

	// Track logical bytes for write amplification metrics.
	var logicalBytes int64
	for _, kv := range s.puts {
		logicalBytes += int64(len(kv.Key) + len(kv.Value))
	}
	atomic.AddInt64(&db.LogicalBytesWritten, logicalBytes)

	// --- HLC assignment with sub-batching for duplicate keys ---
	// Each sub-batch of unique keys shares one HLC tick. When a duplicate
	// key is encountered, we start a new sub-batch with a new HLC tick.
	var firstHLC HLC

	seen := make(map[string]struct{})
	curHLC := db.hlc.CreateSendOrLocalEvent()
	firstHLC = curHLC

	for _, kv := range s.puts {
		k := string(kv.Key)
		if _, dup := seen[k]; dup {
			// Duplicate key in this sub-batch — start new sub-batch.
			seen = make(map[string]struct{})
			curHLC = db.hlc.CreateSendOrLocalEvent()
		}
		seen[k] = struct{}{}
		kv.Hlc = curHLC
	}

	// Write large values to VLOG with a single batch fsync. The WAL then
	// stores VPtrs (not full values), so large values are written exactly once.
	if db.vlog != nil {
		// Collect large values for batch append.
		var largeIndices []int
		var largeValues [][]byte
		var largeHLCs []HLC
		for i, kv := range s.puts {
			if kv.Value != nil && len(kv.Value) > vlogInlineThreshold {
				largeIndices = append(largeIndices, i)
				largeValues = append(largeValues, kv.Value)
				largeHLCs = append(largeHLCs, kv.Hlc)
			}
		}
		if len(largeValues) > 0 {
			// Batch write + single fsync — ensures all values are durable
			// before any WAL entry references them.
			ptrs, err := db.vlog.appendBatchAndSync(largeValues, largeHLCs)
			if err != nil {
				return HLCInterval{}, nil, fmt.Errorf("flexdb: vlog batch append: %w", err)
			}
			for j, idx := range largeIndices {
				// was:
				//s.puts[idx] = &KV{Key: s.puts[idx].Key, Vptr: ptrs[j], HasVPtr: true, Hlc: s.puts[idx].Hlc}
				// one less allocation &KV{} allocation:
				e := s.puts[idx]
				// Key stays same.
				e.Value = nil
				e.Vptr = ptrs[j]
				e.HasVPtr = true
				// Hlc stays same.
			}
		}
	}

	// Bypass the transaction system entirely — no COW snapshots, no ffMu.RLock,
	// no write buffer btree. Instead, insert directly into the memtable and
	// batch WAL writes under a single logMu hold.
	// This amortizes both mutex acquisitions across the entire batch.

	wasActive := db.activeMT
	mt := &db.memtables[wasActive]

	mt.memWalMut.Lock()
	defer mt.memWalMut.Unlock()
	var encoded []byte

	for idx := 0; idx < len(s.puts); idx++ {

		if mt.size >= memtableCap {
			// Memtable full — flush inline.
			db.activeMT = 1 - wasActive
			db.memtables[db.activeMT].empty = true
			db.memtables[db.activeMT].size = 0

			// we already holdmt.memWalMut
			mt.logFlushLocked()

			db.flushMemtable(wasActive)
			db.persistCounters()
			db.ff.Sync()

			mt.bt.Clear()
			mt.empty = true
			mt.size = 0

			// db.go:220
			// flips... why? now that we just cleared it... I don't believe we want this... not sure though.
			//wasActive = db.activeMT
			//mt = &db.memtables[wasActive]
		}
		putKV := *s.puts[idx]
		newState := kvToState(putKV)
		old, replaced := mt.put(putKV)
		var oldState keyState
		if replaced {
			oldState = kvToState(old)
		} else {
			oldState = db.writeLockHeldKeyState(putKV.Key)
		}
		db.adjustKeyCounters(oldState, newState)

		encoded = kv128Encode(encoded[:0], *s.puts[idx])
		if len(mt.memWalBuf)+len(encoded) >= memtableWalBufCap {
			mt.logFlushLocked()
		}
		mt.memWalBuf = append(mt.memWalBuf, encoded...)
	}

	mt.empty = false

	// Batch WAL writes for this chunk under a single logMu hold.

	// WAL stores VPtrs for large values (VLOG was fsynced above).

	if doFsync && !db.cfg.OmitMemWalFsync {
		mt.logSyncLocked() // here in Batch.Commit(doFsync=true)
	}

	// make ready for immediate reuse after a Commit.
	s.puts = nil

	if wantMetrics {
		metrics = db.writeLockHeldSessionMetrics()
	}
	interv = HLCInterval{Begin: firstHLC, Endx: curHLC + 1}
	return
}

// Reset forgets any existing queued up puts.
func (s *Batch) Reset() {
	s.puts = nil
}

// Close forgets any existing queued up puts, and
// frees any other resources associated with the Batch.
func (s *Batch) Close() {
	s.puts = nil
}

// ====================== KV type ======================

// KV is a key-value pair. Value==nil means tombstone (deletion marker).
// When HasVPtr is true, the value is stored in the VLOG file and Vptr
// contains the location; Value holds the resolved bytes (or nil if not yet loaded).
type KV struct {
	Key     []byte
	Value   []byte
	Vptr    VPtr // valid when HasVPtr is true
	HasVPtr bool // true = value lives in VLOG, Vptr is the location
	Hlc     HLC  // hybrid logical clock timestamp. LSN like per mini batch, but has big gaps.
}

func (z *KV) String() (r string) {
	r = "&KV{\n"
	r += fmt.Sprintf("    Key: %v,\n", string(z.Key))
	r += fmt.Sprintf("  Value: %v,\n", string(z.Value))
	r += fmt.Sprintf("   Vptr: %v,\n", z.Vptr)
	r += fmt.Sprintf("HasVPtr: %v,\n", z.HasVPtr)
	r += fmt.Sprintf("    Hlc: %v,\n", z.Hlc.String())
	r += "}\n"
	return
}

// HLCInterval represents a half-open interval [Begin, Endx) of HLC timestamps
// assigned during a Batch.Commit.
type HLCInterval struct {
	Begin HLC // first HLC assigned
	Endx  HLC // exclusive upper bound (one past last)
}

func kvLess(a, b KV) bool { return bytes.Compare(a.Key, b.Key) < 0 }

// kvSizeApprox returns the approximate in-memory size of a KV (matches C kv_size).
func kvSizeApprox(kv KV) int { return 24 + len(kv.Key) + len(kv.Value) }

// isTombstone returns true if this KV is a deletion marker.
// A tombstone has nil Value and no VLOG pointer.
func (kv KV) isTombstone() bool {
	return kv.Value == nil && !kv.HasVPtr
}

// Large returns true if this KV's value is stored in the VLOG
// (too large for inline storage). Use db.FetchLarge(kv) to
// retrieve the value bytes.
func (kv *KV) Large() bool {
	return kv.HasVPtr
}

// ====================== KV128 encoding ======================
// Format: varint(klen) || varint(rawVlen) || key_bytes || value_or_vptr_bytes
// Standard LEB128 (same as Go's encoding/binary.PutUvarint).
//
// rawVlen encoding:
//   rawVlen == 0                -> tombstone (Value = nil), no value bytes follow
//   rawVlen == rawVlenVPtr      -> VLOG pointer: 16 bytes follow (8-byte offset + 8-byte length)
//   rawVlen == len(V) + 1       -> inline value of length len(V), followed by len(V) bytes
//
// The rawVlenVPtr sentinel (max uint64) can never collide with a real inline value
// since that would require a value of length ~2^64 - 2.

const rawVlenVPtr = ^uint64(0) // 0xFFFF FFFF FFFF FFFF — sentinel for VLOG pointer

func kv128Encode(buf []byte, kv KV) []byte {
	recordStart := len(buf)
	var hdr [20]byte
	n := binary.PutUvarint(hdr[:], uint64(len(kv.Key)))
	if kv.isTombstone() {
		n += binary.PutUvarint(hdr[n:], 0) // tombstone
	} else if kv.HasVPtr {
		n += binary.PutUvarint(hdr[n:], rawVlenVPtr) // VLOG pointer sentinel
	} else {
		n += binary.PutUvarint(hdr[n:], uint64(len(kv.Value)+1)) // inline: len+1
	}
	buf = append(buf, hdr[:n]...)
	buf = append(buf, kv.Key...)
	if kv.HasVPtr {
		var vptrBuf [vptrSize]byte
		kv.Vptr.encode(vptrBuf[:])
		buf = append(buf, vptrBuf[:]...)
	} else {
		buf = append(buf, kv.Value...)
	}
	// Append 8-byte HLC (big-endian)
	var hlcBuf [8]byte
	binary.BigEndian.PutUint64(hlcBuf[:], uint64(kv.Hlc))
	buf = append(buf, hlcBuf[:]...)
	// Append 4-byte CRC32C of all preceding record bytes
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], crc32.Checksum(buf[recordStart:], crc32cTable))
	buf = append(buf, crcBuf[:]...)
	return buf
}

func kv128EncodedSize(kv KV) int {
	if kv.isTombstone() {
		return varintSize(uint64(len(kv.Key))) + 1 + len(kv.Key) + 8 + 4
	}
	if kv.HasVPtr {
		return varintSize(uint64(len(kv.Key))) + varintSize(rawVlenVPtr) + len(kv.Key) + vptrSize + 8 + 4
	}
	return varintSize(uint64(len(kv.Key))) + varintSize(uint64(len(kv.Value)+1)) + len(kv.Key) + len(kv.Value) + 8 + 4
}

func kv128Decode(src []byte) (kv KV, n int, ok bool) {
	klen, kn := binary.Uvarint(src)
	if kn <= 0 {
		return
	}
	rawVlen, vn := binary.Uvarint(src[kn:])
	if vn <= 0 {
		return
	}
	hdr := kn + vn
	if rawVlen == 0 {
		// tombstone
		total := hdr + int(klen) + 8
		if len(src) < total+4 {
			return
		}
		if crc32.Checksum(src[:total], crc32cTable) != binary.LittleEndian.Uint32(src[total:total+4]) {
			return
		}
		kv.Key = make([]byte, klen)
		copy(kv.Key, src[hdr:hdr+int(klen)])
		kv.Hlc = HLC(binary.BigEndian.Uint64(src[total-8 : total]))
		return kv, total + 4, true
	}
	if rawVlen == rawVlenVPtr {
		// VLOG pointer
		total := hdr + int(klen) + vptrSize + 8
		if len(src) < total+4 {
			return
		}
		if crc32.Checksum(src[:total], crc32cTable) != binary.LittleEndian.Uint32(src[total:total+4]) {
			return
		}
		kv.Key = make([]byte, klen)
		copy(kv.Key, src[hdr:hdr+int(klen)])
		kv.Vptr = decodeVPtr(src[hdr+int(klen) : hdr+int(klen)+vptrSize])
		kv.HasVPtr = true
		kv.Hlc = HLC(binary.BigEndian.Uint64(src[total-8 : total]))
		return kv, total + 4, true
	}
	vlen := int(rawVlen - 1)
	total := hdr + int(klen) + vlen + 8
	if len(src) < total+4 {
		return
	}
	if crc32.Checksum(src[:total], crc32cTable) != binary.LittleEndian.Uint32(src[total:total+4]) {
		return
	}
	kv.Key = make([]byte, klen)
	copy(kv.Key, src[hdr:hdr+int(klen)])
	kv.Value = make([]byte, vlen)
	copy(kv.Value, src[hdr+int(klen):hdr+int(klen)+vlen])
	kv.Hlc = HLC(binary.BigEndian.Uint64(src[total-8 : total]))
	return kv, total + 4, true
}

// kv128SizePrefix reads just the varint header to determine the total encoded size
// (including the trailing 4-byte CRC32C).
func kv128SizePrefix(src []byte) (int, bool) {
	klen, kn := binary.Uvarint(src)
	if kn <= 0 {
		return 0, false
	}
	rawVlen, vn := binary.Uvarint(src[kn:])
	if vn <= 0 {
		return 0, false
	}
	if rawVlen == 0 {
		return kn + vn + int(klen) + 8 + 4, true
	}
	if rawVlen == rawVlenVPtr {
		return kn + vn + int(klen) + vptrSize + 8 + 4, true
	}
	return kn + vn + int(klen) + int(rawVlen-1) + 8 + 4, true
}

func varintSize(v uint64) int {
	if v == 0 {
		return 1
	}
	return (bits.Len64(v) + 6) / 7
}

// ====================== File tag helpers ======================
// Tag format (16-bit): bit 0 = is_anchor, bits 1-7 = unsorted write count

func flexdbTagGenerate(isAnchor bool, unsorted uint8) uint16 {
	t := uint16(unsorted&0x7f) << 1
	if isAnchor {
		t |= 1
	}
	return t
}

func flexdbTagIsAnchor(tag uint16) bool  { return tag&1 != 0 }
func flexdbTagUnsorted(tag uint16) uint8 { return uint8((tag >> 1) & 0x7f) }

// ====================== CRC32C / fingerprint ======================

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

func kvCRC32(key []byte) uint32 {
	return crc32.Checksum(key, crc32cTable)
}

func fingerprint(h uint32) uint16 {
	fp := uint16(h) ^ uint16(h>>16)
	if fp == 0 {
		fp = 1
	}
	return fp
}

func cachePartitionID(key []byte) int {
	return int(kvCRC32(key) & uint32(intervalCachePartitionMask))
}

func dupBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	dup := make([]byte, len(b))
	copy(dup, b)
	return dup
}

// Config allow configuration of a FlexDB.
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

// PiggybackGCStats tracks statistics for piggyback GC runs.
type PiggybackGCStats struct {
	LastGCTime     time.Time
	LastGCDuration time.Duration
	TotalGCRuns    int64
}

// ====================== FlexDB ======================

// FlexDB is a persistent ordered key-value store backed by FlexSpace. It is
// thread-safe, except for iteration via Ascend/Descend--which allows deletions
// and updates on the fly.
type FlexDB struct {
	// hlc must be first field for 64-bit alignment on 32-bit architectures.
	hlc HLC // hybrid logical clock for timestamping every KV

	closed bool // idempotent Close.

	Path string
	cfg  Config
	vfs  vfs.FS

	piggyGCStats PiggybackGCStats

	ff    *FlexSpace          // underlying FlexSpace
	vlog  *valueLog           // append-only value log for large values (nil if disabled)
	tree  *memSparseIndexTree // in-memory sparse index (rebuilt on open)
	cache *intervalCache

	topMutRW sync.RWMutex

	memtables [2]memtable
	activeMT  int // 0 or 1

	// flush worker
	flushStop    chan struct{}
	flushTrigger chan struct{}
	flushWG      sync.WaitGroup // signals when flush worker has exited

	// scratch buffers (reused; protected by ffMu write lock)
	kvbuf1 []byte
	itvbuf []byte

	// Write-byte counters (accessed atomically)
	MemWALBytesWritten  int64 // WAL (FLEXDB.MEMWAL1 + FLEXDB.MEMWAL2) bytes written
	LogicalBytesWritten int64 // user payload bytes (key+value)

	// Cumulative counters loaded from cowMeta on open.
	// Current total = base + session delta.
	totalLogicalBase  int64
	totalPhysicalBase int64

	// (iterator support — pfSpans are embedded in Iter, no free list needed)

	// Live key counters (maintained incrementally, accessed under topMutRW).
	liveKeys      int64 // total live (non-tombstone) keys = liveBigKeys + liveSmallKeys
	liveBigKeys   int64 // keys whose values are in VLOG (HasVPtr=true)
	liveSmallKeys int64 // keys with inline values
}

// keyState classifies a key's storage state for live-key counter tracking.
type keyState int8

const (
	ksNotExists keyState = iota
	ksLiveSmall
	ksLiveBig
	ksTombstone
)

func kvToState(kv KV) keyState {
	if kv.isTombstone() {
		return ksTombstone
	}
	if kv.HasVPtr {
		return ksLiveBig
	}
	return ksLiveSmall
}

// adjustKeyCounters updates the live key counters for an old→new state transition.
func (db *FlexDB) adjustKeyCounters(oldState, newState keyState) {
	switch oldState {
	case ksLiveSmall:
		db.liveSmallKeys--
		db.liveKeys--
	case ksLiveBig:
		db.liveBigKeys--
		db.liveKeys--
	}
	switch newState {
	case ksLiveSmall:
		db.liveSmallKeys++
		db.liveKeys++
	case ksLiveBig:
		db.liveBigKeys++
		db.liveKeys++
	}
}

// writeLockHeldKeyState checks the inactive memtable and FlexSpace for a key's state.
// Called when a key is new to the active memtable.
// Caller must hold topMutRW.Lock().
func (db *FlexDB) writeLockHeldKeyState(key []byte) keyState {
	inactive := 1 - db.activeMT
	if !db.memtables[inactive].empty {
		if kv, ok := db.memtables[inactive].get(key); ok {
			return kvToState(kv)
		}
	}
	if db.ff.Size() == 0 {
		return ksNotExists
	}
	kv, ok := db.getPassthroughKV(key)
	if !ok {
		return ksNotExists
	}
	return kvToState(kv)
}

// Len returns the total number of live (non-tombstone) keys in the database.
// O(1) — reads a pre-maintained counter.
// Goroutine safe.
func (db *FlexDB) Len() int64 {
	db.topMutRW.RLock()
	v := db.liveKeys
	db.topMutRW.RUnlock()
	return v
}

// LenBigSmall returns the live key count partitioned by storage location.
// big: keys whose values are stored in the VLOG (> 64 bytes).
// small: keys whose values are stored inline.
// O(1) — reads pre-maintained counters.
// Goroutine safe.
func (db *FlexDB) LenBigSmall() (big int64, small int64) {
	db.topMutRW.RLock()
	big = db.liveBigKeys
	small = db.liveSmallKeys
	db.topMutRW.RUnlock()
	return
}

// recomputeKeyCountsLocked walks all FlexSpace intervals via the sparse index
// and counts live (non-tombstone) keys. Called once at open time after recovery.
// No locking needed — called before the flush worker is started and before the
// db reference is returned.
func (db *FlexDB) recomputeKeyCountsLocked() {
	var big, small int64

	leaf := db.tree.leafHead
	for leaf != nil {
		for i := 0; i < leaf.count; i++ {
			anchor := leaf.anchors[i]
			if anchor == nil || anchor.psize == 0 {
				continue
			}

			// Compute absolute loff for this anchor.
			shift := int64(0)
			n := leaf
			for n.parent != nil {
				shift += n.parent.children[n.parentID].shift
				n = n.parent
			}
			anchorLoff := uint64(anchor.loff + shift)

			partition := db.cache.getPartition(anchor)
			fce := partition.getEntry(anchor, anchorLoff, db)
			for _, kv := range fce.kvs {
				if kv.isTombstone() {
					continue
				}
				if kv.HasVPtr {
					big++
				} else {
					small++
				}
			}
			partition.releaseEntry(fce)
		}
		leaf = leaf.next
	}

	db.liveKeys = big + small
	db.liveBigKeys = big
	db.liveSmallKeys = small
}

// OpenFlexDB opens or creates a FlexDB at the given directory path.
// cacheMB is the cache capacity in megabytes.
func OpenFlexDB(path string, pCfg *Config) (*FlexDB, error) {
	cfg := Config{
		// set default Config here:
		CacheMB: 32,
	}
	if pCfg != nil {
		cfg = *pCfg
	}
	if cfg.CacheMB == 0 {
		cfg.CacheMB = 32
	}
	if cfg.LowBlockUtilizationPct <= 0 || cfg.LowBlockUtilizationPct > 1 {
		cfg.LowBlockUtilizationPct = 0.50
	}
	//vv("using cfg.LowBlockUtilizationPct = %v", cfg.LowBlockUtilizationPct)

	// Resolve VFS: explicit FS > NoDisk > RealVFS.
	fs := cfg.FS
	if fs == nil || isNil(fs) {
		if cfg.NoDisk {
			fs = vfs.NewMem()
		} else {
			fs = vfs.Default
		}
		cfg.FS = fs
	}

	if err := fs.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("flexdb: mkdir %s: %w", path, err)
	}

	// Open FlexSpace
	ffPath := path
	ff, err := OpenFlexSpaceCoW(ffPath, cfg.OmitFlexSpaceOpsRedoLog, fs)
	if err != nil {
		return nil, fmt.Errorf("flexdb: open flexspace: %w", err)
	}

	// Open WAL files
	log1Path := filepath.Join(path, "FLEXDB.MEMWAL1")
	//fd1, err := fs.OpenFile(log1Path, os.O_RDWR|os.O_CREATE, 0644)
	fd1, err := fs.OpenReadWrite(log1Path, vfs.WriteCategoryUnspecified)
	if err != nil {
		ff.Close()
		return nil, fmt.Errorf("flexdb: open FLEXDB.MEMWAL1: %w", err)
	}

	// From github.com/tigerbeetle/tigerbeetle/src/io/linux.zig:1640
	// "The best fsync strategy is always to fsync before reading..."
	fd1.Sync()

	log2Path := filepath.Join(path, "FLEXDB.MEMWAL2")
	//fd2, err := fs.OpenFile(log2Path, os.O_RDWR|os.O_CREATE, 0644)
	fd2, err := fs.OpenReadWrite(log2Path, vfs.WriteCategoryUnspecified)
	if err != nil {
		ff.Close()
		fd1.Close()
		return nil, fmt.Errorf("flexdb: open FLEXDB.MEMWAL2: %w", err)
	}
	//vv("begin fd2.Sync")
	//t0sync := time.Now()
	fd2.Sync()
	//vv("end fd2.Sync, took %v", time.Since(t0sync))

	// Open VLOG (value log for large values) unless disabled.
	var vl *valueLog
	if !cfg.DisableVLOG {
		vlogPath := filepath.Join(path, "LARGE.VLOG")
		vl, err = openValueLog(vlogPath, fs)
		if err != nil {
			ff.Close()
			fd1.Close()
			fd2.Close()
			return nil, fmt.Errorf("flexdb: open VLOG: %w", err)
		}
	}

	// Sync the parent directory so that newly created files are durable.
	// Without this, a crash could lose the directory entries even though
	// the file contents were synced.
	if err := syncDir(fs, path); err != nil {
		ff.Close()
		fd1.Close()
		fd2.Close()
		if vl != nil {
			vl.close()
		}
		return nil, fmt.Errorf("flexdb: sync dir %s: %w", path, err)
	}

	db := &FlexDB{
		cfg:          cfg,
		Path:         path,
		vfs:          fs,
		ff:           ff,
		vlog:         vl,
		cache:        newCache(nil, cfg.CacheMB),
		kvbuf1:       make([]byte, 0, MaxKeySize),
		itvbuf:       make([]byte, 0, flexdbSparseIntervalSize+MaxKeySize),
		flushStop:    make(chan struct{}),
		flushTrigger: make(chan struct{}, 1),
	}
	db.cache.db = db
	for i := range db.cache.partitions {
		db.cache.partitions[i].db = db
	}
	db.memtables[0] = *newMemtable(fd1)
	db.memtables[1] = *newMemtable(fd2)
	db.memtables[0].memWalBytesWritten = &db.MemWALBytesWritten
	db.memtables[1].memWalBytesWritten = &db.MemWALBytesWritten

	// Load cumulative counters from the last cowMeta commit.
	db.totalLogicalBase = ff.tree.totalLogicalBytesWrit
	db.totalPhysicalBase = ff.tree.totalPhysicalBytesWrit

	// Restore HLC to be strictly higher than any previously used value.
	if ff.tree.MaxHLC > 0 {
		db.hlc.ReceiveMessageWithHLC(HLC(ff.tree.MaxHLC))
	}

	// Create fresh sparse index tree
	db.tree = memSparseIndexTreeCreate()

	// Recovery or fresh DB
	ffSize := ff.Size()
	if ffSize > 0 {
		db.recovery()
	} else {
		// Tag loff=0 as the first anchor (but FlexSpace is empty, so SetTag may be a no-op)
		tag := flexdbTagGenerate(true, 0)
		_ = ff.SetTag(0, tag) // best-effort on empty FlexSpace
	}

	// Reset WAL logs (always use 20-byte versioned header for consistent disk format)
	ts := uint64(time.Now().UnixNano())
	db.memtables[0].logTruncateWithVersion(ts, db.ff.tree.PersistentVersion)
	ts2 := uint64(time.Now().UnixNano())
	db.memtables[1].logTruncateWithVersion(ts2, db.ff.tree.PersistentVersion)

	// Restore live key counters from persisted cowMeta.
	db.liveKeys = ff.tree.liveKeys
	db.liveBigKeys = ff.tree.liveBigKeys
	db.liveSmallKeys = ff.tree.liveSmallKeys

	// Start flush worker goroutine
	db.flushWG.Add(1)
	go db.flushWorker()

	return db, nil
}

// Close syncs and shuts down the FlexDB.
func (db *FlexDB) Close() *Metrics {
	db.topMutRW.Lock()
	defer db.topMutRW.Unlock()

	if db.closed {
		return nil
	}
	db.closed = true

	// Signal flush worker to stop, then wait for it to finish.
	close(db.flushStop)
	db.flushWG.Wait()

	// Flush any data that is still in the active memtable.
	active := db.activeMT
	hasData := !db.memtables[active].empty
	if hasData {
		newActive := 1 - active
		db.activeMT = newActive
		db.memtables[newActive].empty = true
		db.memtables[newActive].size = 0
	}

	if hasData {
		db.memtables[active].logFlush()

		db.flushMemtable(active)
		db.cache.flushDirtyPages()
		db.persistCounters()
		db.ff.Sync()
		db.verifyAnchorTags()

	} else {
		// Even without new memtable data, flush any dirty cache entries
		// that were modified earlier but not yet written.
		db.cache.flushDirtyPages()
		db.persistCounters()
		db.ff.Sync()
		db.verifyAnchorTags()
	}

	// Truncate WAL logs (always use 20-byte versioned header for consistent disk format)
	ts := uint64(time.Now().UnixNano())
	db.memtables[0].logTruncateWithVersion(ts, db.ff.tree.PersistentVersion)
	db.memtables[1].logTruncateWithVersion(ts, db.ff.tree.PersistentVersion)

	db.cache.destroyAll()
	if db.vlog != nil {
		db.vlog.sync()
		db.vlog.close()
	}
	// Persist counters before final SyncCoW in Close.
	db.persistCounters()
	db.ff.Close()

	// Capture final metrics after all writes are done but before
	// closing file descriptors. This solves the chicken-and-egg
	// problem: accurate cumulative write amplification is only
	// knowable after the final sync, but the DB is closing.
	m := db.finalMetrics()

	db.memtables[0].memWalFD.Close()
	db.memtables[1].memWalFD.Close()
	return m
}

// Metrics holds byte-level write counters for computing write amplification.
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

	// Cumulative counters persisted in cowMeta across all sessions.
	totalLogicalBytesWrit  int64   // cumulative user payload bytes (all sessions)
	totalPhysicalBytesWrit int64   // cumulative physical bytes written (all sessions)
	CumulativeWriteAmp     float64 // totalPhysicalBytesWrit / totalLogicalBytesWrit

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
}

func (z *Metrics) String() (r string) {
	r = "Metrics{\n"
	r += fmt.Sprintf("      (just this) Session: %v\n", z.Session)
	r += fmt.Sprintf("        KV128 BytesWritten: %v\n", z.KV128BytesWritten)
	r += fmt.Sprintf("       MemWAL BytesWritten: %v\n", z.MemWALBytesWritten)
	r += fmt.Sprintf("      REDOLog BytesWritten: %v\n", z.REDOLogBytesWritten)
	r += fmt.Sprintf("FlexTreePages BytesWritten: %v\n", z.FlexTreePagesBytesWritten)
	r += fmt.Sprintf("   LARGE.VLOG BytesWritten: %v\n", z.VLOGBytesWritten)
	r += fmt.Sprintf("      Logical BytesWritten: %v\n", z.LogicalBytesWritten)
	r += fmt.Sprintf("        Total BytesWritten: %v\n", z.TotalBytesWritten)
	r += fmt.Sprintf("                 WriteAmp: %0.3f\n", z.WriteAmp)
	r += fmt.Sprintf("\n   -------- lifetime totals over all sessions  --------  \n")
	r += fmt.Sprintf("    TotalLogical BytesWrit: %v\n", z.totalLogicalBytesWrit)
	r += fmt.Sprintf("   TotalPhysical BytesWrit: %v\n", z.totalPhysicalBytesWrit)
	r += fmt.Sprintf("       CumulativeWriteAmp: %0.3f\n", z.CumulativeWriteAmp)
	r += fmt.Sprintf("\n   -------- free space / block utilization --------  \n")
	r += fmt.Sprintf("            TotalLiveBytes: %v (%0.2f MB)\n", z.TotalLiveBytes, float64(z.TotalLiveBytes)/(1<<20))
	r += fmt.Sprintf("    TotalFreeBytesInBlocks: %v (%0.2f MB)\n", z.TotalFreeBytesInBlocks, float64(z.TotalFreeBytesInBlocks)/(1<<20))
	r += fmt.Sprintf("      FLEXSPACE_BLOCK_SIZE: %0.2f MB\n", float64(FLEXSPACE_BLOCK_SIZE)/(1<<20))
	r += fmt.Sprintf("               BlocksInUse: %v  (%0.2f MB)\n", z.BlocksInUse, float64(z.BlocksInUse*FLEXSPACE_BLOCK_SIZE)/(1<<20))
	r += fmt.Sprintf("  BlocksWithLowUtilization: %v\n", z.BlocksWithLowUtilization)
	r += fmt.Sprintf("\n   -------- based on parameters used --------  \n")
	r += fmt.Sprintf("    LowBlockUtilizationPct: %0.1f %%\n", 100*z.LowBlockUtilizationPct)
	if z.PiggybackGCRuns > 0 {
		r += fmt.Sprintf("\n   -------- piggyback GC --------  \n")
		r += fmt.Sprintf("          PiggybackGCRuns: %v\n", z.PiggybackGCRuns)
		r += fmt.Sprintf("     PiggybackGCLastDurMs: %v\n", z.PiggybackGCLastDurMs)
	}

	r += "}\n"
	return
}

// Metrics returns a snapshot of write-byte counters aggregated from all layers.
func (db *FlexDB) SessionMetrics() *Metrics {
	db.topMutRW.Lock()
	defer db.topMutRW.Unlock()
	return db.writeLockHeldSessionMetrics()
}
func (db *FlexDB) writeLockHeldSessionMetrics() *Metrics {
	m := &Metrics{
		Session:                   true,
		KV128BytesWritten:         atomic.LoadInt64(&db.ff.KV128BytesWritten),
		MemWALBytesWritten:        atomic.LoadInt64(&db.MemWALBytesWritten),
		REDOLogBytesWritten:       atomic.LoadInt64(&db.ff.REDOLogBytesWritten),
		FlexTreePagesBytesWritten: atomic.LoadInt64(&db.ff.tree.FlexTreePagesBytesWritten),
		LogicalBytesWritten:       atomic.LoadInt64(&db.LogicalBytesWritten),
		LowBlockUtilizationPct:    db.cfg.LowBlockUtilizationPct,
	}
	if db.vlog != nil {
		m.VLOGBytesWritten = atomic.LoadInt64(&db.vlog.VLOGBytesWritten)
	}
	m.TotalBytesWritten = m.KV128BytesWritten + m.MemWALBytesWritten +
		m.REDOLogBytesWritten + m.FlexTreePagesBytesWritten + m.VLOGBytesWritten

	if m.LogicalBytesWritten > 0 {
		m.WriteAmp = float64(m.TotalBytesWritten) / float64(m.LogicalBytesWritten)
	}

	m.totalLogicalBytesWrit = db.totalLogicalBytesWrit()
	m.totalPhysicalBytesWrit = db.totalPhysicalBytesWrit()
	if m.totalLogicalBytesWrit > 0 {
		m.CumulativeWriteAmp = float64(m.totalPhysicalBytesWrit) / float64(m.totalLogicalBytesWrit)
	}

	m.TotalLiveBytes, m.TotalFreeBytesInBlocks, m.BlocksInUse, m.BlocksWithLowUtilization =
		db.ff.garbageMetrics(db.cfg.LowBlockUtilizationPct)

	m.PiggybackGCRuns = db.piggyGCStats.TotalGCRuns
	m.PiggybackGCLastDurMs = db.piggyGCStats.LastGCDuration.Milliseconds()

	return m
}

// totalLogicalBytesWrit returns the cumulative total of user payload bytes
// (key + value) written across all sessions, including the current one.
func (db *FlexDB) totalLogicalBytesWrit() int64 {
	return db.totalLogicalBase + atomic.LoadInt64(&db.LogicalBytesWritten)
}

// totalPhysicalBytesWrit returns the cumulative total of all physical bytes
// written to disk across all sessions, including the current one.
func (db *FlexDB) totalPhysicalBytesWrit() int64 {
	return db.totalPhysicalBase + db.sessionPhysicalBytes()
}

// sessionPhysicalBytes sums all physical byte counters for the current session.
func (db *FlexDB) sessionPhysicalBytes() int64 {
	total := atomic.LoadInt64(&db.ff.KV128BytesWritten) +
		atomic.LoadInt64(&db.MemWALBytesWritten) +
		atomic.LoadInt64(&db.ff.REDOLogBytesWritten) +
		atomic.LoadInt64(&db.ff.tree.FlexTreePagesBytesWritten)
	if db.vlog != nil {
		total += atomic.LoadInt64(&db.vlog.VLOGBytesWritten)
	}
	return total
}

// persistCounters sets the cumulative counters on the FlexTree so the next
// SyncCoW() will write them to the cowMeta record.
func (db *FlexDB) persistCounters() {
	db.ff.tree.totalLogicalBytesWrit = db.totalLogicalBytesWrit()
	db.ff.tree.totalPhysicalBytesWrit = db.totalPhysicalBytesWrit()
	db.ff.tree.MaxHLC = int64(db.hlc.CreateSendOrLocalEvent())
	db.ff.tree.liveKeys = db.liveKeys
	db.ff.tree.liveBigKeys = db.liveBigKeys
	db.ff.tree.liveSmallKeys = db.liveSmallKeys
}

// finalMetrics builds a Metrics snapshot after the final sync in Close().
// At this point all session counters are final and the cumulative totals
// have been persisted, so the write amplification numbers are accurate.
func (db *FlexDB) finalMetrics() *Metrics {

	m := &Metrics{
		Session:                   true,
		KV128BytesWritten:         atomic.LoadInt64(&db.ff.KV128BytesWritten),
		MemWALBytesWritten:        atomic.LoadInt64(&db.MemWALBytesWritten),
		REDOLogBytesWritten:       atomic.LoadInt64(&db.ff.REDOLogBytesWritten),
		FlexTreePagesBytesWritten: atomic.LoadInt64(&db.ff.tree.FlexTreePagesBytesWritten),
		LogicalBytesWritten:       atomic.LoadInt64(&db.LogicalBytesWritten),
		LowBlockUtilizationPct:    db.cfg.LowBlockUtilizationPct,
	}
	if db.vlog != nil {
		m.VLOGBytesWritten = atomic.LoadInt64(&db.vlog.VLOGBytesWritten)
	}
	m.TotalBytesWritten = m.KV128BytesWritten + m.MemWALBytesWritten +
		m.REDOLogBytesWritten + m.FlexTreePagesBytesWritten + m.VLOGBytesWritten

	if m.LogicalBytesWritten > 0 {
		m.WriteAmp = float64(m.TotalBytesWritten) / float64(m.LogicalBytesWritten)
	}

	m.totalLogicalBytesWrit = db.totalLogicalBytesWrit()
	m.totalPhysicalBytesWrit = db.totalPhysicalBytesWrit()
	if m.totalLogicalBytesWrit > 0 {
		m.CumulativeWriteAmp = float64(m.totalPhysicalBytesWrit) / float64(m.totalLogicalBytesWrit)
	}

	m.TotalLiveBytes, m.TotalFreeBytesInBlocks, m.BlocksInUse, m.BlocksWithLowUtilization =
		db.ff.garbageMetrics(db.cfg.LowBlockUtilizationPct)

	m.PiggybackGCRuns = db.piggyGCStats.TotalGCRuns
	m.PiggybackGCLastDurMs = db.piggyGCStats.LastGCDuration.Milliseconds()

	return m
}

// CumulativeMetrics reports file sizes on disk, reflecting the cumulative
// history of all sessions. Each physical metric is the current file size
// (via Stat), so it captures bytes written by previous sessions as well.
// LogicalBytesWritten uses FlexSpace's MaxLoff as an approximation of total
// user payload stored (it includes kv128 encoding overhead of ~10-20 bytes
// per entry; values separated to VLOG are represented by 16-byte VPtrs).
func (db *FlexDB) CumulativeMetrics() *Metrics {
	db.topMutRW.RLock()
	defer db.topMutRW.RUnlock()

	m := &Metrics{}

	// FLEXSPACE.KV128_BLOCKS file: total FlexSpace data on disk.
	if fi, err := db.ff.fdKV128blocks.Stat(); err == nil {
		m.KV128BytesWritten = fi.Size()
	}

	// WAL files: FLEXDB.MEMWAL1 + FLEXDB.MEMWAL2 current sizes.
	if fi, err := db.memtables[0].memWalFD.Stat(); err == nil {
		m.MemWALBytesWritten += fi.Size()
	}
	if fi, err := db.memtables[1].memWalFD.Stat(); err == nil {
		m.MemWALBytesWritten += fi.Size()
	}

	// FlexSpace redo LOG file.
	if fi, err := db.ff.redoLogFD.Stat(); err == nil {
		m.REDOLogBytesWritten = fi.Size()
	}

	// Tree persistence files.

	// CoW mode (always now): FLEXTREE.PAGES + FLEXTREE.COMMIT files.
	if db.ff.tree.nodeFD != nil {
		if fi, err := db.ff.tree.nodeFD.Stat(); err == nil {
			m.FlexTreePagesBytesWritten += fi.Size()
		}
	}
	if db.ff.tree.metaFD != nil {
		if fi, err := db.ff.tree.metaFD.Stat(); err == nil {
			m.FlexTreePagesBytesWritten += fi.Size()
		}
	}

	// VLOG file: vlog.tail tracks the append-only file size.
	if db.vlog != nil {
		m.VLOGBytesWritten = db.vlog.size()
	}

	// LogicalBytesWritten: FlexSpace MaxLoff is the total kv128-encoded
	// data size across all sessions—the best cumulative approximation
	// of user payload without scanning every KV or persisting a counter.
	m.LogicalBytesWritten = int64(db.ff.tree.MaxLoff)

	m.TotalBytesWritten = m.KV128BytesWritten + m.MemWALBytesWritten +
		m.REDOLogBytesWritten + m.FlexTreePagesBytesWritten + m.VLOGBytesWritten

	if m.LogicalBytesWritten > 0 {
		m.WriteAmp = float64(m.TotalBytesWritten) / float64(m.LogicalBytesWritten)
	}

	m.totalLogicalBytesWrit = db.totalLogicalBytesWrit()
	m.totalPhysicalBytesWrit = db.totalPhysicalBytesWrit()
	if m.totalLogicalBytesWrit > 0 {
		m.CumulativeWriteAmp = float64(m.totalPhysicalBytesWrit) / float64(m.totalLogicalBytesWrit)
	}

	m.TotalLiveBytes, m.TotalFreeBytesInBlocks, m.BlocksInUse, m.BlocksWithLowUtilization =
		db.ff.garbageMetrics(db.cfg.LowBlockUtilizationPct)

	m.PiggybackGCRuns = db.piggyGCStats.TotalGCRuns
	m.PiggybackGCLastDurMs = db.piggyGCStats.LastGCDuration.Milliseconds()

	return m
}

// resolveVPtr reads the value from the VLOG file for a KV that has HasVPtr set.
// Returns the resolved value bytes, or an error.
func (db *FlexDB) resolveVPtr(kv KV) ([]byte, error) {
	if !kv.HasVPtr {
		return kv.Value, nil
	}
	if db.vlog == nil {
		return nil, fmt.Errorf("flexdb: VPtr but VLOG is nil")
	}
	return db.vlog.read(kv.Vptr)
}

// FetchLarge retrieves the value bytes for a KV whose value
// is stored in the VLOG (kv.Large() returns true). For inline
// values, it simply returns kv.Value. The returned bytes are
// a fresh copy safe to retain.
func (db *FlexDB) FetchLarge(kv *KV) ([]byte, error) {
	if kv == nil {
		return nil, fmt.Errorf("flexdb: FetchLarge called with nil KV")
	}
	return db.resolveVPtr(*kv)
}

// VacuumVLOGStats reports the results of a VacuumVLOG operation.
type VacuumVLOGStats struct {
	OldVLOGSize        int64
	NewVLOGSize        int64
	BytesReclaimed     int64
	EntriesCopied      int64
	IntervalsRewritten int64
}

func (z *VacuumVLOGStats) String() (r string) {
	r = "VacuumVLOGStats{\n"
	r += fmt.Sprintf("       OldVLOGSize: %v,\n", z.OldVLOGSize)
	r += fmt.Sprintf("       NewVLOGSize: %v,\n", z.NewVLOGSize)
	r += fmt.Sprintf("    BytesReclaimed: %v,\n", z.BytesReclaimed)
	r += fmt.Sprintf("     EntriesCopied: %v,\n", z.EntriesCopied)
	r += fmt.Sprintf("IntervalsRewritten: %v,\n", z.IntervalsRewritten)
	r += "}\n"
	return
}

// VacuumVLOG reclaims dead LARGE.VLOG space by copying live values to a new VLOG
// file and rewriting their VPtrs in FlexSpace. This is an exclusive operation
// that acquires topMutRW.
//
// Crash safety: if the process crashes before the rename completes, the old
// VLOG and old intervals remain intact. The stale VLOG.new file (if present)
// is harmless and will be overwritten on the next vacuum.
func (db *FlexDB) VacuumVLOG() (*VacuumVLOGStats, error) {
	db.topMutRW.Lock()
	defer db.topMutRW.Unlock()

	if db.vlog == nil {
		return nil, fmt.Errorf("flexdb: VLOG is disabled")
	}
	stats := &VacuumVLOGStats{}

	// Flush memtables so all live VPtrs are in FlexSpace.
	db.writeLockHeldSync()

	// Exclusive access to FlexSpace and memtables by topMutRW

	stats.OldVLOGSize = db.vlog.size()

	// Create new VLOG file.
	newPath := filepath.Join(db.Path, "VLOG.new")
	newVL, err := openValueLog(newPath, db.vfs)
	if err != nil {
		return stats, fmt.Errorf("vacuum: open new VLOG: %w", err)
	}

	// Walk all leaf nodes via the linked list.
	t := db.tree
	for node := t.leafHead; node != nil; node = node.next {
		// Compute shift for this leaf.
		nh := memSparseIndexTreeHandler{node: node}
		memSparseIndexTreeHandlerInfoUpdate(&nh)

		for ai := 0; ai < node.count; ai++ {
			anchor := node.anchors[ai]
			if anchor.psize == 0 {
				continue
			}
			anchorLoff := uint64(anchor.loff + nh.shift)
			partition := db.cache.getPartition(anchor)
			fce := partition.getEntry(anchor, anchorLoff, db)

			// Check if any KV in this interval has a VPtr.
			hasVPtr := false
			for i := 0; i < fce.count; i++ {
				if fce.kvs[i].HasVPtr {
					hasVPtr = true
					break
				}
			}
			if !hasVPtr {
				partition.releaseEntry(fce)
				continue
			}

			// Copy live VPtr values to new VLOG and update VPtrs.
			for i := 0; i < fce.count; i++ {
				if !fce.kvs[i].HasVPtr {
					continue
				}
				// Read value from old VLOG.
				val, err := db.vlog.read(fce.kvs[i].Vptr)
				if err != nil {
					partition.releaseEntry(fce)
					newVL.close()
					db.vfs.Remove(newPath)
					return stats, fmt.Errorf("vacuum: read old vptr: %w", err)
				}
				// Append to new VLOG (preserve HLC from the KV).
				newVP, err := newVL.appendLocked(val, fce.kvs[i].Hlc)
				if err != nil {
					partition.releaseEntry(fce)
					newVL.close()
					db.vfs.Remove(newPath)
					return stats, fmt.Errorf("vacuum: append new VLOG: %w", err)
				}
				fce.kvs[i].Vptr = newVP
				stats.EntriesCopied++
			}

			// Re-encode the entire interval and rewrite in FlexSpace.
			buf := db.itvbuf[:0]
			for i := 0; i < fce.count; i++ {
				buf = kv128Encode(buf, fce.kvs[i])
			}
			newPSize := uint32(len(buf))
			db.ff.Update(buf, anchorLoff, uint64(newPSize), uint64(anchor.psize))
			if newPSize != anchor.psize {
				nh.idx = ai
				nh.shiftUpPropagate(int64(newPSize) - int64(anchor.psize))
				anchor.psize = newPSize
			}
			anchor.unsorted = 0
			stats.IntervalsRewritten++
			partition.releaseEntry(fce)
		}
	}

	// Sync new VLOG and FlexSpace.
	if err := newVL.sync(); err != nil {
		newVL.close()
		db.vfs.Remove(newPath)
		return stats, fmt.Errorf("vacuum: sync new VLOG: %w", err)
	}
	db.ff.Sync()

	// Close old VLOG fd, rename new -> old, reopen.
	oldPath := filepath.Join(db.Path, "LARGE.VLOG")
	newVL.close()
	if err := db.vfs.Rename(newPath, oldPath); err != nil {
		return stats, fmt.Errorf("vacuum: rename: %w", err)
	}
	if err := db.vlog.reopen(oldPath); err != nil {
		return stats, fmt.Errorf("vacuum: reopen: %w", err)
	}

	stats.NewVLOGSize = db.vlog.size()
	stats.BytesReclaimed = stats.OldVLOGSize - stats.NewVLOGSize

	// Cache entries were updated in-place (VPtrs updated) and FlexSpace
	// was rewritten, so the cache remains consistent. Invalidate anyway
	// to be safe and free memory from intervals that were not rewritten.
	for node := t.leafHead; node != nil; node = node.next {
		for ai := 0; ai < node.count; ai++ {
			anchor := node.anchors[ai]
			if anchor.fce != nil {
				anchor.fce.anchor = nil
				anchor.fce = nil
			}
		}
	}
	db.cache.destroyAll()

	return stats, nil
}

// VacuumKVStats reports the results of a VacuumKV operation.
type VacuumKVStats struct {
	OldFileSize      int64
	NewFileSize      int64
	BytesReclaimed   int64
	ExtentsRewritten int64
}

func (z *VacuumKVStats) String() (r string) {
	r = "VacuumKVStats{\n"
	r += fmt.Sprintf("      OldFileSize: %v,\n", z.OldFileSize)
	r += fmt.Sprintf("      NewFileSize: %v,\n", z.NewFileSize)
	r += fmt.Sprintf("   BytesReclaimed: %v,\n", z.BytesReclaimed)
	r += fmt.Sprintf("ExtentsRewritten: %v,\n", z.ExtentsRewritten)
	r += "}\n"
	return
}

// VacuumKV reclaims dead FLEXSPACE.KV128_BLOCKS space by rewriting all live extents
// sequentially to a new file and replacing the old file. This is an exclusive
// operation that acquires topMutRW.
//
// Crash safety: if the process crashes before the rename completes, the old
// FLEXSPACE.KV128_BLOCKS and old FlexTree remain intact. The stale .vacuum file (if
// present) is harmless and will be overwritten on the next vacuum.
//
// VacuumKV does a one-time compaction of already-bloated databases. Algorithm:
// 1. Flush memtables, acquire exclusive locks
// 2. Walk FlexTree leaf linked list, read/rewrite all live extents sequentially to a .vacuum file
// 3. Close old fd, rename .vacuum -> FLEXSPACE.KV128_BLOCKS, reopen
// 4. Rebuild block manager, checkpoint FlexTree, invalidate caches
// 5. Clean up stale .vacuum files on OpenFlexSpaceCoW
//
// See the tests:
// TestFlexDB_VacuumKV_Basic — overwrites 200 keys, vacuums, verifies data integrity across reopen
// TestFlexDB_VacuumKV_WithDeletes — deletes half of 100 keys, vacuums, verifies correct keys survive
// .
func (db *FlexDB) VacuumKV() (*VacuumKVStats, error) {
	db.topMutRW.Lock()
	defer db.topMutRW.Unlock()

	stats := &VacuumKVStats{}

	// Flush memtables so all live data is in FlexSpace.
	db.writeLockHeldSync()

	// Exclusive access to FlexSpace and memtables by topMutRW.

	ff := db.ff

	// Explicitly flush the FlexSpace block manager. db.Sync() may have been
	// a no-op (empty memtable), but the block manager could still have
	// unflushed data from a previous write that didn't fill a block.
	ff.Sync()

	// Record old file size.
	fi, err := ff.fdKV128blocks.Stat()
	if err != nil {
		return stats, fmt.Errorf("vacuumkv: stat old file: %w", err)
	}
	stats.OldFileSize = fi.Size()

	if ff.tree.LeafHead.IsIllegal() {
		// Empty tree — nothing to vacuum.
		return stats, nil
	}

	// Create new file.
	dataPath := filepath.Join(ff.Path, "FLEXSPACE.KV128_BLOCKS")
	vacuumPath := dataPath + ".vacuum"
	//newFD, err := db.vfs.OpenFile(vacuumPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	newFD, err := db.vfs.OpenReadWrite(vacuumPath, vfs.WriteCategoryUnspecified)
	if err != nil {
		return stats, fmt.Errorf("vacuumkv: create vacuum file: %w", err)
	}

	// Walk all leaf nodes via the linked list, rewriting extents sequentially.
	writeOffset := uint64(0)
	nodeID := ff.tree.LeafHead
	for !nodeID.IsIllegal() {
		le := ff.tree.GetLeaf(nodeID)
		for i := uint32(0); i < le.Count; i++ {
			ext := &le.Extents[i]
			if ext.IsHole() {
				continue // holes have no physical storage
			}
			length := uint64(ext.Len)
			poff := ext.Address()

			// Read data from old file. After ff.Sync() above, the block
			// manager has been flushed (blkoff=0), so all data is on disk.
			// Read directly from the file to avoid any stale-buffer issues.
			buf := make([]byte, length)
			n, readErr := ff.fdKV128blocks.ReadAt(buf, int64(poff))
			if readErr != nil || uint64(n) != length {
				newFD.Close()
				db.vfs.Remove(vacuumPath)
				return stats, fmt.Errorf("vacuumkv: read poff=%d len=%d: %w", poff, length, readErr)
			}

			// Write sequentially to new file.
			_, writeErr := newFD.WriteAt(buf, int64(writeOffset))
			if writeErr != nil {
				newFD.Close()
				db.vfs.Remove(vacuumPath)
				return stats, fmt.Errorf("vacuumkv: write offset=%d len=%d: %w", writeOffset, length, writeErr)
			}

			// Update extent's physical offset to the new location.
			ext.SetPoff(writeOffset)
			le.Dirty = true

			writeOffset += length
			stats.ExtentsRewritten++
		}
		nodeID = le.Next
	}

	// Sync the new file to ensure all data is durable before we rename.
	if err := newFD.Sync(); err != nil {
		newFD.Close()
		db.vfs.Remove(vacuumPath)
		return stats, fmt.Errorf("vacuumkv: sync new file: %w", err)
	}
	newFD.Close()

	// Close old fd, rename new -> old, reopen.
	ff.fdKV128blocks.Close()
	if err := db.vfs.Rename(vacuumPath, dataPath); err != nil { // oldpath, newpath
		return stats, fmt.Errorf("vacuumkv: rename: %w", err)
	}
	//newFD2, err := db.vfs.OpenFile(dataPath, os.O_RDWR, 0644)
	newFD2, err := db.vfs.OpenReadWrite(dataPath, vfs.WriteCategoryUnspecified)

	if err != nil {
		return stats, fmt.Errorf("vacuumkv: reopen: %w", err)
	}
	ff.fdKV128blocks = newFD2

	// Truncate the new file to exactly writeOffset so that no stale
	// data from the old file lingers beyond the live extents. Without
	// this, a second VacuumKV would try to read poff values that
	// point into the compacted region — past the actual data — and
	// get EOF because the .vacuum file is smaller than those offsets.
	if err := ff.fdKV128blocks.Truncate(int64(writeOffset)); err != nil {
		return stats, fmt.Errorf("vacuumkv: truncate: %w", err)
	}

	// Reset block manager: rebuild blkusage from the updated FlexTree.
	// Zero the arrays, then bmInit sets blkdist[0] and freeBlocks itself.
	for i := range ff.bm.blkusage {
		ff.bm.blkusage[i] = 0
	}
	for i := range ff.bm.blkdist {
		ff.bm.blkdist[i] = 0
	}
	ff.bm.freeBlocks = 0
	// Clear the stale write buffer so bm.read() can't serve old data.
	for i := range ff.bm.buf {
		ff.bm.buf[i] = 0
	}
	bmInit(ff.bm, ff.tree)

	// Also truncate trailing empty blocks to reclaim any remaining
	// slack from block-alignment rounding.
	ff.truncateTrailingBlocks()

	// Invalidate the sequential IO cache — poffs have all changed.
	ff.globalEpoch++

	// Mark ALL internal nodes dirty so SyncCoW will traverse them
	// and persist the dirty leaves underneath. Without this, SyncCoW's
	// syncCowRec() skips clean internal nodes (returns nil early),
	// leaving dirty leaves with updated poff values unpersisted.
	// On the next open, those leaves would still have stale pre-vacuum
	// poffs pointing beyond the compacted file, causing EOF errors.
	ff.tree.MarkAllInternalsDirty()

	// Checkpoint FlexTree (poffs changed, nodes are dirty).
	if err := ff.tree.SyncCoW(); err != nil {
		return stats, fmt.Errorf("vacuumkv: sync cow: %w", err)
	}

	// Truncate and rewrite redo log header.
	ff.logTruncate()
	ff.writeLogVersion()
	ff.redoLogFlushAndSync()

	// Invalidate all interval caches (poffs changed).
	for node := db.tree.leafHead; node != nil; node = node.next {
		for ai := 0; ai < node.count; ai++ {
			anchor := node.anchors[ai]
			if anchor.fce != nil {
				anchor.fce.anchor = nil
				anchor.fce = nil
			}
		}
	}
	db.cache.destroyAll()

	stats.NewFileSize = int64(writeOffset)
	stats.BytesReclaimed = stats.OldFileSize - stats.NewFileSize

	return stats, nil
}

// IntegrityError describes a single integrity violation.
type IntegrityError struct {
	Check  string // which check failed
	Detail string // human-readable details
	Fatal  bool   // if true, subsequent checks may be unreliable
}

func (e IntegrityError) Error() string {
	return fmt.Sprintf("[%s] %s", e.Check, e.Detail)
}

// CheckIntegrity performs a read-only consistency check of the FlexDB.
// It flushes memtables first, then acquires a read lock on FlexSpace.
//
// Checks performed:
//  1. FlexTree leaf linked list: no cycles, prev/next consistency
//  2. Extent validity: every non-hole extent has poff + len within file bounds
//  3. Extent readability: data at every extent can be read from disk
//  4. Block usage: recomputed from FlexTree matches the block manager's state
//  5. Sparse index: every anchor interval is readable and kv128-decodable
//  6. Sorted keys: keys within each decoded interval are in sorted order
//  7. Anchor coverage: anchor loff+psize spans tile the FlexSpace without gaps/overlaps
//
// Returns nil if no errors found.
func (db *FlexDB) CheckIntegrity() []IntegrityError {
	db.topMutRW.Lock()
	defer db.topMutRW.Unlock()

	// Flush memtables so FlexSpace has all live data.
	db.writeLockHeldSync()

	var errs []IntegrityError
	addErr := func(check, detail string, fatal bool) {
		errs = append(errs, IntegrityError{Check: check, Detail: detail, Fatal: fatal})
	}

	ff := db.ff
	tree := ff.tree

	// ---- Check 1: File stat ----
	fi, err := ff.fdKV128blocks.Stat()
	if err != nil {
		addErr("file_stat", fmt.Sprintf("cannot stat FLEXSPACE.KV128_BLOCKS: %v", err), true)
		return errs
	}
	fileSize := fi.Size()

	// ---- Check 2: FlexTree leaf linked list + extent validity ----
	leafCount := 0
	extentCount := uint64(0)
	totalExtentBytes := uint64(0)
	computedBlkUsage := make([]uint64, FLEXSPACE_BLOCK_COUNT)

	nodeID := tree.LeafHead
	visited := make(map[NodeID]bool)
	prevNodeID := IllegalID

	for !nodeID.IsIllegal() {
		if visited[nodeID] {
			addErr("leaf_linked_list", fmt.Sprintf("cycle detected at nodeID=%d", nodeID), true)
			break
		}
		visited[nodeID] = true
		leafCount++

		le := tree.GetLeaf(nodeID)

		// Verify prev pointer
		if le.Prev != prevNodeID {
			addErr("leaf_linked_list",
				fmt.Sprintf("leaf %d: prev=%d, expected=%d", nodeID, le.Prev, prevNodeID), false)
		}

		// Verify extents within this leaf
		for i := uint32(0); i < le.Count; i++ {
			ext := &le.Extents[i]
			extentCount++

			if ext.IsHole() {
				continue
			}

			poff := ext.Address()
			length := uint64(ext.Len)

			if length == 0 {
				addErr("extent_validity",
					fmt.Sprintf("leaf %d ext %d: zero-length non-hole extent", nodeID, i), false)
				continue
			}

			// Check that poff + length is within file bounds
			if int64(poff+length) > fileSize {
				addErr("extent_validity",
					fmt.Sprintf("leaf %d ext %d: poff=%d len=%d exceeds file size %d",
						nodeID, i, poff, length, fileSize), false)
				continue
			}

			// Accumulate block usage
			blkid := poff >> FLEXSPACE_BLOCK_BITS
			endBlkid := (poff + length - 1) >> FLEXSPACE_BLOCK_BITS
			if blkid != endBlkid {
				addErr("extent_validity",
					fmt.Sprintf("leaf %d ext %d: extent spans blocks %d-%d (poff=%d len=%d)",
						nodeID, i, blkid, endBlkid, poff, length), false)
			}
			if blkid < FLEXSPACE_BLOCK_COUNT {
				computedBlkUsage[blkid] += length
			}

			totalExtentBytes += length

			// Check that data is readable from disk
			readBuf := make([]byte, length)
			n, readErr := ff.fdKV128blocks.ReadAt(readBuf, int64(poff))
			if readErr != nil || uint64(n) != length {
				addErr("extent_readable",
					fmt.Sprintf("leaf %d ext %d: read failed at poff=%d len=%d: err=%v n=%d",
						nodeID, i, poff, length, readErr, n), false)
			}
		}

		// Verify loffs are non-decreasing within leaf
		for i := uint32(1); i < le.Count; i++ {
			if le.Extents[i].Loff < le.Extents[i-1].Loff {
				addErr("extent_order",
					fmt.Sprintf("leaf %d: loff[%d]=%d < loff[%d]=%d (not sorted)",
						nodeID, i, le.Extents[i].Loff, i-1, le.Extents[i-1].Loff), false)
			}
		}

		prevNodeID = nodeID
		nodeID = le.Next
	}

	// ---- Check 3: Block usage consistency ----
	for i := uint64(0); i < FLEXSPACE_BLOCK_COUNT; i++ {
		actual := uint64(ff.bm.blkusage[i])
		computed := computedBlkUsage[i]
		if actual != computed {
			addErr("block_usage",
				fmt.Sprintf("block %d: bm.blkusage=%d, computed from tree=%d",
					i, actual, computed), false)
		}
	}

	// ---- Check 4: MaxLoff consistency ----
	// Sum of all extent lengths (including holes) should equal MaxLoff
	sumLoff := uint64(0)
	nodeID = tree.LeafHead
	for !nodeID.IsIllegal() {
		le := tree.GetLeaf(nodeID)
		for i := uint32(0); i < le.Count; i++ {
			sumLoff += uint64(le.Extents[i].Len)
		}
		nodeID = le.Next
	}
	if sumLoff != tree.MaxLoff {
		addErr("maxloff",
			fmt.Sprintf("sum of extent lengths=%d != tree.MaxLoff=%d",
				sumLoff, tree.MaxLoff), false)
	}

	// ---- Check 5: Sparse index anchor intervals ----
	if db.tree == nil || db.tree.leafHead == nil {
		return errs
	}

	anchorCount := 0
	totalAnchorBytes := uint64(0)
	prevAnchorEndLoff := uint64(0)
	ffSize := ff.Size()

	for snode := db.tree.leafHead; snode != nil; snode = snode.next {
		var nh memSparseIndexTreeHandler
		nh.node = snode
		memSparseIndexTreeHandlerInfoUpdate(&nh)

		for ai := 0; ai < snode.count; ai++ {
			anchor := snode.anchors[ai]
			if anchor == nil {
				addErr("sparse_index",
					fmt.Sprintf("nil anchor at node pos %d", ai), false)
				continue
			}
			anchorLoff := uint64(anchor.loff + nh.shift)
			psize := uint64(anchor.psize)
			anchorCount++

			// Check for gaps/overlaps between adjacent anchors
			if anchorCount > 1 && anchorLoff != prevAnchorEndLoff {
				addErr("anchor_coverage",
					fmt.Sprintf("anchor %d (key=%q): loff=%d but previous anchor ended at %d (gap/overlap=%d)",
						anchorCount, anchor.key, anchorLoff, prevAnchorEndLoff,
						int64(anchorLoff)-int64(prevAnchorEndLoff)), false)
			}
			prevAnchorEndLoff = anchorLoff + psize
			totalAnchorBytes += psize

			if psize == 0 {
				continue // empty anchor (e.g., sentinel at start)
			}

			// Verify the interval is within FlexSpace bounds
			if anchorLoff+psize > ffSize {
				addErr("anchor_bounds",
					fmt.Sprintf("anchor %d (key=%q): loff=%d psize=%d exceeds FlexSpace size %d",
						anchorCount, anchor.key, anchorLoff, psize, ffSize), false)
				continue
			}

			// Read the interval from FlexSpace
			itvBuf := make([]byte, psize)
			n, readErr := ff.Read(itvBuf, anchorLoff, psize)
			if readErr != nil || uint64(n) != psize {
				addErr("anchor_readable",
					fmt.Sprintf("anchor %d (key=%q): read loff=%d psize=%d failed: err=%v n=%d",
						anchorCount, anchor.key, anchorLoff, psize, readErr, n), false)
				continue
			}

			// Decode all KVs in the interval (slotted page + kv128 overflow)
			src := itvBuf
			kvCount := 0
			var prevKey []byte

			if slottedPageIsSlotted(src) {
				kvs, consumed, decErr := slottedPageDecode(src)
				if decErr != nil {
					addErr("slotted_decode",
						fmt.Sprintf("anchor %d (key=%q): slottedPageDecode failed: %v",
							anchorCount, anchor.key, decErr), false)
				} else {
					for _, kv := range kvs {
						kvCount++
						if prevKey != nil && bytes.Compare(kv.Key, prevKey) < 0 {
							addErr("key_order",
								fmt.Sprintf("anchor %d (key=%q): key %q < prev key %q at position %d",
									anchorCount, anchor.key, kv.Key, prevKey, kvCount), false)
						}
						prevKey = kv.Key
					}
					src = src[consumed:]
				}
			}

			// Decode remaining kv128 entries (overflow or legacy)
			for len(src) > 0 {
				kv, sz, ok := kv128Decode(src)
				if !ok {
					addErr("kv128_decode",
						fmt.Sprintf("anchor %d (key=%q): kv128Decode failed at byte %d of %d (decoded %d KVs so far)",
							anchorCount, anchor.key, int(psize)-len(src), psize, kvCount), false)
					break
				}
				kvCount++
				prevKey = kv.Key
				src = src[sz:]
			}

			if len(src) != 0 {
				addErr("kv128_trailing",
					fmt.Sprintf("anchor %d (key=%q): %d trailing bytes after decoding %d KVs",
						anchorCount, anchor.key, len(src), kvCount), false)
			}
		}
	}

	// ---- Check 6: Anchor coverage matches FlexSpace size ----
	if totalAnchorBytes != ffSize && ffSize > 0 {
		addErr("anchor_total_size",
			fmt.Sprintf("total anchor psize sum=%d != FlexSpace size=%d",
				totalAnchorBytes, ffSize), false)
	}

	return errs
}

// Sync flushes all in-memory data in the active memtable to
// disk in FLEXSPACE.KV128.BLOCKS and fsyncs it.
// Users must call Sync after Puts for them to be durable.
func (db *FlexDB) Sync() error {
	db.topMutRW.Lock()
	defer db.topMutRW.Unlock()

	return db.writeLockHeldSync()
}

// maybePiggybackGC runs GC if PiggybackGC_on_SyncOrFlush is enabled
// and the garbage fraction exceeds GCGarbagePct. Called with write lock held.
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

func (db *FlexDB) writeLockHeldSync() error {

	wasActive := db.activeMT
	if db.memtables[wasActive].empty {
		//vv("FlexDB.Sync(): memtable[%v] is empty, no syncing today", wasActive)
		return nil // nothing to flush
	}
	//vv("FlexDB.Sync(): memtable[%v] NOT empty, so syncing it! '%v'", wasActive, db.memtables[wasActive].memWalFD.Name())

	// Switch writes to the other table.
	newActive := 1 - wasActive
	db.activeMT = newActive
	db.memtables[newActive].empty = true
	db.memtables[newActive].size = 0

	// Flush the old active table.
	if db.cfg.OmitMemWalFsync {
		db.memtables[wasActive].logFlush() // insufficient for safety: does not fdatasync!
	} else {
		db.memtables[wasActive].logSync() // flush + fdatasync. here in FlexDB.Sync()
	}
	db.flushMemtable(wasActive)
	db.cache.flushDirtyPages()
	db.persistCounters()
	db.ff.Sync() // fsyncs FLEXSPACE.KV128.BLOCKS
	db.maybePiggybackGC()
	db.verifyAnchorTags()

	// Sync the parent directory so new/renamed files are durable.
	if err := syncDir(db.vfs, db.Path); err != nil {
		return fmt.Errorf("flexdb: sync dir: %w", err)
	}

	ts := uint64(time.Now().UnixNano())
	db.memtables[wasActive].logTruncateWithVersion(ts, db.ff.tree.PersistentVersion)

	db.memtables[wasActive].bt.Clear()
	db.memtables[wasActive].empty = true
	db.memtables[wasActive].size = 0

	return nil
}

// Put writes key -> value. value==nil deletes the key.
// Values of any size are accepted. Values > vlogInlineThreshold (64 bytes) are
// stored in the VLOG file; smaller values are stored inline in
// the FLEXSPACE.KV128_BLOCKS file with the keys.
// Large values are written exactly once: to the VLOG. The WAL stores only
// the VPtr (16 bytes), not the full value.
//
// Puts are not durably on disk until after the user has also
// completed a db.Sync() call. This allows the user to control
// the rate of fsyncs and trade that against their durability
// requirements.
func (db *FlexDB) Put(key, value []byte) error {
	db.topMutRW.Lock()
	defer db.topMutRW.Unlock()
	return db.writeLockHeldPut(key, value)
}

func (db *FlexDB) writeLockHeldPut(key, value []byte) error {

	if len(key)+16 >= MaxKeySize {
		return fmt.Errorf("flexdb: key too large (max %d bytes)", MaxKeySize-16)
	}
	// For inline values (no VLOG), the old total size limit still applies.
	if db.vlog == nil && len(key)+len(value)+16 >= MaxKeySize {
		return fmt.Errorf("flexdb: KV too large (max %d bytes)", MaxKeySize)
	}
	atomic.AddInt64(&db.LogicalBytesWritten, int64(len(key)+len(value)))

	// Tick the HLC for this write.
	hlcVal := db.hlc.CreateSendOrLocalEvent()

	// Defensive copy: the btree and interval cache store KV structs whose
	// Key/Value slice headers point to the backing array. If we stored the
	// caller's slices directly, the caller could mutate them after Put
	// returns, corrupting btree ordering and cached/flushed data.
	key = append([]byte{}, key...)
	if value != nil {
		value = append([]byte{}, value...)
	}

	// Build the KV for the memtable. Large values go to VLOG.
	kv := KV{Key: key, Value: value, Hlc: hlcVal}

	if db.vlog != nil && value != nil && len(value) > vlogInlineThreshold {
		// Write value to VLOG and fsync before WAL references it.
		// This ensures the VLOG entry is durable before the WAL VPtr.
		vp, err := db.vlog.appendAndSync(value, hlcVal)
		if err != nil {
			return fmt.Errorf("flexdb: vlog append: %w", err)
		}
		kv = KV{Key: key, Vptr: vp, HasVPtr: true, Hlc: hlcVal}
	}

	if db.memtables[db.activeMT].size >= memtableCap {
		// Inline flush — same pattern as Batch.Commit overflow path.
		wasActive := db.activeMT
		db.activeMT = 1 - wasActive
		db.memtables[db.activeMT].empty = true
		db.memtables[db.activeMT].size = 0

		db.memtables[wasActive].logFlush() // we don't hold memWalMut yet

		db.flushMemtable(wasActive)
		db.persistCounters()
		db.ff.Sync()

		db.memtables[wasActive].bt.Clear()
		db.memtables[wasActive].empty = true
		db.memtables[wasActive].size = 0
	}
	active := db.activeMT
	newState := kvToState(kv)
	old, replaced := db.memtables[active].put(kv)
	db.memtables[active].empty = false

	var oldState keyState
	if replaced {
		oldState = kvToState(old)
	} else {
		oldState = db.writeLockHeldKeyState(key)
	}
	db.adjustKeyCounters(oldState, newState)

	// WAL stores VPtr (not full value) for large values. Since LARGE.VLOG was
	// fsynced above, the VPtr is safe to reference on crash recovery.
	db.memtables[active].logAppend(kv)

	return nil
}

// SearchModifier controls the matching behavior of Find.
type SearchModifier int

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

// findOwnedKV controls whether Find/FindIt return an owned
// copy of the KV (safe to retain indefinitely) or a zero-copy
// pointer into cache memory (faster but only valid until the
// next iterator operation or cache eviction). Set to false
// to benchmark the zero-copy fast path.
const findOwnedKV = true

// findSeekIter positions it according to smod and key.
// Returns (found, exact). On return, it is either Valid
// (found=true) or invalid (found=false).
func findSeekIter(it *Iter, smod SearchModifier, key []byte) (found, exact bool) {
	switch smod {
	case GTE:
		it.Seek(key)
	case GT:
		it.Seek(key)
		if it.Valid() && key != nil && bytes.Equal(it.Key(), key) {
			it.Next()
		}
	case LTE:
		if key == nil {
			it.SeekToLast()
		} else {
			it.seekLE(key, false)
		}
	case LT:
		if key == nil {
			it.SeekToLast()
		} else {
			it.seekLE(key, true)
		}
	case Exact:
		it.Seek(key)
		if it.Valid() && !bytes.Equal(it.Key(), key) {
			it.releaseIterState()
			it.valid = false
			return false, false
		}
	}
	if !it.Valid() {
		return false, false
	}
	return true, key != nil && bytes.Equal(it.Key(), key)
}

// findBuildKV constructs a *KV from the iterator's current
// position. When findOwnedKV is true, the returned KV owns
// copies of Key and Value (safe to retain). When false, Key
// and Value point into cache memory (zero-copy, valid only
// until the next iterator operation).
func findBuildKV(it *Iter) *KV {
	if it.pKV == nil {
		return nil
	}
	src := it.pKV
	if !findOwnedKV {
		// Zero-copy fast path: return a shallow copy of the
		// internal KV. Key and Value alias cache memory.
		out := *src
		return &out
	}
	// Owned copy: duplicate Key and inline Value.
	out := KV{
		Vptr:    src.Vptr,
		HasVPtr: src.HasVPtr,
		Hlc:     src.Hlc,
	}
	if src.Key != nil {
		out.Key = make([]byte, len(src.Key))
		copy(out.Key, src.Key)
	}
	if !src.HasVPtr && src.Value != nil {
		// Trigger lazy copy in Vin() first so we read the
		// right bytes, then copy into our own buffer.
		v := it.Vin()
		out.Value = make([]byte, len(v))
		copy(out.Value, v)
	}
	return &out
}

// FindIt allows GTE, GT, LTE, LT, and Exact searches.
//
// GTE: find the smallest key greater-than-or-equal to key.
//
// GT: find the smallest key strictly greater-than key.
//
// LTE: find the largest key less-than-or-equal to key.
//
// LT: find the largest key strictly less-than key.
//
// Exact: find a matching key exactly.
//
// If key is nil, then GTE and GT return the first key
// in the tree, while LTE and LT return the last key.
//
// The returned *KV contains the found key and its inline
// value (for non-large values). For large values stored
// in the VLOG, kv.Large() returns true and db.FetchLarge(kv)
// retrieves the value bytes.
//
// found indicates whether any key was found.
// exact indicates an exact match to the query key.
//
// The returned iterator is positioned at the found key
// and can be used to scan beyond it (Next/Prev). The caller
// must call it.Close() when done. If found is false, the
// iterator is not valid but must still be closed.
func (db *FlexDB) FindIt(smod SearchModifier, key []byte) (kv *KV, found, exact bool, it *Iter) {
	it = db.NewIter()
	found, exact = findSeekIter(it, smod, key)
	if !found {
		return
	}
	kv = findBuildKV(it)
	return
}

// Find is a convenience wrapper around FindIt that closes
// the iterator before returning. Use FindIt instead if you
// want to scan beyond the found key.
func (db *FlexDB) Find(smod SearchModifier, key []byte) (kv *KV, found, exact bool) {
	var it *Iter
	kv, found, exact, it = db.FindIt(smod, key)
	it.Close()
	return
}

// Get retrieves the value for key. Returns nil, false if not found.
func (db *FlexDB) Get(key []byte) ([]byte, bool) {
	db.topMutRW.RLock()
	defer db.topMutRW.RUnlock()

	// Check active memtable
	active := db.activeMT
	if !db.memtables[active].empty {
		kv, ok := db.memtables[active].get(key)
		if ok {
			if kv.isTombstone() {
				return nil, false // tombstone
			}
			val, err := db.resolveVPtr(kv)
			if err != nil {
				return nil, false
			}
			out := make([]byte, len(val))
			copy(out, val)
			return out, true
		}
	}

	// Check inactive memtable...
	// Why is this needed, you may ask (as I did)?
	// The inactive memtable never receives new puts, but it
	// still contains data that hasn't been flushed to FlexSpace yet.
	//
	// Here's the window:
	//
	// doFlush():
	//   2247:  db.activeMT = 1 - db.activeMT   // swap (under mtMu)
	//   2250:  db.mtMu.Unlock()
	//          // ── WINDOW: inactive has data, FlexSpace doesn't yet ──
	// 	2258:  db.ffMu.Lock()
	// 	2259:  db.flushMemtable(inactive)        // writes to FlexSpace
	//          ...
	// 			   2269:  db.mtMu.Lock()
	// 	2270:  db.memtables[inactive].bt.Clear()  // now it's gone
	// 	2271:  db.memtables[inactive].empty = true
	//
	// 	Between lines 2250 and 2271, a concurrent Get() call would:
	// 	- Not find the key in the (new, empty) active memtable
	// 	- Not find it in FlexSpace (not flushed yet)
	// - Only find it by checking the inactive memtable at line 1621
	//
	// The inactive memtable is a read-only buffer during that window.
	// No puts go in, but the existing data is the only copy until
	// flushMemtable completes.

	inactive := 1 - db.activeMT
	if !db.memtables[inactive].empty {
		kv, ok := db.memtables[inactive].get(key)
		if ok {
			if kv.isTombstone() {
				return nil, false
			}
			val, err := db.resolveVPtr(kv)
			if err != nil {
				return nil, false
			}
			out := make([]byte, len(val))
			copy(out, val)
			return out, true // not val!
		}
	}

	// Check FlexSpace via sparse index
	val, found := db.getPassthrough(key)
	return val, found
}

// Delete removes key from the store.
func (db *FlexDB) Delete(key []byte) error {
	// Put already does locking.

	return db.Put(key, nil) // tombstone
}

// DeleteRange deletes all keys in the range [begKey, endKey] with
// configurable inclusivity on each bound.
//
// Returns:
//   - n: number of tombstones written (0 when allGone is true)
//   - allGone: true if the entire database was wiped and re-initialized.
//     When true, ALL previously held iterators, cursors, and pointers
//     into the database are invalid and must be re-acquired.
//   - err: non-nil on failure
//
// When includeLarge is false, keys whose values are stored in the VLOG
// (large values, > 64 bytes) are skipped and survive the deletion.
//
// The begInclusive and endInclusive parameters control whether the
// bounds are inclusive or exclusive:
//
//	DeleteRange(true,  a, z, true,  true)   // [a, z]  — both inclusive, include large values
//	DeleteRange(true,  a, z, true,  false)  // [a, z)  — half-open, include large values
//	DeleteRange(false, a, z, true,  true)   // [a, z]  — both inclusive, skip large values
//
// Goroutine safe. Concurrent reads and writes are serialized via the
// database write lock. However, when allGone is returned true, all
// previously held iterators, cursors, and references are invalidated.
func (db *FlexDB) DeleteRange(includeLarge bool, begKey, endKey []byte, begInclusive, endInclusive bool) (n int64, allGone bool, err error) {
	cmp := bytes.Compare(begKey, endKey)
	if cmp > 0 {
		return 0, false, fmt.Errorf("yogadb: DeleteRange: begKey > endKey")
	}
	// Equal keys with both exclusive means empty range.
	if cmp == 0 && (!begInclusive || !endInclusive) {
		return 0, false, nil
	}

	db.topMutRW.Lock()
	defer db.topMutRW.Unlock()

	// Fast path: if the range covers every key in the DB and we're
	// including large values, reinitialize instead of iterating.
	// When !includeLarge, large-value keys survive so we can't wipe.
	if includeLarge && db.writeLockHeldCoversAllKeys(begKey, endKey, begInclusive, endInclusive) {
		err := db.writeLockHeldDeleteAll()
		return 0, true, err
	}

	// Phase 1: Tombstone all non-tombstone keys in range in both memtables.
	for _, mtIdx := range []int{db.activeMT, 1 - db.activeMT} {
		if db.memtables[mtIdx].empty {
			continue
		}
		// Collect keys first since writeLockHeldPut mutates the active memtable.
		var keys [][]byte
		db.memtables[mtIdx].bt.Ascend(KV{Key: begKey}, func(item KV) bool {
			if !deleteRangeInBounds(item.Key, begKey, endKey, begInclusive, endInclusive) {
				// Past endKey — stop iteration.
				if deleteRangePastEnd(item.Key, endKey, endInclusive) {
					return false
				}
				// Before begKey (exclusive match) — skip but continue.
				return true
			}
			if !item.isTombstone() {
				if !includeLarge && item.HasVPtr {
					return true // skip large-value keys
				}
				keys = append(keys, append([]byte{}, item.Key...))
			}
			return true
		})
		for _, key := range keys {
			if err := db.writeLockHeldPut(key, nil); err != nil {
				return n, false, err
			}
			n++
		}
	}

	// Phase 2: Walk FlexSpace sparse index directly, decode intervals
	// without cache, and tombstone every non-tombstone key in range.
	n2, err := db.deleteRangeFlexSpace(begKey, endKey, begInclusive, endInclusive, includeLarge)
	n += n2
	return n, false, err
}

// Clear deletes all keys in the database.
//
// When includeLarge is true, the entire database is wiped and
// re-initialized (fast path). When false, only keys with inline
// (small) values are deleted; keys with large values stored in the
// VLOG survive.
//
// Returns allGone=true when the database was re-initialized. In that
// case, ALL previously held iterators, cursors, and pointers into the
// database are invalid and must be re-acquired.
//
// Goroutine safe. Acquires the database write lock for the
// duration of the call, serializing against all other operations.
func (db *FlexDB) Clear(includeLarge bool) (allGone bool, err error) {
	db.topMutRW.Lock()
	defer db.topMutRW.Unlock()

	if includeLarge {
		err := db.writeLockHeldDeleteAll()
		return true, err
	}

	// !includeLarge: must iterate and tombstone only small-value keys.
	// Use the same machinery as DeleteRange but with maximal bounds.
	// Unlock first since deleteRangeSlow acquires its own lock
	// — actually, we already hold the lock. Call the internal helpers directly.

	// Phase 1: Tombstone small-value keys in both memtables.
	for _, mtIdx := range []int{db.activeMT, 1 - db.activeMT} {
		if db.memtables[mtIdx].empty {
			continue
		}
		var keys [][]byte
		db.memtables[mtIdx].bt.Scan(func(item KV) bool {
			if !item.isTombstone() && !item.HasVPtr {
				keys = append(keys, append([]byte{}, item.Key...))
			}
			return true
		})
		for _, key := range keys {
			if err := db.writeLockHeldPut(key, nil); err != nil {
				return false, err
			}
		}
	}

	// Phase 2: Walk FlexSpace and tombstone small-value keys.
	// Use nil begKey/endKey sentinels to cover entire range.
	_, err = db.deleteRangeFlexSpaceClearSmall()
	return false, err
}

// writeLockHeldCoversAllKeys returns true if the given range covers every
// key in the database (memtables + FlexSpace). When true, the caller can
// use the fast "delete all" path instead of iterating.
//
// The check is conservative: it finds the actual min and max keys across
// all sources and verifies they fall within the range. If the DB is empty,
// returns true (nothing to delete, reinit is a no-op).
//
// Caller must hold topMutRW.Lock().
func (db *FlexDB) writeLockHeldCoversAllKeys(begKey, endKey []byte, begInclusive, endInclusive bool) bool {
	inBounds := func(key []byte) bool {
		return deleteRangeInBounds(key, begKey, endKey, begInclusive, endInclusive)
	}

	// Check memtable min/max keys.
	for i := 0; i < 2; i++ {
		if db.memtables[i].empty {
			continue
		}
		// Min key (first in ascending order).
		var minKV KV
		var minFound bool
		db.memtables[i].bt.Scan(func(item KV) bool {
			minKV = item
			minFound = true
			return false
		})
		if minFound && !inBounds(minKV.Key) {
			return false
		}
		// Max key (first in descending order).
		var maxKV KV
		var maxFound bool
		db.memtables[i].bt.Reverse(func(item KV) bool {
			maxKV = item
			maxFound = true
			return false
		})
		if maxFound && !inBounds(maxKV.Key) {
			return false
		}
	}

	// Check FlexSpace. We need the actual last key, not just the last
	// anchor key (anchors store the first key of each interval).
	t := db.tree
	if t == nil || t.root == nil || t.leafHead == nil {
		return true // empty FlexSpace
	}

	// First anchor with a real key gives us the minimum FlexSpace key.
	node := t.leafHead
	firstKeyChecked := false
	for node != nil && !firstKeyChecked {
		for i := 0; i < node.count; i++ {
			a := node.anchors[i]
			if a != nil && a.key != nil { // skip nil-key sentinel
				if !inBounds(a.key) {
					return false
				}
				firstKeyChecked = true
				break
			}
		}
		if !firstKeyChecked {
			node = node.next
		}
	}

	// Find the last interval and decode it to get the actual last key.
	lastNode := t.leafHead
	for lastNode.next != nil {
		lastNode = lastNode.next
	}
	// Walk backward through the last leaf's anchors to find the last non-empty interval.
	for i := lastNode.count - 1; i >= 0; i-- {
		a := lastNode.anchors[i]
		if a == nil || a.psize == 0 {
			continue
		}
		nh := memSparseIndexTreeHandler{node: lastNode}
		memSparseIndexTreeHandlerInfoUpdate(&nh)
		kvs, err := db.decodeIntervalDirect(a, uint64(a.loff+nh.shift))
		if err != nil || len(kvs) == 0 {
			continue
		}
		lastKey := kvs[len(kvs)-1].Key
		if !inBounds(lastKey) {
			return false
		}
		break
	}

	return true
}

// writeLockHeldDeleteAll reinitializes the database, discarding all data.
// This is the fast path for DeleteRange when the range covers all keys.
// Stops the flush worker, closes FlexSpace, truncates all data files,
// reopens FlexSpace, and restarts the flush worker.
//
// Caller must hold topMutRW.Lock().
func (db *FlexDB) writeLockHeldDeleteAll() error {
	// 1. Stop flush worker.
	close(db.flushStop)
	// Temporarily drop the lock so the flush worker (which also needs
	// topMutRW) can exit if it's blocked waiting for it.
	db.topMutRW.Unlock()
	db.flushWG.Wait()
	db.topMutRW.Lock()

	// 2. Clear both memtables.
	for i := 0; i < 2; i++ {
		db.memtables[i].bt.Clear()
		db.memtables[i].empty = true
		db.memtables[i].size = 0
	}
	db.activeMT = 0

	// 3. Destroy interval cache.
	db.cache.destroyAll()

	// 4. Close FlexSpace (closes KV128_BLOCKS, REDO.LOG, CoW files).
	db.ff.Close()

	// 5. Remove FlexSpace data files and reopen fresh.
	fs := db.vfs
	path := db.Path
	filesToRemove := []string{
		"FLEXSPACE.KV128_BLOCKS",
		"FLEXSPACE.KV128_BLOCKS.vacuum",
		"FLEXSPACE.REDO.LOG",
		"FLEXTREE.PAGES",
		"FLEXTREE.COMMIT",
	}
	for _, name := range filesToRemove {
		fs.Remove(filepath.Join(path, name))
	}

	// Truncate VLOG if present.
	if db.vlog != nil {
		db.vlog.sync()
		db.vlog.close()
		fs.Remove(filepath.Join(path, "LARGE.VLOG"))
		vl, err := openValueLog(filepath.Join(path, "LARGE.VLOG"), fs)
		if err != nil {
			return fmt.Errorf("yogadb: DeleteAll: reopen VLOG: %w", err)
		}
		db.vlog = vl
	}

	ff, err := OpenFlexSpaceCoW(path, db.cfg.OmitFlexSpaceOpsRedoLog, fs)
	if err != nil {
		return fmt.Errorf("yogadb: DeleteAll: reopen FlexSpace: %w", err)
	}
	db.ff = ff

	// 6. Reinitialize sparse index tree and cache.
	db.tree = memSparseIndexTreeCreate()
	db.cache = newCache(nil, db.cfg.CacheMB)
	db.cache.db = db
	for i := range db.cache.partitions {
		db.cache.partitions[i].db = db
	}

	// 7. Reset counters.
	db.totalLogicalBase = 0
	db.totalPhysicalBase = 0
	atomic.StoreInt64(&db.LogicalBytesWritten, 0)
	atomic.StoreInt64(&db.MemWALBytesWritten, 0)
	db.liveKeys = 0
	db.liveBigKeys = 0
	db.liveSmallKeys = 0

	// 8. Truncate WAL files.
	ts := uint64(time.Now().UnixNano())
	db.memtables[0].logTruncateWithVersion(ts, db.ff.tree.PersistentVersion)
	ts2 := uint64(time.Now().UnixNano())
	db.memtables[1].logTruncateWithVersion(ts2, db.ff.tree.PersistentVersion)

	// 9. Restart flush worker.
	db.flushStop = make(chan struct{})
	db.flushTrigger = make(chan struct{}, 1)
	db.flushWG.Add(1)
	go db.flushWorker()

	return nil
}

// deleteRangeInBounds returns true if key is within the range defined by
// [begKey, endKey] with the given inclusivity flags.
func deleteRangeInBounds(key, begKey, endKey []byte, begInclusive, endInclusive bool) bool {
	cmpBeg := bytes.Compare(key, begKey)
	if begInclusive {
		if cmpBeg < 0 {
			return false
		}
	} else {
		if cmpBeg <= 0 {
			return false
		}
	}
	cmpEnd := bytes.Compare(key, endKey)
	if endInclusive {
		if cmpEnd > 0 {
			return false
		}
	} else {
		if cmpEnd >= 0 {
			return false
		}
	}
	return true
}

// deleteRangePastEnd returns true if key is beyond the end bound.
func deleteRangePastEnd(key, endKey []byte, endInclusive bool) bool {
	cmp := bytes.Compare(key, endKey)
	if endInclusive {
		return cmp > 0
	}
	return cmp >= 0
}

// deleteRangeFlexSpace walks the sparse index tree's leaf linked list,
// decodes each interval directly from FlexSpace (bypassing the interval
// cache to avoid pollution), and writes tombstones for all non-tombstone
// keys within the specified bounds.
//
// On memtable flush (detected by activeMT flip), re-seeks from the last
// processed key in the rebuilt sparse index tree.
//
// Caller must hold topMutRW.Lock().
func (db *FlexDB) deleteRangeFlexSpace(begKey, endKey []byte, begInclusive, endInclusive, includeLarge bool) (int64, error) {
	var n int64
	target := begKey
	// On first seek, whether we include target depends on begInclusive.
	// After a flush re-seek, we always use strict=true (skip the last processed key).
	seekStrict := !begInclusive

	for {
		t := db.tree
		if t == nil || t.root == nil {
			return n, nil
		}

		var nh memSparseIndexTreeHandler
		t.findAnchorPos(target, &nh)
		node := nh.node
		anchorIdx := nh.idx
		shift := nh.shift

		if node == nil || node.count == 0 {
			return n, nil
		}

		flushed := false
		for !flushed {
			if anchorIdx >= node.count {
				next := node.next
				if next == nil {
					return n, nil
				}
				node = next
				anchorIdx = 0
				nh2 := memSparseIndexTreeHandler{node: node}
				memSparseIndexTreeHandlerInfoUpdate(&nh2)
				shift = nh2.shift
			}

			anchor := node.anchors[anchorIdx]
			if anchor == nil || anchor.psize == 0 {
				anchorIdx++
				continue
			}

			// Early exit: if anchor's first key is past end bound, we're done.
			if anchor.key != nil && deleteRangePastEnd(anchor.key, endKey, endInclusive) {
				return n, nil
			}

			// Decode interval directly from FlexSpace (no cache).
			kvs, err := db.decodeIntervalDirect(anchor, uint64(anchor.loff+shift))
			if err != nil {
				anchorIdx++
				continue
			}

			// Process each KV in this interval.
			for _, kv := range kvs {
				// Skip keys before our current seek position.
				cmpTarget := bytes.Compare(kv.Key, target)
				if seekStrict {
					if cmpTarget <= 0 {
						continue
					}
				} else {
					if cmpTarget < 0 {
						continue
					}
				}
				// Check end bound.
				if deleteRangePastEnd(kv.Key, endKey, endInclusive) {
					return n, nil
				}
				if kv.isTombstone() {
					continue
				}
				if !includeLarge && kv.HasVPtr {
					continue // skip large-value keys
				}

				// Write tombstone. Track activeMT to detect flush.
				prevActive := db.activeMT
				if err := db.writeLockHeldPut(kv.Key, nil); err != nil {
					return n, err
				}
				n++

				if db.activeMT != prevActive {
					// Memtable flushed — sparse index tree was rebuilt.
					// Re-seek strictly past this key in the new tree.
					target = kv.Key
					seekStrict = true
					flushed = true
					break
				}
			}

			if !flushed {
				anchorIdx++
			}
		}
		// Loop back to re-seek in the new tree after flush.
	}
}

// deleteRangeFlexSpaceClearSmall walks all FlexSpace intervals and
// tombstones every non-tombstone, non-large-value key. Used by
// Clear(includeLarge=false). No bounds checking needed since we
// cover the entire keyspace.
//
// Caller must hold topMutRW.Lock().
func (db *FlexDB) deleteRangeFlexSpaceClearSmall() (int64, error) {
	var n int64
	var target []byte
	seekStrict := false

	for {
		t := db.tree
		if t == nil || t.root == nil || t.leafHead == nil {
			return n, nil
		}

		// Start from leafHead (first leaf) or re-seek after flush.
		var node *memSparseIndexTreeNode
		var anchorIdx int
		var shift int64

		if target == nil && !seekStrict {
			// First iteration: start from the beginning.
			node = t.leafHead
			anchorIdx = 0
			nh := memSparseIndexTreeHandler{node: node}
			memSparseIndexTreeHandlerInfoUpdate(&nh)
			shift = nh.shift
		} else {
			// Re-seek after flush.
			var nh memSparseIndexTreeHandler
			t.findAnchorPos(target, &nh)
			node = nh.node
			anchorIdx = nh.idx
			shift = nh.shift
		}

		if node == nil || node.count == 0 {
			return n, nil
		}

		flushed := false
		for !flushed {
			if anchorIdx >= node.count {
				next := node.next
				if next == nil {
					return n, nil
				}
				node = next
				anchorIdx = 0
				nh2 := memSparseIndexTreeHandler{node: node}
				memSparseIndexTreeHandlerInfoUpdate(&nh2)
				shift = nh2.shift
			}

			anchor := node.anchors[anchorIdx]
			if anchor == nil || anchor.psize == 0 {
				anchorIdx++
				continue
			}

			kvs, err := db.decodeIntervalDirect(anchor, uint64(anchor.loff+shift))
			if err != nil {
				anchorIdx++
				continue
			}

			for _, kv := range kvs {
				if target != nil {
					cmp := bytes.Compare(kv.Key, target)
					if seekStrict && cmp <= 0 {
						continue
					}
					if !seekStrict && cmp < 0 {
						continue
					}
				}
				if kv.isTombstone() || kv.HasVPtr {
					continue // skip tombstones and large-value keys
				}

				prevActive := db.activeMT
				if err := db.writeLockHeldPut(kv.Key, nil); err != nil {
					return n, err
				}
				n++

				if db.activeMT != prevActive {
					target = kv.Key
					seekStrict = true
					flushed = true
					break
				}
			}

			if !flushed {
				anchorIdx++
			}
		}
	}
}

// decodeIntervalDirect reads and decodes an interval from FlexSpace
// without using the interval cache. Returns the decoded KV slice.
func (db *FlexDB) decodeIntervalDirect(anchor *dbAnchor, anchorLoff uint64) ([]KV, error) {
	if anchor.psize == 0 {
		return nil, nil
	}
	buf := make([]byte, anchor.psize)
	n, _, err := db.ff.ReadFragmentation(buf, anchorLoff, uint64(anchor.psize))
	if err != nil || n != int(anchor.psize) {
		return nil, fmt.Errorf("decodeIntervalDirect: read error: %w", err)
	}

	var kvs []KV
	src := buf
	if slottedPageIsSlotted(src) {
		decoded, consumed, err := slottedPageDecode(src)
		if err == nil {
			kvs = append(kvs, decoded...)
			src = src[consumed:]
		} else {
			src = nil
		}
	}
	for len(src) > 0 {
		kv, size, ok := kv128Decode(src)
		if !ok {
			break
		}
		kvs = append(kvs, kv)
		src = src[size:]
	}

	if anchor.unsorted > 0 && len(kvs) > 1 {
		sort.SliceStable(kvs, func(i, j int) bool {
			return kvLess(kvs[i], kvs[j])
		})
		kvs = deleteRangeDedup(kvs)
	}
	return kvs, nil
}

// deleteRangeDedup deduplicates a sorted KV slice, keeping the highest-HLC
// entry for each key. Simpler than intervalCacheDedup since we don't need
// fingerprints or size tracking.
func deleteRangeDedup(kvs []KV) []KV {
	out := kvs[:0]
	i := 0
	for i < len(kvs) {
		best := i
		j := i + 1
		for j < len(kvs) && bytes.Equal(kvs[i].Key, kvs[j].Key) {
			if kvs[j].Hlc > kvs[best].Hlc {
				best = j
			}
			j++
		}
		out = append(out, kvs[best])
		i = j
	}
	return out
}

// Merge performs an atomic read-modify-write on key.
// The merge function fn receives the old value (nil if not found) and
// whether the key existed. It returns the new value to write and whether
// to perform the write. If write is false, the merge is a no-op.
// If newVal is nil and write is true, a tombstone (delete) is written.
//
// This mirrors C's flexdb_merge: it looks up the old value across all
// layers (active memtable, inactive memtable, FlexSpace), applies the
// user function, and writes the result atomically.
func (db *FlexDB) Merge(key []byte, fn func(oldVal []byte, exists bool) (newVal []byte, write bool)) error {
	db.topMutRW.Lock()
	defer db.topMutRW.Unlock()

	if len(key)+16 >= MaxKeySize {
		return fmt.Errorf("flexdb: key too large for merge (max %d bytes)", MaxKeySize)
	}

	// Phase 1: check active memtable.
	active := db.activeMT
	var oldVal []byte
	var exists bool

	if !db.memtables[active].empty {
		kv, ok := db.memtables[active].get(key)
		if ok {
			if !kv.isTombstone() {
				val, err := db.resolveVPtr(kv)
				if err == nil {
					oldVal = val
					exists = true
				}
			}
		}
	}

	if !exists {
		// Phase 2: check inactive memtable.
		inactive := 1 - active
		if !db.memtables[inactive].empty {
			kv, ok := db.memtables[inactive].get(key)
			if ok {
				if !kv.isTombstone() {
					val, err := db.resolveVPtr(kv)
					if err == nil {
						oldVal = val
						exists = true
					}
				}
			}
		}
	}

	if !exists {
		// Phase 3: check FlexSpace (getPassthrough already resolves VPtrs).
		val, found := db.getPassthrough(key)
		if found {
			oldVal = val
			exists = true
		}
	}

	// Apply user merge function.
	newVal, write := fn(oldVal, exists)
	if !write {
		return nil
	}

	// Validate size (only key limit applies when VLOG is enabled).
	if db.vlog == nil && len(key)+len(newVal)+16 >= MaxKeySize {
		return fmt.Errorf("flexdb: merged KV too large (max %d bytes)", MaxKeySize)
	}

	// Write result via Put.
	return db.writeLockHeldPut(key, newVal)
}

// ====================== Passthrough operations ======================
// These operate directly on FlexSpace + sparse index.
// Caller must hold db.topMutRW. but is RLock sufficient? should be since we change nothing.

func (db *FlexDB) getPassthrough(key []byte) ([]byte, bool) {
	var nh memSparseIndexTreeHandler
	db.tree.findAnchorPos(key, &nh)
	anchor := nh.node.anchors[nh.idx]
	anchorLoff := uint64(anchor.loff + nh.shift)
	partition := db.cache.getPartition(anchor)
	fce := partition.getEntry(anchor, anchorLoff, db)
	defer partition.releaseEntry(fce)

	idx, ok := intervalCacheEntryFindKeyEQ(fce, key)
	if !ok {
		return nil, false
	}
	kv := fce.kvs[idx]
	if kv.isTombstone() {
		return nil, false
	}
	val, err := db.resolveVPtr(kv)
	if err != nil {
		return nil, false
	}
	out := make([]byte, len(val))
	copy(out, val)
	return out, true
}

// getPassthroughKV returns the full KV (including HLC) from the passthrough layer.
func (db *FlexDB) getPassthroughKV(key []byte) (KV, bool) {
	var nh memSparseIndexTreeHandler
	db.tree.findAnchorPos(key, &nh)
	anchor := nh.node.anchors[nh.idx]
	anchorLoff := uint64(anchor.loff + nh.shift)
	partition := db.cache.getPartition(anchor)
	fce := partition.getEntry(anchor, anchorLoff, db)
	defer partition.releaseEntry(fce)

	idx, ok := intervalCacheEntryFindKeyEQ(fce, key)
	if !ok {
		return KV{}, false
	}
	return fce.kvs[idx], true
}

func (db *FlexDB) putPassthrough(kv KV, nh *memSparseIndexTreeHandler) {
	db.tree.treeNodeHandlerNextAnchor(nh, kv.Key)
	anchor := nh.node.anchors[nh.idx]
	anchorLoff := uint64(anchor.loff + nh.shift)
	partition := db.cache.getPartition(anchor)

	// Always load cache — no unsorted kv128 append path.
	fce := partition.getEntry(anchor, anchorLoff, db)

	// First write to this anchor: allocate a fixed-size page via Insert.
	if anchor.psize == 0 {
		db.putPassthroughInitial(kv, nh, anchor, partition, fce)
	} else {
		db.putPassthroughR(kv, nh, anchor, partition, fce)
	}
	if fce.count >= flexdbSparseIntervalCount {
		db.treeInsertAnchor(nh, partition, fce)
	}
	partition.releaseEntry(fce)
}

// putPassthroughInitial handles the first write to an anchor: allocates a
// fixed-size slottedPageMaxSize page via ff.Insert and populates the cache.
func (db *FlexDB) putPassthroughInitial(kv KV, nh *memSparseIndexTreeHandler, anchor *dbAnchor, partition *intervalCachePartition, fce *intervalCacheEntry) {
	anchorLoff := uint64(anchor.loff + nh.shift)

	// Insert into cache.
	idx, eq := intervalCacheEntryFindKeyGE(fce, kv.Key)
	if eq {
		partition.cacheEntryReplace(fce, kv, idx)
	} else {
		partition.cacheEntryInsert(fce, kv, idx)
	}

	// Encode as fixed-size padded page and insert.
	buf := slottedPageEncodePadded(fce.kvs[:fce.count], slottedPageMaxSize)
	psize := uint32(len(buf))

	db.ff.Insert(buf, anchorLoff, uint64(psize))
	nh.shiftUpPropagate(int64(psize))
	anchor.psize = psize
	anchor.unsorted = 0

	tag := flexdbTagGenerate(true, 0)
	if err := db.ff.SetTag(anchorLoff, tag); err != nil {
		panicf("putPassthroughInitial: SetTag anchorLoff=%d: %v", anchorLoff, err)
	}

	if nh.node.parent != nil {
		memSparseIndexTreeNodeRebase(nh.node)
	}
}

func (db *FlexDB) putPassthroughR(kv KV, nh *memSparseIndexTreeHandler, anchor *dbAnchor, partition *intervalCachePartition, fce *intervalCacheEntry) {
	idx, eq := intervalCacheEntryFindKeyGE(fce, kv.Key)

	// Check if the new KV would fit in the fixed-size page.
	replaceIdx := -1
	if eq {
		replaceIdx = idx
	}
	if !slottedPageWouldFit(fce.kvs, fce.count, kv, replaceIdx, int(anchor.psize)) {
		// Page full — split first, then retry on the correct half.
		// Insert into cache first so split sees the new entry.
		if eq {
			partition.cacheEntryReplace(fce, kv, idx)
		} else {
			partition.cacheEntryInsert(fce, kv, idx)
		}
		db.treeInsertAnchor(nh, partition, fce)
		// After split, fce is the left half. Mark dirty.
		db.putPassthroughMarkDirty(nh, anchor, fce)
		return
	}

	// Update cache entry.
	if eq {
		partition.cacheEntryReplace(fce, kv, idx)
	} else {
		partition.cacheEntryInsert(fce, kv, idx)
	}

	// Mark dirty — will be written to disk on Sync or eviction.
	db.putPassthroughMarkDirty(nh, anchor, fce)
}

// putPassthroughMarkDirty marks fce as dirty so it will be written to disk
// on Sync or cache eviction. No disk I/O, no CRC computation.
func (db *FlexDB) putPassthroughMarkDirty(nh *memSparseIndexTreeHandler, anchor *dbAnchor, fce *intervalCacheEntry) {
	fce.dirty = true
	fce.dirtyNode = nh.node
	anchor.unsorted = 0
}

func (db *FlexDB) treeInsertAnchor(nh *memSparseIndexTreeHandler, partition *intervalCachePartition, fce *intervalCacheEntry) {
	anchor := nh.node.anchors[nh.idx]
	anchorLoff := uint64(anchor.loff + nh.shift)

	count := fce.count
	rightCount := count / 2
	leftCount := count - rightCount

	// Left half: mark dirty (will be written on Sync/eviction).
	// If psize changed (shouldn't with fixed pages), do a real Update.
	if anchor.psize != uint32(slottedPageMaxSize) {
		leftBuf := slottedPageEncodePadded(fce.kvs[:leftCount], slottedPageMaxSize)
		leftPSize := uint32(len(leftBuf))
		db.ff.Update(leftBuf, anchorLoff, uint64(leftPSize), uint64(anchor.psize))
		if leftPSize != anchor.psize {
			nh.shiftUpPropagate(int64(leftPSize) - int64(anchor.psize))
		}
		anchor.psize = leftPSize
	}
	// Left fce will be marked dirty by caller (putPassthroughMarkDirty or
	// putPassthroughR's split path). Content written on flush.

	// Right half: allocate new fixed-size page via Insert (structural change).
	rightBuf := slottedPageEncodePadded(fce.kvs[leftCount:fce.count], slottedPageMaxSize)
	rightPSize := uint32(len(rightBuf))
	newAnchorLoff := anchorLoff + uint64(anchor.psize)
	db.ff.Insert(rightBuf, newAnchorLoff, uint64(rightPSize))
	nh.shiftUpPropagate(int64(rightPSize))

	// Compute left/right sizes for cache.
	leftSize := 0
	for i := 0; i < leftCount; i++ {
		leftSize += kvSizeApprox(fce.kvs[i])
	}

	newAnchorKey := dupBytes(fce.kvs[leftCount].Key)
	nh.idx++
	newAnchor := nh.handlerInsert(newAnchorKey, newAnchorLoff, rightPSize)
	nh.idx--

	newPartition := db.cache.getPartition(newAnchor)
	newFce := newPartition.allocEntryForNewAnchor(newAnchor)

	rightSize := fce.size - leftSize
	newFce.kvs = make([]KV, rightCount)
	newFce.fps = make([]uint16, rightCount)
	copy(newFce.kvs, fce.kvs[leftCount:fce.count])
	copy(newFce.fps, fce.fps[leftCount:fce.count])
	newFce.count = rightCount
	newFce.size = rightSize
	newFce.frag = fce.frag

	if partition != newPartition {
		partition.mu.Lock()
		partition.size -= int64(rightSize)
		partition.mu.Unlock()
		newPartition.mu.Lock()
		newPartition.size += int64(rightSize)
		newPartition.mu.Unlock()
	}

	// Update left fce
	fce.kvs = fce.kvs[:leftCount]
	fce.fps = fce.fps[:leftCount]
	fce.count = leftCount
	fce.size = leftSize

	newPartition.releaseEntry(newFce)

	// Tag both anchors in FlexSpace
	tag := flexdbTagGenerate(true, 0)
	if err := db.ff.SetTag(anchorLoff, tag); err != nil {
		panicf("treeInsertAnchor: SetTag left anchorLoff=%d: %v", anchorLoff, err)
	}
	if err := db.ff.SetTag(newAnchorLoff, tag); err != nil {
		panicf("treeInsertAnchor: SetTag right newAnchorLoff=%d: %v", newAnchorLoff, err)
	}
}

// verifyAnchorTags walks the sparse index tree and verifies that every anchor
// with psize>0 has a matching tag in the FlexTree. This is a diagnostic tool
// to find where tags go missing (causing psize=2*slottedPageMaxSize on recovery).
func (db *FlexDB) verifyAnchorTags() {
	tree := db.tree
	if tree == nil {
		return
	}
	leaf := tree.leafHead
	anchorIdx := 0
	ffSize := db.ff.Size()
	for leaf != nil {
		// Compute shift for this leaf.
		shift := int64(0)
		n := leaf
		for n.parent != nil {
			shift += n.parent.children[n.parentID].shift
			n = n.parent
		}
		for i := 0; i < leaf.count; i++ {
			anchor := leaf.anchors[i]
			if anchor == nil {
				continue
			}
			absLoff := uint64(anchor.loff + shift)
			if anchor.psize == 0 {
				anchorIdx++
				continue
			}
			tag, err := db.ff.GetTag(absLoff)
			if err != nil || !flexdbTagIsAnchor(tag) {
				// Tag missing! Dump diagnostic info.
				alwaysPrintf("VERIFY_ANCHOR_TAGS FAIL: anchorIdx=%d absLoff=%d psize=%d key=%q tag=%d err=%v ffSize=%d",
					anchorIdx, absLoff, anchor.psize, anchor.key, tag, err, ffSize)
				// Also check what extent is at this loff.
				fp := db.ff.tree.PosGet(absLoff)
				if fp.Valid() {
					ext := &fp.node.Extents[fp.Idx]
					alwaysPrintf("  extent at loff: Loff=%d Len=%d Tag=%d Poff=%d Diff=%d",
						ext.Loff, ext.Len, ext.Tag(), ext.Poff(), fp.Diff)
				} else {
					alwaysPrintf("  no extent at absLoff=%d (maxLoff=%d)", absLoff, db.ff.tree.MaxLoff)
				}
				panicf("verifyAnchorTags: anchor %d at absLoff=%d has no tag (psize=%d key=%q)",
					anchorIdx, absLoff, anchor.psize, anchor.key)
			}
			anchorIdx++
		}
		leaf = leaf.next
	}
}

/*
	====================== Recovery ======================

recovery: on startup after a power off/crash,

memSparseIndexTree is rebuilt on recovery from tags. But what are tags?

"Tags" are 16-bit metadata values stored inside FlexTree extents --
specifically the lower 16 bits of each extent's TagPoff uint64 field.

What a tag encodes:

In flextree.go:384, the tag layout is (in the TagPoff bit-packed uint64 field):

┌────────┬───────────────────────────────────────────────────────────────────┐
│ Bit(s) │                              Meaning                              │
├────────┼───────────────────────────────────────────────────────────────────┤
│ 0      │ Anchor flag — 1 = this extent starts a sparse index interval      │
├────────┼───────────────────────────────────────────────────────────────────┤
│ 1–7    │ Unsorted count — number of unsorted KVs appended to this interval │
├────────┼───────────────────────────────────────────────────────────────────┤
│ 8–15   │ Reserved                                                          │
└────────┴───────────────────────────────────────────────────────────────────┘

Generated by flexdbTagGenerate(isAnchor bool, unsorted uint8) uint16 in db.go.

# Where tags are written

During flush (when MemTable is flushed to FlexSpace), FlexDB calls
ff.SetTag(loff, tag) to stamp each anchor extent. For example:

- New anchor after split: flexdbTagGenerate(true, 0)
- Unsorted append: flexdbTagGenerate(true, anchor.unsorted) with incremented unsorted count
- Non-anchor data: tag = 0 (anchor bit clear)

# Where the rebuild happens

db.go:recovery() (called from OpenFlexDB() when FlexSpace has existing data):

1. Creates a FlexSpaceHandler at loff=0
2. Walks every extent sequentially via fh.ForwardExtent()
3. For each extent, calls fh.GetTag() — if flexdbTagIsAnchor(tag) is true:
  - Records loff, the anchor key (read from FlexSpace), and unsorted count

4. Inserts all collected anchors into a fresh memSparseIndexTree in order
5. Computes each anchor's psize as the gap between consecutive anchor loffs
6. Then replays WAL logs to restore any unflushed transactions

So the tags are a lightweight out-of-band marking mechanism: the 16-bit tag
field in each FlexTree extent is large enough to carry the anchor/unsorted metadata,
and a linear scan of all extents is sufficient to reconstruct the entire sparse
index tree from scratch on every open.

Q: How do we know there are only at most 7 bits (128) worh of unsorted KVs

	appended to the interval? Why cannot there be more, or what invariant says
	that we cannot overflow the unsorted count in the 7 bits of the Tag?

A: Here's the chain of invariants:

	The quota is flexdbUnsortedWriteQuota = 15. That's the cap, and it's enforced
	by the write path itself — not by the tag bit-width.

	The flow for every unsorted write (putPassthrough -> putPassthroughUnsorted) is:

	1. getEntryUnsorted() checks anchor.unsorted >= flexdbUnsortedWriteQuota (i.e., >= 15)
	2. If the quota is hit, it forces a cache load (fce != nil), which
	   routes into putPassthroughR
	3. putPassthroughR re-encodes the full interval as a sorted slotted page,
	   replaces it in-place via ff.Update, and resets anchor.unsorted = 0

	So the sequence is:
	- Unsorted writes 1–14: take the fce == nil path -> call putPassthroughUnsorted ->
	  increment anchor.unsorted -> append blindly as kv128
	- Unsorted write 15: getEntryUnsorted sees unsorted >= 15 -> forces a load ->
	  putPassthroughR re-encodes sorted -> resets unsorted = 0

	The unsorted count can never exceed 15 because the re-encode is forced
	before the 16th unsorted append can happen. With a max of 15, only 4 bits are
	actually needed. The 7 bits in the tag (max 127) have ample headroom — the operational
	invariant (flexdbUnsortedWriteQuota = 15) is far below the representational
	limit (0x7f = 127).

	The tag bit-width is not the safety mechanism. The quota check in getEntryUnsorted is.
*/
// clampAnchorPsizes walks the sparse index tree and fixes any anchor
// whose psize exceeds slottedPageMaxSize. This handles the case where
// recovery computed psize from tag distances and a tag was missing
// (e.g. a split's right-half tag was lost). The over-sized anchor
// actually contains multiple slottedPageMaxSize pages; we split them
// into separate anchors by reading the first key of each sub-page.
func (db *FlexDB) clampAnchorPsizes() {
	leaf := db.tree.leafHead
	for leaf != nil {
		for i := 0; i < leaf.count; i++ {
			anchor := leaf.anchors[i]
			if anchor == nil || anchor.psize <= uint32(slottedPageMaxSize) {
				continue
			}

			// Compute absolute loff for this anchor.
			shift := int64(0)
			n := leaf
			for n.parent != nil {
				shift += n.parent.children[n.parentID].shift
				n = n.parent
			}
			absLoff := uint64(anchor.loff + shift)

			alwaysPrintf("clampAnchorPsizes: anchor key=%q loff=%d absLoff=%d psize=%d > slottedPageMaxSize=%d; splitting into sub-anchors",
				anchor.key, anchor.loff, absLoff, anchor.psize, slottedPageMaxSize)

			// Read the over-sized interval and split into slottedPageMaxSize chunks.
			remaining := uint64(anchor.psize)
			subLoff := absLoff + uint64(slottedPageMaxSize) // skip first page (current anchor)
			remaining -= uint64(slottedPageMaxSize)
			anchor.psize = uint32(slottedPageMaxSize)

			var nh memSparseIndexTreeHandler
			for remaining >= uint64(slottedPageMaxSize) {
				// Read first key of this sub-page.
				buf := make([]byte, slottedPageMaxSize)
				nn, err := db.ff.Read(buf, subLoff, uint64(slottedPageMaxSize))
				if err != nil || nn != slottedPageMaxSize {
					alwaysPrintf("clampAnchorPsizes: read at loff=%d failed: n=%d err=%v; stopping", subLoff, nn, err)
					break
				}
				subKey, ok := slottedPageFirstKey(buf)
				if !ok {
					alwaysPrintf("clampAnchorPsizes: no first key at loff=%d; stopping", subLoff)
					break
				}

				// Insert a new anchor for this sub-page.
				db.tree.findAnchorPos(subKey, &nh)
				nh.idx++
				newAnchor := nh.handlerInsert(dupBytes(subKey), subLoff, uint32(slottedPageMaxSize))
				_ = newAnchor
				nh.idx--

				// Set the tag on this extent.
				tag := flexdbTagGenerate(true, 0)
				db.ff.SetTag(subLoff, tag)

				subLoff += uint64(slottedPageMaxSize)
				remaining -= uint64(slottedPageMaxSize)
			}

			// Re-scan this leaf since we may have inserted anchors.
			// Just restart from the beginning of this leaf.
			i = -1 // will be incremented to 0
		}
		leaf = leaf.next
	}
}

func (db *FlexDB) recovery() {
	db.topMutRW.Lock()
	defer db.topMutRW.Unlock()

	type anchorInfo struct {
		key      []byte
		loff     uint64
		unsorted uint8
	}

	ffSize := db.ff.Size()
	if ffSize == 0 {
		return
	}

	var anchors []anchorInfo
	kvbuf := make([]byte, MaxKeySize)
	fh := db.ff.GetHandler(0)

	for fh.Valid() && fh.Loff() < ffSize {
		tag, err := fh.GetTag()
		if err == nil && flexdbTagIsAnchor(tag) {
			loff := fh.Loff()
			unsorted := flexdbTagUnsorted(tag)
			kv, ok := flexdbReadKVFromHandler(fh, kvbuf)
			if ok {
				var anchorKey []byte
				if loff > 0 {
					anchorKey = dupBytes(kv.Key)
				}
				anchors = append(anchors, anchorInfo{key: anchorKey, loff: loff, unsorted: unsorted})
			}
		}
		fh.ForwardExtent()
	}

	// Build sparse index tree from collected anchors (in order)
	var nh memSparseIndexTreeHandler
	db.tree.findAnchorPos(nil, &nh)
	lastAnchorLoff := uint64(0)

	for _, ai := range anchors {
		if ai.loff == 0 {
			nh.node.anchors[nh.idx].unsorted = ai.unsorted
		} else {
			prevAnchor := nh.node.anchors[nh.idx]
			actualPrevLoff := uint64(prevAnchor.loff) + uint64(nh.shift)
			prevAnchor.psize = uint32(ai.loff - actualPrevLoff)

			nh.idx++
			newAnchor := nh.handlerInsert(ai.key, ai.loff, 0)
			newAnchor.unsorted = ai.unsorted
			nh.idx--

			db.tree.findAnchorPos(ai.key, &nh)
		}
		lastAnchorLoff = ai.loff
	}

	// Set last anchor's psize
	if nh.node != nil && nh.idx < nh.node.count {
		last := nh.node.anchors[nh.idx]
		lastPsize := uint32(ffSize - lastAnchorLoff)
		last.psize = lastPsize
		if lastPsize > uint32(slottedPageMaxSize) {
			alwaysPrintf("RECOVERY WARNING: last anchor psize=%d > slottedPageMaxSize=%d (ffSize=%d lastAnchorLoff=%d nAnchors=%d)",
				lastPsize, slottedPageMaxSize, ffSize, lastAnchorLoff, len(anchors))
			// Dump the last few anchors to see where the gap is.
			start := 0
			if len(anchors) > 5 {
				start = len(anchors) - 5
			}
			for j := start; j < len(anchors); j++ {
				alwaysPrintf("  anchor[%d]: loff=%d key=%q unsorted=%d", j, anchors[j].loff, anchors[j].key, anchors[j].unsorted)
			}
			// Walk extents around lastAnchorLoff to see what tags exist.
			alwaysPrintf("  extents near lastAnchorLoff=%d:", lastAnchorLoff)
			scanFh := db.ff.GetHandler(lastAnchorLoff)
			for k := 0; k < 10 && scanFh.Valid() && scanFh.Loff() < ffSize; k++ {
				stag, serr := scanFh.GetTag()
				sLoff := scanFh.Loff()
				alwaysPrintf("    extent loff=%d tag=%d isAnchor=%v err=%v", sLoff, stag, serr == nil && flexdbTagIsAnchor(stag), serr)
				scanFh.ForwardExtent()
			}
		}
	}

	// Replay WAL logs (replay older timestamp first)
	treeVer := db.ff.tree.PersistentVersion
	size1 := db.memtables[0].memWalSize()
	size2 := db.memtables[1].memWalSize()
	hdr1 := db.memtables[0].memWalDataOffset()
	hdr2 := db.memtables[1].memWalDataOffset()

	// Check if each WAL's data was already flushed to the tree
	skip1 := false
	skip2 := false
	if db.ff.omitRedoLog {
		if v := db.memtables[0].logTreeVersion(); v > 0 && v <= treeVer {
			skip1 = true
		}
		if v := db.memtables[1].logTreeVersion(); v > 0 && v <= treeVer {
			skip2 = true
		}
	}

	has1 := size1 > hdr1 && !skip1
	has2 := size2 > hdr2 && !skip2
	if has1 && has2 {
		t1 := db.memtables[0].logTimestamp()
		t2 := db.memtables[1].logTimestamp()
		if t1 > t2 {
			db.logRedo(db.memtables[0].memWalFD, size1)
			db.logRedo(db.memtables[1].memWalFD, size2)
		} else {
			db.logRedo(db.memtables[1].memWalFD, size2)
			db.logRedo(db.memtables[0].memWalFD, size1)
		}
	} else if has1 {
		db.logRedo(db.memtables[0].memWalFD, size1)
	} else if has2 {
		db.logRedo(db.memtables[1].memWalFD, size2)
	}
	db.ff.Sync()
}

// flexdbReadKVFromHandler reads the first KV (key only needed for anchor)
// from a handler's current position (does NOT advance the handler).
// Handles both slotted page and kv128 formats.
func flexdbReadKVFromHandler(fh FlexSpaceHandler, buf []byte) (KV, bool) {
	var header [16]byte
	n, err := fh.Read(header[:], 16)
	if n < 1 || err != nil {
		return KV{}, false
	}

	// Check if this is a slotted page.
	if header[0] == slottedPageMagic {
		// New interleaved format: first entry at offset 12 is
		// [2B keyLen][2B valInfo][varint HLC][keyLen key bytes].
		// We already have 16 bytes in header, which covers header(12) + keyLen(2) + valInfo(2).
		if n < slottedPageHeaderSize+4 {
			return KV{}, false
		}
		keyLen := int(binary.LittleEndian.Uint16(header[slottedPageHeaderSize : slottedPageHeaderSize+2]))
		// Need header(12) + 4B slot + up to 10B varint + keyLen bytes.
		needBytes := slottedPageHeaderSize + 4 + binary.MaxVarintLen64 + keyLen
		if needBytes > len(buf) {
			return KV{}, false
		}
		nr, err2 := fh.Read(buf[:needBytes], uint64(needBytes))
		if nr < needBytes || err2 != nil {
			return KV{}, false
		}
		key, ok := slottedPageFirstKey(buf[:nr])
		if !ok {
			return KV{}, false
		}
		return KV{Key: key}, true
	}

	// Legacy kv128 format.
	size, ok := kv128SizePrefix(header[:])
	if !ok || size > len(buf) {
		return KV{}, false
	}
	n, err = fh.Read(buf[:size], uint64(size))
	if n != size || err != nil {
		return KV{}, false
	}
	kv, _, ok2 := kv128Decode(buf[:size])
	return kv, ok2
}

// logRedo replays a WAL log file, applying operations to FlexSpace.
func (db *FlexDB) logRedo(fd vfs.File, fileSize int64) {
	buf := make([]byte, MaxKeySize)
	var nh memSparseIndexTreeHandler

	// Detect header size: try 20-byte format first, fall back to 12-byte
	var hdrBuf [20]byte
	n, _ := fd.ReadAt(hdrBuf[:], 0)
	offset := int64(12) // default: old 12-byte header
	if n >= 20 && crc32.Checksum(hdrBuf[:16], crc32cTable) == binary.LittleEndian.Uint32(hdrBuf[16:20]) {
		offset = 20 // new 20-byte header
	}

	for offset < fileSize {
		// Read first few bytes to determine size
		n, err := fd.ReadAt(buf[:4], offset)
		if n < 4 || err != nil {
			break
		}
		size, ok := kv128SizePrefix(buf[:n])
		if !ok || size > len(buf) {
			break
		}
		n, err = fd.ReadAt(buf[:size], offset)
		if n != size || err != nil {
			vv("flexdb: logRedo: truncated at offset %d (n=%d size=%d): %v", offset, n, size, err)
			break
		}
		kv, _, ok2 := kv128Decode(buf[:size])
		if !ok2 {
			break
		}
		db.putPassthrough(kv, &nh)
		offset += int64(size)
	}
}

// ====================== Flush worker ======================

func (db *FlexDB) flushWorker() {
	defer db.flushWG.Done()
	ticker := time.NewTicker(memtableFlushTime)
	defer ticker.Stop()

	for {
		select {
		case <-db.flushStop:
			return
		case <-db.flushTrigger:
			db.doFlush()
		case <-ticker.C:
			db.doFlush()
		}
	}
}

// only called by the flushWorker goroutine.
func (db *FlexDB) doFlush() {
	db.topMutRW.Lock()
	defer db.topMutRW.Unlock()

	wasActive := db.activeMT
	if db.memtables[wasActive].empty {
		return
	}
	// Swap memtables
	db.activeMT = 1 - db.activeMT
	db.memtables[db.activeMT].empty = true
	db.memtables[db.activeMT].size = 0

	inactive := wasActive
	// Flush log to disk
	db.memtables[inactive].logFlush()
	panicOn(db.memtables[inactive].memWalFD.Sync())

	// Flush memtable to FlexSpace
	db.flushMemtable(inactive)
	db.persistCounters()
	db.ff.Sync()
	db.maybePiggybackGC()

	// Truncate WAL (always use 20-byte versioned header for consistent disk format)
	ts := uint64(time.Now().UnixNano())
	db.memtables[inactive].logTruncateWithVersion(ts, db.ff.tree.PersistentVersion)

	// Clear the btree

	db.memtables[inactive].bt.Clear()
	db.memtables[inactive].empty = true
	db.memtables[inactive].size = 0
}

func (db *FlexDB) flushMemtable(mtIdx int) {
	m := &db.memtables[mtIdx]
	var nh memSparseIndexTreeHandler
	batch := make([]KV, 0, memtableFlushBatch)

	m.bt.Ascend(KV{}, func(item KV) bool {
		batch = append(batch, item)
		if len(batch) >= memtableFlushBatch {
			for _, kv := range batch {
				db.putPassthrough(kv, &nh)
				nh.node = nil // reset hint after each for simplicity
			}
			batch = batch[:0]
		}
		return true
	})
	for _, kv := range batch {
		db.putPassthrough(kv, &nh)
		nh.node = nil
	}
}

// syncDir opens the directory at path and fsyncs it so that newly
// created or renamed files have durable directory entries.
// syncDir syncs the directory at path and all its ancestor directories
// up to and including the root. This ensures that newly created files and
// subdirectories have durable directory entries at every level of the path.
func syncDir(fs vfs.FS, path string) error {
	for {
		dir, err := fs.OpenDir(path)
		if err != nil {
			return err
		}
		err = dir.Sync()
		dir.Close()
		if err != nil {
			return err
		}
		parent := filepath.Dir(path)
		if parent == path || parent == "/" {
			break
		}
		if parent == "." {
			// Sync the root directory too.
			dir, err := fs.OpenDir(parent)
			if err != nil {
				return err
			}
			err = dir.Sync()
			dir.Close()
			return err
		}
		path = parent
	}
	return nil
}
