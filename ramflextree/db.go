package ramflextree

// yogadb/ramflextree/db.go - RAM-only FlexDB orchestrator.
// All disk I/O (VLOG, WAL, recovery, flush worker, CoW persistence,
// syncDir, vacuum) has been removed. The FlexSpace operates entirely
// in memory.

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math/bits"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"
)

// ====================== Constants ======================

const (
	// MaxKeySize is actually 16 bytes smaller (really the limit is 4080 bytes).
	MaxKeySize                        = 4096
	flexMemSparseIndexTreeLeafCap     = 100
	flexMemSparseIndexTreeInternalCap = 40
	flexdbSparseIntervalCount         = 1000
	flexdbSparseIntervalSize          = SLOTTED_PAGE_KB << 10 // 64 KB
	memtableCap                       = 1 << 30               // 1 GB
	memtableFlushBatch                = 1024
	flexdbUnsortedWriteQuota          = 6
	// sparseInterval = sortedCount + unsortedQuota + 1 = 32
	flexdbSparseInterval       = flexdbSparseIntervalCount + flexdbUnsortedWriteQuota + 1
	intervalCachePartitionCount = 1024
	intervalCachePartitionMask  = intervalCachePartitionCount - 1

	// see intervalcache.go for intervalCacheEntry
	intervalCacheEntryChance = 2
)

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
func (s *Batch) Set(key string, value []byte) (err error) {
	if key == "" {
		return ErrKeyEmpty
	}

	// String keys are immutable - no copy needed.
	// Value is []byte, so we must copy it.
	s.puts = append(s.puts, &KV{
		Key:   key,
		Value: append([]byte{}, value...),
	})
	return nil
}

// Delete marks key for deletion in this batch.
func (s *Batch) Delete(key string) {
	s.puts = append(s.puts, &KV{
		Key:  key,
		Vptr: VPtr{Length: tombstoneVPtrLength},
	})
}

// Commit flushes the batch atomically into the in-memory FlexSpace.
//
// After Commit the batch is empty and can be re-used immediately.
//
// Returns the half-open HLC interval [Begin, Endx) assigned to this batch.
//
// The doFsync parameter is accepted for API compatibility but ignored
// (there is no disk to sync in the RAM-only implementation).
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
// FLEXSPACE.KV.SLOT_BLOCKS to see how much free space could be reclaimed.
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
			// Duplicate key in this sub-batch - start new sub-batch.
			seen = make(map[string]struct{})
			curHLC = db.hlc.CreateSendOrLocalEvent()
		}
		seen[k] = struct{}{}
		kv.Hlc = curHLC
	}

	// Insert directly into the memtable.
	mt := &db.mt

	for idx := 0; idx < len(s.puts); idx++ {

		if mt.size >= memtableCap {
			// Memtable full - flush inline.
			db.flushMemtable()
			db.cache.flushDirtyPages()

			mt.bt.Clear()
			mt.empty = true
			mt.size = 0
			db.flushSeq++
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
	}

	mt.empty = false

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

// KV is a key-value pair. Tombstones are marked by Vptr.Length == tombstoneVPtrLength == 1
// A nil Value with Vptr.Length == 0 is a live key with nil value (just a
// key that is present but has no value; this is fine).
//
// KV is currently 64 bytes, a cache line on most systems. Be very wary of
// making it any bigger, as this could really slow things down.
//
// The Key is just a string. There is no loss of generality over []byte,
// just advantages: a) being immutable, we can avoid slow copies;
// and being smaller (2 words, not 3) than a []byte our KV now
// fits in one 64B cache line. Moreover users cannot corrupt
// the Key, so it is safe to return from our internal caches.
type KV struct {
	Key   string
	Value []byte
	Vptr  VPtr // Vptr.Length==1 means tombstone; Length>1: VLOG pointer; Length==0: inline/nil
	Hlc   HLC  // hybrid logical clock timestamp. LSN like per mini batch, but has big gaps.
}

// HasVPtr returns true if the value is stored in the VLOG file.
// Real VLOG entries always have Length > tombstoneVPtrLength.
func (kv *KV) HasVPtr() bool { return kv.Vptr.Length > tombstoneVPtrLength }

func (z *KV) String() (r string) {
	r = "&KV{\n"
	r += fmt.Sprintf("    Key: %v,\n", z.Key)
	r += fmt.Sprintf("  Value: %v,\n", string(z.Value))
	r += fmt.Sprintf("   Vptr: %v,\n", z.Vptr)
	r += fmt.Sprintf("HasVPtr: %v,\n", z.HasVPtr())
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

func kvLess(a, b KV) bool { return a.Key < b.Key }

// kvSizeApprox returns the approximate in-memory size of a KV (matches C kv_size).
func kvSizeApprox(kv *KV) int { return 24 + len(kv.Key) + len(kv.Value) }

// isTombstone returns true if this KV is a deletion marker.
// A tombstone is marked by the sentinel VPtr.Length == tombstoneVPtrLength == 1.
func (kv *KV) isTombstone() bool {
	return kv.Vptr.Length == tombstoneVPtrLength
}

// Large returns true if this KV's value is stored in the VLOG
// (too large for inline storage). Use db.FetchLarge(kv) to
// retrieve the value bytes.
func (kv *KV) Large() bool {
	return kv.Vptr.Length > tombstoneVPtrLength
}

// ====================== KV128 encoding ======================
// Format: varint(klen) || varint(rawVlen) || key_bytes || value_or_vptr_bytes
// Standard LEB128 (same as Go's encoding/binary.PutUvarint).
//
// rawVlen encoding:
//   rawVlen == 0                   -> live key, nil value (0 value bytes follow)
//   rawVlen == rawVlenTombstone    -> tombstone (0 value bytes follow)
//   rawVlen == rawVlenVPtr         -> VLOG pointer: 16 bytes follow (8-byte offset + 8-byte length)
//   rawVlen == len(V) + 2          -> inline value of length len(V), followed by len(V) bytes
//
// The two high sentinels (rawVlenVPtr, rawVlenTombstone) can never collide with
// a real inline value since that would require a value of length ~2^64 - 3.

const rawVlenVPtr = ^uint64(0)      // 0xFFFF FFFF FFFF FFFF - sentinel for VLOG pointer
const rawVlenTombstone = ^uint64(1) // 0xFFFF FFFF FFFF FFFE - sentinel for tombstone

// tombstoneVPtrLength is the sentinel value stored in VPtr.Length
// to mark a KV as a tombstone. Real VLOG entries always have
// Length > 1, so 1 is safe.
const tombstoneVPtrLength uint64 = 1

func kv128Encode(buf []byte, kv KV) []byte {
	recordStart := len(buf)
	var hdr [20]byte
	n := binary.PutUvarint(hdr[:], uint64(len(kv.Key)))
	if kv.isTombstone() {
		n += binary.PutUvarint(hdr[n:], rawVlenTombstone) // tombstone sentinel
	} else if kv.HasVPtr() {
		n += binary.PutUvarint(hdr[n:], rawVlenVPtr) // VLOG pointer sentinel
	} else if kv.Value == nil {
		n += binary.PutUvarint(hdr[n:], 0) // live nil value
	} else {
		n += binary.PutUvarint(hdr[n:], uint64(len(kv.Value)+2)) // inline: len+2
	}
	buf = append(buf, hdr[:n]...)
	buf = append(buf, kv.Key...)
	if kv.HasVPtr() {
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
		return varintSize(uint64(len(kv.Key))) + varintSize(rawVlenTombstone) + len(kv.Key) + 8 + 4
	}
	if kv.HasVPtr() {
		return varintSize(uint64(len(kv.Key))) + varintSize(rawVlenVPtr) + len(kv.Key) + vptrSize + 8 + 4
	}
	if kv.Value == nil {
		return varintSize(uint64(len(kv.Key))) + 1 + len(kv.Key) + 8 + 4 // rawVlen=0 is 1 byte
	}
	return varintSize(uint64(len(kv.Key))) + varintSize(uint64(len(kv.Value)+2)) + len(kv.Key) + len(kv.Value) + 8 + 4
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
	if rawVlen == rawVlenTombstone {
		// tombstone
		total := hdr + int(klen) + 8
		if len(src) < total+4 {
			return
		}
		if crc32.Checksum(src[:total], crc32cTable) != binary.LittleEndian.Uint32(src[total:total+4]) {
			return
		}
		kv.Key = string(src[hdr : hdr+int(klen)])
		kv.Vptr.Length = tombstoneVPtrLength
		kv.Hlc = HLC(binary.BigEndian.Uint64(src[total-8 : total]))
		return kv, total + 4, true
	}
	if rawVlen == 0 {
		// live key, nil value
		total := hdr + int(klen) + 8
		if len(src) < total+4 {
			return
		}
		if crc32.Checksum(src[:total], crc32cTable) != binary.LittleEndian.Uint32(src[total:total+4]) {
			return
		}
		kv.Key = string(src[hdr : hdr+int(klen)])
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
		kv.Key = string(src[hdr : hdr+int(klen)])
		kv.Vptr = decodeVPtr(src[hdr+int(klen) : hdr+int(klen)+vptrSize])
		kv.Hlc = HLC(binary.BigEndian.Uint64(src[total-8 : total]))
		return kv, total + 4, true
	}
	vlen := int(rawVlen - 2)
	total := hdr + int(klen) + vlen + 8
	if len(src) < total+4 {
		return
	}
	if crc32.Checksum(src[:total], crc32cTable) != binary.LittleEndian.Uint32(src[total:total+4]) {
		return
	}
	kv.Key = string(src[hdr : hdr+int(klen)])
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
	if rawVlen == rawVlenTombstone || rawVlen == 0 {
		return kn + vn + int(klen) + 8 + 4, true
	}
	if rawVlen == rawVlenVPtr {
		return kn + vn + int(klen) + vptrSize + 8 + 4, true
	}
	return kn + vn + int(klen) + int(rawVlen-2) + 8 + 4, true
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

func kvCRC32(key string) uint32 {
	if len(key) == 0 {
		return crc32.Checksum(nil, crc32cTable)
	}
	b := unsafe.Slice(unsafe.StringData(key), len(key))
	return crc32.Checksum(b, crc32cTable)
}

func fingerprint(h uint32) uint16 {
	fp := uint16(h) ^ uint16(h>>16)
	if fp == 0 {
		fp = 1
	}
	return fp
}

func cachePartitionID(key string) int {
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

// Config allows configuration of a FlexDB.
type Config struct {
	CacheMB uint64 // default 32 (for 32 MB)

	// DisableBackgroundFlush disables the background flush worker goroutine.
	// When true, memtable flushes only happen on explicit Sync() or Close() calls.
	// This is useful for fuzz testing where background goroutines can crash
	// the entire fuzz worker subprocess if they panic (the test's recover()
	// only catches panics on the test goroutine, not background goroutines).
	DisableBackgroundFlush bool

	// PaddedSplits controls whether treeInsertAnchor pads both split
	// halves to slottedPageMaxSize. Default false uses tight encoding,
	// which cuts space amplification substantially. When true, the old
	// padded behavior is used (useful for A/B comparison).
	// Padding allows in-place additions to a slotted page to
	// occur, making key updates more efficient. The trade-off
	// is pre-allocating space for new additions.
	PaddedSplits bool
}

// ====================== FlexDB ======================

// FlexDB is an in-memory ordered key-value store backed by FlexSpace. It is
// thread-safe, except for iteration via Ascend/Descend--which allows deletions
// and updates on the fly.
type FlexDB struct {
	// hlc must be first field for 64-bit alignment on 32-bit architectures.
	hlc HLC // hybrid logical clock for timestamping every KV

	closed bool // idempotent Close.

	Path string
	cfg  Config

	ff    *FlexSpace          // underlying FlexSpace (in-memory)
	tree  *memSparseIndexTree // in-memory sparse index (rebuilt on open)
	cache *intervalCache

	topMutRW sync.RWMutex

	mt       memtable // single memtable
	flushSeq uint64   // incremented on each memtable flush

	// scratch buffers (reused; protected by ffMu write lock)
	kvbuf1 []byte
	itvbuf []byte

	// Write-byte counters (accessed atomically)
	LogicalBytesWritten int64 // user payload bytes (key+value)

	// (iterator support - pfSpans are embedded in Iter, no free list needed)

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
	if kv.HasVPtr() {
		return ksLiveBig
	}
	return ksLiveSmall
}

// adjustKeyCounters updates the live key counters for an old->new state transition.
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

// writeLockHeldKeyState checks FlexSpace for a key's state.
// Called when a key is new to the memtable.
// Caller must hold topMutRW.Lock().
func (db *FlexDB) writeLockHeldKeyState(key string) keyState {
	if db.ff.Size() == 0 {
		return ksNotExists
	}
	kv, ok, err := db.getPassthroughKV(key)
	if err != nil || !ok {
		return ksNotExists
	}
	return kvToState(kv)
}

// Len returns the total number of live (non-tombstone) keys in the database.
// O(1) - reads a pre-maintained counter.
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
// O(1) - reads pre-maintained counters.
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
// No locking needed - called before the db reference is returned.
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
			fce, err := partition.getEntry(anchor, anchorLoff, db)
			if err != nil {
				partition.releaseEntry(fce)
				continue
			}
			for _, kv := range fce.kvs {
				if kv.isTombstone() {
					continue
				}
				if kv.HasVPtr() {
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

// OpenFlexDB opens or creates a RAM-only FlexDB.
// The path argument is stored for identification but no files are created.
func OpenFlexDB(path string, pCfg *Config) (*FlexDB, error) {
	cfg := Config{CacheMB: 32}
	if pCfg != nil {
		cfg = *pCfg
	}
	if cfg.CacheMB == 0 {
		cfg.CacheMB = 32
	}

	ff := NewFlexSpace()

	db := &FlexDB{
		cfg:    cfg,
		Path:   path,
		ff:     ff,
		cache:  newCache(nil, cfg.CacheMB),
		kvbuf1: make([]byte, 0, MaxKeySize),
		itvbuf: make([]byte, 0, flexdbSparseIntervalSize+MaxKeySize),
	}
	db.cache.db = db
	for i := range db.cache.partitions {
		db.cache.partitions[i].db = db
	}
	db.mt = *newMemtable()
	db.tree = memSparseIndexTreeCreate()

	// Tag loff=0 as the first anchor
	tag := flexdbTagGenerate(true, 0)
	_ = ff.SetTag(0, tag)

	return db, nil
}

// Close shuts down the FlexDB, flushing any remaining memtable data
// to the in-memory FlexSpace.
func (db *FlexDB) Close() *Metrics {
	db.topMutRW.Lock()
	defer db.topMutRW.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true
	if !db.mt.empty {
		db.flushMemtable()
		db.cache.flushDirtyPages()
	} else {
		db.cache.flushDirtyPages()
	}
	m := db.writeLockHeldFinalMetrics(0, 0)
	db.cache.destroyAll()
	return m
}

// Metrics holds byte-level write counters for computing write amplification.
type Metrics struct {
	Session                   bool
	LiveKeyCount              int64
	KV128BytesWritten         int64 // FlexSpace KV data
	MemWALBytesWritten        int64 // always 0 in RAM-only mode
	REDOLogBytesWritten       int64 // always 0 in RAM-only mode
	FlexTreePagesBytesWritten int64 // always 0 in RAM-only mode
	VLOGBytesWritten          int64 // always 0 in RAM-only mode
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
	TotalFreeBytesInBlocks int64

	// BlocksInUse shows how many 4MB blocks FlexSpace is using.
	BlocksInUse int64

	// BlocksWithLowUtilization is the count of non-empty blocks whose
	// utilization (live bytes / block size) is below a threshold.
	BlocksWithLowUtilization int64

	// KVBlocksTotalLiveBytes is the sum of live (used) bytes across all blocks,
	// as tracked by the block manager.
	KVBlocksTotalLiveBytes int64

	// KVBlocksOnDiskFootprintBytes is 0 in RAM-only mode.
	KVBlocksOnDiskFootprintBytes int64

	// VlogOnDiskFootprintBytes is 0 in RAM-only mode.
	VlogOnDiskFootprintBytes int64

	// LowBlockUtilizationPct we used.
	LowBlockUtilizationPct float64
}

func (z *Metrics) String() (r string) {
	r = "Metrics{\n"
	r += fmt.Sprintf("      (just this) Session: %v\n", z.Session)
	r += fmt.Sprintf("              LiveKeyCount: %v\n", formatInt64Under(z.LiveKeyCount))
	r += fmt.Sprintf("        KV128 BytesWritten: %v\n", formatInt64Under(z.KV128BytesWritten))
	r += fmt.Sprintf("      Logical BytesWritten: %v\n", formatInt64Under(z.LogicalBytesWritten))
	r += fmt.Sprintf("        Total BytesWritten: %v\n", formatInt64Under(z.TotalBytesWritten))
	r += fmt.Sprintf("                 WriteAmp: %0.3f\n", z.WriteAmp)
	r += fmt.Sprintf("\n   --- block utilization ---  \n")
	r += fmt.Sprintf("    KVBlocksTotalLiveBytes: %v (%0.2f MB)\n", formatInt64Under(z.KVBlocksTotalLiveBytes), float64(z.KVBlocksTotalLiveBytes)/(1<<20))
	r += fmt.Sprintf("    TotalFreeBytesInBlocks: %v (%0.2f MB)\n", formatInt64Under(z.TotalFreeBytesInBlocks), float64(z.TotalFreeBytesInBlocks)/(1<<20))
	r += fmt.Sprintf("      FLEXSPACE_BLOCK_SIZE: %0.2f MB\n", float64(FLEXSPACE_BLOCK_SIZE)/(1<<20))
	r += fmt.Sprintf("               BlocksInUse: %v  (%0.2f MB)\n", formatInt64Under(z.BlocksInUse), float64(z.BlocksInUse*FLEXSPACE_BLOCK_SIZE)/(1<<20))
	r += fmt.Sprintf("  BlocksWithLowUtilization: %v\n", formatInt64Under(z.BlocksWithLowUtilization))
	r += "}\n"
	return
}

// SessionMetrics returns a snapshot of write-byte counters aggregated from all layers.
func (db *FlexDB) SessionMetrics() *Metrics {
	db.topMutRW.Lock()
	defer db.topMutRW.Unlock()
	return db.writeLockHeldSessionMetrics()
}

func (db *FlexDB) writeLockHeldSessionMetrics() *Metrics {
	m := &Metrics{
		LiveKeyCount:        db.liveKeys,
		Session:             true,
		KV128BytesWritten:   atomic.LoadInt64(&db.ff.KV128BytesWritten),
		LogicalBytesWritten: atomic.LoadInt64(&db.LogicalBytesWritten),
	}
	m.TotalBytesWritten = m.KV128BytesWritten
	if m.LogicalBytesWritten > 0 {
		m.WriteAmp = float64(m.TotalBytesWritten) / float64(m.LogicalBytesWritten)
	}

	m.totalLogicalBytesWrit = atomic.LoadInt64(&db.LogicalBytesWritten)
	m.totalPhysicalBytesWrit = m.TotalBytesWritten
	if m.totalLogicalBytesWrit > 0 {
		m.CumulativeWriteAmp = float64(m.totalPhysicalBytesWrit) / float64(m.totalLogicalBytesWrit)
	}

	m.KVBlocksTotalLiveBytes, m.TotalFreeBytesInBlocks, m.BlocksInUse, m.BlocksWithLowUtilization =
		db.ff.garbageMetrics(0.50)

	return m
}

// writeLockHeldFinalMetrics builds a Metrics snapshot after the final flush in Close().
func (db *FlexDB) writeLockHeldFinalMetrics(kvFoot, vlogFoot int64) *Metrics {
	m := &Metrics{
		Session:             true,
		LiveKeyCount:        db.liveKeys,
		KV128BytesWritten:   atomic.LoadInt64(&db.ff.KV128BytesWritten),
		LogicalBytesWritten: atomic.LoadInt64(&db.LogicalBytesWritten),
	}
	m.TotalBytesWritten = m.KV128BytesWritten
	if m.LogicalBytesWritten > 0 {
		m.WriteAmp = float64(m.TotalBytesWritten) / float64(m.LogicalBytesWritten)
	}

	m.totalLogicalBytesWrit = atomic.LoadInt64(&db.LogicalBytesWritten)
	m.totalPhysicalBytesWrit = m.TotalBytesWritten
	if m.totalLogicalBytesWrit > 0 {
		m.CumulativeWriteAmp = float64(m.totalPhysicalBytesWrit) / float64(m.totalLogicalBytesWrit)
	}

	m.KVBlocksTotalLiveBytes, m.TotalFreeBytesInBlocks, m.BlocksInUse, m.BlocksWithLowUtilization =
		db.ff.garbageMetrics(0.50)

	m.KVBlocksOnDiskFootprintBytes = kvFoot
	m.VlogOnDiskFootprintBytes = vlogFoot

	return m
}

// CumulativeMetrics reports cumulative write metrics.
// In RAM-only mode, this is equivalent to SessionMetrics.
func (db *FlexDB) CumulativeMetrics() *Metrics {
	db.topMutRW.RLock()
	defer db.topMutRW.RUnlock()

	m := &Metrics{}
	m.KV128BytesWritten = atomic.LoadInt64(&db.ff.KV128BytesWritten)
	m.LogicalBytesWritten = int64(db.ff.tree.MaxLoff)

	m.TotalBytesWritten = m.KV128BytesWritten
	if m.LogicalBytesWritten > 0 {
		m.WriteAmp = float64(m.TotalBytesWritten) / float64(m.LogicalBytesWritten)
	}

	m.totalLogicalBytesWrit = m.LogicalBytesWritten
	m.totalPhysicalBytesWrit = m.TotalBytesWritten
	if m.totalLogicalBytesWrit > 0 {
		m.CumulativeWriteAmp = float64(m.totalPhysicalBytesWrit) / float64(m.totalLogicalBytesWrit)
	}

	m.KVBlocksTotalLiveBytes, m.TotalFreeBytesInBlocks, m.BlocksInUse, m.BlocksWithLowUtilization =
		db.ff.garbageMetrics(0.50)

	return m
}

// resolveVPtr returns the inline value. In RAM-only mode there is no VLOG,
// so VPtr entries cannot be resolved. This will return an error if HasVPtr is true.
func (db *FlexDB) resolveVPtr(kv KV) ([]byte, error) {
	if !kv.HasVPtr() {
		return kv.Value, nil
	}
	return nil, fmt.Errorf("flexdb: VPtr but no VLOG in RAM-only mode")
}

// FetchLarge retrieves the value bytes for a KV whose value
// is stored in the VLOG (kv.Large() returns true). For inline
// values, it simply returns kv.Value.
// In RAM-only mode, returns kv.Value (no VLOG).
func (db *FlexDB) FetchLarge(kv *KV) ([]byte, error) {
	if kv == nil {
		return nil, fmt.Errorf("flexdb: FetchLarge called with nil KV")
	}
	return kv.Value, nil
}

// lockHeldFetchLarge is the lock-held body of FetchLarge.
// Caller must hold topMutRW.RLock() or topMutRW.Lock().
func (db *FlexDB) lockHeldFetchLarge(kv *KV) ([]byte, error) {
	if kv == nil {
		return nil, fmt.Errorf("flexdb: FetchLarge called with nil KV")
	}
	return kv.Value, nil
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
	r += fmt.Sprintf("       OldVLOGSize: %v,\n", formatInt64Under(z.OldVLOGSize))
	r += fmt.Sprintf("       NewVLOGSize: %v,\n", formatInt64Under(z.NewVLOGSize))
	r += fmt.Sprintf("    BytesReclaimed: %v,\n", formatInt64Under(z.BytesReclaimed))
	r += fmt.Sprintf("     EntriesCopied: %v,\n", formatInt64Under(z.EntriesCopied))
	r += fmt.Sprintf("IntervalsRewritten: %v,\n", formatInt64Under(z.IntervalsRewritten))
	r += "}\n"
	return
}

// VacuumVLOG is a no-op in RAM-only mode.
func (db *FlexDB) VacuumVLOG() (*VacuumVLOGStats, error) {
	return &VacuumVLOGStats{}, nil
}

// VacuumKVStats reports the results of a VacuumKV operation.
type VacuumKVStats struct {
	OldFileSize      int64
	NewFileSize      int64
	BytesReclaimed   int64
	PaddingReclaimed int64
	ExtentsRewritten int64
}

func (z *VacuumKVStats) String() (r string) {
	r = "VacuumKVStats{\n"
	r += fmt.Sprintf("       OldFileSize: %v,\n", formatInt64Under(z.OldFileSize))
	r += fmt.Sprintf("       NewFileSize: %v,\n", formatInt64Under(z.NewFileSize))
	r += fmt.Sprintf("   BytesReclaimed: %v,\n", formatInt64Under(z.BytesReclaimed))
	r += fmt.Sprintf("PaddingReclaimed: %v,\n", formatInt64Under(z.PaddingReclaimed))
	r += fmt.Sprintf("ExtentsRewritten: %v,\n", formatInt64Under(z.ExtentsRewritten))
	r += "}\n"
	return
}

// VacuumKV is a no-op in RAM-only mode.
func (db *FlexDB) VacuumKV() (*VacuumKVStats, error) {
	return &VacuumKVStats{}, nil
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

// CheckIntegrity performs a read-only consistency check of the in-memory FlexDB.
// It flushes the memtable first, then checks:
//  1. FlexTree leaf linked list: no cycles, prev/next consistency
//  2. Extent validity
//  3. Sparse index: every anchor interval is readable and decodable
//  4. Sorted keys: keys within each decoded interval are in sorted order
//  5. Anchor coverage: anchor loff+psize spans tile the FlexSpace without gaps/overlaps
//
// Returns nil if no errors found.
func (db *FlexDB) CheckIntegrity() []IntegrityError {
	db.topMutRW.Lock()
	defer db.topMutRW.Unlock()

	// Flush memtable so FlexSpace has all live data.
	db.writeLockHeldSync()

	var errs []IntegrityError
	addErr := func(check, detail string, fatal bool) {
		errs = append(errs, IntegrityError{Check: check, Detail: detail, Fatal: fatal})
	}

	ff := db.ff
	tree := ff.tree

	// ---- Check 1: FlexTree leaf linked list + extent validity ----
	leafCount := 0
	extentCount := uint64(0)

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

			length := uint64(ext.Len)
			if length == 0 {
				addErr("extent_validity",
					fmt.Sprintf("leaf %d ext %d: zero-length non-hole extent", nodeID, i), false)
				continue
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

	// ---- Check 2: MaxLoff consistency ----
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

	// ---- Check 3: Sparse index anchor intervals ----
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

			// Decode all KVs in the interval
			src := itvBuf
			kvCount := 0
			var prevKey string
			hasPrev := false

			if slottedPageIsSlotted(src) {
				kvs, consumed, decErr := slottedPageDecode(src)
				if decErr != nil {
					addErr("slotted_decode",
						fmt.Sprintf("anchor %d (key=%q): slottedPageDecode failed: %v",
							anchorCount, anchor.key, decErr), false)
				} else {
					for ki := range kvs {
						kvCount++
						if hasPrev && kvs[ki].Key < prevKey {
							addErr("key_order",
								fmt.Sprintf("anchor %d (key=%q): key %q < prev key %q at position %d",
									anchorCount, anchor.key, kvs[ki].Key, prevKey, kvCount), false)
						}
						prevKey = kvs[ki].Key
						hasPrev = true
					}
					src = src[consumed:]
				}
			}

			// All data should be slotted page format.
			if len(src) > 0 {
				addErr("unexpected_format",
					fmt.Sprintf("anchor %d (key=%q): %d unexpected non-slotted trailing bytes at byte %d of %d, first bytes: %x",
						anchorCount, anchor.key, len(src), int(psize)-len(src), psize, src[:min(len(src), 16)]), false)
			}
		}
	}

	// ---- Check 4: Anchor coverage matches FlexSpace size ----
	if totalAnchorBytes != ffSize && ffSize > 0 {
		addErr("anchor_total_size",
			fmt.Sprintf("total anchor psize sum=%d != FlexSpace size=%d",
				totalAnchorBytes, ffSize), false)
	}

	return errs
}

// Sync flushes all in-memory data in the active memtable to
// the in-memory FlexSpace.
func (db *FlexDB) Sync() error {
	db.topMutRW.Lock()
	defer db.topMutRW.Unlock()

	return db.writeLockHeldSync()
}

func (db *FlexDB) writeLockHeldSync() error {

	if db.mt.empty {
		return nil // nothing to flush
	}

	// Flush memtable to FlexSpace (in memory).
	db.flushMemtable()
	db.cache.flushDirtyPages()
	db.verifyAnchorTags()

	db.mt.bt.Clear()
	db.mt.empty = true
	db.mt.size = 0

	return nil
}

var ErrKeyEmpty = fmt.Errorf("key cannot be the empty string")

// recoverIterIOErr is deferred in Find/Get/Update/View to convert
// iterIOErr panics (FlexSpace I/O failures) into returned errors.
func recoverIterIOErr(errp *error) {
	if r := recover(); r != nil {
		if ioe, ok := r.(iterIOErr); ok {
			*errp = ioe.err
		} else {
			panic(r) // re-panic anything else
		}
	}
}

// Put writes key -> value. len(value) == 0 is fine, if desired.
// Call Delete instead of Put to delete a key and any associated value.
//
// All values are stored inline in RAM-only mode.
//
// Puts are buffered in the memtable and flushed to the in-memory
// FlexSpace on Sync() or Close().
func (db *FlexDB) Put(key string, value []byte) error {
	if key == "" {
		return ErrKeyEmpty
	}
	db.topMutRW.Lock()
	defer db.topMutRW.Unlock()
	return db.writeLockHeldPut(key, value, false)
}

func (db *FlexDB) writeLockHeldPut(key string, value []byte, doDelete bool) error {

	if doDelete && len(value) > 0 {
		return fmt.Errorf("flexdb: cannot supply a value and also delete it")
	}

	if len(key)+16 >= MaxKeySize {
		return fmt.Errorf("flexdb: key too large (max %d bytes)", MaxKeySize-16)
	}
	if len(key)+len(value)+16 >= MaxKeySize {
		return fmt.Errorf("flexdb: KV too large (max %d bytes)", MaxKeySize)
	}
	atomic.AddInt64(&db.LogicalBytesWritten, int64(len(key)+len(value)))

	// Tick the HLC for this write.
	hlcVal := db.hlc.CreateSendOrLocalEvent()

	// String keys are immutable - no defensive copy needed.
	// Value is []byte, so we must copy it.
	if value != nil {
		value = append([]byte{}, value...)
	}

	// Build the KV for the memtable.
	kv := KV{Key: key, Value: value, Hlc: hlcVal}
	if doDelete {
		kv.Vptr.Length = tombstoneVPtrLength
	}

	if db.mt.size >= memtableCap {
		// Inline flush when memtable is full.
		db.flushMemtable()

		db.mt.bt.Clear()
		db.mt.empty = true
		db.mt.size = 0
		db.flushSeq++
	}
	newState := kvToState(kv)
	old, replaced := db.mt.put(kv)
	db.mt.empty = false

	var oldState keyState
	if replaced {
		oldState = kvToState(old)
	} else {
		oldState = db.writeLockHeldKeyState(key)
	}
	db.adjustKeyCounters(oldState, newState)

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

	// SKIP_VALUES returns KV.Values = nil; we make
	// no effort to retrieve values, only keys. This is
	// useful for very fast full-table scans of just the keys,
	// when the user knows they will not inspect values
	// at all.
	SKIP_VALUES SearchModifier = 16

	// LAZY_SMALL requests zero-copy return of inline values.
	// The returned KVcloser.Value aliases interval cache memory.
	// The caller MUST call Close() to release the cache pin.
	LAZY_SMALL SearchModifier = 32

	// LAZY_LARGE means we do not fetch large values
	// automatically. The User must call FetchLarge() explicitly
	// when they are desired.
	LAZY_LARGE SearchModifier = 64

	// LAZY means do both LAZY_SMALL and LAZY_LARGE
	LAZY SearchModifier = 96
)

// findSeekIter positions it according to smod and key.
// Returns (found, exact). On return, it is either Valid
// (found=true) or invalid (found=false).
func findSeekIter(it *Iter, smod SearchModifier, key string) (found, exact bool) {
	switch smod {
	case GTE:
		it.Seek(key)
	case GT:
		it.Seek(key)
		if it.Valid() && it.Key() == key {
			it.Next()
		}
	case LTE:
		it.seekLE(key, false)
	case LT:
		it.seekLE(key, true)
	case Exact:
		it.Seek(key)
		if it.Valid() && it.Key() != key {
			it.releaseIterState()
			it.valid = false
			return false, false
		}
	}
	if !it.Valid() {
		return false, false
	}
	return true, it.Key() == key
}

// findBuildKV constructs a *KV from the iterator's current
// position. Returns a shallow copy of the internal KV - Key
// and Value alias cache memory (zero-copy).
func findBuildKV(it *Iter) *KV {
	if it.pKV == nil {
		return nil
	}
	out := *it.pKV
	return &out
}

// Find allows GTE, GT, LTE, LT, and Exact searches.
//
// The returned *KVcloser contains the found key and its value.
//
// The returned bool, 'exact', indicates an exact match to the query key.
//
// If the returned kvc *KVcloser is nil, this means that
// the key was not found, or there was an I/O error.
//
// Warning: if kvc != nil, the user must call Close() on the
// returned kvc *KVcloser when done copying any Value out, or
// else memory and resource leaks will ensue.
func (db *FlexDB) Find(smod SearchModifier, key string) (kvc *KVcloser, exact bool, err error) {
	db.topMutRW.RLock()
	defer db.topMutRW.RUnlock()
	defer recoverIterIOErr(&err)

	it := &Iter{db: db}
	lazyLarge := (smod&LAZY_LARGE != 0)
	lazySmall := (smod&LAZY_SMALL != 0)
	skipValues := (smod&SKIP_VALUES != 0)
	if lazyLarge || skipValues {
		it.lazyLarge = true
		smod &^= LAZY_LARGE
	}
	if skipValues {
		it.skipValues = true
		smod &^= SKIP_VALUES
	}
	smod &^= LAZY_SMALL // strip LAZY_SMALL before passing to findSeekIter

	var found bool
	found, exact = findSeekIter(it, smod, key)
	if found {
		zc := findBuildKV(it)
		resultKey := zc.Key

		// Release iterator state early - we have what we need.
		it.releaseIterState()

		if skipValues {
			kvc = &KVcloser{KV: KV{Key: resultKey, Hlc: zc.Hlc}, db: db}
			return
		}

		// LAZY_SMALL path: try zero-copy via cache pinning.
		if lazySmall && !it.valueResolved && !zc.HasVPtr() && zc.Value != nil {
			kvc, err = db.findBuildKVZeroCopy(resultKey)
			if err != nil {
				return
			}
			if kvc != nil {
				return
			}
			// Fallback: key was in memtable or edge case. Copy below.
		}

		// Standard path: copy inline value.
		owned := KV{Vptr: zc.Vptr, Hlc: zc.Hlc}
		owned.Key = resultKey
		if !zc.HasVPtr() && zc.Value != nil {
			owned.Value = append([]byte{}, zc.Value...)
		}
		kvc = &KVcloser{KV: owned, db: db}

		// In RAM-only mode, no VLOG auto-fetch needed.
		_ = lazyLarge
		return
	}
	it.releaseIterState()
	return
}

// KVcloser is the result of a Find or GetKV call.
// The user must call Close() on the KVcloser when done copying any
// value out, or else memory and resource leaks will ensue.
type KVcloser struct {
	KV
	partition *intervalCachePartition // nil when no pin needed
	entry     *intervalCacheEntry     // nil when no pin needed
	db        *FlexDB
}

// Close must be called when done with the non-nil *KVcloser
// result of a GetKV or Find call. Otherwise memory and resource
// leaks will ensue.
// Close() is a no-op if called on a nil *KVcloser.
func (s *KVcloser) Close() {
	if s == nil {
		return
	}
	if s.entry != nil {
		s.partition.releaseEntry(s.entry)
		s.partition = nil
		s.entry = nil
	}
	s.Value = nil // prevent use-after-close
}

// Fetch retrieves the large value from the VLOG if this KV has
// a VPtr (kvc.Large() == true). For inline values, Fetch is a
// no-op. In RAM-only mode, this returns kvc.Value (no VLOG).
func (s *KVcloser) Fetch() error {
	if s == nil {
		return nil
	}
	if !s.HasVPtr() {
		return nil // inline value already present
	}
	// RAM-only: no VLOG to fetch from.
	return nil
}

// findBuildKVZeroCopy returns a KVcloser whose Value aliases
// interval cache memory (zero-copy). The cache entry is pinned
// via refcnt; the caller must call Close() to release.
// Caller must hold topMutRW.RLock() or topMutRW.Lock().
func (db *FlexDB) findBuildKVZeroCopy(key string) (*KVcloser, error) {
	if db.tree == nil || db.tree.leafHead == nil {
		return nil, nil
	}
	var nh memSparseIndexTreeHandler
	db.tree.findAnchorPos(key, &nh)
	anchor := nh.node.anchors[nh.idx]
	if anchor == nil || anchor.psize == 0 {
		return nil, nil
	}
	anchorLoff := uint64(anchor.loff + nh.shift)
	partition := db.cache.getPartition(anchor)
	fce, err := partition.getEntry(anchor, anchorLoff, db)
	if err != nil {
		partition.releaseEntry(fce) // getEntry always bumps refcnt
		return nil, err
	}

	idx, ok := intervalCacheEntryFindKeyEQ(fce, key)
	if !ok || fce.kvs[idx].isTombstone() {
		partition.releaseEntry(fce)
		return nil, nil
	}
	// Transfer cache entry ownership to KVcloser (don't release).
	return &KVcloser{
		KV:        fce.kvs[idx],
		partition: partition,
		entry:     fce,
		db:        db,
	}, nil
}

// GetKV is like Get but allows lazy loading of Large values;
// they are not fetched automatically. If the user sees kv.Large() true,
// then db.FetchLarge(kv) will return the large value.
// GetKV is equivalent to db.Find(Exact, key).
func (db *FlexDB) GetKV(key string) (kv *KVcloser, err error) {
	kv, _, err = db.Find(Exact, key)
	return
}

// Get retrieves the value for key. Returns nil, false if not found.
// Get can return nil, true if a nil value was stored with the key.
func (db *FlexDB) Get(key string) (value []byte, found bool, err error) {
	db.topMutRW.RLock()
	defer db.topMutRW.RUnlock()
	defer recoverIterIOErr(&err)

	// Check memtable
	if !db.mt.empty {
		kv, ok := db.mt.get(key)
		if ok {
			if kv.isTombstone() {
				return nil, false, nil // tombstone
			}
			if kv.Value == nil {
				return nil, true, nil // live key, nil value
			}
			out := make([]byte, len(kv.Value))
			copy(out, kv.Value)
			return out, true, nil
		}
	}

	// Check FlexSpace via sparse index
	val, found, err := db.getPassthrough(key)
	return val, found, err
}

// someLockHeldGet retrieves the value for key without acquiring topMutRW.
// Caller must already hold topMutRW.Lock() or topMutRW.RLock().
func (db *FlexDB) someLockHeldGet(key string) ([]byte, bool, error) {
	// Check memtable
	if !db.mt.empty {
		kv, ok := db.mt.get(key)
		if ok {
			if kv.isTombstone() {
				return nil, false, nil
			}
			if kv.Value == nil {
				return nil, true, nil // live key, nil value
			}
			out := make([]byte, len(kv.Value))
			copy(out, kv.Value)
			return out, true, nil
		}
	}

	// Check FlexSpace via sparse index
	val, found, err := db.getPassthrough(key)
	return val, found, err
}

// Delete removes key from the store.
func (db *FlexDB) Delete(key string) error {
	if key == "" {
		return ErrKeyEmpty
	}
	db.topMutRW.Lock()
	defer db.topMutRW.Unlock()
	return db.writeLockHeldPut(key, nil, true)
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
func (db *FlexDB) DeleteRange(includeLarge bool, begKey, endKey string, begInclusive, endInclusive bool) (n int64, allGone bool, err error) {
	db.topMutRW.Lock()
	defer db.topMutRW.Unlock()
	return db.writeLockHeldDeleteRange(includeLarge, begKey, endKey, begInclusive, endInclusive)
}

// writeLockHeldDeleteRange is the lock-held body of DeleteRange.
// Caller must hold topMutRW.Lock().
func (db *FlexDB) writeLockHeldDeleteRange(includeLarge bool, begKey, endKey string, begInclusive, endInclusive bool) (n int64, allGone bool, err error) {
	if begKey > endKey {
		return 0, false, fmt.Errorf("yogadb: DeleteRange: begKey > endKey")
	}
	// Equal keys with both exclusive means empty range.
	if begKey == endKey && (!begInclusive || !endInclusive) {
		return 0, false, nil
	}

	// Fast path: if the range covers every key in the DB and we're
	// including large values, reinitialize instead of iterating.
	if includeLarge && db.writeLockHeldCoversAllKeys(begKey, endKey, begInclusive, endInclusive) {
		err := db.writeLockHeldDeleteAll()
		return 0, true, err
	}

	// Phase 1: Tombstone all non-tombstone keys in range in the memtable.
	if !db.mt.empty {
		var keys []string
		db.mt.bt.Ascend(KV{Key: begKey}, func(item KV) bool {
			if !deleteRangeInBounds(item.Key, begKey, endKey, begInclusive, endInclusive) {
				if deleteRangePastEnd(item.Key, endKey, endInclusive) {
					return false
				}
				return true
			}
			if !item.isTombstone() {
				if !includeLarge && item.HasVPtr() {
					return true // skip large-value keys
				}
				keys = append(keys, item.Key)
			}
			return true
		})
		for _, key := range keys {
			if err := db.writeLockHeldPut(key, nil, true); err != nil {
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
// (small) values are deleted; keys with large values survive.
//
// Returns allGone=true when the database was re-initialized.
func (db *FlexDB) Clear(includeLarge bool) (allGone bool, err error) {
	db.topMutRW.Lock()
	defer db.topMutRW.Unlock()
	return db.writeLockHeldClear(includeLarge)
}

// writeLockHeldClear is the lock-held body of Clear.
// Caller must hold topMutRW.Lock().
func (db *FlexDB) writeLockHeldClear(includeLarge bool) (allGone bool, err error) {
	if includeLarge {
		err := db.writeLockHeldDeleteAll()
		return true, err
	}

	// !includeLarge: must iterate and tombstone only small-value keys.

	// Phase 1: Tombstone small-value keys in the memtable.
	if !db.mt.empty {
		var keys []string
		db.mt.bt.Scan(func(item KV) bool {
			if !item.isTombstone() && !item.HasVPtr() {
				keys = append(keys, item.Key)
			}
			return true
		})
		for _, key := range keys {
			if err := db.writeLockHeldPut(key, nil, true); err != nil {
				return false, err
			}
		}
	}

	// Phase 2: Walk FlexSpace and tombstone small-value keys.
	_, err = db.deleteRangeFlexSpaceClearSmall()
	return false, err
}

// writeLockHeldCoversAllKeys returns true if the given range covers every
// key in the database (memtable + FlexSpace). When true, the caller can
// use the fast "delete all" path instead of iterating.
//
// Caller must hold topMutRW.Lock().
func (db *FlexDB) writeLockHeldCoversAllKeys(begKey, endKey string, begInclusive, endInclusive bool) bool {
	inBounds := func(key string) bool {
		return deleteRangeInBounds(key, begKey, endKey, begInclusive, endInclusive)
	}

	// Check memtable min/max keys.
	if !db.mt.empty {
		var minKV KV
		var minFound bool
		db.mt.bt.Scan(func(item KV) bool {
			minKV = item
			minFound = true
			return false
		})
		if minFound && !inBounds(minKV.Key) {
			return false
		}
		var maxKV KV
		var maxFound bool
		db.mt.bt.Reverse(func(item KV) bool {
			maxKV = item
			maxFound = true
			return false
		})
		if maxFound && !inBounds(maxKV.Key) {
			return false
		}
	}

	// Check FlexSpace.
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
			if a != nil && a.key != "" {
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

// writeLockHeldDeleteAll reinitializes the database,
// discarding all data. This is the fast path for DeleteRange
// when the range covers all keys.
//
// Caller must hold topMutRW.Lock().
func (db *FlexDB) writeLockHeldDeleteAll() error {

	// 1. Clear memtable.
	db.mt.bt.Clear()
	db.mt.empty = true
	db.mt.size = 0

	// 2. Destroy interval cache.
	db.cache.destroyAll()

	// 3. Re-create a fresh FlexSpace.
	db.ff = NewFlexSpace()

	// 4. Reinitialize sparse index tree and cache.
	db.tree = memSparseIndexTreeCreate()
	db.cache = newCache(nil, db.cfg.CacheMB)
	db.cache.db = db
	for i := range db.cache.partitions {
		db.cache.partitions[i].db = db
	}

	// 5. Reset counters.
	atomic.StoreInt64(&db.LogicalBytesWritten, 0)
	db.liveKeys = 0
	db.liveBigKeys = 0
	db.liveSmallKeys = 0

	return nil
}

// deleteRangeInBounds returns true if key is within the range defined by
// [begKey, endKey] with the given inclusivity flags.
func deleteRangeInBounds(key, begKey, endKey string, begInclusive, endInclusive bool) bool {
	if begInclusive {
		if key < begKey {
			return false
		}
	} else {
		if key <= begKey {
			return false
		}
	}
	if endInclusive {
		if key > endKey {
			return false
		}
	} else {
		if key >= endKey {
			return false
		}
	}
	return true
}

// deleteRangePastEnd returns true if key is beyond the end bound.
func deleteRangePastEnd(key, endKey string, endInclusive bool) bool {
	if endInclusive {
		return key > endKey
	}
	return key >= endKey
}

// deleteRangeFlexSpace walks the sparse index tree's leaf linked list,
// decodes each interval directly from FlexSpace (bypassing the interval
// cache to avoid pollution), and writes tombstones for all non-tombstone
// keys within the specified bounds.
//
// Caller must hold topMutRW.Lock().
func (db *FlexDB) deleteRangeFlexSpace(begKey, endKey string, begInclusive, endInclusive, includeLarge bool) (int64, error) {
	var n int64
	target := begKey
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
			if anchor.key != "" && deleteRangePastEnd(anchor.key, endKey, endInclusive) {
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
				if seekStrict {
					if kv.Key <= target {
						continue
					}
				} else {
					if kv.Key < target {
						continue
					}
				}
				if deleteRangePastEnd(kv.Key, endKey, endInclusive) {
					return n, nil
				}
				if kv.isTombstone() {
					continue
				}
				if !includeLarge && kv.HasVPtr() {
					continue // skip large-value keys
				}

				// Write tombstone. Track flushSeq to detect inline flush.
				prevSeq := db.flushSeq
				if err := db.writeLockHeldPut(kv.Key, nil, true); err != nil {
					return n, err
				}
				n++

				if db.flushSeq != prevSeq {
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
// Clear(includeLarge=false).
//
// Caller must hold topMutRW.Lock().
func (db *FlexDB) deleteRangeFlexSpaceClearSmall() (int64, error) {
	var n int64
	var target string
	seekStrict := false
	firstIteration := true

	for {
		t := db.tree
		if t == nil || t.root == nil || t.leafHead == nil {
			return n, nil
		}

		var node *memSparseIndexTreeNode
		var anchorIdx int
		var shift int64

		if firstIteration && !seekStrict {
			firstIteration = false
			node = t.leafHead
			anchorIdx = 0
			nh := memSparseIndexTreeHandler{node: node}
			memSparseIndexTreeHandlerInfoUpdate(&nh)
			shift = nh.shift
		} else {
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
				if seekStrict {
					if kv.Key <= target {
						continue
					}
				} else if target != "" {
					if kv.Key < target {
						continue
					}
				}
				if kv.isTombstone() || kv.HasVPtr() {
					continue // skip tombstones and large-value keys
				}

				prevSeq := db.flushSeq
				if err := db.writeLockHeldPut(kv.Key, nil, true); err != nil {
					return n, err
				}
				n++

				if db.flushSeq != prevSeq {
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
	// All KV.SLOT_BLOCKS data should be slotted page format.
	if len(src) > 0 {
		panicf("decodeIntervalDirect: unexpected non-slotted data at loff=%d, %d trailing bytes, first 16 bytes: %x",
			anchorLoff, len(src), src[:min(len(src), 16)])
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
// entry for each key.
func deleteRangeDedup(kvs []KV) []KV {
	out := kvs[:0]
	i := 0
	for i < len(kvs) {
		best := i
		j := i + 1
		for j < len(kvs) && kvs[i].Key == kvs[j].Key {
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
func (db *FlexDB) Merge(key string, fn func(oldVal []byte, exists bool) (newVal []byte, write bool, doDelete bool)) error {
	db.topMutRW.Lock()
	defer db.topMutRW.Unlock()
	return db.writeLockHeldMerge(key, fn)
}

// writeLockHeldMerge is the lock-held body of Merge.
// Caller must hold topMutRW.Lock().
func (db *FlexDB) writeLockHeldMerge(key string, fn func(oldVal []byte, exists bool) (newVal []byte, write bool, doDelete bool)) error {
	if len(key)+16 >= MaxKeySize {
		return fmt.Errorf("flexdb: key too large for merge (max %d bytes)", MaxKeySize)
	}

	// Phase 1: check memtable.
	var oldVal []byte
	var exists bool

	if !db.mt.empty {
		kv, ok := db.mt.get(key)
		if ok {
			if !kv.isTombstone() {
				oldVal = kv.Value
				exists = true
			}
		}
	}

	if !exists {
		// Phase 2: check FlexSpace.
		val, found, err := db.getPassthrough(key)
		if err != nil {
			return fmt.Errorf("flexdb: merge getPassthrough: %w", err)
		}
		if found {
			oldVal = val
			exists = true
		}
	}

	// Apply user merge function.
	newVal, write, doDelete := fn(oldVal, exists)
	if write && doDelete {
		return fmt.Errorf("flexdb: Merge callback returned both doWrite=true and doDelete=true; these are mutually exclusive")
	}
	if !write && !doDelete {
		return nil
	}

	if doDelete {
		return db.writeLockHeldPut(key, nil, true)
	}

	// Validate size.
	if len(key)+len(newVal)+16 >= MaxKeySize {
		return fmt.Errorf("flexdb: merged KV too large (max %d bytes)", MaxKeySize)
	}

	return db.writeLockHeldPut(key, newVal, false)
}

// ====================== Passthrough operations ======================
// These operate directly on FlexSpace + sparse index.
// Caller must hold db.topMutRW.

func (db *FlexDB) getPassthrough(key string) ([]byte, bool, error) {
	var nh memSparseIndexTreeHandler
	db.tree.findAnchorPos(key, &nh)
	anchor := nh.node.anchors[nh.idx]
	anchorLoff := uint64(anchor.loff + nh.shift)
	partition := db.cache.getPartition(anchor)
	fce, err := partition.getEntry(anchor, anchorLoff, db)
	if err != nil {
		return nil, false, err
	}
	defer partition.releaseEntry(fce)

	idx, ok := intervalCacheEntryFindKeyEQ(fce, key)
	if !ok {
		return nil, false, nil
	}
	kv := fce.kvs[idx]
	if kv.isTombstone() {
		return nil, false, nil
	}
	// In RAM-only mode, all values are inline.
	val := kv.Value
	if val == nil {
		return nil, true, nil // live key, nil value
	}
	out := make([]byte, len(val))
	copy(out, val)
	return out, true, nil
}

// getPassthroughKV returns the full KV (including HLC) from the passthrough layer.
func (db *FlexDB) getPassthroughKV(key string) (KV, bool, error) {
	var nh memSparseIndexTreeHandler
	db.tree.findAnchorPos(key, &nh)
	anchor := nh.node.anchors[nh.idx]
	anchorLoff := uint64(anchor.loff + nh.shift)
	partition := db.cache.getPartition(anchor)
	fce, err := partition.getEntry(anchor, anchorLoff, db)
	if err != nil {
		return KV{}, false, err
	}
	defer partition.releaseEntry(fce)

	idx, ok := intervalCacheEntryFindKeyEQ(fce, key)
	if !ok {
		return KV{}, false, nil
	}
	return fce.kvs[idx], true, nil
}

func (db *FlexDB) putPassthrough(kv KV, nh *memSparseIndexTreeHandler) error {
	db.tree.treeNodeHandlerNextAnchor(nh, kv.Key)
	anchor := nh.node.anchors[nh.idx]
	anchorLoff := uint64(anchor.loff + nh.shift)
	partition := db.cache.getPartition(anchor)

	// Always load cache - no unsorted kv128 append path.
	fce, err := partition.getEntry(anchor, anchorLoff, db)
	if err != nil {
		partition.releaseEntry(fce) // getEntry always bumps refcnt
		return err
	}

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
	return nil
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

	// Encode as tight (unpadded) page.
	buf := slottedPageEncode(fce.kvs[:fce.count])
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

	// Check if the new KV would fit.
	replaceIdx := -1
	if eq {
		replaceIdx = idx
	}
	fitTarget := int(anchor.psize)
	if fitTarget < slottedPageMaxSize {
		fitTarget = slottedPageMaxSize
	}
	if !slottedPageWouldFit(fce.kvs, fce.count, kv, replaceIdx, fitTarget) {
		if eq {
			partition.cacheEntryReplace(fce, kv, idx)
			newSize := slottedPageComputeSize(fce.kvs[:fce.count])
			if newSize > int(anchor.psize) && newSize < 2*slottedPageMaxSize {
				buf := slottedPageEncode(fce.kvs[:fce.count])
				anchorLoff := uint64(anchor.loff + nh.shift)
				db.ff.Update(buf, anchorLoff, uint64(len(buf)), uint64(anchor.psize))
				nh.shiftUpPropagate(int64(len(buf)) - int64(anchor.psize))
				anchor.psize = uint32(len(buf))
				fce.dirty = false // just written
				fce.dirtyNode = nil
			} else if newSize >= 2*slottedPageMaxSize {
				db.treeInsertAnchor(nh, partition, fce)
				db.putPassthroughMarkDirty(nh, anchor, fce)
			} else {
				db.putPassthroughMarkDirty(nh, anchor, fce)
			}
			return
		}
		// Inserting a new key - page genuinely full. Split.
		partition.cacheEntryInsert(fce, kv, idx)
		db.treeInsertAnchor(nh, partition, fce)
		db.putPassthroughMarkDirty(nh, anchor, fce)
		return
	}

	// Update cache entry.
	if eq {
		partition.cacheEntryReplace(fce, kv, idx)
	} else {
		partition.cacheEntryInsert(fce, kv, idx)
	}

	// Mark dirty - will be written to FlexSpace on Sync or eviction.
	db.putPassthroughMarkDirty(nh, anchor, fce)
}

// putPassthroughMarkDirty marks fce as dirty so it will be written to
// FlexSpace on Sync or cache eviction.
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

	// Left half: encode and Update if psize changed.
	var leftBuf []byte
	if db.cfg.PaddedSplits {
		leftBuf = slottedPageEncodePadded(fce.kvs[:leftCount], slottedPageMaxSize)
	} else {
		leftBuf = slottedPageEncode(fce.kvs[:leftCount])
	}
	leftPSize := uint32(len(leftBuf))
	if leftPSize != anchor.psize {
		db.ff.Update(leftBuf, anchorLoff, uint64(leftPSize), uint64(anchor.psize))
		nh.shiftUpPropagate(int64(leftPSize) - int64(anchor.psize))
		anchor.psize = leftPSize
	}

	// Right half: encode and Insert.
	var rightBuf []byte
	if db.cfg.PaddedSplits {
		rightBuf = slottedPageEncodePadded(fce.kvs[leftCount:fce.count], slottedPageMaxSize)
	} else {
		rightBuf = slottedPageEncode(fce.kvs[leftCount:fce.count])
	}
	rightPSize := uint32(len(rightBuf))
	newAnchorLoff := anchorLoff + uint64(anchor.psize)
	db.ff.Insert(rightBuf, newAnchorLoff, uint64(rightPSize))
	nh.shiftUpPropagate(int64(rightPSize))

	// Compute left/right sizes for cache.
	leftSize := 0
	for i := 0; i < leftCount; i++ {
		leftSize += kvSizeApprox(&fce.kvs[i])
	}

	newAnchorKey := fce.kvs[leftCount].Key
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
// to find where tags go missing.
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

// clampAnchorPsizes walks the sparse index tree and fixes any anchor
// whose psize exceeds slottedPageMaxSize.
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
				newAnchor := nh.handlerInsert(subKey, subLoff, uint32(slottedPageMaxSize))
				_ = newAnchor
				nh.idx--

				// Set the tag on this extent.
				tag := flexdbTagGenerate(true, 0)
				db.ff.SetTag(subLoff, tag)

				subLoff += uint64(slottedPageMaxSize)
				remaining -= uint64(slottedPageMaxSize)
			}

			// Re-scan this leaf since we may have inserted anchors.
			i = -1 // will be incremented to 0
		}
		leaf = leaf.next
	}
}

// rebuildAnchorsFromTags walks all FlexTree extents, finds anchor tags,
// and rebuilds the sparse index tree from scratch. Caller must hold topMutRW.
func (db *FlexDB) rebuildAnchorsFromTags(panicOnFailure bool) {
	type anchorInfo struct {
		key      string
		loff     uint64
		unsorted uint8
	}

	ffSize := db.ff.Size()
	if ffSize == 0 {
		return
	}

	// Destroy old anchor tree, create fresh one with sentinel.
	db.tree = memSparseIndexTreeCreate()

	var anchors []anchorInfo
	kvbuf := make([]byte, MaxKeySize)
	fh := db.ff.GetHandler(0)

	for fh.Valid() && fh.Loff() < ffSize {
		tag, err := fh.GetTag()
		if err == nil && flexdbTagIsAnchor(tag) {
			loff := fh.Loff()
			unsorted := flexdbTagUnsorted(tag)
			kv, ok := flexdbReadKVFromHandler(fh, kvbuf, panicOnFailure)
			if ok {
				var anchorKey string
				if loff > 0 {
					anchorKey = kv.Key
				}
				anchors = append(anchors, anchorInfo{key: anchorKey, loff: loff, unsorted: unsorted})
			} else {
				if panicOnFailure {
					alwaysPrintf("rebuildAnchorsFromTags: corrupt anchor at loff=%d tag=0x%04x: flexdbReadKVFromHandler could not read first key", loff, tag)
					panicf("rebuildAnchorsFromTags: corrupt anchor at loff=%d tag=0x%04x: flexdbReadKVFromHandler could not read first key", loff, tag)
				} else {
					alwaysPrintf("rebuildAnchorsFromTags: WARNING: skipping unreadable anchor at loff=%d tag=0x%04x", loff, tag)
				}
			}
		}
		fh.ForwardExtent()
	}

	// Build sparse index tree from collected anchors (in order).
	var nh memSparseIndexTreeHandler
	db.tree.findAnchorPos("", &nh)
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

	// Set last anchor's psize.
	if nh.node != nil && nh.idx < nh.node.count {
		last := nh.node.anchors[nh.idx]
		last.psize = uint32(ffSize - lastAnchorLoff)
	}
}

// flexdbReadKVFromHandler reads the first KV (key only needed for anchor)
// from a handler's current position (does NOT advance the handler).
// Handles slotted page format.
func flexdbReadKVFromHandler(fh FlexSpaceHandler, buf []byte, panicOnFailure bool) (KV, bool) {
	// NOTE: FlexSpaceHandler.Read does NOT advance the handler position.

	// Read the 16-byte magic prefix.
	var magic [slottedPageMagicSize]byte
	n, err := fh.Read(magic[:], slottedPageMagicSize)
	if n < slottedPageMagicSize || err != nil {
		alwaysPrintf("flexdbReadKVFromHandler: magic read fail at loff=%d n=%d err=%v", fh.Loff(), n, err)
		if panicOnFailure {
			panicf("flexdbReadKVFromHandler: magic read fail at loff=%d n=%d err=%v", fh.Loff(), n, err)
		}
		return KV{}, false
	}

	if slottedPageHasMagic(magic[:]) {
		needHeader := slottedPageHeaderSize + 4 + binary.MaxVarintLen64
		if needHeader > len(buf) {
			alwaysPrintf("flexdbReadKVFromHandler: buf too small for slotted header at loff=%d need=%d bufLen=%d", fh.Loff(), needHeader, len(buf))
			if panicOnFailure {
				panicf("flexdbReadKVFromHandler: buf too small for slotted header at loff=%d need=%d bufLen=%d", fh.Loff(), needHeader, len(buf))
			}
			return KV{}, false
		}
		nr, err2 := fh.Read(buf[:needHeader], uint64(needHeader))
		if nr < slottedPageHeaderSize+4 || err2 != nil {
			alwaysPrintf("flexdbReadKVFromHandler: slotted header read fail at loff=%d nr=%d err=%v", fh.Loff(), nr, err2)
			if panicOnFailure {
				panicf("flexdbReadKVFromHandler: slotted header read fail at loff=%d nr=%d err=%v", fh.Loff(), nr, err2)
			}
			return KV{}, false
		}
		keyLen := int(binary.LittleEndian.Uint16(buf[slottedPageHeaderSize : slottedPageHeaderSize+2]))
		needBytes := slottedPageHeaderSize + 4 + binary.MaxVarintLen64 + keyLen
		if needBytes > len(buf) {
			alwaysPrintf("flexdbReadKVFromHandler: buf too small at loff=%d needBytes=%d bufLen=%d keyLen=%d", fh.Loff(), needBytes, len(buf), keyLen)
			if panicOnFailure {
				panicf("flexdbReadKVFromHandler: buf too small at loff=%d needBytes=%d bufLen=%d keyLen=%d", fh.Loff(), needBytes, len(buf), keyLen)
			}
			return KV{}, false
		}
		if needBytes > nr {
			nr2, err3 := fh.Read(buf[:needBytes], uint64(needBytes))
			if nr2 < needBytes || err3 != nil {
				alwaysPrintf("flexdbReadKVFromHandler: slotted data read fail at loff=%d nr=%d needBytes=%d err=%v", fh.Loff(), nr2, needBytes, err3)
				if panicOnFailure {
					panicf("flexdbReadKVFromHandler: slotted data read fail at loff=%d nr=%d needBytes=%d err=%v", fh.Loff(), nr2, needBytes, err3)
				}
				return KV{}, false
			}
			nr = nr2
		}
		key, ok := slottedPageFirstKey(buf[:nr])
		if !ok {
			alwaysPrintf("flexdbReadKVFromHandler: slottedPageFirstKey fail at loff=%d datalen=%d header=%x", fh.Loff(), nr, buf[:min(nr, 32)])
			if panicOnFailure {
				panicf("flexdbReadKVFromHandler: slottedPageFirstKey fail at loff=%d datalen=%d header=%x", fh.Loff(), nr, buf[:min(nr, 32)])
			}
			return KV{}, false
		}
		return KV{Key: key}, true
	}

	// kv128 format is no longer written to KV.SLOT_BLOCKS.
	alwaysPrintf("flexdbReadKVFromHandler: unexpected format at loff=%d magic=%x (expected slotted page)", fh.Loff(), magic[:])
	if panicOnFailure {
		panicf("flexdbReadKVFromHandler: unexpected format at loff=%d magic=%x (expected slotted page)", fh.Loff(), magic[:])
	}
	return KV{}, false
}

func (db *FlexDB) flushMemtable() {
	m := &db.mt
	var nh memSparseIndexTreeHandler
	batch := make([]KV, 0, memtableFlushBatch)

	m.bt.Ascend(KV{}, func(item KV) bool {
		batch = append(batch, item)
		if len(batch) >= memtableFlushBatch {
			for _, kv := range batch {
				if err := db.putPassthrough(kv, &nh); err != nil {
					panicf("flushMemtable: putPassthrough: %v", err)
				}
				nh.node = nil // reset hint after each for simplicity
			}
			batch = batch[:0]
		}
		return true
	})
	for _, kv := range batch {
		if err := db.putPassthrough(kv, &nh); err != nil {
			panicf("flushMemtable: putPassthrough: %v", err)
		}
		nh.node = nil
	}
}
