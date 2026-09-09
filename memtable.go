package yogadb

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"sync"
	"sync/atomic"

	"github.com/tidwall/btree"

	"github.com/glycerine/vfs"
)

// ====================== memtable ======================

type memtable struct {
	// backing in memory B-tree (was skiplist in C).
	bt *btree.BTreeG[KV]
	// bulk is used only for pristine initial batch loads. It
	// avoid per-key B-tree insertion until a read requires materialization or
	// Sync streams the sorted entries directly to FlexSpace.
	bulk bulkIngestBuilder

	memWalFD          vfs.File // FLEXDB.MEMWAL
	memWalBuf         []byte
	memWalEncodeBuf   []byte
	memWalWriteOffset int64

	memWalMut sync.Mutex
	size      int64 // approximate bytes in this memtable
	empty     bool  // true when memtable has no data (freshly created or flushed+cleared)

	// metric to update for observability
	memWalBytesWritten *int64 // points to FlexDB.WALBytesWritten (nil if standalone)
}

func newMemtable(memWalFD vfs.File) *memtable {
	return &memtable{
		// default degree is 32, to change it:
		bt:                btree.NewBTreeGOptions[KV](kvLess, btree.Options{Degree: 32}),
		memWalFD:          memWalFD,
		memWalBuf:         make([]byte, 0, memtableWalBufCap),
		memWalWriteOffset: memWalHeaderSize,
		empty:             true,
	}
}

func (m *memtable) reset() {
	m.empty = true
	m.size = 0
	m.bulk.reset()
}

// caller should set m.empty to false after calling put()
// (e.g. db.go:165 in Batch.Commit)
// Returns the previous KV for the same key and whether it was replaced.
func (m *memtable) put(kv KV) (KV, bool) {
	old, replaced := m.bt.Set(kv)
	if replaced {
		m.size -= int64(kvSizeApprox(&old))
	}
	m.size += int64(kvSizeApprox(&kv))
	if m.size <= 0 {
		panicf("bad: memtable with some content should have size(%v) > 0: %#v", m.size, m)
	}
	return old, replaced
}

func (m *memtable) putBulk(kv KV) (KV, bool) {
	m.bulk.appendBatch([]KV{kv})
	m.size = m.bulk.size
	if m.size <= 0 {
		panicf("bad: memtable with some content should have size(%v) > 0: %#v", m.size, m)
	}
	return KV{}, false
}

func (m *memtable) appendBulkBatch(kvs []KV) {
	m.bulk.appendBatch(kvs)
	m.size = m.bulk.size
	if m.size <= 0 {
		panicf("bad: memtable with some content should have size(%v) > 0: %#v", m.size, m)
	}
}

func (m *memtable) materializeBulk() {
	if m.bulk.count == 0 {
		return
	}
	for si := range m.bulk.segments {
		kvs := m.bulk.segments[si].kvs
		for i := range kvs {
			m.bt.Set(kvs[i])
		}
	}
	m.bulk.reset()
}

func (m *memtable) get(key string) (KV, bool) {
	if kv, ok := m.bulk.get(key); ok {
		return kv, true
	}
	return m.bt.Get(KV{Key: key})
}

func (m *memtable) logAppend(kv KV) error {
	m.memWalMut.Lock()
	defer m.memWalMut.Unlock()
	return m.logAppendKVLocked(kv)
}

func (m *memtable) logAppendKVLocked(kv KV) error {
	g := GreenMEMWAL_KV{
		WalRecordType: MEMWAL_KV,
		VptrLength:    kv.Vptr.Length,
		VptrOffset:    kv.Vptr.Offset,
		Hlc:           int64(kv.Hlc),
		Key:           kv.Key,
		InlineVal:     kv.Value,
	}
	return m.logAppendGreenLocked(&g)
}

func (m *memtable) logAppendWalRecordType(recordType int32) error {
	m.memWalMut.Lock()
	defer m.memWalMut.Unlock()
	return m.logAppendWalRecordTypeLocked(recordType)
}

func (m *memtable) logAppendWalRecordTypeLocked(recordType int32) error {
	g := GreenMEMWAL_KV{WalRecordType: recordType}
	return m.logAppendGreenLocked(&g)
}

func (m *memtable) logAppendGreenLocked(g *GreenMEMWAL_KV) error {
	payload := g.appendCompactPayload(m.memWalEncodeBuf[:0])
	m.memWalEncodeBuf = payload
	return m.logAppendPayloadLocked(payload)
}

func (m *memtable) logAppendBatchLocked(kvs []KV) (bool, error) {
	payloadSize := compactBatchHLCPayloadMaxSize(kvs)
	recordSize := msgpackByteSliceFrameSize(payloadSize) + msgpackByteSliceFrameSize(8)
	if recordSize >= memtableWalBufCap {
		return false, nil
	}
	var hlc HLC
	if len(kvs) > 0 {
		hlc = kvs[0].Hlc
	}
	payload := appendCompactBatchHLCPayload(m.memWalEncodeBuf[:0], kvs, hlc)
	m.memWalEncodeBuf = payload
	return true, m.logAppendPayloadLocked(payload)
}

func (m *memtable) logAppendPayloadLocked(payload []byte) error {
	recordSize := msgpackByteSliceFrameSize(len(payload)) + msgpackByteSliceFrameSize(8)
	if recordSize >= memtableWalBufCap {
		return fmt.Errorf("memtable WAL record too large: size %d, max %d", recordSize, memtableWalBufCap-1)
	}
	if len(m.memWalBuf)+recordSize >= memtableWalBufCap {
		if err := m.logFlushLocked(); err != nil {
			return err
		}
	}
	m.memWalBuf = appendMsgpackByteSlice(m.memWalBuf, payload)
	var crcBuf [8]byte = [8]byte{'1', '2', '3', '4', '=', '=', '=', '\n'}
	binary.LittleEndian.PutUint32(crcBuf[:4], crc32.Checksum(payload, crc32cTable))
	m.memWalBuf = appendMsgpackByteSlice(m.memWalBuf, crcBuf[:])
	return nil
}

func msgpackByteSliceFrameSize(n int) int {
	switch {
	case n <= 0xff:
		return 2 + n
	case n <= 0xffff:
		return 3 + n
	default:
		return 5 + n
	}
}

func appendMsgpackByteSlice(dst []byte, b []byte) []byte {
	switch n := len(b); {
	case n <= 0xff:
		dst = append(dst, bin8, byte(n))
	case n <= 0xffff:
		dst = append(dst, bin16, byte(n>>8), byte(n))
	default:
		dst = append(dst, bin32, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	}
	return append(dst, b...)
}

func (m *memtable) logFlushLocked() error {
	if len(m.memWalBuf) == 0 {
		return nil
	}
	nw, err := m.memWalFD.WriteAt(m.memWalBuf, m.memWalWriteOffset)
	if err != nil {
		return fmt.Errorf("memtable WAL write offset=%d len=%d: %w", m.memWalWriteOffset, len(m.memWalBuf), err)
	}
	if nw != len(m.memWalBuf) {
		return fmt.Errorf("memtable WAL short write offset=%d: wrote %d bytes, want %d",
			m.memWalWriteOffset, nw, len(m.memWalBuf))
	}
	m.memWalWriteOffset += int64(nw)
	if m.memWalBytesWritten != nil {
		atomic.AddInt64(m.memWalBytesWritten, int64(nw))
	}
	m.memWalBuf = m.memWalBuf[:0]
	return nil
}

func (m *memtable) logFlush() error {
	m.memWalMut.Lock()
	defer m.memWalMut.Unlock()
	return m.logFlushLocked()
}

// logSyncLocked flushes any buffered WAL data and fdatasyncs the WAL file.
// Caller must hold m.memWalMut.
func (m *memtable) logSyncLocked() error {
	if err := m.logFlushLocked(); err != nil {
		return err
	}
	if err := m.memWalFD.SyncData(); err != nil {
		return fmt.Errorf("memtable WAL sync: %w", err)
	}
	return nil
}

func (m *memtable) logSync() error {
	m.memWalMut.Lock()
	defer m.memWalMut.Unlock()
	return m.logSyncLocked()
}

func (m *memtable) logTimestamp() (uint64, error) {
	var buf [memWalHeaderSize]byte
	n, err := m.memWalFD.ReadAt(buf[:], 0)
	if err != nil && err != io.EOF {
		return 0, fmt.Errorf("memtable WAL read header timestamp: %w", err)
	}
	if n == memWalHeaderSize && memWalHeaderValid(buf[:]) {
		return binary.BigEndian.Uint64(buf[:8]), nil
	}
	return 0, nil
}

const memWalHeaderSize = 20

func memWalHeaderValid(buf []byte) bool {
	return len(buf) >= memWalHeaderSize &&
		crc32.Checksum(buf[:16], crc32cTable) == binary.LittleEndian.Uint32(buf[16:20])
}

// logTruncateWithVersion writes a 20-byte header including tree version.
// Format: [8-byte timestamp BE][8-byte treeVersion LE][4-byte CRC32C(first 16 bytes)]
func (m *memtable) logTruncateWithVersion(timestamp, treeVersion uint64) error {
	if err := m.memWalFD.Truncate(0); err != nil {
		return fmt.Errorf("memtable WAL truncate: %w", err)
	}
	var buf [memWalHeaderSize]byte
	binary.BigEndian.PutUint64(buf[:8], timestamp)
	binary.LittleEndian.PutUint64(buf[8:16], treeVersion)
	binary.LittleEndian.PutUint32(buf[16:20], crc32.Checksum(buf[:16], crc32cTable))
	nw, err := m.memWalFD.WriteAt(buf[:], 0)
	if err != nil {
		return fmt.Errorf("memtable WAL write header: %w", err)
	}
	if nw != len(buf) {
		return fmt.Errorf("memtable WAL header short write: wrote %d bytes, want %d", nw, len(buf))
	}
	m.memWalWriteOffset = memWalHeaderSize
	if m.memWalBytesWritten != nil {
		atomic.AddInt64(m.memWalBytesWritten, memWalHeaderSize)
	}
	m.memWalBuf = m.memWalBuf[:0]
	return nil
}

// logTreeVersion returns the tree PersistentVersion from a 20-byte WAL header.
// Returns 0 for a missing or corrupt 20-byte header.
func (m *memtable) logTreeVersion() (uint64, error) {
	var buf [memWalHeaderSize]byte
	n, err := m.memWalFD.ReadAt(buf[:], 0)
	if err != nil && err != io.EOF {
		return 0, fmt.Errorf("memtable WAL read header tree version: %w", err)
	}
	if n == memWalHeaderSize && memWalHeaderValid(buf[:]) {
		return binary.LittleEndian.Uint64(buf[8:16]), nil
	}
	return 0, nil
}

// memWalDataOffset returns the byte offset where KV entries start (after the header).
func (m *memtable) memWalDataOffset() int64 {
	return memWalHeaderSize
}

func (m *memtable) memWalSize() (int64, error) {
	fi, err := m.memWalFD.Stat()
	if err != nil {
		return 0, fmt.Errorf("memtable WAL stat: %w", err)
	}
	return fi.Size(), nil
}
