package yogadb

import (
	"encoding/binary"
	"hash/crc32"
	"sync"
	"sync/atomic"

	"github.com/tidwall/btree"

	"github.com/glycerine/vfs"
)

// ====================== memtable ======================

type memtable struct {
	// backing in memory B-tree (was skiplist in C).
	bt *btree.BTreeG[KV]

	memWalFD  vfs.File // FLEXDB.MEMWAL1 or FLEXDB.MEMWAL2
	memWalBuf []byte

	memWalMut sync.Mutex
	size      int64 // approximate bytes in this memtable
	empty     bool  // true when memtable has no data (freshly created or flushed+cleared)

	// metric to update for observability
	memWalBytesWritten *int64 // points to FlexDB.WALBytesWritten (nil if standalone)
}

func newMemtable(memWalFD vfs.File) *memtable {
	return &memtable{
		// default degree is 32, to change it:
		bt:        btree.NewBTreeGOptions[KV](kvLess, btree.Options{Degree: 32}),
		memWalFD:  memWalFD,
		memWalBuf: make([]byte, 0, memtableWalBufCap),
		empty:     true,
	}
}

func (m *memtable) reset() {
	m.empty = true
	m.size = 0
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

func (m *memtable) get(key []byte) (KV, bool) {
	return m.bt.Get(KV{Key: key})
}

func (m *memtable) logAppend(kv KV) {
	encoded := kv128Encode(nil, kv)
	m.memWalMut.Lock()
	defer m.memWalMut.Unlock()
	if len(m.memWalBuf)+len(encoded) >= memtableWalBufCap {
		m.logFlushLocked()
	}
	m.memWalBuf = append(m.memWalBuf, encoded...)
}

func (m *memtable) logFlushLocked() {
	if len(m.memWalBuf) == 0 {
		return
	}
	nw, err := m.memWalFD.Write(m.memWalBuf)
	panicOn(err) // TODO handle file system out of space/disk error.
	if m.memWalBytesWritten != nil {
		atomic.AddInt64(m.memWalBytesWritten, int64(nw))
	}
	m.memWalBuf = m.memWalBuf[:0]
}

func (m *memtable) logFlush() {
	m.memWalMut.Lock()
	m.logFlushLocked()
	m.memWalMut.Unlock()
}

// logSyncLocked flushes any buffered WAL data and fdatasyncs the WAL file.
// Caller must hold m.memWalMut.
func (m *memtable) logSyncLocked() {
	m.logFlushLocked()
	panicOn(m.memWalFD.SyncData())
}

func (m *memtable) logSync() {
	m.memWalMut.Lock()
	defer m.memWalMut.Unlock()
	m.logSyncLocked()
}

func (m *memtable) logTimestamp() uint64 {
	var buf [20]byte
	n, _ := m.memWalFD.ReadAt(buf[:], 0)
	// Try 20-byte format first: [8 ts][8 ver][4 CRC(0:16)]
	if n >= 20 && crc32.Checksum(buf[:16], crc32cTable) == binary.LittleEndian.Uint32(buf[16:20]) {
		return binary.BigEndian.Uint64(buf[:8])
	}
	return 0
}

// logTruncateWithVersion writes a 20-byte header including tree version.
// Format: [8-byte timestamp BE][8-byte treeVersion LE][4-byte CRC32C(first 16 bytes)]
func (m *memtable) logTruncateWithVersion(timestamp, treeVersion uint64) {
	panicOn(m.memWalFD.Truncate(0))
	var buf [20]byte
	binary.BigEndian.PutUint64(buf[:8], timestamp)
	binary.LittleEndian.PutUint64(buf[8:16], treeVersion)
	binary.LittleEndian.PutUint32(buf[16:20], crc32.Checksum(buf[:16], crc32cTable))
	_, err := m.memWalFD.WriteAt(buf[:], 0)
	panicOn(err)
	if m.memWalBytesWritten != nil {
		atomic.AddInt64(m.memWalBytesWritten, 20)
	}
}

// logTreeVersion returns the tree PersistentVersion from a 20-byte WAL header.
// Returns 0 for old 12-byte format or corrupt headers.
func (m *memtable) logTreeVersion() uint64 {
	var buf [20]byte
	n, _ := m.memWalFD.ReadAt(buf[:], 0)
	if n >= 20 && crc32.Checksum(buf[:16], crc32cTable) == binary.LittleEndian.Uint32(buf[16:20]) {
		return binary.LittleEndian.Uint64(buf[8:16])
	}
	return 0
}

// memWalDataOffset returns the byte offset where KV entries start (after the header).
// Detects 20-byte vs 12-byte header format.
func (m *memtable) memWalDataOffset() int64 {
	var buf [20]byte
	n, _ := m.memWalFD.ReadAt(buf[:], 0)
	if n >= 20 && crc32.Checksum(buf[:16], crc32cTable) == binary.LittleEndian.Uint32(buf[16:20]) {
		return 20
	}
	return 12
}

func (m *memtable) memWalSize() int64 {
	fi, err := m.memWalFD.Stat()
	if err != nil {
		return 0
	}
	return fi.Size()
}
