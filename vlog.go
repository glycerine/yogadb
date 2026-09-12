package yogadb

// vlog.go - Append-only value log for large-value separation (WiscKey-style).
//
// Values larger than vlogInlineThreshold are stored in the VLOG file instead of
// inline in FlexSpace. This reduces write amplification: large values are written
// once to VLOG and never rewritten by FlexSpace GC, compaction, or interval rewrites.
//
// On-disk layout:
//   {db_dir}/VLOG - append-only file of value entries
//
// Each VLOG entry (56-byte header + N-byte value):
//   [4 bytes]  hdrCRC  - CRC32C of bytes 4..56 (HLC + length + valCRC + blake3)
//   [8 bytes]  HLC timestamp (big-endian int64 - preserves sort order in byte scans)
//   [8 bytes]  value length (little-endian uint64 - supports values up to int64 max)
//   [4 bytes]  valCRC  - CRC32C of the N value bytes that follow
//   [32 bytes] blake3  - Blake3 cryptographic checksum of the value bytes
//   [N bytes]  raw value bytes
//
// The blake3 checksum enables cheap dedup: before appending a new VLOG entry,
// we can read just the 56-byte header of the old entry and compare blake3
// checksums. If they match (and lengths match), the value is unchanged and
// we can skip the VLOG write entirely, reusing the old VPtr.
//
// IMPORTANT: HLC STALENESS IN VLOG HEADERS
//
// When dedup reuses an old VPtr, the HLC stored in the VLOG entry header
// becomes stale - it reflects the original write, not the latest overwrite.
// This is intentional and correct. The AUTHORITATIVE HLC for any key-value
// pair lives in the KV128 encoding in FLEXSPACE.KV.SLOT_BLOCKS (and in the
// memtable/WAL before flush). The VLOG header HLC is NOT authoritative.
// It is only consulted by VacuumVLOG when rewriting entries, at which point
// the KV128's HLC should be used instead. This design avoids bloating the
// VLOG with duplicate value bytes just because the HLC changed.
//
// The two-CRC design allows validating the header (HLC + length + blake3)
// without reading the value bytes.
//
// The KV encoding in FlexSpace stores a "value pointer" (VPtr) instead of inline
// value bytes when the value is in VLOG. VPtr is 16 bytes: 8-byte offset + 8-byte length.

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	///"os"
	"sync"
	"sync/atomic"

	"github.com/glycerine/vfs"
)

const (
	// Values <= than this threshold remain inline with their key in FlexSpace.
	// Values larger than this threshold are stored in the VLOG file.
	// 64 bytes balances scan performance (small values inline) vs write
	// amplification (large values written once to VLOG, never rewritten).
	vlogInlineThreshold = 64

	// VLOG entry header: 4-byte hdrCRC + 8-byte HLC + 8-byte length + 4-byte valCRC + 32-byte blake3 = 56 bytes
	vlogEntryHeaderSize = 56

	// VPtr size in kv128 encoding: 8-byte offset + 8-byte length
	vptrSize = 16
)

// VPtr is a pointer to a value stored in the VLOG file.
type VPtr struct {
	Offset uint64 // byte offset in VLOG file
	Length uint64 // value length in bytes
}

func (vp VPtr) encode(buf []byte) {
	binary.LittleEndian.PutUint64(buf[0:8], vp.Offset)
	binary.LittleEndian.PutUint64(buf[8:16], vp.Length)
}

func decodeVPtr(buf []byte) VPtr {
	return VPtr{
		Offset: binary.LittleEndian.Uint64(buf[0:8]),
		Length: binary.LittleEndian.Uint64(buf[8:16]),
	}
}

// valueLog is an append-only log for storing large values.
type valueLog struct {
	vfs  vfs.FS
	fd   vfs.File
	mu   sync.Mutex // protects writes (appends are serialized)
	tail int64      // next write offset (also = file size)

	batchBuf       []byte
	batchB3s       [][32]byte
	batchDedup     []bool
	headerBatchBuf []byte

	// Write-byte counter (accessed atomically)
	VLOGBytesWritten int64
	VLOGFsyncs       int64
}

// openValueLog opens or creates the VLOG file.
func openValueLog(path string, fs vfs.FS) (*valueLog, error) {
	//fd, err := fs.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	fd, err := fs.OpenReadWrite(path, vfs.WriteCategoryUnspecified)

	if err != nil {
		return nil, fmt.Errorf("vlog: open %s: %w", path, err)
	}
	fi, err := fd.Stat()
	if err != nil {
		fd.Close()
		return nil, fmt.Errorf("vlog: stat %s: %w", path, err)
	}
	return &valueLog{
		vfs:  fs,
		fd:   fd,
		tail: fi.Size(),
	}, nil
}

// append writes a value to the VLOG and returns a VPtr.
// Does NOT fsync - caller must call sync() when durability is needed.
// Thread-safe (serialized by mu).
func (vl *valueLog) append(value []byte, hlc HLC) (VPtr, error) {
	vl.mu.Lock()
	defer vl.mu.Unlock()
	return vl.appendLocked(value, hlc)
}

// appendLocked appends a value while mu is already held.
// On-disk format: [4B hdrCRC][8B HLC][8B length][4B valCRC][32B blake3][NB value]
// hdrCRC covers bytes 4..56 (HLC + length + valCRC + blake3).
func (vl *valueLog) appendLocked(value []byte, hlc HLC) (VPtr, error) {
	return vl.appendLockedWithHash(value, hlc, blake3checksum32(value))
}

// appendLockedWithHash is like appendLocked but accepts a pre-computed blake3
// hash to avoid recomputing it when the caller already has it (e.g., dedup path).
func (vl *valueLog) appendLockedWithHash(value []byte, hlc HLC, b3 [32]byte) (VPtr, error) {
	vlen := uint64(len(value))
	entrySize := vlogEntryHeaderSize + int(vlen)

	// Build entry: [hdrCRC][HLC][length][valCRC][blake3][value...]
	buf := make([]byte, entrySize)
	binary.BigEndian.PutUint64(buf[4:12], uint64(hlc))
	binary.LittleEndian.PutUint64(buf[12:20], vlen)
	copy(buf[vlogEntryHeaderSize:], value)
	// valCRC covers the value bytes.
	valCRC := crc32.Checksum(buf[vlogEntryHeaderSize:], crc32cTable)
	binary.LittleEndian.PutUint32(buf[20:24], valCRC)
	// blake3 checksum of value bytes.
	copy(buf[24:56], b3[:])
	// hdrCRC covers bytes 4..56 (HLC + length + valCRC + blake3).
	hdrCRC := crc32.Checksum(buf[4:56], crc32cTable)
	binary.LittleEndian.PutUint32(buf[0:4], hdrCRC)

	offset := vl.tail
	if err := writeAtFull(vl.fd, buf, offset, "vlog"); err != nil {
		return VPtr{}, err
	}
	atomic.AddInt64(&vl.VLOGBytesWritten, int64(entrySize))
	vl.tail = offset + int64(entrySize)

	return VPtr{
		Offset: uint64(offset),
		Length: vlen,
	}, nil
}

func appendVLOGEntryToBuffer(buf []byte, value []byte, hlc HLC, b3 [32]byte) []byte {
	start := len(buf)
	entrySize := vlogEntryHeaderSize + len(value)
	buf = buf[:start+entrySize]
	entry := buf[start : start+entrySize]

	binary.BigEndian.PutUint64(entry[4:12], uint64(hlc))
	binary.LittleEndian.PutUint64(entry[12:20], uint64(len(value)))
	valCRC := crc32.Checksum(value, crc32cTable)
	binary.LittleEndian.PutUint32(entry[20:24], valCRC)
	copy(entry[24:56], b3[:])
	copy(entry[vlogEntryHeaderSize:], value)
	hdrCRC := crc32.Checksum(entry[4:56], crc32cTable)
	binary.LittleEndian.PutUint32(entry[0:4], hdrCRC)
	return buf
}

// appendAndSync writes a value to the VLOG, fsyncs, and returns a VPtr.
// This ensures the value is durable before the caller writes the VPtr to WAL.
func (vl *valueLog) appendAndSync(value []byte, hlc HLC, skipSync bool) (VPtr, error) {
	vl.mu.Lock()
	defer vl.mu.Unlock()
	vp, err := vl.appendLocked(value, hlc)
	if err != nil {
		return vp, err
	}
	if !skipSync {
		if err := vl.syncFile(); err != nil {
			return vp, fmt.Errorf("vlog: sync: %w", err)
		}
	}
	return vp, nil
}

// appendDedupAndSync checks if the new value matches the old VLOG entry at
// oldVP (by comparing blake3 checksums of the 56-byte headers). If the value
// is unchanged, it returns the old VPtr without writing anything. Otherwise
// it appends the new value normally.
//
// The caller provides the pre-computed blake3 of the new value to avoid
// computing it twice (once here, once in appendLocked).
//
// See "HLC STALENESS IN VLOG HEADERS" comment at the top of this file:
// when dedup reuses an old VPtr, the VLOG header HLC becomes stale.
// The authoritative HLC lives in KV.SLOT_BLOCKS / memtable / WAL.
func (vl *valueLog) appendDedupAndSync(value []byte, hlc HLC, oldVP VPtr, skipSync bool) (VPtr, bool, error) {
	newB3 := blake3checksum32(value)

	vl.mu.Lock()
	defer vl.mu.Unlock()

	// Check if old entry has the same blake3. Length match is implied by
	// the caller only calling us when oldVP.Length == len(value).
	if oldVP.Length > 0 {
		oldB3, err := vl.readBlake3(oldVP)
		if err == nil && newB3 == oldB3 {
			// Value unchanged - reuse old VPtr, skip VLOG write.
			return oldVP, true, nil
		}
	}

	// Value changed (or old entry unreadable) - append new entry.
	vp, err := vl.appendLockedWithHash(value, hlc, newB3)
	if err != nil {
		return vp, false, err
	}
	if !skipSync {
		if err := vl.syncFile(); err != nil {
			return vp, false, fmt.Errorf("vlog: sync: %w", err)
		}
	}
	return vp, false, nil
}

// appendBatchDedupAndSync is like appendBatchAndSync but skips VLOG writes
// for values whose blake3 matches the existing entry at oldVPs[i].
// An oldVP with Length==0 means "no previous entry" (always append).
// Returns one VPtr per value and the count of dedup hits.
func (vl *valueLog) appendBatchDedupAndSync(values [][]byte, hlc HLC, oldVPs []VPtr, skipSync bool) ([]VPtr, int, error) {
	vl.mu.Lock()
	defer vl.mu.Unlock()

	if cap(vl.batchB3s) < len(values) {
		vl.batchB3s = make([][32]byte, len(values))
	} else {
		vl.batchB3s = vl.batchB3s[:len(values)]
	}
	b3s := vl.batchB3s
	for i, v := range values {
		b3s[i] = blake3checksum32(v)
	}

	ptrs := make([]VPtr, len(values))
	dedupHits := 0
	startOffset := vl.tail
	writeBufLen := 0
	hasDedupCandidates := false
	for i, v := range values {
		writeBufLen += vlogEntryHeaderSize + len(v)
		if oldVPs[i].Length == uint64(len(v)) && oldVPs[i].Length > 0 {
			hasDedupCandidates = true
		}
	}
	if cap(vl.batchBuf) < writeBufLen {
		vl.batchBuf = make([]byte, 0, writeBufLen)
	} else {
		vl.batchBuf = vl.batchBuf[:0]
	}
	writeBuf := vl.batchBuf
	var dedup []bool
	if hasDedupCandidates {
		if cap(vl.batchDedup) < len(values) {
			vl.batchDedup = make([]bool, len(values))
		} else {
			vl.batchDedup = vl.batchDedup[:len(values)]
			clear(vl.batchDedup)
		}
		dedup = vl.batchDedup
		vl.markBatchDedupHits(values, b3s, oldVPs, dedup)
	}

	for i, v := range values {
		if hasDedupCandidates && dedup[i] {
			ptrs[i] = oldVPs[i]
			dedupHits++
			continue
		}
		// Append new entry.
		ptrs[i] = VPtr{
			Offset: uint64(startOffset) + uint64(len(writeBuf)),
			Length: uint64(len(v)),
		}
		writeBuf = appendVLOGEntryToBuffer(writeBuf, v, hlc, b3s[i])
	}
	vl.batchBuf = writeBuf[:0]
	if len(writeBuf) > 0 {
		if err := writeAtFull(vl.fd, writeBuf, startOffset, "vlog batch"); err != nil {
			return nil, dedupHits, err
		}
		atomic.AddInt64(&vl.VLOGBytesWritten, int64(len(writeBuf)))
		vl.tail = startOffset + int64(len(writeBuf))
	}
	if len(writeBuf) > 0 && !skipSync {
		if err := vl.syncFile(); err != nil {
			return nil, dedupHits, fmt.Errorf("vlog: batch sync: %w", err)
		}
	}
	return ptrs, dedupHits, nil
}

func (vl *valueLog) markBatchDedupHits(values [][]byte, b3s [][32]byte, oldVPs []VPtr, dedup []bool) {
	const minCoalesceHeaders = 8
	const maxCoalescedOverread = 4

	for i := 0; i < len(values); {
		if oldVPs[i].Length != uint64(len(values[i])) || oldVPs[i].Length == 0 {
			i++
			continue
		}

		start := i
		spanStart := oldVPs[i].Offset
		spanEnd := oldVPs[i].Offset + vlogEntryHeaderSize
		prev := oldVPs[i]
		i++
		for i < len(values) &&
			oldVPs[i].Length == uint64(len(values[i])) &&
			oldVPs[i].Length > 0 &&
			oldVPs[i].Offset == prev.Offset+uint64(vlogEntryHeaderSize)+prev.Length {
			spanEnd = oldVPs[i].Offset + vlogEntryHeaderSize
			prev = oldVPs[i]
			i++
		}

		count := i - start
		headerBytes := count * vlogEntryHeaderSize
		spanLen64 := spanEnd - spanStart
		maxInt := int(^uint(0) >> 1)
		if count >= minCoalesceHeaders &&
			spanLen64 <= uint64(maxInt) &&
			int(spanLen64) <= headerBytes*maxCoalescedOverread {
			spanLen := int(spanLen64)
			if cap(vl.headerBatchBuf) < spanLen {
				vl.headerBatchBuf = make([]byte, spanLen)
			} else {
				vl.headerBatchBuf = vl.headerBatchBuf[:spanLen]
			}
			if err := readAtFull(vl.fd, vl.headerBatchBuf, int64(spanStart), "vlog batch readBlake3"); err == nil {
				for j := start; j < i; j++ {
					off := int(oldVPs[j].Offset - spanStart)
					if vlogHeaderMatchesB3(vl.headerBatchBuf[off:off+vlogEntryHeaderSize], oldVPs[j], b3s[j]) {
						dedup[j] = true
					}
				}
				continue
			}
		}

		for j := start; j < i; j++ {
			oldB3, err := vl.readBlake3(oldVPs[j])
			if err == nil && b3s[j] == oldB3 {
				dedup[j] = true
			}
		}
	}
}

func vlogHeaderMatchesB3(hdr []byte, vp VPtr, b3 [32]byte) bool {
	_ = hdr[vlogEntryHeaderSize-1]
	storedHdrCRC := binary.LittleEndian.Uint32(hdr[0:4])
	computedHdrCRC := crc32.Checksum(hdr[4:56], crc32cTable)
	if computedHdrCRC != storedHdrCRC {
		return false
	}
	if binary.LittleEndian.Uint64(hdr[12:20]) != vp.Length {
		return false
	}
	var oldB3 [32]byte
	copy(oldB3[:], hdr[24:56])
	return oldB3 == b3
}

// appendBatchAndSync writes multiple values to the VLOG with a single fsync.
// Returns one VPtr per value. Thread-safe.
func (vl *valueLog) appendBatchAndSync(values [][]byte, hlcs []HLC, skipSync bool) ([]VPtr, error) {
	vl.mu.Lock()
	defer vl.mu.Unlock()

	ptrs := make([]VPtr, len(values))
	for i, v := range values {
		vp, err := vl.appendLocked(v, hlcs[i])
		if err != nil {
			return nil, err
		}
		ptrs[i] = vp
	}
	if !skipSync {
		if err := vl.syncFile(); err != nil {
			return nil, fmt.Errorf("vlog: batch sync: %w", err)
		}
	}
	return ptrs, nil
}

// equal32 compares a 32-byte slice with a 32-byte array without allocating.
func equal32(a []byte, b []byte) bool {
	_ = a[31]
	_ = b[31]
	for i := 0; i < 32; i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

var ErrTomb = fmt.Errorf("error tombstone found")

func validateVPtrForRead(vp VPtr) error {
	if vp.Length == rawVlenTombstone {
		return ErrTomb
	}
	if vp.Length <= vlogInlineThreshold {
		return fmt.Errorf("vlog: invalid VPtr length %d: inline values are not stored in VLOG", vp.Length)
	}
	maxInt := int(^uint(0) >> 1)
	maxValueLen := uint64(maxInt - vlogEntryHeaderSize)
	if vp.Length > maxValueLen {
		return fmt.Errorf("vlog: VPtr length too large: %d > %d", vp.Length, maxValueLen)
	}
	return nil
}

// read reads a value from the VLOG at the given VPtr.
// Thread-safe (uses pread).
func (vl *valueLog) read(vp VPtr) ([]byte, error) {
	if err := validateVPtrForRead(vp); err != nil {
		return nil, err
	}
	// INVAR: vp.Length != rawVlenTombstone, since validateVPtrForRead(vp) would have errored out.

	entrySize := vlogEntryHeaderSize + int(vp.Length)
	buf := make([]byte, entrySize)

	if err := readAtFull(vl.fd, buf, int64(vp.Offset), "vlog"); err != nil {
		return nil, err
	}

	// Verify length field.
	storedLen := binary.LittleEndian.Uint64(buf[12:20])
	if storedLen != vp.Length {
		return nil, fmt.Errorf("vlog: length mismatch at offset %d: stored %d, expected %d", vp.Offset, storedLen, vp.Length)
	}
	// Verify hdrCRC (covers bytes 4..56: HLC + length + valCRC + blake3).
	storedHdrCRC := binary.LittleEndian.Uint32(buf[0:4])
	computedHdrCRC := crc32.Checksum(buf[4:56], crc32cTable)
	if computedHdrCRC != storedHdrCRC {
		return nil, fmt.Errorf("vlog: header CRC mismatch at offset %d: stored %08x, computed %08x", vp.Offset, storedHdrCRC, computedHdrCRC)
	}
	// Verify valCRC (covers the N value bytes at buf[56:]).
	storedValCRC := binary.LittleEndian.Uint32(buf[20:24])
	computedValCRC := crc32.Checksum(buf[vlogEntryHeaderSize:], crc32cTable)
	if computedValCRC != storedValCRC {
		return nil, fmt.Errorf("vlog: value CRC mismatch at offset %d: stored %08x, computed %08x", vp.Offset, storedValCRC, computedValCRC)
	}

	value := buf[vlogEntryHeaderSize:]

	// Verify blake3 checksum of value bytes matches stored blake3.
	storedB3 := buf[24:56]
	computedB3 := blake3checksum32(value)
	if !equal32(storedB3, computedB3[:]) {
		return nil, fmt.Errorf("vlog: blake3 mismatch at offset %d", vp.Offset)
	}

	return value, nil
}

// readBlake3 reads just the 56-byte header of a VLOG entry and returns the
// 32-byte blake3 checksum without reading the value bytes. This is used for
// dedup checks: if the blake3 of the new value matches, we can skip the write.
func (vl *valueLog) readBlake3(vp VPtr) ([32]byte, error) {
	var hdr [vlogEntryHeaderSize]byte
	var b3 [32]byte

	if err := readAtFull(vl.fd, hdr[:], int64(vp.Offset), "vlog readBlake3"); err != nil {
		return b3, err
	}

	// Verify hdrCRC before trusting the blake3 bytes.
	storedHdrCRC := binary.LittleEndian.Uint32(hdr[0:4])
	computedHdrCRC := crc32.Checksum(hdr[4:56], crc32cTable)
	if computedHdrCRC != storedHdrCRC {
		return b3, fmt.Errorf("vlog: readBlake3 hdrCRC mismatch at offset %d", vp.Offset)
	}

	copy(b3[:], hdr[24:56])
	return b3, nil
}

// sync fsyncs the VLOG file.
func (vl *valueLog) sync() error {
	return vl.syncFile()
}

func (vl *valueLog) syncFile() error {
	if err := vl.fd.Sync(); err != nil {
		return err
	}
	atomic.AddInt64(&vl.VLOGFsyncs, 1)
	return nil
}

// close closes the VLOG file.
func (vl *valueLog) close() (onDiskFootprintBytes int64, err error) {
	vl.mu.Lock()
	defer vl.mu.Unlock()
	return vl.closeLocked()
}

func (vl *valueLog) closeLocked() (onDiskFootprintBytes int64, err error) {
	if vl.fd == nil || isNil(vl.fd) {
		return vl.tail, nil
	}
	onDiskFootprintBytes = mustStatFileSize(vl.fd)
	err = vl.fd.Close()
	if err == nil {
		vl.fd = nil
	}
	return
}

// size returns the current VLOG file size.
func (vl *valueLog) size() int64 {
	return vl.tail
}

// reopen closes the current fd and reopens at the given path.
// Used by VacuumVLOG after renaming the new VLOG file into place.
func (vl *valueLog) reopen(path string) error {
	vl.mu.Lock()
	defer vl.mu.Unlock()
	if _, err := vl.closeLocked(); err != nil {
		return err
	}
	//fd, err := vl.vfs.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	fd, err := vl.vfs.OpenReadWrite(path, vfs.WriteCategoryUnspecified)

	if err != nil {
		return err
	}
	fi, err := fd.Stat()
	if err != nil {
		fd.Close()
		return err
	}
	vl.fd = fd
	vl.tail = fi.Size()
	return nil
}
