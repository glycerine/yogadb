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
// Each VLOG entry (72-byte header + N-byte value):
//   [4 bytes]  hdrCRC         offset 0  - CRC32C of bytes 4..72
//   [8 bytes]  HLC            offset 4  - big-endian int64 timestamp
//   [8 bytes]  uncompressedLen offset 12 - little-endian uint64
//   [8 bytes]  flags          offset 20 - little-endian uint64 (see below)
//   [8 bytes]  onDiskLen      offset 28 - little-endian uint64 (== N)
//   [4 bytes]  valCRC         offset 36 - CRC32C of the N value bytes
//   [32 bytes] blake3         offset 40 - blake3 of UNCOMPRESSED value bytes
//   [N bytes]  value bytes    offset 72 - raw or s2-compressed
//
// The VLOG is fully self-describing: both the uncompressed and on-disk
// (compressed) lengths are in the header. No external data (VPtr, KV structs)
// is needed to parse or recover an entry. On-disk entry size = 72 + onDiskLen.
//
// Flags field:
//   flags & 1  =>  uncompressed (value bytes are raw, onDiskLen == uncompressedLen)
//   flags & 2  =>  s2 compressed (value bytes are s2-encoded)
//   Flags value 0 is invalid/reserved.
//
// Field semantics with compression:
//   uncompressedLen: always the original value byte count before compression
//   onDiskLen:       byte count of value data following the header (== N)
//   valCRC:          CRC32C of the on-disk value bytes (compressed bytes when s2)
//   blake3:          hash of UNCOMPRESSED value bytes (preserves dedup correctness)
//
// The blake3 checksum enables cheap dedup: before appending a new VLOG entry,
// we can read just the 72-byte header of the old entry and compare blake3
// checksums and uncompressed lengths. If they match, the value is unchanged
// and we can skip the VLOG write entirely, reusing the old VPtr.
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
// The two-CRC design allows validating the header without reading the value bytes.
//
// The KV encoding in FlexSpace stores a "value pointer" (VPtr) instead of inline
// value bytes when the value is in VLOG. VPtr is 16 bytes: 8-byte offset + 8-byte length.
// VPtr.Length stores the on-disk length (same as onDiskLen in the header).

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	///"os"
	"sync"
	"sync/atomic"

	"github.com/glycerine/vfs"
	"github.com/klauspost/compress/s2"
)

const (
	// Values <= than this threshold remain inline with their key in FlexSpace.
	// Values larger than this threshold are stored in the VLOG file.
	// 64 bytes balances scan performance (small values inline) vs write
	// amplification (large values written once to VLOG, never rewritten).
	vlogInlineThreshold = 64

	// VLOG entry header layout (72 bytes):
	//   [0:4]   hdrCRC          (4B)
	//   [4:12]  HLC             (8B)
	//   [12:20] uncompressedLen (8B)
	//   [20:28] flags           (8B)
	//   [28:36] onDiskLen       (8B)
	//   [36:40] valCRC          (4B)
	//   [40:72] blake3          (32B)
	vlogEntryHeaderSize = 72

	// Header field offsets.
	vlogOffHdrCRC    = 0
	vlogOffHLC       = 4
	vlogOffLen       = 12 // uncompressed length
	vlogOffFlags     = 20
	vlogOffOnDiskLen = 28 // on-disk (compressed) length
	vlogOffValCRC    = 36
	vlogOffBlake3    = 40

	// Flags values.
	vlogFlagUncompressed = 1
	vlogFlagS2           = 2

	// VPtr size in kv128 encoding: 8-byte offset + 8-byte length
	vptrSize = 16
)

// VPtr is a pointer to a value stored in the VLOG file.
type VPtr struct {
	Offset uint64 // byte offset in VLOG file
	Length uint64 // on-disk value length in bytes (== onDiskLen in header)
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
	vfs      vfs.FS
	fd       vfs.File
	mu       sync.Mutex // protects writes (appends are serialized)
	tail     int64      // next write offset (also = file size)
	compress string     // "s2" or "none"

	// Write-byte counter (accessed atomically)
	VLOGBytesWritten int64
}

// openValueLog opens or creates the VLOG file.
// compress is "s2" (default when "") or "none".
func openValueLog(path string, fs vfs.FS, compress string) (*valueLog, error) {
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
	if compress == "" {
		compress = "s2"
	}
	return &valueLog{
		vfs:      fs,
		fd:       fd,
		tail:     fi.Size(),
		compress: compress,
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
func (vl *valueLog) appendLocked(value []byte, hlc HLC) (VPtr, error) {
	return vl.appendLockedWithHash(value, hlc, nil)
}

// appendLockedWithHash is like appendLocked but accepts a pre-computed blake3
// hash to avoid recomputing it when the caller already has it (e.g., dedup path).
func (vl *valueLog) appendLockedWithHash(value []byte, hlc HLC, b3 []byte) (VPtr, error) {
	uncompressedLen := uint64(len(value))

	// Compress or store raw based on vl.compress setting.
	var onDiskValue []byte
	var flags uint64
	switch vl.compress {
	case "none":
		onDiskValue = value
		flags = vlogFlagUncompressed
	case "s2", "":
		onDiskValue = s2.Encode(nil, value)
		flags = vlogFlagS2
	default:
		panicf("unknown compression: '%v'", vl.compress)
	}
	onDiskLen := uint64(len(onDiskValue))

	entrySize := vlogEntryHeaderSize + int(onDiskLen)

	// Build entry: [hdrCRC][HLC][uncompressedLen][flags][onDiskLen][valCRC][blake3][value...]
	buf := make([]byte, entrySize)
	binary.BigEndian.PutUint64(buf[vlogOffHLC:vlogOffHLC+8], uint64(hlc))
	binary.LittleEndian.PutUint64(buf[vlogOffLen:vlogOffLen+8], uncompressedLen)
	binary.LittleEndian.PutUint64(buf[vlogOffFlags:vlogOffFlags+8], flags)
	binary.LittleEndian.PutUint64(buf[vlogOffOnDiskLen:vlogOffOnDiskLen+8], onDiskLen)
	copy(buf[vlogEntryHeaderSize:], onDiskValue)
	// valCRC covers the on-disk value bytes (compressed or raw).
	valCRC := crc32.Checksum(buf[vlogEntryHeaderSize:], crc32cTable)
	binary.LittleEndian.PutUint32(buf[vlogOffValCRC:vlogOffValCRC+4], valCRC)
	// blake3 checksum of UNCOMPRESSED value bytes.
	if b3 == nil {
		b3 = blake3checksum32(value)
	}
	copy(buf[vlogOffBlake3:vlogOffBlake3+32], b3)
	// hdrCRC covers bytes 4..72 (everything after hdrCRC itself).
	hdrCRC := crc32.Checksum(buf[4:vlogEntryHeaderSize], crc32cTable)
	binary.LittleEndian.PutUint32(buf[vlogOffHdrCRC:vlogOffHdrCRC+4], hdrCRC)

	offset := vl.tail
	_, err := vl.fd.WriteAt(buf, offset)
	if err != nil {
		return VPtr{}, fmt.Errorf("vlog: write at %d: %w", offset, err)
	}
	atomic.AddInt64(&vl.VLOGBytesWritten, int64(entrySize))
	vl.tail = offset + int64(entrySize)

	return VPtr{
		Offset: uint64(offset),
		Length: onDiskLen,
	}, nil
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
		if err := vl.fd.Sync(); err != nil {
			return vp, fmt.Errorf("vlog: sync: %w", err)
		}
	}
	return vp, nil
}

// appendDedupAndSync checks if the new value matches the old VLOG entry at
// oldVP (by comparing blake3 checksums and uncompressed lengths from the
// 72-byte headers). If the value is unchanged, it returns the old VPtr
// without writing anything. Otherwise it appends the new value normally.
//
// See "HLC STALENESS IN VLOG HEADERS" comment at the top of this file:
// when dedup reuses an old VPtr, the VLOG header HLC becomes stale.
// The authoritative HLC lives in KV.SLOT_BLOCKS / memtable / WAL.
func (vl *valueLog) appendDedupAndSync(value []byte, hlc HLC, oldVP VPtr, skipSync bool) (VPtr, bool, error) {
	newB3 := blake3checksum32(value)

	vl.mu.Lock()
	defer vl.mu.Unlock()

	// Compare uncompressed lengths from the header (not VPtr.Length which
	// is the on-disk/compressed length). This makes dedup work correctly
	// across compressed and uncompressed entries.
	if oldVP.Length > 0 {
		oldUncompLen, oldB3, err := vl.readBlake3AndLen(oldVP)
		if err == nil && oldUncompLen == uint64(len(value)) && equal32(newB3, oldB3[:]) {
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
		if err := vl.fd.Sync(); err != nil {
			return vp, false, fmt.Errorf("vlog: sync: %w", err)
		}
	}
	return vp, false, nil
}

// appendBatchDedupAndSync is like appendBatchAndSync but skips VLOG writes
// for values whose blake3 matches the existing entry at oldVPs[i].
// An oldVP with Length==0 means "no previous entry" (always append).
// Returns one VPtr per value and the count of dedup hits.
func (vl *valueLog) appendBatchDedupAndSync(values [][]byte, hlcs []HLC, oldVPs []VPtr, skipSync bool) ([]VPtr, int, error) {
	// Pre-compute blake3 checksums for all new values (outside the lock).
	b3s := make([][]byte, len(values))
	for i, v := range values {
		b3s[i] = blake3checksum32(v)
	}

	vl.mu.Lock()
	defer vl.mu.Unlock()

	ptrs := make([]VPtr, len(values))
	dedupHits := 0
	wrote := false

	for i, v := range values {
		// Try dedup: compare uncompressed lengths from headers.
		if oldVPs[i].Length > 0 {
			oldUncompLen, oldB3, err := vl.readBlake3AndLen(oldVPs[i])
			if err == nil && oldUncompLen == uint64(len(v)) && equal32(b3s[i], oldB3[:]) {
				ptrs[i] = oldVPs[i]
				dedupHits++
				continue
			}
		}
		// Append new entry.
		vp, err := vl.appendLockedWithHash(v, hlcs[i], b3s[i])
		if err != nil {
			return nil, dedupHits, err
		}
		ptrs[i] = vp
		wrote = true
	}
	if wrote && !skipSync {
		if err := vl.fd.Sync(); err != nil {
			return nil, dedupHits, fmt.Errorf("vlog: batch sync: %w", err)
		}
	}
	return ptrs, dedupHits, nil
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
		if err := vl.fd.Sync(); err != nil {
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

// read reads a value from the VLOG at the given VPtr, decompressing if needed.
// The header contains both uncompressed and on-disk lengths, so the VLOG is
// fully self-describing. Thread-safe (uses pread).
func (vl *valueLog) read(vp VPtr) ([]byte, error) {
	entrySize := vlogEntryHeaderSize + int(vp.Length)
	buf := make([]byte, entrySize)

	_, err := vl.fd.ReadAt(buf, int64(vp.Offset))
	if err != nil {
		return nil, fmt.Errorf("vlog: read at offset %d len %d: %w", vp.Offset, vp.Length, err)
	}

	// Verify hdrCRC (covers bytes 4..72).
	storedHdrCRC := binary.LittleEndian.Uint32(buf[vlogOffHdrCRC : vlogOffHdrCRC+4])
	computedHdrCRC := crc32.Checksum(buf[4:vlogEntryHeaderSize], crc32cTable)
	if computedHdrCRC != storedHdrCRC {
		return nil, fmt.Errorf("vlog: header CRC mismatch at offset %d: stored %08x, computed %08x", vp.Offset, storedHdrCRC, computedHdrCRC)
	}

	// Parse header fields.
	uncompressedLen := binary.LittleEndian.Uint64(buf[vlogOffLen : vlogOffLen+8])
	flags := binary.LittleEndian.Uint64(buf[vlogOffFlags : vlogOffFlags+8])
	onDiskLen := binary.LittleEndian.Uint64(buf[vlogOffOnDiskLen : vlogOffOnDiskLen+8])

	// Cross-check header's onDiskLen against VPtr.Length.
	if onDiskLen != vp.Length {
		return nil, fmt.Errorf("vlog: onDiskLen mismatch at offset %d: header %d, vptr %d", vp.Offset, onDiskLen, vp.Length)
	}

	// Verify valCRC (covers the on-disk value bytes).
	storedValCRC := binary.LittleEndian.Uint32(buf[vlogOffValCRC : vlogOffValCRC+4])
	computedValCRC := crc32.Checksum(buf[vlogEntryHeaderSize:], crc32cTable)
	if computedValCRC != storedValCRC {
		return nil, fmt.Errorf("vlog: value CRC mismatch at offset %d: stored %08x, computed %08x", vp.Offset, storedValCRC, computedValCRC)
	}

	onDiskValue := buf[vlogEntryHeaderSize:]
	var value []byte

	switch {
	case flags&vlogFlagS2 != 0:
		// s2 compressed - decompress.
		value, err = s2.Decode(nil, onDiskValue)
		if err != nil {
			return nil, fmt.Errorf("vlog: s2 decompress at offset %d: %w", vp.Offset, err)
		}
		if uint64(len(value)) != uncompressedLen {
			return nil, fmt.Errorf("vlog: decompressed length mismatch at offset %d: got %d, header says %d", vp.Offset, len(value), uncompressedLen)
		}
	case flags&vlogFlagUncompressed != 0:
		if onDiskLen != uncompressedLen {
			return nil, fmt.Errorf("vlog: uncompressed entry has mismatched lengths at offset %d: onDisk %d, uncompressed %d", vp.Offset, onDiskLen, uncompressedLen)
		}
		value = onDiskValue
	default:
		return nil, fmt.Errorf("vlog: invalid flags %d at offset %d", flags, vp.Offset)
	}

	// Verify blake3 checksum of UNCOMPRESSED value bytes.
	storedB3 := buf[vlogOffBlake3 : vlogOffBlake3+32]
	computedB3 := blake3checksum32(value)
	if !equal32(storedB3, computedB3) {
		return nil, fmt.Errorf("vlog: blake3 mismatch at offset %d", vp.Offset)
	}

	return value, nil
}

// readBlake3AndLen reads just the 72-byte header of a VLOG entry and returns
// the uncompressed length and 32-byte blake3 checksum without reading the
// value bytes. This is used for dedup checks.
func (vl *valueLog) readBlake3AndLen(vp VPtr) (uncompressedLen uint64, b3 [32]byte, err error) {
	var hdr [vlogEntryHeaderSize]byte

	_, err = vl.fd.ReadAt(hdr[:], int64(vp.Offset))
	if err != nil {
		err = fmt.Errorf("vlog: readBlake3AndLen at offset %d: %w", vp.Offset, err)
		return
	}

	// Verify hdrCRC before trusting the header bytes.
	storedHdrCRC := binary.LittleEndian.Uint32(hdr[vlogOffHdrCRC : vlogOffHdrCRC+4])
	computedHdrCRC := crc32.Checksum(hdr[4:vlogEntryHeaderSize], crc32cTable)
	if computedHdrCRC != storedHdrCRC {
		err = fmt.Errorf("vlog: readBlake3AndLen hdrCRC mismatch at offset %d", vp.Offset)
		return
	}

	uncompressedLen = binary.LittleEndian.Uint64(hdr[vlogOffLen : vlogOffLen+8])
	copy(b3[:], hdr[vlogOffBlake3:vlogOffBlake3+32])
	return
}

// readBlake3 reads just the 72-byte header of a VLOG entry and returns the
// 32-byte blake3 checksum without reading the value bytes. This is used for
// dedup checks: if the blake3 of the new value matches, we can skip the write.
func (vl *valueLog) readBlake3(vp VPtr) ([32]byte, error) {
	_, b3, err := vl.readBlake3AndLen(vp)
	return b3, err
}

// sync fsyncs the VLOG file.
func (vl *valueLog) sync() error {
	return vl.fd.Sync()
}

// close closes the VLOG file.
func (vl *valueLog) close() (onDiskFootprintBytes int64, err error) {
	onDiskFootprintBytes = mustStatFileSize(vl.fd)
	err = vl.fd.Close()
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
	vl.fd.Close()
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
