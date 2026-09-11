package yogadb

// yogadb/db.go - Go port of flexspace/flexdb.c
// FlexDB: a persistent ordered key-value store backed by FlexSpace.
// Uses keyStable's sorted key arena for the in-memory write buffer (memtable).
//
// Architecture:
//   Active Memtable (keyStable + WAL) -> (flush) -> FlexSpace
//   Reads: check active memtable -> check inactive memtable -> check FlexSpace via sparse index
//   Crash recovery: rebuild sparse index from FlexSpace tags, replay WAL logs.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/bits"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/glycerine/greenpack/msgp"
	"github.com/glycerine/idem"
	"github.com/glycerine/vfs"
)

// ====================== Constants ======================

const (
	// MaxKeySize is the maximum accepted user key length in bytes.
	// It is derived from slotted-page capacity so a single key can always fit
	// in a FlexSpace interval with zero-length, small inline, or VLOG values.
	MaxKeySize                        = MAX_KEY_BYTES
	flexMemSparseIndexTreeLeafCap     = 100                   // 100                   // (5s benchtime) 122=>5.888; 100=>5.477; 80=>8.557; 90=>5.165,6.598,7.288; 95=>5.675; 85=>8.322; 110=>6.070; 120=>5.308; 150=>5.582; 100=>5.638
	flexMemSparseIndexTreeInternalCap = 40                    // was 40;
	flexdbSparseIntervalCount         = 2000                  //2000(6.3); 100(8.974); 1000(5.894, 5.6) ; 1500(7.993), 4000(10.35), 500(7.142);                  // with flexdbUnsortedWriteQuota=6;              // old:500=>15.77 ; 2000=>14.64 ; 3000=>14.53; 1000=>16.4; 200=>16.68; 4000(2000 flexdbUnsortedWriteQuota)=>
	flexdbSparseIntervalSize          = SLOTTED_PAGE_KB << 10 // 64 KB
	memtableCap                       = 1 << 30               // 1 GB
	memtableWalBufCap                 = 8 << 20               // 8 MB log buffer (must be at least 2x than MaxKeySize + space for a KV struct) so we don't deadlock trying to flush the memtable and write a new large key.
	memtableFlushBatch                = 1024
	flexdbUnsortedWriteQuota          = 6 // 8 // 15 // 200 // was 200 ; limited to 127 anyway in flexdbTagUnsorted(tag uint16) uint8 { return uint8((tag >> 1) & 0x7f) }; 2=>6.261; 4=>5.553; 8=>5.662, 9.699; 15=>6.037; 3=>6.753; 6=>6.819,6.312,6.329; 5=>7.3; 4=> 9.401, 7.485;
	// sparseInterval = sortedCount + unsortedQuota + 1 = 32
	flexdbSparseInterval        = flexdbSparseIntervalCount + flexdbUnsortedWriteQuota + 1
	intervalCachePartitionCount = 1024 // 1024=>7.288; 512=>7.840; 2048=>7.505
	intervalCachePartitionMask  = intervalCachePartitionCount - 1

	// see intervalcache.go for intervalCacheEntry
	intervalCacheEntryChance = 2

	// see memtable.go for memtable
	defaultBackgroundFlushInterval = 5 * time.Second
)

var sep = string(os.PathSeparator)

var testHookVacuumVLOGAfterFlexSpaceSyncBeforeRename func(*FlexDB) error
var testHookResolveVPtr func(KV) error

// Batch submits a set of writes all together at once for load efficiency.
// It writes standalone MEMWAL_KV records; use Update for BEGIN/COMMIT grouped
// crash recovery of multiple key writes.
type Batch struct {
	db                    *FlexDB
	puts                  []KV
	aliasKeys             []string
	aliasSorted           bool
	aliasLastKey          string
	keyArena              []byte
	valueArena            []byte
	logicalBytes          int64
	recordsNeedValidation bool
	allValuesAliasKeys    bool
	err                   error
}

const (
	batchInitialPutCap        = 10000
	batchInitialKeyArenaCap   = 280 << 10
	batchInitialValueArenaCap = 512 << 10
)

// NewBatch returns an empty new Batch.
//
// Before AllowReads is called, the only supported data-loading operations are:
// create a Batch, call Batch.Set, SetBytes, and/or Delete, and Commit it.
// db.Sync is also allowed. On a newly-created empty database these batches use
// the optimized initial bulk builder. On a database reopened with existing data,
// the pre-AllowReads batch data is kept as a sorted reload run and merged into
// the existing database when AllowReads or Sync is called. The general-purpose
// DB/transaction write APIs require AllowReads first.
func (db *FlexDB) NewBatch() (b *Batch) {
	b = &Batch{
		db:                 db,
		aliasSorted:        true,
		allValuesAliasKeys: true,
	}
	return
}

func (s *Batch) ensurePutsCap() {
	if s.puts == nil {
		s.puts = make([]KV, 0, batchInitialPutCap)
	}
}

func (s *Batch) ensureAliasKeysCap() {
	if s.aliasKeys == nil {
		s.aliasKeys = make([]string, 0, batchInitialPutCap)
	}
}

func (s *Batch) materializeAliasKeys() {
	if len(s.aliasKeys) == 0 {
		return
	}
	if s.puts == nil {
		s.puts = make([]KV, 0, len(s.aliasKeys))
	}
	for _, key := range s.aliasKeys {
		s.puts = append(s.puts, valueIsKeyKV(key, 0))
	}
	s.aliasKeys = nil
	s.aliasSorted = true
	s.aliasLastKey = ""
}

// Set copies key and value internally, so the
// original memory is safe to be re-used by the
// caller immediately after Set returns.
//
// During the read-disabled load phase before AllowReads, Batch.Set is the only
// supported data mutation, along with its byte-slice form SetBytes.
func (s *Batch) Set(key string, value []byte, vtyp uint64) (err error) {
	if err := validateUserKey(key); err != nil {
		if s.err == nil {
			s.err = err
		}
		return err
	}

	// String keys are immutable - no copy needed.
	// Nil and empty values are the same zero-length live value.
	var valueCopy []byte
	s.materializeAliasKeys()
	s.allValuesAliasKeys = false
	if len(value) > 0 {
		if len(value) > vlogInlineThreshold {
			s.recordsNeedValidation = true
		}
		if s.valueArena == nil {
			capHint := batchInitialValueArenaCap
			if len(value) > capHint {
				capHint = len(value)
			}
			s.valueArena = make([]byte, 0, capHint)
		}
		start := len(s.valueArena)
		s.valueArena = append(s.valueArena, value...)
		valueCopy = s.valueArena[start:]
	}
	kv := KV{
		Key:   key,
		Value: valueCopy,
	}
	// Note the strangeness! We
	// store vtyp type information in Offset even for large values, before
	// we have written to the VLOG at all(!)
	kv.Vptr.Offset = vtyp
	kv.Vptr.Length = uint64(len(value))
	s.ensurePutsCap()
	s.puts = append(s.puts, kv)
	s.logicalBytes += int64(len(key) + len(value))
	return nil
}

// SetBytes is the byte-slice form of Set. It is also allowed during the
// read-disabled load phase before AllowReads.
func (s *Batch) SetBytes(key []byte, value []byte, vtyp uint64) (err error) {
	if len(key) == 0 {
		err = ErrKeyEmpty
		if s.err == nil {
			s.err = err
		}
		return err
	}
	if len(key) > MaxKeySize {
		err = fmt.Errorf("flexdb: key too large (max %d bytes)", MaxKeySize)
		if s.err == nil {
			s.err = err
		}
		return err
	}
	if s.keyArena == nil {
		capHint := batchInitialKeyArenaCap
		if len(key) > capHint {
			capHint = len(key)
		}
		s.keyArena = make([]byte, 0, capHint)
	}
	start := len(s.keyArena)
	s.keyArena = append(s.keyArena, key...)
	keyCopy := s.keyArena[start:]
	keyString := unsafe.String(unsafe.SliceData(keyCopy), len(keyCopy))

	var valueCopy []byte
	if len(value) > 0 {
		if len(value) > vlogInlineThreshold {
			s.recordsNeedValidation = true
		}
		if len(value) == len(key) && unsafe.SliceData(value) == unsafe.SliceData(key) {
			if vtyp == 0 && len(value) <= vlogInlineThreshold && len(s.puts) == 0 && s.allValuesAliasKeys {
				s.ensureAliasKeysCap()
				if s.aliasSorted && len(s.aliasKeys) > 0 && s.aliasLastKey > keyString {
					s.aliasSorted = false
				}
				s.aliasLastKey = keyString
				s.aliasKeys = append(s.aliasKeys, keyString)
				s.logicalBytes += int64(len(key) + len(value))
				return nil
			}
			s.materializeAliasKeys()
			s.allValuesAliasKeys = false
			valueCopy = keyCopy
		} else {
			s.materializeAliasKeys()
			s.allValuesAliasKeys = false
			if s.valueArena == nil {
				capHint := batchInitialValueArenaCap
				if len(value) > capHint {
					capHint = len(value)
				}
				s.valueArena = make([]byte, 0, capHint)
			}
			start := len(s.valueArena)
			s.valueArena = append(s.valueArena, value...)
			valueCopy = s.valueArena[start:]
		}
	} else {
		s.materializeAliasKeys()
		s.allValuesAliasKeys = false
	}
	kv := KV{
		Key:   keyString,
		Value: valueCopy,
	}
	kv.Vptr.Offset = vtyp
	kv.Vptr.Length = uint64(len(value))
	s.ensurePutsCap()
	s.puts = append(s.puts, kv)
	s.logicalBytes += int64(len(key) + len(value))
	return nil
}

// Delete marks key for deletion in this batch. During the read-disabled load
// phase before AllowReads, Batch.Delete is treated as a reload tombstone and is
// merged with any existing database contents when AllowReads is called.
func (s *Batch) Delete(key string) {
	if err := validateUserKey(key); err != nil {
		if s.err == nil {
			s.err = err
		}
		return
	}
	s.materializeAliasKeys()
	s.ensurePutsCap()
	s.puts = append(s.puts, KV{
		Key:  key,
		Vptr: VPtr{Length: rawVlenTombstone},
	})
	s.allValuesAliasKeys = false
	s.logicalBytes += int64(len(key))
}

// Commit applies the batch as a grouped write path for load efficiency.
// It does not fsync unless set doFsync true.
//
// After Commit the batch is empty and can be re-used immediately.
//
// Returns the half-open HLC interval [Begin, Endx) assigned to this batch.
//
// Again, with doFsync false, we do not wait for the data
// to be fdatasynced to disk. Set doFsync true to fsync into
// a MEMWAL log, or do multiple batches and then db.Sync()
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
// FLEXSPACE.KV.SLOT_BLOCKS to see how much free space could be reclaimed.
func (s *Batch) CommitGetMetrics(doFsync bool) (HLCInterval, *Metrics, error) {
	return s.commitMaybeMetrics(doFsync, true)
}

func (s *Batch) commitMaybeMetrics(doFsync bool, wantMetrics bool) (interv HLCInterval, metrics *Metrics, err error) {
	db := s.db

	db.topMutRW.Lock()
	x := true

	autoVacuumHandoff := false
	defer func() {
		if err == nil {
			autoVacuumHandoff = db.maybeStartAutoVacuumXLocked()
		}
		if !autoVacuumHandoff {
			x = false
			db.topMutRW.Unlock()
		}
	}()

	if s.err != nil {
		err := s.err
		s.puts = nil
		s.aliasKeys = nil
		s.aliasSorted = true
		s.aliasLastKey = ""
		s.keyArena = nil
		s.valueArena = nil
		s.logicalBytes = 0
		s.recordsNeedValidation = false
		s.allValuesAliasKeys = true
		s.err = nil
		return HLCInterval{}, nil, err
	}

	if len(s.puts) == 0 && len(s.aliasKeys) == 0 {
		if wantMetrics {
			return HLCInterval{}, db.writeLockHeldSessionMetrics(), nil
		}
		return HLCInterval{}, nil, nil
	}
	if doFsync && db.cfg.OmitMemWalFsync {
		return HLCInterval{}, nil, fmt.Errorf("cannot request doFsync on a database opened with Config.OmitMemWalFsync")
	}

	// Track logical bytes for write amplification metrics.
	atomic.AddInt64(&db.LogicalBytesWritten, s.logicalBytes)

	// One HLC identifies the whole batch. Duplicate keys within the same batch
	// are resolved by normal last-write-wins replacement in the memtable.
	curHLC := db.hlc.CreateSendOrLocalEvent()
	for i := range s.puts {
		s.puts[i].Hlc = curHLC
	}
	mt := &db.mt
	useBulkInitial := db.bulkInitialFastPathEligibleLocked(mt)
	autoVacuumEnabled := db.cfg.AutoVacuumPct > 0

	// Write large values to VLOG with a single batch fsync. The WAL then
	// stores VPtrs (not full values), so large values are written exactly once.
	// Blake3 dedup: if the old VLOG entry has the same checksum, reuse the
	// old VPtr and skip the write. See "HLC STALENESS IN VLOG HEADERS" in vlog.go.
	var largeIndices []int
	var largeOldStates []keyState
	var largeOldKVs []KV
	var largeOldFound []bool
	if db.vlog != nil {
		// Collect large values for batch append. Old FlexSpace probes are done
		// in key order below so random-key batches do not thrash the interval
		// cache. Results stay indexed by the original batch order.
		var largeValues [][]byte
		var oldVPs []VPtr
		for i, kv := range s.puts {
			if kv.Value != nil && len(kv.Value) > vlogInlineThreshold {
				largeIndices = append(largeIndices, i)
				largeValues = append(largeValues, kv.Value)
			}
		}
		if len(largeIndices) > 0 {
			oldVPs = make([]VPtr, len(largeIndices))
			if useBulkInitial && db.ff.Size() == 0 {
				// Pristine initial load: there is no old VLOG entry to dedup.
			} else {
				var nh memSparseIndexTreeHandler
				if useBulkInitial {
					lookupOrder := make([]int, len(largeIndices))
					for i := range lookupOrder {
						lookupOrder[i] = i
					}
					sort.Slice(lookupOrder, func(i, j int) bool {
						return s.puts[largeIndices[lookupOrder[i]]].Key < s.puts[largeIndices[lookupOrder[j]]].Key
					})
					for _, j := range lookupOrder {
						oldVPs[j] = db.lookupOldFlexSpaceVPtrWithHint(s.puts[largeIndices[j]].Key, &nh, x)
					}
				} else {
					largeOldStates = make([]keyState, len(largeIndices))
					if autoVacuumEnabled {
						largeOldKVs = make([]KV, len(largeIndices))
						largeOldFound = make([]bool, len(largeIndices))
					}
					lookupOrder := make([]int, 0, len(largeIndices))
					for j, idx := range largeIndices {
						if db.keyBloomComplete && db.keyBloom != nil {
							h1, h2 := keyBloomHashes(s.puts[idx].Key)
							if !db.keyBloom.mayContainHashes(h1, h2) {
								db.keyBloom.addHashesKnownAbsent(h1, h2)
								largeOldStates[j] = ksNotExistsKeyBloomAdded
								continue
							}
							lookupOrder = append(lookupOrder, j)
							continue
						}
						if db.keyBloomMayExistLocked(s.puts[idx].Key, x) {
							lookupOrder = append(lookupOrder, j)
						} else {
							largeOldStates[j] = ksNotExists
						}
					}
					sort.Slice(lookupOrder, func(i, j int) bool {
						return s.puts[largeIndices[lookupOrder[i]]].Key < s.puts[largeIndices[lookupOrder[j]]].Key
					})
					for _, j := range lookupOrder {
						oldKV, oldFound, oldVP := db.lookupOldKVAndVPtrWithHintNoBloom(s.puts[largeIndices[j]].Key, &nh, x)
						oldVPs[j] = oldVP
						if oldFound {
							largeOldStates[j] = kvToState(oldKV)
						} else {
							largeOldStates[j] = ksNotExists
						}
						if autoVacuumEnabled {
							largeOldKVs[j] = oldKV
							largeOldFound[j] = oldFound
						}
					}
				}
			}
		}
		if len(largeValues) > 0 {
			s.allValuesAliasKeys = false
			// Batch write + single fsync with blake3 dedup.
			ptrs, _, err := db.vlog.appendBatchDedupAndSync(largeValues, curHLC, oldVPs, db.cfg.OmitMemWalFsync)
			if err != nil {
				return HLCInterval{}, nil, fmt.Errorf("flexdb: vlog batch append: %w", err)
			}
			for j, idx := range largeIndices {
				e := &s.puts[idx]
				vtyp := e.Vptr.Offset
				// Key stays same.
				e.Value = nil
				e.Vptr = ptrs[j]
				if vtyp != 0 {
					e.Value = mt.vtypBytes(vtyp)
				}
				// Hlc stays same.
			}
		}
	}

	if s.recordsNeedValidation {
		for i := range s.puts {
			if err := validateKV128RecordSizeAfterUserKey(s.puts[i]); err != nil {
				s.puts = nil
				s.aliasKeys = nil
				s.aliasSorted = true
				s.aliasLastKey = ""
				s.keyArena = nil
				s.valueArena = nil
				s.recordsNeedValidation = false
				s.allValuesAliasKeys = true
				return HLCInterval{}, nil, err
			}
		}
	}

	// Bypass the transaction system entirely - no COW snapshots, no ffMu.RLock,
	// no write buffer tree. Instead, insert directly into the memtable and
	// batch WAL writes under a single mt.memWalMut hold.
	// This amortizes both mutex acquisitions across the entire batch.

	mt.memWalMut.Lock()
	defer mt.memWalMut.Unlock()
	batchWalAppended := false
	if useBulkInitial {
		if len(s.puts) == 0 && len(s.aliasKeys) > 0 {
			if doFsync || s.aliasSorted {
				s.materializeAliasKeys()
				for i := range s.puts {
					s.puts[i].Hlc = curHLC
				}
			} else {
				mt.appendBulkValueIsKeyBatch(s.aliasKeys, curHLC)
				mt.empty.Store(false)
				for _, key := range s.aliasKeys {
					db.rememberKeyBloomLocked(key)
				}
				if wantMetrics {
					if mt.bulk.dirty {
						db.reconcileBulkInitialCountsLocked(mt)
					}
					metrics = db.writeLockHeldSessionMetrics()
				}
				s.puts = nil
				s.aliasKeys = nil
				s.aliasSorted = true
				s.aliasLastKey = ""
				s.keyArena = nil
				s.valueArena = nil
				s.logicalBytes = 0
				s.recordsNeedValidation = false
				s.allValuesAliasKeys = true
				interv = HLCInterval{Begin: curHLC, Endx: curHLC + 1}
				return
			}
		}
		if doFsync {
			var ok bool
			ok, err = mt.logAppendBatchLocked(s.puts, s.allValuesAliasKeys)
			if err != nil {
				return HLCInterval{}, nil, fmt.Errorf("flexdb: batch append compact memwal: %w", err)
			}
			batchWalAppended = ok
		} else {
			batchWalAppended = true
		}
		if batchWalAppended {
			mt.appendBulkBatch(s.puts, s.allValuesAliasKeys)
			mt.empty.Store(false)
			for i := range s.puts {
				db.rememberKeyBloomLocked(s.puts[i].Key)
			}
			if doFsync && !db.cfg.OmitMemWalFsync {
				if err := mt.logSyncLocked(); err != nil {
					return HLCInterval{}, nil, fmt.Errorf("flexdb: batch sync memwal: %w", err)
				}
			}
			if wantMetrics {
				if mt.bulk.dirty {
					db.reconcileBulkInitialCountsLocked(mt)
				}
				metrics = db.writeLockHeldSessionMetrics()
			}
			s.puts = nil
			s.aliasKeys = nil
			s.aliasSorted = true
			s.aliasLastKey = ""
			s.keyArena = nil
			s.valueArena = nil
			s.logicalBytes = 0
			s.recordsNeedValidation = false
			s.allValuesAliasKeys = true
			interv = HLCInterval{Begin: curHLC, Endx: curHLC + 1}
			return
		}
	}

	s.materializeAliasKeys()
	for i := range s.puts {
		if s.puts[i].Hlc == 0 {
			s.puts[i].Hlc = curHLC
		}
	}

	if !batchWalAppended && len(s.puts) > 0 {
		batchMemSize := int64(0)
		for i := range s.puts {
			batchMemSize += int64(kvSizeApprox(&s.puts[i]))
		}
		if mt.size+batchMemSize < memtableCap {
			var ok bool
			ok, err = mt.logAppendBatchLocked(s.puts, s.allValuesAliasKeys)
			if err != nil {
				return HLCInterval{}, nil, fmt.Errorf("flexdb: batch append compact memwal: %w", err)
			}
			batchWalAppended = ok
		}
	}

	largeLookupIdx := 0
	for idx := 0; idx < len(s.puts); idx++ {

		if mt.size >= memtableCap {
			// Memtable full - flush inline.
			if err := mt.logFlushLocked(); err != nil {
				return HLCInterval{}, nil, fmt.Errorf("flexdb: batch flush memwal: %w", err)
			}

			if err := db.flushMemtable(x); err != nil {
				return HLCInterval{}, nil, fmt.Errorf("flexdb: batch flush memtable: %w", err)
			}
			if err := db.cache.flushDirtyPages(); err != nil {
				return HLCInterval{}, nil, fmt.Errorf("flexdb: batch flush dirty pages: %w", err)
			}
			db.persistCounters()
			db.ff.Sync()

			mt.ks.clear(x)
			mt.vtypArena = nil
			mt.empty.Store(true)
			mt.size = 0
			db.flushSeq++
		}
		putKV := s.puts[idx]
		newState := kvToState(putKV)
		largeOldLookupDone := false
		largeOldState := ksNotExists
		largeOldKeyBloomAdded := false
		var largeOldKV KV
		var largeOldKVFound bool
		if !useBulkInitial && largeLookupIdx < len(largeIndices) && largeIndices[largeLookupIdx] == idx {
			largeOldState = largeOldStates[largeLookupIdx]
			if largeOldState == ksNotExistsKeyBloomAdded {
				largeOldState = ksNotExists
				largeOldKeyBloomAdded = true
			}
			if autoVacuumEnabled {
				largeOldKV = largeOldKVs[largeLookupIdx]
				largeOldKVFound = largeOldFound[largeLookupIdx]
			}
			largeOldLookupDone = true
			largeLookupIdx++
		}

		if !batchWalAppended {
			if err := mt.logAppendKVLocked(putKV); err != nil {
				return HLCInterval{}, nil, fmt.Errorf("flexdb: batch append memwal key=%q: %w", putKV.Key, err)
			}
		}
		var old KV
		var replaced bool
		if useBulkInitial {
			old, replaced = mt.putBulk(putKV)
		} else {
			old, replaced = mt.put(putKV, x)
		}
		mt.empty.Store(false)
		oldState := ksNotExists
		oldKV := old
		oldKVFound := replaced
		if replaced {
			oldState = kvToState(old)
		} else if !useBulkInitial {
			if largeOldLookupDone {
				oldState = largeOldState
				if autoVacuumEnabled {
					oldKV = largeOldKV
					oldKVFound = largeOldKVFound
				}
			} else if db.ff.Size() != 0 && db.keyBloomMayExistLocked(putKV.Key, x) {
				oldKV, oldKVFound, _ = db.getPassthroughKV(putKV.Key, x)
				if oldKVFound {
					oldState = kvToState(oldKV)
				}
			}
		}
		if autoVacuumEnabled {
			db.noteAutoVacuumObsoleteKV(oldKV, oldKVFound, putKV)
		}
		if !useBulkInitial {
			db.adjustKeyCounters(oldState, newState)
		}
		if oldState == ksNotExists && !largeOldKeyBloomAdded {
			db.rememberNewKeyBloomLocked(putKV.Key)
		}
	}

	mt.empty.Store(false)

	// Batch WAL writes for this chunk under a single logMu hold.

	// WAL stores VPtrs for large values (VLOG was fsynced above).

	if doFsync && !db.cfg.OmitMemWalFsync {
		if err := mt.logSyncLocked(); err != nil { // here in Batch.Commit(doFsync=true)
			return HLCInterval{}, nil, fmt.Errorf("flexdb: batch sync memwal: %w", err)
		}
	}

	// make ready for immediate reuse after a Commit.
	s.puts = nil
	s.aliasKeys = nil
	s.aliasSorted = true
	s.aliasLastKey = ""
	s.keyArena = nil
	s.valueArena = nil
	s.logicalBytes = 0
	s.recordsNeedValidation = false
	s.allValuesAliasKeys = true

	if wantMetrics {
		if useBulkInitial && mt.bulk.dirty {
			db.reconcileBulkInitialCountsLocked(mt)
		}
		metrics = db.writeLockHeldSessionMetrics()
	}
	interv = HLCInterval{Begin: curHLC, Endx: curHLC + 1}
	return
}

// Reset forgets any existing queued up puts.
func (s *Batch) Reset() {
	s.puts = nil
	s.aliasKeys = nil
	s.aliasSorted = true
	s.aliasLastKey = ""
	s.keyArena = nil
	s.valueArena = nil
	s.logicalBytes = 0
	s.recordsNeedValidation = false
	s.allValuesAliasKeys = true
	s.err = nil
}

// Close forgets any existing queued up puts, and
// frees any other resources associated with the Batch.
func (s *Batch) Close() {
	s.puts = nil
	s.aliasKeys = nil
	s.aliasSorted = true
	s.aliasLastKey = ""
	s.keyArena = nil
	s.valueArena = nil
	s.logicalBytes = 0
	s.recordsNeedValidation = false
	s.allValuesAliasKeys = true
	s.err = nil
}

// ====================== KV type ======================

// KV is a key-value pair. Tombstones are marked by Vptr.Length == rawVlenTombstone.
// A zero-length Value with Vptr.Length == 0 is a live key with no value bytes.
// Nil and empty value slices are intentionally equivalent; only Delete writes
// the tombstone sentinel.
//
// When Vptr.Length > vlogInlineThreshold (== 64), the value is stored in the VLOG file
// and Vptr contains the location. Use kv.HasVPtr() to test this.
//
// KV is currently 64 bytes, a cache line on most systems. Be very wary of
// making it any bigger, as this could really slow things down.
//
// The Key is just a string. There is no loss of generality over []byte,
// just advantages: a) being immutable, we can avoid slow copies;
// and being smaller (2 words, not 3) than a []byte our KV now
// fits in one 64B cache line. Moreover users cannot corrupt
// the Key, so it is safe to return from our internal caches.
// The technical reason memory-mapped systems must use []byte
// keys is that they are read straight from a read-only
// memory map. Since we do not use a memory-mapped design
// we can get compile time safety, cache speed, and the benefits of
// immutability by making Key a string.
type KV struct {
	Key string

	// Important encoding INVARIANTS for the uint64 Vtyp: to avoid
	// going over the cache-line friendly 64-byte size of struct KV,
	// the Vtype information is encoded in two different ways,
	// depending on the size of the Value.
	//
	// When Vptr.Length <= vlogInlineThreshold(64), then the .Value field holds the inline
	// value, and Vptr.Offset holds the Vtyp, and Vptr.Length == len(KV.Value).
	//
	// When vlogInlineThreshold(64) < Vptr.Length < rawVlenTombstone(^0),
	// then KV.Value holds the 8 bytes of the uint64 Vtyp (value type).
	//
	// The important new rule is that you must key off the Vptr.Length before you know
	// how to interpret the Value field at all.
	Value []byte

	// Vptr.Length <= vlogInlineThreshold(64) means inline Value. In this case,
	//                  Vptr.Offset is re-purposed used for Vtyp (value type uint64 information)
	// Vptr.Length >  vlogInlineThreshold(64) and < rawVlenTombstone: means real VLOG pointer.
	// Vptr.Length == rawVlenTombstone(^0, all bits set uint64) means tombstone.
	Vptr VPtr

	Hlc HLC // hybrid logical clock timestamp. LSN like per mini batch, but has big gaps.
}

// HasVPtr returns true if the value is stored in the VLOG file.
// Real VLOG entries always have Length > vlogInlineThreshold(64) and < rawVlenTombstone
func (kv *KV) HasVPtr() bool {
	return kv.Vptr.Length > vlogInlineThreshold && kv.Vptr.Length < rawVlenTombstone
}

// Vtyp extracts value-type information from the KV. Due to the
// 64B size restriction on KV, we encode the vtyp in two different
// ways depending on whether the value is inline (vtyp is in kv.Vptr.Offset)
// or a large VLOG value (vtyp is in kv.Value which is exactly 8 bytes).
// Tombstones always have a vtyp of 0.
func (kv *KV) Vtyp() uint64 {
	if kv.Vptr.Length == rawVlenTombstone {
		return 0
	}
	if kv.Vptr.Length > vlogInlineThreshold {
		if len(kv.Value) != 8 {
			// allow typ 0 to not be skipped on disk.
			return 0
		}
		return getUint64(kv.Value)
	}
	return kv.Vptr.Offset
}

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
func kvSizeApprox(kv *KV) int {
	size := 24 + len(kv.Key) + len(kv.Value)
	if len(kv.Value) > 0 &&
		len(kv.Value) == len(kv.Key) &&
		unsafe.StringData(kv.Key) == unsafe.SliceData(kv.Value) {
		size -= len(kv.Value)
	}
	return size
}

// isTombstone returns true if this KV is a deletion marker.
// A tombstone is marked by the sentinel VPtr.Length == rawVlenTombstone.
func (kv *KV) isTombstone() bool {
	return kv.Vptr.Length == rawVlenTombstone
}

// Large returns true if this KV's value is stored in the VLOG
// (too large for inline storage). Use db.FetchLarge(kv) to
// retrieve the value bytes.
func (kv *KV) Large() bool {
	x := kv.Vptr.Length
	return vlogInlineThreshold < x && x < rawVlenTombstone
}

// ====================== KV128 encoding ======================
// Format: Vptr.Offset (8 bytes) ||
//         Vptr.Length (8 bytes) ||
//         varint(klen) (up to 10 bytes) ||
//            key_bytes (klen bytes) ||
//         inline value_bytes if Vptr.Length <= vlogInlineThreshold(64) [0-64 bytes] ||
//         HLC ||
//         CRC32c
// Standard LEB128 (same as Go's encoding/binary.PutUvarint).
//
// The high sentinel rawVlenTombstone is unlikely to collide with
// a real value since that would require a value of length of about 2^64 - 1.

// rawVlenTombstone is the sentinel value stored in VPtr.Length
// to mark a KV as a tombstone. The value is
// 0xFFFF FFFF FFFF FFFF - sentinel for tombstone
const rawVlenTombstone uint64 = ^uint64(0)

func kv128Encode(buf []byte, kv KV) []byte {

	recordStart := len(buf)

	var vptrBuf [vptrSize]byte
	kv.Vptr.encode(vptrBuf[:])
	buf = append(buf, vptrBuf[:]...)

	var hdr [20]byte
	n := binary.PutUvarint(hdr[:], uint64(len(kv.Key)))
	buf = append(buf, hdr[:n]...)
	buf = append(buf, kv.Key...)
	if kv.Vptr.Length <= vlogInlineThreshold {
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

	const hlc_plus_crc = 12
	if kv.isTombstone() || kv.HasVPtr() {
		// no inline value bytes
		return vptrSize + varintSize(uint64(len(kv.Key))) + len(kv.Key) + hlc_plus_crc
	}

	// size if VLOG entry VPtr
	sz := vptrSize + varintSize(uint64(len(kv.Key))) + len(kv.Key) + hlc_plus_crc

	vn := len(kv.Value)
	if vn <= vlogInlineThreshold {
		// inline value
		sz += vn
	}
	return sz
}

func kv128Decode(src []byte) (kv KV, n int, ok bool) {
	if len(src) < vptrSize {
		return
	}
	kv.Vptr = decodeVPtr(src[:vptrSize])

	klen64, kn := binary.Uvarint(src[vptrSize:])
	if kn <= 0 {
		return
	}
	maxInt := int(^uint(0) >> 1)
	if klen64 > uint64(maxInt-vptrSize-kn-8-4) {
		return
	}
	klen := int(klen64)
	hdr := vptrSize + kn // header includes the Vptr bytes and the key length.

	vn := kv.Vptr.Length
	if vn == rawVlenTombstone || vn == 0 || vn > vlogInlineThreshold {
		// tombstone, zero-length value, or VLOG pointer
		total := hdr + klen + 8
		if len(src) < total+4 {
			return
		}
		if crc32.Checksum(src[:total], crc32cTable) != binary.LittleEndian.Uint32(src[total:total+4]) {
			return
		}
		kv.Key = string(src[hdr : hdr+klen])
		kv.Hlc = HLC(binary.BigEndian.Uint64(src[total-8 : total]))
		return kv, total + 4, true
	}
	// INVAR: we have inline value bytes of length vn
	vlen := int(vn)
	total := hdr + klen + vlen + 8
	if len(src) < total+4 {
		return
	}
	if crc32.Checksum(src[:total], crc32cTable) != binary.LittleEndian.Uint32(src[total:total+4]) {
		return
	}
	kv.Key = string(src[hdr : hdr+klen])
	kv.Value = make([]byte, vlen)
	copy(kv.Value, src[hdr+klen:hdr+klen+vlen])
	kv.Hlc = HLC(binary.BigEndian.Uint64(src[total-8 : total]))
	return kv, total + 4, true
}

// kv128SizePrefix reads just the varint header to determine the total encoded size
// (including the trailing 4-byte CRC32C).
func kv128SizePrefix(src []byte) (int, bool) {
	if len(src) < vptrSize+1 {
		return 0, false
	}
	vptr := decodeVPtr(src[:vptrSize])

	klen64, kn := binary.Uvarint(src[vptrSize:])
	if kn <= 0 {
		return 0, false
	}
	maxInt := int(^uint(0) >> 1)
	base := vptrSize + kn + 8 + 4
	if klen64 > uint64(maxInt-base) {
		return 0, false
	}
	klen := int(klen64)
	vn := vptr.Length

	if vn == rawVlenTombstone || vn == 0 || vn > vlogInlineThreshold {
		// no inline value bytes
		return base + klen, true
	}
	if vn > uint64(maxInt-base-klen) {
		return 0, false
	}
	return base + klen + int(vn), true
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

func (db *FlexDB) clearInteriorAnchorTags(anchorLoff uint64, psize uint64) error {
	if psize <= 1 || db == nil || db.ff == nil || db.ff.tree == nil {
		return nil
	}
	fp := db.ff.tree.PosGet(anchorLoff)
	if !fp.Valid() {
		return nil
	}
	end := anchorLoff + psize
	var loffs []uint64
	fp.ForwardExtent()
	for fp.Valid() {
		loff := fp.GetLoff()
		if loff >= end {
			break
		}
		if tag, ok := fp.GetTag(); ok && flexdbTagIsAnchor(tag) {
			loffs = append(loffs, loff)
		}
		fp.ForwardExtent()
	}
	for _, loff := range loffs {
		if err := db.ff.setTagR(loff, 0, false); err != nil {
			return err
		}
	}
	return nil
}

func (db *FlexDB) updateAnchorPage(anchor *dbAnchor, anchorLoff uint64, buf []byte, oldPSize uint32) (int, error) {
	if anchor == nil {
		return -1, fmt.Errorf("flexdb: cannot update nil anchor at loff=%d", anchorLoff)
	}
	tag := flexdbTagGenerate(true, anchor.unsorted)
	n, err := db.ff.updateR(buf, anchorLoff, uint64(len(buf)), uint64(oldPSize), tag)
	if err != nil {
		return -1, fmt.Errorf("flexdb: update anchor page key=%q loff=%d oldPSize=%d newPSize=%d tag=0x%04x: %w",
			anchor.key, anchorLoff, oldPSize, len(buf), tag, err)
	}
	if err := db.clearInteriorAnchorTags(anchorLoff, uint64(len(buf))); err != nil {
		return -1, fmt.Errorf("flexdb: clear interior anchor tags key=%q loff=%d psize=%d: %w",
			anchor.key, anchorLoff, len(buf), err)
	}
	return n, nil
}

// ====================== CRC32C / fingerprint ======================

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

func kvCRC32(key string) uint32 {
	if len(key) == 0 {
		return crc32.Checksum(nil, crc32cTable)
	}
	b := unsafe.Slice(unsafe.StringData(key), len(key))
	return crc32.Checksum(b, crc32cTable)
}

func (db *FlexDB) rememberKeyBloomLocked(key string) {
	if db.keyBloom != nil {
		db.keyBloom.addString(key)
	}
}

func (db *FlexDB) rememberNewKeyBloomLocked(key string) {
	if db.keyBloom != nil && db.keyBloomComplete {
		db.keyBloom.addStringKnownAbsent(key)
	}
}

func (db *FlexDB) keyBloomMayExistLocked(key string, x bool) bool {
	if !db.keyBloomComplete || db.keyBloom == nil {
		return true
	}
	return db.keyBloom.mayContainString(key)
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

// note: DisableVLOG bool is not supported any longer. This options was
// removed from the Config.

// Config allows configuration of a FlexDB.
type Config struct {
	CacheMB uint64 // default 32 (for 32 MB)

	// NoDisk runs the entire database in-memory using MemVFS.
	// No files are created on disk. Useful for testing.
	NoDisk bool

	// FS overrides the filesystem implementation. When nil, RealVFS{}
	// is used (or MemVFS if NoDisk is true). Allows injecting a
	// custom VFS for testing (e.g. fault injection).
	FS vfs.FS

	// OmitFlexSpaceOpsRedoLog skips FlexSpace redo-log writes and instead
	// calls SyncCoW() on every Sync(). This eliminates ~0.86x write
	// amplification from the redo log at the cost of slightly more CoW
	// tree page writes. The net effect is lower total write amp (~3.2x
	// vs ~4.1x).
	OmitFlexSpaceOpsRedoLog bool

	// LowBlockUtilizationPct sets the threshold (0.0–1.0) for counting
	// blocks as "low utilization" in Metrics.BlocksWithLowUtilization.
	// A block whose live bytes / FLEXSPACE_BLOCK_SIZE is below this
	// fraction is counted. Default 0.25 (25%) when zero.
	LowBlockUtilizationPct float64

	// OmitMemWalFsync true means we do not durably fdatasync FLEXDB.MEMWAL.
	// This is useful for batch loading a lot of data quickly, and then doing
	// one fsync at the end for durability. The proviso of course is that
	// if your process crashes you have no intermediate state and have to
	// start again at the beginning; which may be fine. True also disables
	// the LARGE.VLOG fsyncs until db.Sync() is called.
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

	// DisableBackgroundFlush disables the background flush worker goroutine.
	// When true, memtable flushes only happen on explicit Sync() or Close() calls.
	// This is useful for fuzz testing where background goroutines can crash
	// the entire fuzz worker subprocess if they panic (the test's recover()
	// only catches panics on the test goroutine, not background goroutines).
	DisableBackgroundFlush bool

	// BackgroundFlushInterval controls how often the background flush worker
	// wakes up to flush the memtable. If zero or negative, the default is 5s.
	// DisableBackgroundFlush still disables the worker entirely.
	BackgroundFlushInterval time.Duration

	// PaddedSplits controls whether treeInsertAnchor pads both split
	// halves to slottedPageMaxSize. Default false uses tight encoding,
	// which cuts space amplification substantially. When true, the old
	// padded behavior is used (useful for A/B comparison).
	// Padding allows in-place additions to a slotted page to
	// occur, making key updates more efficient. The trade-off
	// is pre-allocating space for new additions.
	PaddedSplits bool

	// AutoVacuumPct enables background vacuum when > 0. The value is the
	// fraction of deleted logical bytes over resident+deleted bytes that
	// should trigger automatic VacuumVLOG and VacuumKV. Values above 1 are
	// clamped to 1. The default of 0 means off (no auto-vacuuming).
	AutoVacuumPct float64

	// AutoVacuumDeletedAboveKB is the minimum deleted logical data threshold
	// before AutoVacuumPct can trigger. If AutoVacuumPct > 0 and this is zero
	// or negative, the default is 100 MB.
	AutoVacuumDeletedAboveKB int64
}

// PiggybackGCStats tracks statistics for piggyback GC runs.
type PiggybackGCStats struct {
	LastGCTime     time.Time
	LastGCDuration time.Duration
	TotalGCRuns    int64
}

// ====================== FlexDB ======================

// FlexDB is a persistent ordered key-value store backed by FlexSpace.
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

	allowReads atomic.Bool

	keyBloom         *keyBloom
	keyBloomComplete bool

	mt            memtable // single memtable (was dual; see commit history)
	flushSeq      uint64   // incremented on each memtable flush (inline or background)
	dirSyncNeeded bool

	// flush worker
	flushTrigger       chan struct{}
	flushHalt          *idem.Halter
	flushWorkerStarted atomic.Bool

	// scratch buffers (reused; protected by ffMu write lock)
	kvbuf1 []byte
	itvbuf []byte

	// Write-byte counters (accessed atomically)
	MemWALBytesWritten  int64 // WAL (FLEXDB.MEMWAL) bytes written
	MemWALFsyncs        int64 // FLEXDB.MEMWAL SyncData calls
	LogicalBytesWritten int64 // user payload bytes (key+value)

	// Cumulative counters loaded from cowMeta on open.
	// Current total = base + session delta.
	totalLogicalBase  int64
	totalPhysicalBase int64

	autoVacuumDeletedBytes     int64
	autoVacuumVLOGDeletedBytes int64
	autoVacuumRuns             int64
	autoVacuumLastDurMs        int64
	autoVacuumLastErr          string

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
	ksNotExistsKeyBloomAdded
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

func (db *FlexDB) noteAutoVacuumObsoleteKV(oldKV KV, oldFound bool, newKV KV) {
	if !oldFound || oldKV.isTombstone() {
		return
	}
	if newKV.isTombstone() {
		db.noteAutoVacuumDeletedLogical(oldKV, true)
		return
	}
	if oldKV.HasVPtr() && (!newKV.HasVPtr() || oldKV.Vptr != newKV.Vptr) {
		db.noteAutoVacuumDeletedLogical(oldKV, false)
	}
}

func (db *FlexDB) noteAutoVacuumDeletedLogical(kv KV, includeKey bool) {
	if db.cfg.AutoVacuumPct <= 0 {
		return
	}
	var n int64
	if includeKey {
		n += int64(len(kv.Key))
	}
	if kv.HasVPtr() {
		n += int64(kv.Vptr.Length)
		atomic.AddInt64(&db.autoVacuumVLOGDeletedBytes, int64(vlogEntryHeaderSize)+int64(kv.Vptr.Length))
	} else {
		n += int64(len(kv.Value))
	}
	if n > 0 {
		atomic.AddInt64(&db.autoVacuumDeletedBytes, n)
	}
}

// writeLockHeldKeyState checks FlexSpace for a key's state.
// Called when a key is new to the memtable.
// Caller must hold topMutRW.Lock().
func (db *FlexDB) writeLockHeldKeyState(key string) keyState {
	const x = true
	if db.ff.Size() == 0 {
		return ksNotExists
	}
	if !db.keyBloomMayExistLocked(key, true) {
		return ksNotExists
	}
	kv, ok, err := db.getPassthroughKV(key, x)
	if err != nil || !ok {
		return ksNotExists
	}
	return kvToState(kv)
}

// lookupOldVPtr searches for an existing VPtr for the given key.
// Checks the active memtable, inactive memtable, and FlexSpace (in that order).
// Returns a zero VPtr if the key doesn't exist or doesn't have a VLOG value.
// Used by the VLOG dedup path to avoid writing duplicate large values.
// Caller must hold topMutRW.Lock().
func (db *FlexDB) lookupOldVPtr(key string, x bool) VPtr {
	_, _, vp := db.lookupOldKVAndVPtr(key, x)
	return vp
}

func (db *FlexDB) lookupOldKVAndVPtr(key string, x bool) (KV, bool, VPtr) {
	if !db.keyBloomMayExistLocked(key, x) {
		return KV{}, false, VPtr{}
	}
	return db.lookupOldKVAndVPtrNoBloom(key, x)
}

func (db *FlexDB) lookupOldKVAndVPtrNoBloom(key string, x bool) (KV, bool, VPtr) {
	// Check memtable first (most likely hit for repeated overwrites).
	if kv, ok := db.mt.get(key, x); ok {
		if kv.HasVPtr() {
			return kv, true, kv.Vptr
		}
		return kv, true, VPtr{}
	}
	// Check FlexSpace (loads interval cache if needed).
	if db.ff.Size() == 0 {
		return KV{}, false, VPtr{}
	}
	kv, ok, _ := db.getPassthroughKV(key, x)
	if ok && kv.HasVPtr() {
		return kv, true, kv.Vptr
	}
	return kv, ok, VPtr{}
}

func (db *FlexDB) lookupOldKVAndVPtrWithHint(key string, nh *memSparseIndexTreeHandler, x bool) (KV, bool, VPtr) {
	if !db.keyBloomMayExistLocked(key, x) {
		return KV{}, false, VPtr{}
	}
	return db.lookupOldKVAndVPtrWithHintNoBloom(key, nh, x)
}

func (db *FlexDB) lookupOldKVAndVPtrWithHintNoBloom(key string, nh *memSparseIndexTreeHandler, x bool) (KV, bool, VPtr) {
	// Check memtable first (most likely hit for repeated overwrites).
	if kv, ok := db.mt.get(key, x); ok {
		if kv.HasVPtr() {
			return kv, true, kv.Vptr
		}
		return kv, true, VPtr{}
	}
	// Check FlexSpace (loads interval cache if needed).
	if db.ff.Size() == 0 {
		return KV{}, false, VPtr{}
	}
	kv, ok, _ := db.getPassthroughKVWithHint(key, nh, x)
	if ok && kv.HasVPtr() {
		return kv, true, kv.Vptr
	}
	return kv, ok, VPtr{}
}

// lookupOldFlexSpaceVPtr searches only the already-materialized FlexSpace.
// Pre-AllowReads batch ingest keeps new writes in an append-only bulk builder;
// asking that builder for old VPtrs forces a large transient index and defeats
// the load path. During pristine initial bulk loads there is no old FlexSpace
// value anyway, and during reload/merge loads this still permits dedup against
// the previously materialized database.
func (db *FlexDB) lookupOldFlexSpaceVPtr(key string, x bool) VPtr {
	if db.ff.Size() == 0 {
		return VPtr{}
	}
	if !db.keyBloomMayExistLocked(key, x) {
		return VPtr{}
	}
	kv, ok, _ := db.getPassthroughKV(key, x)
	if ok && kv.HasVPtr() {
		return kv.Vptr
	}
	return VPtr{}
}

func (db *FlexDB) lookupOldFlexSpaceVPtrWithHint(key string, nh *memSparseIndexTreeHandler, x bool) VPtr {
	if db.ff.Size() == 0 {
		return VPtr{}
	}
	if !db.keyBloomMayExistLocked(key, x) {
		return VPtr{}
	}
	kv, ok, _ := db.getPassthroughKVWithHint(key, nh, x)
	if ok && kv.HasVPtr() {
		return kv.Vptr
	}
	return VPtr{}
}

// Len returns the total number of live (non-tombstone) keys in the database.
// O(1) - reads a pre-maintained counter.
// Goroutine safe.
func (db *FlexDB) Len() int64 {
	db.requireReadsAllowed()
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
	db.requireReadsAllowed()
	db.topMutRW.RLock()
	big = db.liveBigKeys
	small = db.liveSmallKeys
	db.topMutRW.RUnlock()
	return
}

// reconcileBulkInitialCountsLocked computes exact counters for the pristine
// append-only bulk memtable. Caller holds topMutRW.Lock().
func (db *FlexDB) reconcileBulkInitialCountsLocked(m *memtable) {
	if m.bulk.count == 0 {
		db.liveKeys = 0
		db.liveSmallKeys = 0
		db.liveBigKeys = 0
		m.bulk.dirty = false
		return
	}
	m.bulk.ensureIndex()
	var big, small int64
	for _, ref := range m.bulk.index {
		kv := m.bulk.kv(ref)
		if kv.isTombstone() {
			continue
		}
		if kv.HasVPtr() {
			big++
		} else {
			small++
		}
	}
	db.liveBigKeys = big
	db.liveSmallKeys = small
	db.liveKeys = big + small
	m.bulk.dirty = false
}

func (db *FlexDB) materializeBulkInitialXLocked() error {
	if db.mt.bulk.count > 0 && db.ff.Size() > 0 {
		return db.mergeReloadBulkXLocked()
	}
	if db.mt.bulk.count > 0 && db.mt.bulk.dirty {
		db.reconcileBulkInitialCountsLocked(&db.mt)
	}
	db.mt.materializeBulk()
	return nil
}

func (db *FlexDB) bulkInitialFastPathEligibleLocked(m *memtable) bool {
	return !db.allowReads.Load() && m.ks.Len() == 0
}

func (db *FlexDB) requireReadsAllowed() {
	if !db.allowReads.Load() {
		panic("must call db.AllowReads() first. AllowReads() marks the end of the read-disabled load phase.")
	}
}

// recomputeKeyCountsLocked walks all FlexSpace intervals via the sparse index
// and counts live (non-tombstone) keys. Called once at open time after recovery.
// No locking needed - called before the flush worker is started and before the
// db reference is returned.
func (db *FlexDB) recomputeKeyCountsLocked() {
	var big, small int64
	if db.keyBloom != nil {
		db.keyBloom.reset()
		db.keyBloomComplete = true
	}

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
				db.rememberKeyBloomLocked(kv.Key)
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

// OpenFlexDB opens or creates a FlexDB at the given directory path.
// cacheMB is the cache capacity in megabytes.
//
// Every newly opened handle starts in a read-disabled load phase. During this
// phase only Batch.Set(), Batch.SetBytes(), Batch.Delete(), Batch.Commit(), and
// db.Sync() are supported. The optimized bulk builder is used when the database
// is empty; if the directory already contains data, pre-AllowReads batches are
// kept as a sorted reload run and merged into the existing database when
// AllowReads or Sync is called.
//
// The user must call FlexDB.AllowReads() to end the load phase and enable
// Get/Find, singleton Put/Delete, transactions, range deletes, Clear, Merge,
// vacuum, and integrity checks. Violations of this contract panic immediately to
// teach the expected use pattern.
func OpenFlexDB(path string, pCfg *Config) (*FlexDB, error) {

	//fmt.Printf("rnd0 = '%v'\n rnd1 = '%v'\n", cryRand33B(), cryRand33B())
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
	if cfg.BackgroundFlushInterval <= 0 {
		cfg.BackgroundFlushInterval = defaultBackgroundFlushInterval
	}
	if cfg.LowBlockUtilizationPct <= 0 || cfg.LowBlockUtilizationPct > 1 {
		cfg.LowBlockUtilizationPct = 0.50
	}
	if cfg.AutoVacuumPct > 1 {
		cfg.AutoVacuumPct = 1
	}
	if cfg.AutoVacuumPct > 0 && cfg.AutoVacuumDeletedAboveKB <= 0 {
		cfg.AutoVacuumDeletedAboveKB = 100 * 1024
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

	// Open the single 20-byte-header MEMWAL.
	walPath := filepath.Join(path, "FLEXDB.MEMWAL")
	walFD, err := fs.OpenReadWrite(walPath, vfs.WriteCategoryUnspecified)
	if err != nil {
		ff.Close()
		return nil, fmt.Errorf("flexdb: open FLEXDB.MEMWAL: %w", err)
	}

	// From github.com/tigerbeetle/tigerbeetle/src/io/linux.zig:1640
	// "The best fsync strategy is always to fsync before reading..."
	walFD.Sync()

	// Open VLOG (value log for large values) unless disabled.
	var vl *valueLog

	//if !cfg.DisableVLOG {
	vlogPath := filepath.Join(path, "LARGE.VLOG")
	vl, err = openValueLog(vlogPath, fs)
	if err != nil {
		ff.Close()
		walFD.Close()
		return nil, fmt.Errorf("flexdb: open VLOG: %w", err)
	}
	//}

	// Sync the parent directory so that newly created files are durable.
	// Without this, a crash could lose the directory entries even though
	// the file contents were synced.
	if err := syncDir(fs, path); err != nil {
		ff.Close()
		walFD.Close()
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
		keyBloom:     newKeyBloom(),
		kvbuf1:       make([]byte, 0, MaxKeySize),
		itvbuf:       make([]byte, 0, flexdbSparseIntervalSize+MaxKeySize),
		flushTrigger: make(chan struct{}, 1),
		flushHalt:    idem.NewHalterNamed("flushWorker-orig"),
	}
	db.cache.db = db
	for i := range db.cache.partitions {
		db.cache.partitions[i].db = db
	}
	db.mt = *newMemtable(walFD)
	db.mt.memWalBytesWritten = &db.MemWALBytesWritten
	db.mt.memWalFsyncs = &db.MemWALFsyncs

	// Load cumulative counters from the last cowMeta commit.
	db.totalLogicalBase = ff.tree.totalLogicalBytesWrit
	db.totalPhysicalBase = ff.tree.totalPhysicalBytesWrit

	// Restore HLC to be strictly higher than any previously used value.
	if ff.tree.MaxHLC > 0 {
		db.hlc.ReceiveMessageWithHLC(HLC(ff.tree.MaxHLC))
	}

	// Create fresh sparse index tree
	db.tree = memSparseIndexTreeCreate()

	// Restore live key counters before replay. If recovery applies WAL records,
	// it recomputes these counters from the recovered FlexSpace.
	db.liveKeys = ff.tree.liveKeys
	db.liveBigKeys = ff.tree.liveBigKeys
	db.liveSmallKeys = ff.tree.liveSmallKeys

	// Recovery or fresh DB.
	ffSize := ff.Size()
	db.keyBloomComplete = ffSize == 0
	walSize, err := db.mt.memWalSize()
	if err != nil {
		ff.Close()
		walFD.Close()
		if vl != nil {
			vl.close()
		}
		return nil, fmt.Errorf("flexdb: inspect memwal: %w", err)
	}
	if ffSize > 0 || walSize > memWalHeaderSize {
		if err := db.recovery(); err != nil {
			ff.Close()
			walFD.Close()
			if vl != nil {
				vl.close()
			}
			return nil, err
		}
	} else {
		// Tag loff=0 as the first anchor (but FlexSpace is empty, so SetTag may be a no-op)
		tag := flexdbTagGenerate(true, 0)
		_ = ff.SetTag(0, tag) // best-effort on empty FlexSpace
	}
	if err := db.recoverVLOGVacuumTemp(); err != nil {
		ff.Close()
		walFD.Close()
		if vl != nil {
			vl.close()
		}
		return nil, err
	}

	// Reset WAL (always use 20-byte versioned header for consistent disk format)
	ts := uint64(time.Now().UnixNano())
	if err := db.mt.logTruncateSyncWithVersion(ts, db.ff.tree.PersistentVersion); err != nil {
		ff.Close()
		walFD.Close()
		if vl != nil {
			vl.close()
		}
		return nil, fmt.Errorf("flexdb: initialize memwal: %w", err)
	}

	db.resetFsyncMetricsAfterStartup()

	return db, nil
}

func (db *FlexDB) resetFsyncMetricsAfterStartup() {
	atomic.StoreInt64(&db.MemWALFsyncs, 0)
	if db.ff != nil {
		atomic.StoreInt64(&db.ff.KV128Fsyncs, 0)
		atomic.StoreInt64(&db.ff.REDOLogFsyncs, 0)
		if db.ff.tree != nil {
			atomic.StoreInt64(&db.ff.tree.FlexTreePagesFsyncs, 0)
			atomic.StoreInt64(&db.ff.tree.FlexTreeCommitFsyncs, 0)
		}
	}
	if db.vlog != nil {
		atomic.StoreInt64(&db.vlog.VLOGFsyncs, 0)
	}
}

// Close syncs and shuts down the FlexDB.
func (db *FlexDB) Close() *Metrics {

	if !db.cfg.DisableBackgroundFlush && db.flushWorkerStarted.CompareAndSwap(true, false) {
		// Signal flush worker to stop, then wait for it to finish.
		// We do this first to avoid deadlock: if
		// we grab the topMutRW and then flush worker waits
		// on it rather than getting our halt request.
		db.flushHalt.RequestStop()
		<-db.flushHalt.Done.Chan
	}

	db.topMutRW.Lock()
	const x = true
	defer db.topMutRW.Unlock()

	if db.closed {
		return nil
	}
	db.closed = true

	// Flush any data that is still in the memtable.
	if !db.mt.empty.Load() {
		if err := db.mt.logFlush(); err != nil {
			panicf("Close flush memwal: %v", err)
		}
		if err := db.flushMemtable(x); err != nil {
			panicf("Close flush memtable: %v", err)
		}
		if err := db.cache.flushDirtyPages(); err != nil {
			panicf("Close flush dirty pages: %v", err)
		}
		db.persistCounters()
		db.ff.Sync()
		db.verifyAnchorTags()
	} else {
		// Even without new memtable data, flush any dirty cache entries
		// that were modified earlier but not yet written.
		if err := db.cache.flushDirtyPages(); err != nil {
			panicf("Close flush dirty pages: %v", err)
		}
		db.persistCounters()
		db.ff.Sync()
		db.verifyAnchorTags()
	}

	// Truncate WAL (always use 20-byte versioned header for consistent disk format)
	ts := uint64(time.Now().UnixNano())
	if err := db.mt.logTruncateSyncWithVersion(ts, db.ff.tree.PersistentVersion); err != nil {
		panicf("Close truncate memwal: %v", err)
	}

	db.cache.destroyAll()
	var vlogFoot, kvFoot int64
	if db.vlog != nil {
		db.vlog.sync()
		vlogFoot, _ = db.vlog.close()
	}
	// Persist counters before final SyncCoW in Close.
	db.persistCounters()
	kvFoot = db.ff.Close()

	// Capture final metrics after all writes are done but before
	// closing file descriptors. This solves the chicken-and-egg
	// problem: accurate cumulative write amplification is only
	// knowable after the final sync, but the DB is closing.
	m := db.writeLockHeldFinalMetrics(kvFoot, vlogFoot)

	db.mt.memWalFD.Close()
	return m
}

// Metrics holds byte-level write counters for computing write amplification.
type Metrics struct {
	Session                   bool
	LiveKeyCount              int64
	KV128BytesWritten         int64 // FlexSpace FLEXSPACE.KV.SLOT_BLOCKS file
	MemWALBytesWritten        int64 // FlexDB WAL (FLEXDB.MEMWAL)
	REDOLogBytesWritten       int64 // FLEXSPACE.REDO.LOG
	FlexTreePagesBytesWritten int64 // CoW FLEXTREE.PAGES + FLEXTREE.COMMIT
	VLOGBytesWritten          int64 // VLOG value log
	LogicalBytesWritten       int64 // user payload (key + value)
	TotalBytesWritten         int64 // sum of all physical writes
	KV128Fsyncs               int64 // FLEXSPACE.KV.SLOT_BLOCKS Sync calls after OpenFlexDB startup
	MemWALFsyncs              int64 // FLEXDB.MEMWAL Sync/SyncData calls after OpenFlexDB startup
	REDOLogFsyncs             int64 // FLEXSPACE.REDO.LOG Sync calls after OpenFlexDB startup
	FlexTreePagesFsyncs       int64 // FLEXTREE.PAGES Sync/SyncData calls after OpenFlexDB startup
	FlexTreeCommitFsyncs      int64 // FLEXTREE.COMMIT Sync/SyncData calls after OpenFlexDB startup
	VLOGFsyncs                int64 // LARGE.VLOG Sync calls after OpenFlexDB startup
	TotalFsyncs               int64 // sum of tracked core-file Sync/SyncData calls after startup

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

	// BlocksInUse shows how many 4MB blocks FLEXSPACE.KV.SLOT_BLOCKS is using.
	BlocksInUse int64

	// BlocksWithLowUtilization is the count of non-empty blocks whose
	// utilization (live bytes / block size) is below the configured
	// LowBlockUtilizationPct threshold (default 25%).
	BlocksWithLowUtilization int64

	// KVBlocksTotalLiveBytes is the sum of live (used) bytes across all blocks,
	// as tracked by the block manager.
	KVBlocksTotalLiveBytes int64

	// KVBlocksOnDiskFootprintBytes is the on-disk footprint of bytes for FLEXSPACE.KV.SLOT_BLOCKS.
	KVBlocksOnDiskFootprintBytes int64

	// VlogOnDiskFootprintBytes is the on-disk footprints for LARGE.VLOG.
	VlogOnDiskFootprintBytes int64

	// LowBlockUtilizationPct we used; copied from Config
	// or what default we used if not set.
	LowBlockUtilizationPct float64

	// PiggybackGCRuns is the number of piggyback GC runs during this session.
	PiggybackGCRuns int64

	// PiggybackGCLastDurMs is the duration of the last piggyback GC run in milliseconds.
	PiggybackGCLastDurMs int64

	// AutoVacuumRuns is the number of background autovacuum runs completed.
	AutoVacuumRuns int64

	// AutoVacuumLastDurMs is the duration of the last autovacuum run in milliseconds.
	AutoVacuumLastDurMs int64

	// AutoVacuumDeletedBytes is the current deleted logical byte estimate
	// waiting to be reclaimed by autovacuum.
	AutoVacuumDeletedBytes int64

	// AutoVacuumVLOGDeletedBytes is the portion of AutoVacuumDeletedBytes
	// associated with obsolete VLOG value entries.
	AutoVacuumVLOGDeletedBytes int64

	// AutoVacuumLastErr is the last background autovacuum error, if any.
	AutoVacuumLastErr string
}

func (z *Metrics) String() (r string) {
	r = "Metrics{\n"
	r += fmt.Sprintf("      (just this) Session: %v\n", z.Session)
	r += fmt.Sprintf("              LiveKeyCount: %v\n", formatInt64Under(z.LiveKeyCount))
	r += fmt.Sprintf("        KV128 BytesWritten: %v\n", formatInt64Under(z.KV128BytesWritten))
	r += fmt.Sprintf("       MemWAL BytesWritten: %v\n", formatInt64Under(z.MemWALBytesWritten))
	r += fmt.Sprintf("      REDOLog BytesWritten: %v\n", formatInt64Under(z.REDOLogBytesWritten))
	r += fmt.Sprintf("FlexTreePages BytesWritten: %v\n", formatInt64Under(z.FlexTreePagesBytesWritten))
	r += fmt.Sprintf("   LARGE.VLOG BytesWritten: %v\n", formatInt64Under(z.VLOGBytesWritten))
	r += fmt.Sprintf("      Logical BytesWritten: %v\n", formatInt64Under(z.LogicalBytesWritten))
	r += fmt.Sprintf("        Total BytesWritten: %v\n", formatInt64Under(z.TotalBytesWritten))
	r += fmt.Sprintf("                 WriteAmp: %0.3f\n", z.WriteAmp)
	r += fmt.Sprintf("              TotalFsyncs: %v\n", formatInt64Under(z.TotalFsyncs))
	r += fmt.Sprintf("                KV128Sync: %v\n", formatInt64Under(z.KV128Fsyncs))
	r += fmt.Sprintf("               MemWALSync: %v\n", formatInt64Under(z.MemWALFsyncs))
	r += fmt.Sprintf("              REDOLogSync: %v\n", formatInt64Under(z.REDOLogFsyncs))
	r += fmt.Sprintf("        FlexTreePagesSync: %v\n", formatInt64Under(z.FlexTreePagesFsyncs))
	r += fmt.Sprintf("       FlexTreeCommitSync: %v\n", formatInt64Under(z.FlexTreeCommitFsyncs))
	r += fmt.Sprintf("                 VLOGSync: %v\n", formatInt64Under(z.VLOGFsyncs))
	r += fmt.Sprintf("\n   -------- lifetime totals over all sessions  --------  \n")
	r += fmt.Sprintf("    TotalLogical BytesWrit: %v\n", formatInt64Under(z.totalLogicalBytesWrit))
	r += fmt.Sprintf("   TotalPhysical BytesWrit: %v\n", formatInt64Under(z.totalPhysicalBytesWrit))
	r += fmt.Sprintf("       CumulativeWriteAmp: %0.3f\n", z.CumulativeWriteAmp)
	r += fmt.Sprintf("\n   --- free space / block utilization in FLEXSPACE.KV.SLOT_BLOCKS ---  \n")
	r += fmt.Sprintf("    KVBlocksTotalLiveBytes: %v (%0.2f MB)\n", formatInt64Under(z.KVBlocksTotalLiveBytes), float64(z.KVBlocksTotalLiveBytes)/(1<<20))
	r += fmt.Sprintf("    TotalFreeBytesInBlocks: %v (%0.2f MB)\n", formatInt64Under(z.TotalFreeBytesInBlocks), float64(z.TotalFreeBytesInBlocks)/(1<<20))
	r += fmt.Sprintf("      FLEXSPACE_BLOCK_SIZE: %0.2f MB\n", float64(FLEXSPACE_BLOCK_SIZE)/(1<<20))
	r += fmt.Sprintf("               BlocksInUse: %v  (%0.2f MB)\n", formatInt64Under(z.BlocksInUse), float64(z.BlocksInUse*FLEXSPACE_BLOCK_SIZE)/(1<<20))
	r += fmt.Sprintf("  BlocksWithLowUtilization: %v\n", formatInt64Under(z.BlocksWithLowUtilization))
	r += fmt.Sprintf("\n   -------- based on parameters used --------  \n")
	r += fmt.Sprintf("    LowBlockUtilizationPct: %0.1f %%\n", 100*z.LowBlockUtilizationPct)
	if z.PiggybackGCRuns > 0 {
		r += fmt.Sprintf("\n   -------- piggyback GC --------  \n")
		r += fmt.Sprintf("          PiggybackGCRuns: %v\n", formatInt64Under(z.PiggybackGCRuns))
		r += fmt.Sprintf("     PiggybackGCLastDurMs: %v\n", z.PiggybackGCLastDurMs)
	}
	if z.AutoVacuumRuns > 0 || z.AutoVacuumDeletedBytes > 0 || z.AutoVacuumLastErr != "" {
		r += fmt.Sprintf("\n   -------- auto vacuum --------  \n")
		r += fmt.Sprintf("          AutoVacuumRuns: %v\n", formatInt64Under(z.AutoVacuumRuns))
		r += fmt.Sprintf("     AutoVacuumLastDurMs: %v\n", z.AutoVacuumLastDurMs)
		r += fmt.Sprintf("     AutoVacuumDeletedBy: %v\n", formatInt64Under(z.AutoVacuumDeletedBytes))
		r += fmt.Sprintf(" AutoVacuumVLOGDeletedBy: %v\n", formatInt64Under(z.AutoVacuumVLOGDeletedBytes))
		if z.AutoVacuumLastErr != "" {
			r += fmt.Sprintf("       AutoVacuumLastErr: %v\n", z.AutoVacuumLastErr)
		}
	}
	r += fmt.Sprintf("\n   -------- on disk big files summary --------  \n")

	r += fmt.Sprintf(" FLEXSPACE.KV.SLOT_BLOCKS: %7.3f MB :%15s bytes\n", float64(z.KVBlocksOnDiskFootprintBytes)/(1<<20), formatInt64Under(z.KVBlocksOnDiskFootprintBytes))
	r += fmt.Sprintf("               LARGE.VLOG: %7.3f MB :%15s bytes\n", float64(z.VlogOnDiskFootprintBytes)/(1<<20), formatInt64Under(z.VlogOnDiskFootprintBytes))

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
		LiveKeyCount:              db.liveKeys,
		Session:                   true,
		KV128BytesWritten:         atomic.LoadInt64(&db.ff.KV128BytesWritten),
		MemWALBytesWritten:        atomic.LoadInt64(&db.MemWALBytesWritten),
		REDOLogBytesWritten:       atomic.LoadInt64(&db.ff.REDOLogBytesWritten),
		FlexTreePagesBytesWritten: atomic.LoadInt64(&db.ff.tree.FlexTreePagesBytesWritten),
		LogicalBytesWritten:       atomic.LoadInt64(&db.LogicalBytesWritten),
		KV128Fsyncs:               atomic.LoadInt64(&db.ff.KV128Fsyncs),
		MemWALFsyncs:              atomic.LoadInt64(&db.MemWALFsyncs),
		REDOLogFsyncs:             atomic.LoadInt64(&db.ff.REDOLogFsyncs),
		FlexTreePagesFsyncs:       atomic.LoadInt64(&db.ff.tree.FlexTreePagesFsyncs),
		FlexTreeCommitFsyncs:      atomic.LoadInt64(&db.ff.tree.FlexTreeCommitFsyncs),
		LowBlockUtilizationPct:    db.cfg.LowBlockUtilizationPct,
	}
	if db.vlog != nil {
		m.VLOGBytesWritten = atomic.LoadInt64(&db.vlog.VLOGBytesWritten)
		m.VLOGFsyncs = atomic.LoadInt64(&db.vlog.VLOGFsyncs)
	}
	m.TotalBytesWritten = m.KV128BytesWritten + m.MemWALBytesWritten +
		m.REDOLogBytesWritten + m.FlexTreePagesBytesWritten + m.VLOGBytesWritten
	m.TotalFsyncs = m.KV128Fsyncs + m.MemWALFsyncs + m.REDOLogFsyncs +
		m.FlexTreePagesFsyncs + m.FlexTreeCommitFsyncs + m.VLOGFsyncs

	if m.LogicalBytesWritten > 0 {
		m.WriteAmp = float64(m.TotalBytesWritten) / float64(m.LogicalBytesWritten)
	}

	m.totalLogicalBytesWrit = db.totalLogicalBytesWrit()
	m.totalPhysicalBytesWrit = db.totalPhysicalBytesWrit()
	if m.totalLogicalBytesWrit > 0 {
		m.CumulativeWriteAmp = float64(m.totalPhysicalBytesWrit) / float64(m.totalLogicalBytesWrit)
	}

	m.KVBlocksTotalLiveBytes, m.TotalFreeBytesInBlocks, m.BlocksInUse, m.BlocksWithLowUtilization =
		db.ff.garbageMetrics(db.cfg.LowBlockUtilizationPct)

	m.PiggybackGCRuns = db.piggyGCStats.TotalGCRuns
	m.PiggybackGCLastDurMs = db.piggyGCStats.LastGCDuration.Milliseconds()
	db.writeLockHeldAutoVacuumMetrics(m)

	m.KVBlocksOnDiskFootprintBytes = mustStatFileSize(db.ff.fdKV128blocks)
	m.VlogOnDiskFootprintBytes = mustStatFileSize(db.vlog.fd)

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
func (db *FlexDB) writeLockHeldFinalMetrics(kvFoot, vlogFoot int64) *Metrics {

	m := &Metrics{
		Session:                   true,
		LiveKeyCount:              db.liveKeys,
		KV128BytesWritten:         atomic.LoadInt64(&db.ff.KV128BytesWritten),
		MemWALBytesWritten:        atomic.LoadInt64(&db.MemWALBytesWritten),
		REDOLogBytesWritten:       atomic.LoadInt64(&db.ff.REDOLogBytesWritten),
		FlexTreePagesBytesWritten: atomic.LoadInt64(&db.ff.tree.FlexTreePagesBytesWritten),
		LogicalBytesWritten:       atomic.LoadInt64(&db.LogicalBytesWritten),
		KV128Fsyncs:               atomic.LoadInt64(&db.ff.KV128Fsyncs),
		MemWALFsyncs:              atomic.LoadInt64(&db.MemWALFsyncs),
		REDOLogFsyncs:             atomic.LoadInt64(&db.ff.REDOLogFsyncs),
		FlexTreePagesFsyncs:       atomic.LoadInt64(&db.ff.tree.FlexTreePagesFsyncs),
		FlexTreeCommitFsyncs:      atomic.LoadInt64(&db.ff.tree.FlexTreeCommitFsyncs),
		LowBlockUtilizationPct:    db.cfg.LowBlockUtilizationPct,
	}
	if db.vlog != nil {
		m.VLOGBytesWritten = atomic.LoadInt64(&db.vlog.VLOGBytesWritten)
		m.VLOGFsyncs = atomic.LoadInt64(&db.vlog.VLOGFsyncs)
	}
	m.TotalBytesWritten = m.KV128BytesWritten + m.MemWALBytesWritten +
		m.REDOLogBytesWritten + m.FlexTreePagesBytesWritten + m.VLOGBytesWritten
	m.TotalFsyncs = m.KV128Fsyncs + m.MemWALFsyncs + m.REDOLogFsyncs +
		m.FlexTreePagesFsyncs + m.FlexTreeCommitFsyncs + m.VLOGFsyncs

	if m.LogicalBytesWritten > 0 {
		m.WriteAmp = float64(m.TotalBytesWritten) / float64(m.LogicalBytesWritten)
	}

	m.totalLogicalBytesWrit = db.totalLogicalBytesWrit()
	m.totalPhysicalBytesWrit = db.totalPhysicalBytesWrit()
	if m.totalLogicalBytesWrit > 0 {
		m.CumulativeWriteAmp = float64(m.totalPhysicalBytesWrit) / float64(m.totalLogicalBytesWrit)
	}

	m.KVBlocksTotalLiveBytes, m.TotalFreeBytesInBlocks, m.BlocksInUse, m.BlocksWithLowUtilization =
		db.ff.garbageMetrics(db.cfg.LowBlockUtilizationPct)

	m.PiggybackGCRuns = db.piggyGCStats.TotalGCRuns
	m.PiggybackGCLastDurMs = db.piggyGCStats.LastGCDuration.Milliseconds()
	db.writeLockHeldAutoVacuumMetrics(m)

	m.KVBlocksOnDiskFootprintBytes = kvFoot
	m.VlogOnDiskFootprintBytes = vlogFoot

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

	// FLEXSPACE.KV.SLOT_BLOCKS file: total FlexSpace data on disk.
	if fi, err := db.ff.fdKV128blocks.Stat(); err == nil {
		m.KV128BytesWritten = fi.Size()
	}

	// WAL file: FLEXDB.MEMWAL current size.
	if fi, err := db.mt.memWalFD.Stat(); err == nil {
		m.MemWALBytesWritten = fi.Size()
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
	// data size across all sessions-the best cumulative approximation
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

	m.KVBlocksTotalLiveBytes, m.TotalFreeBytesInBlocks, m.BlocksInUse, m.BlocksWithLowUtilization =
		db.ff.garbageMetrics(db.cfg.LowBlockUtilizationPct)

	m.PiggybackGCRuns = db.piggyGCStats.TotalGCRuns
	m.PiggybackGCLastDurMs = db.piggyGCStats.LastGCDuration.Milliseconds()
	db.writeLockHeldAutoVacuumMetrics(m)

	return m
}

func (db *FlexDB) writeLockHeldAutoVacuumMetrics(m *Metrics) {
	m.AutoVacuumRuns = atomic.LoadInt64(&db.autoVacuumRuns)
	m.AutoVacuumLastDurMs = atomic.LoadInt64(&db.autoVacuumLastDurMs)
	m.AutoVacuumDeletedBytes = atomic.LoadInt64(&db.autoVacuumDeletedBytes)
	m.AutoVacuumVLOGDeletedBytes = atomic.LoadInt64(&db.autoVacuumVLOGDeletedBytes)
	m.AutoVacuumLastErr = db.autoVacuumLastErr
}

// resolveVPtr reads the value from the VLOG file for a KV that has HasVPtr() true.
// For small inline values where HasVPtr() is false, we return kv.Value.
// Returns the resolved value bytes, or an error.
func (db *FlexDB) resolveVPtr(kv KV, x bool) (val []byte, vtyp uint64, hlc HLC, err error) {
	if testHookResolveVPtr != nil {
		if err := testHookResolveVPtr(kv); err != nil {
			return nil, 0, 0, err
		}
	}
	if kv.Vptr.Length == rawVlenTombstone {
		return nil, 0, 0, ErrTomb
	}
	if !kv.HasVPtr() {
		return kv.Value, kv.Vptr.Offset, kv.Hlc, nil
	}

	if db.vlog == nil {
		return nil, 0, 0, fmt.Errorf("flexdb: VPtr but VLOG is nil")
	}
	// allow kv.Value to be skipped for vtyp 0 on disk.
	if len(kv.Value) == 8 {
		vtyp = getUint64(kv.Value)
	}
	val, err = db.vlog.read(kv.Vptr, x)
	hlc = kv.Hlc
	return
}

// FetchLarge retrieves the value bytes for a KV whose value
// is stored in the VLOG (kv.Large() returns true). For inline
// values, it simply returns kv.Value. The returned bytes are
// a fresh copy safe to retain.
//
// Goroutine safe. Acquires the read lock internally.
func (db *FlexDB) FetchLarge(kv *KV) (val []byte, vtyp uint64, hlc HLC, err error) {
	if kv == nil {
		return nil, 0, 0, fmt.Errorf("flexdb: FetchLarge called with nil KV")
	}
	db.requireReadsAllowed()
	db.topMutRW.RLock()
	defer db.topMutRW.RUnlock()
	return db.resolveVPtrForUserKV(*kv, false)
}

// lockHeldFetchLarge is the lock-held body of FetchLarge.
// Caller must hold topMutRW.RLock() or topMutRW.Lock(), and
// indicate which using the x argument (true=>exclusive access, top write lock held).
func (db *FlexDB) lockHeldFetchLarge(kv *KV, x bool) (val []byte, vtyp uint64, hlc HLC, err error) {
	if kv == nil {
		return nil, 0, 0, fmt.Errorf("flexdb: FetchLarge called with nil KV")
	}
	return db.resolveVPtrForUserKV(*kv, x)
}

func (db *FlexDB) resolveVPtrForUserKV(kv KV, x bool) (val []byte, vtyp uint64, hlc HLC, err error) {
	val, vtyp, hlc, err = db.resolveVPtr(kv, x)
	if err != nil || !kv.HasVPtr() || len(kv.Value) == 8 {
		return
	}
	if lookedUp, ok := db.lookupLargeVtypLocked(kv, x); ok {
		vtyp = lookedUp
	}
	return
}

func (db *FlexDB) lookupLargeVtypLocked(kv KV, x bool) (uint64, bool) {
	if kv.Key == "" || !kv.HasVPtr() {
		return 0, false
	}
	sameKV := func(cur KV) bool {
		return cur.HasVPtr() && cur.Vptr == kv.Vptr && cur.Hlc == kv.Hlc
	}
	if !db.mt.empty.Load() {
		cur, ok := db.mt.get(kv.Key, x)
		if ok {
			if sameKV(cur) {
				return cur.Vtyp(), true
			}
			return 0, false
		}
	}
	if db.ff.Size() == 0 || !db.keyBloomMayExistLocked(kv.Key, x) {
		return 0, false
	}
	cur, ok, err := db.getPassthroughKV(kv.Key, x)
	if err != nil || !ok || !sameKV(cur) {
		return 0, false
	}
	return cur.Vtyp(), true
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

// VacuumVLOG reclaims dead LARGE.VLOG space by copying live values to a new VLOG
// file and rewriting their VPtrs in FlexSpace. This is an exclusive operation
// that acquires topMutRW.
//
// Crash safety: if the process crashes before the rename completes, the old
// VLOG and old intervals remain intact. The stale VLOG.new file (if present)
// is harmless and will be overwritten on the next vacuum.
//
// VacuumVLOG requires AllowReads.
func (db *FlexDB) VacuumVLOG() (*VacuumVLOGStats, error) {
	db.requireReadsAllowed()
	db.topMutRW.Lock()
	defer db.topMutRW.Unlock()
	return db.vacuumVLOGXLocked()
}

func (db *FlexDB) vacuumVLOGXLocked() (*VacuumVLOGStats, error) {
	const x = true
	if db.vlog == nil {
		return nil, fmt.Errorf("flexdb: VLOG is disabled")
	}
	stats := &VacuumVLOGStats{}

	// Flush memtable so all live VPtrs are in FlexSpace.
	if err := db.writeLockHeldSync(); err != nil {
		return stats, fmt.Errorf("vacuum: sync before vacuum: %w", err)
	}

	// Exclusive access to FlexSpace and memtable by topMutRW

	stats.OldVLOGSize = db.vlog.size()

	// Create new VLOG file.
	newPath := filepath.Join(db.Path, "VLOG.new")
	readyPath := filepath.Join(db.Path, "VLOG.new.ready")
	_ = db.vfs.Remove(newPath)
	_ = db.vfs.Remove(readyPath)
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
			fce, err := partition.getEntry(anchor, anchorLoff, db)
			if err != nil {
				partition.releaseEntry(fce)
				return stats, fmt.Errorf("vacuum: getEntry: %w", err)
			}

			// Check if any KV in this interval has a VPtr.
			hasVPtr := false
			for i := 0; i < fce.count; i++ {
				if fce.kvs[i].HasVPtr() {
					hasVPtr = true
					break
				}
			}
			if !hasVPtr {
				partition.releaseEntry(fce)
				continue
			}

			// Copy live VPtr values to new VLOG and update VPtrs in a temporary
			// slice. The cache is updated only after FlexSpace accepts the rewrite.
			updated := append([]KV(nil), fce.kvs[:fce.count]...)
			intervalEntriesCopied := int64(0)
			for i := 0; i < len(updated); i++ {
				if !updated[i].HasVPtr() {
					continue
				}
				// Read value from old VLOG.
				val, err := db.vlog.read(updated[i].Vptr, x)
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
				updated[i].Vptr = newVP
				intervalEntriesCopied++
			}

			// Re-encode the entire interval as a slotted page and rewrite
			// in FlexSpace. Using slotted page format (same as all other
			// write paths) avoids the kv128/slotted format mismatch that
			// caused bloat on subsequent loads.
			buf := slottedPageEncode(updated)
			newPSize := uint32(len(buf))
			anchor.unsorted = 0
			if _, err := db.updateAnchorPage(anchor, anchorLoff, buf, anchor.psize); err != nil {
				partition.releaseEntry(fce)
				newVL.close()
				db.vfs.Remove(newPath)
				return stats, fmt.Errorf("vacuum: update FlexSpace anchor key=%q loff=%d oldPSize=%d newPSize=%d maxLoff=%d: %w",
					anchor.key, anchorLoff, anchor.psize, newPSize, db.ff.tree.MaxLoff, err)
			}
			copy(fce.kvs[:fce.count], updated)
			if newPSize != anchor.psize {
				nh.idx = ai
				nh.shiftUpPropagate(int64(newPSize) - int64(anchor.psize))
				anchor.psize = newPSize
			}
			stats.EntriesCopied += intervalEntriesCopied
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
	if err := truncateFileToZero(db.vfs, readyPath); err != nil {
		newVL.close()
		return stats, fmt.Errorf("vacuum: create VLOG.new ready marker: %w", err)
	}
	if err := syncDir(db.vfs, db.Path); err != nil {
		newVL.close()
		return stats, fmt.Errorf("vacuum: sync VLOG.new ready marker: %w", err)
	}
	if testHookVacuumVLOGAfterFlexSpaceSyncBeforeRename != nil {
		if err := testHookVacuumVLOGAfterFlexSpaceSyncBeforeRename(db); err != nil {
			newVL.close()
			return stats, err
		}
	}

	// Close old VLOG fd, rename new -> old, reopen.
	oldPath := filepath.Join(db.Path, "LARGE.VLOG")
	newVL.close()
	if err := db.vfs.Rename(newPath, oldPath); err != nil {
		return stats, fmt.Errorf("vacuum: rename: %w", err)
	}
	_ = db.vfs.Remove(readyPath)
	db.dirSyncNeeded = true
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
			if fce := anchor.loadFce(); fce != nil {
				fce.anchor = nil
				anchor.storeFce(nil)
			}
		}
	}
	db.cache.destroyAll()

	return stats, nil
}

func (db *FlexDB) recoverVLOGVacuumTemp() error {
	if db.vlog == nil {
		return nil
	}
	newPath := filepath.Join(db.Path, "VLOG.new")
	readyPath := filepath.Join(db.Path, "VLOG.new.ready")
	if !fileExists(db.vfs, newPath) {
		_ = db.vfs.Remove(readyPath)
		return nil
	}
	oldPath := filepath.Join(db.Path, "LARGE.VLOG")

	currentErr := db.checkVLOGPointersNoLock()
	ready := fileExists(db.vfs, readyPath)
	if currentErr == nil && !ready {
		_ = db.vfs.Remove(newPath)
		return nil
	}

	currentVL := db.vlog
	tempVL, err := openValueLog(newPath, db.vfs)
	if err != nil {
		return fmt.Errorf("flexdb: recover VLOG vacuum temp: current VLOG invalid (%v), open VLOG.new: %w", currentErr, err)
	}
	db.vlog = tempVL
	tempErr := db.checkVLOGPointersNoLock()
	_, closeErr := tempVL.close()
	db.vlog = currentVL
	if closeErr != nil {
		return fmt.Errorf("flexdb: recover VLOG vacuum temp: close VLOG.new: %w", closeErr)
	}
	if tempErr != nil {
		return fmt.Errorf("flexdb: recover VLOG vacuum temp: current VLOG invalid (%v), VLOG.new invalid too: %w", currentErr, tempErr)
	}

	_, _ = currentVL.close()
	_ = db.vfs.Remove(oldPath)
	if err := db.vfs.Rename(newPath, oldPath); err != nil {
		return fmt.Errorf("flexdb: recover VLOG vacuum temp: promote VLOG.new: %w", err)
	}
	db.dirSyncNeeded = true
	if err := syncDir(db.vfs, db.Path); err != nil {
		return fmt.Errorf("flexdb: recover VLOG vacuum temp: sync dir: %w", err)
	}
	_ = db.vfs.Remove(readyPath)
	if err := currentVL.reopen(oldPath); err != nil {
		return fmt.Errorf("flexdb: recover VLOG vacuum temp: reopen promoted VLOG: %w", err)
	}
	db.vlog = currentVL
	return nil
}

func (db *FlexDB) checkVLOGPointersNoLock() error {
	const x = false
	if db.tree == nil {
		return nil
	}
	for snode := db.tree.leafHead; snode != nil; snode = snode.next {
		var nh memSparseIndexTreeHandler
		nh.node = snode
		memSparseIndexTreeHandlerInfoUpdate(&nh)
		for ai := 0; ai < snode.count; ai++ {
			anchor := snode.anchors[ai]
			if anchor == nil || anchor.psize == 0 {
				continue
			}
			anchorLoff := uint64(anchor.loff + nh.shift)
			partition := db.cache.getPartition(anchor)
			fce, err := partition.getEntry(anchor, anchorLoff, db)
			if err != nil {
				partition.releaseEntry(fce)
				return fmt.Errorf("anchor key=%q loff=%d: %w", anchor.key, anchorLoff, err)
			}
			for i := 0; i < fce.count; i++ {
				kv := &fce.kvs[i]
				if !kv.HasVPtr() {
					continue
				}
				if _, err := db.vlog.read(kv.Vptr, x); err != nil {
					partition.releaseEntry(fce)
					return fmt.Errorf("anchor key=%q kv=%q vptr={off:%d len:%d}: %w",
						anchor.key, kv.Key, kv.Vptr.Offset, kv.Vptr.Length, err)
				}
			}
			partition.releaseEntry(fce)
		}
	}
	return nil
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

// VacuumKV reclaims dead FLEXSPACE.KV.SLOT_BLOCKS space by rewriting all live extents
// sequentially to a new file and replacing the old file. This is an exclusive
// operation that acquires topMutRW.
//
// Crash safety: if the process crashes before the rename completes, the old
// FLEXSPACE.KV.SLOT_BLOCKS and old FlexTree remain intact. The stale .vacuum file (if
// present) is harmless and will be overwritten on the next vacuum.
//
// VacuumKV does a one-time compaction of already-bloated databases. Algorithm:
// 1. Flush memtable, acquire exclusive locks
// 1b. Compact slotted page padding: decode each page, compute tight size,
//
//	Collapse the zero-padding in reverse loff order
//
// 2. Walk FlexTree leaf linked list, read/rewrite all live extents sequentially to a .vacuum file
// 3. Close old fd, rename .vacuum -> FLEXSPACE.KV.SLOT_BLOCKS, reopen
// 4. Rebuild block manager, checkpoint FlexTree
// 5. Rebuild anchor tree from FlexTree tags (replaces stale anchor loffs)
// 6. Clean up stale .vacuum files on OpenFlexSpaceCoW
//
// See the tests:
// TestFlexDB_VacuumKV_Basic - overwrites 200 keys, vacuums, verifies data integrity across reopen
// TestFlexDB_VacuumKV_WithDeletes - deletes half of 100 keys, vacuums, verifies correct keys survive
// .
func (db *FlexDB) VacuumKV() (*VacuumKVStats, error) {
	db.requireReadsAllowed()
	db.topMutRW.Lock()
	defer db.topMutRW.Unlock()
	return db.vacuumKVLocked()
}

func (db *FlexDB) vacuumKVLocked() (*VacuumKVStats, error) {
	stats := &VacuumKVStats{}

	// Flush memtable so all live data is in FlexSpace.
	if err := db.writeLockHeldSync(); err != nil {
		return stats, fmt.Errorf("vacuumkv: sync before vacuum: %w", err)
	}

	// Exclusive access to FlexSpace and memtable by topMutRW.

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
		// Empty tree - nothing to vacuum.
		return stats, nil
	}

	// Create new file.
	dataPath := filepath.Join(ff.Path, "FLEXSPACE.KV.SLOT_BLOCKS")
	vacuumPath := dataPath + ".vacuum"
	//newFD, err := db.vfs.OpenFile(vacuumPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	newFD, err := db.vfs.OpenReadWrite(vacuumPath, vfs.WriteCategoryUnspecified)
	if err != nil {
		return stats, fmt.Errorf("vacuumkv: create vacuum file: %w", err)
	}

	// Walk all leaf nodes via the linked list, rewriting extents sequentially.
	// Slotted pages are decoded and re-encoded tightly to remove zero-padding
	// (which sits between entry records and values in the page layout).
	// We build a new FlexTree rather than modifying the old one, since changing
	// extent Len values would require fixing all internal node pivots and shifts.
	type compactedExtent struct {
		poff uint64
		len  uint32
		tag  uint16
	}
	var extents []compactedExtent
	writeOffset := uint64(0)
	nodeID := ff.tree.LeafHead
	for !nodeID.IsIllegal() {
		le := ff.tree.GetLeaf(nodeID)
		for i := uint32(0); i < le.Count; i++ {
			ext := &le.Extents[i]
			if ext.IsHole() {
				continue // holes have no physical storage
			}
			oldLen := uint64(ext.Len)
			poff := ext.Address()
			tag := ext.Tag()

			// Read data from old file. After ff.Sync() above, the block
			// manager has been flushed (blkoff=0), so all data is on disk.
			// Read directly from the file to avoid any stale-buffer issues.
			buf := make([]byte, oldLen)
			readErr := readAtFull(ff.fdKV128blocks, buf, int64(poff), "vacuumkv old extent")
			if readErr != nil {
				newFD.Close()
				db.vfs.Remove(vacuumPath)
				return stats, readErr
			}

			// Compact slotted pages by removing tombstones and zero-padding.
			writeBuf := buf
			if slottedPageIsSlotted(buf) {
				kvs, _, decErr := slottedPageDecode(buf)
				if decErr == nil && len(kvs) > 0 {
					live := kvs[:0]
					for _, kv := range kvs {
						if !kv.isTombstone() {
							live = append(live, kv)
						}
					}
					if len(live) == 0 {
						stats.PaddingReclaimed += int64(oldLen)
						continue
					}
					tight := slottedPageEncode(live)
					writeBuf = tight
					if len(tight) < len(buf) {
						stats.PaddingReclaimed += int64(len(buf) - len(tight))
					}
				}
			}

			newLen := uint32(len(writeBuf))

			// Ensure the extent does not cross a block boundary.
			// The block manager expects all extents to reside within
			// a single 4 MB block. If the extent would cross, skip
			// to the start of the next block.
			blkEnd := ((writeOffset >> FLEXSPACE_BLOCK_BITS) + 1) << FLEXSPACE_BLOCK_BITS
			if writeOffset+uint64(newLen) > blkEnd {
				writeOffset = blkEnd
			}

			// Write sequentially to new file.
			if writeErr := writeAtFull(newFD, writeBuf, int64(writeOffset), "vacuumkv new extent"); writeErr != nil {
				newFD.Close()
				db.vfs.Remove(vacuumPath)
				return stats, writeErr
			}

			extents = append(extents, compactedExtent{
				poff: writeOffset,
				len:  newLen,
				tag:  tag,
			})

			writeOffset += uint64(newLen)
			stats.ExtentsRewritten++
		}
		nodeID = le.Next
	}

	// Build a new FlexTree from the compacted extents. This ensures all
	// internal node pivots and shifts are consistent with the new loff layout.
	oldTree := ff.tree
	newTree := NewFlexTree(oldTree.fs)
	newTree.MaxExtentSize = oldTree.MaxExtentSize
	newTree.cowEnabled = oldTree.cowEnabled
	newTree.metaFD = oldTree.metaFD
	newTree.nodeFD = oldTree.nodeFD
	newTree.metaNextOff = oldTree.metaNextOff
	newTree.metaFileCap = oldTree.metaFileCap
	newTree.nodesFileCap = oldTree.nodesFileCap
	// Copy cumulative counters so they survive vacuum.
	newTree.totalLogicalBytesWrit = oldTree.totalLogicalBytesWrit
	newTree.totalPhysicalBytesWrit = oldTree.totalPhysicalBytesWrit
	newTree.PersistentVersion = oldTree.PersistentVersion
	newTree.MaxHLC = oldTree.MaxHLC
	newTree.liveKeys = oldTree.liveKeys
	newTree.liveBigKeys = oldTree.liveBigKeys
	newTree.liveSmallKeys = oldTree.liveSmallKeys
	// Initialize root leaf node (InsertWTag needs a valid root).
	root := newTree.AllocLeaf()
	root.Dirty = true
	newTree.NodeCount++
	newTree.Root = root.NodeID
	newTree.LeafHead = root.NodeID
	loff := uint64(0)
	for _, ext := range extents {
		newTree.InsertWTag(loff, ext.poff, ext.len, ext.tag)
		loff += uint64(ext.len)
	}
	ff.tree = newTree

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
	db.dirSyncNeeded = true
	//newFD2, err := db.vfs.OpenFile(dataPath, os.O_RDWR, 0644)
	newFD2, err := db.vfs.OpenReadWrite(dataPath, vfs.WriteCategoryUnspecified)

	if err != nil {
		return stats, fmt.Errorf("vacuumkv: reopen: %w", err)
	}
	ff.fdKV128blocks = newFD2

	// Truncate the new file to exactly writeOffset so that no stale
	// data from the old file lingers beyond the live extents. Without
	// this, a second VacuumKV would try to read poff values that
	// point into the compacted region - past the actual data - and
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
	bmInit(ff.bm, ff.tree, ff.fdKV128blocks)

	// After vacuum, data is compacted sequentially from offset 0. The last
	// block may be partially filled. Override bmInit's block selection to
	// continue writing within the last data block instead of jumping to the
	// next empty block (which would leave a gap in the file).
	if writeOffset > 0 {
		lastDataBlk := writeOffset >> FLEXSPACE_BLOCK_BITS
		blkOff := writeOffset & (FLEXSPACE_BLOCK_SIZE - 1)
		if blkOff > 0 {
			// Partially filled block - continue writing here.
			ff.bm.blkid = lastDataBlk
			ff.bm.blkoff = blkOff
			// Data at buf[0:blkOff] was loaded from the already-synced
			// vacuum file, so mark it as flushed. Without this, a stale
			// flushedOff from before vacuum could cause flush() to skip
			// writing new data appended after blkOff.
			ff.bm.flushedOff = blkOff

			// Populate the write buffer with existing data from this block
			// so that bm.read() can serve data from the unflushed portion.
			blkStart := int64(lastDataBlk << FLEXSPACE_BLOCK_BITS)
			if err := readAtFull(ff.fdKV128blocks, ff.bm.buf[:blkOff], blkStart, "vacuumkv reload partial block"); err != nil {
				return stats, err
			}
		}
	}

	// Also truncate trailing empty blocks to reclaim any remaining
	// slack from block-alignment rounding.
	ff.truncateTrailingBlocks()

	// Invalidate the sequential IO cache - poffs have all changed.
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

	// Rebuild anchor tree from FlexTree tags (loffs and poffs changed).
	// This also invalidates all interval caches since the old anchor tree
	// is destroyed and replaced.
	db.cache.destroyAll()
	db.rebuildAnchorsFromTags(true)

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

func (db *FlexDB) extraAnchorTagsInInterval(anchorLoff uint64, psize uint64) []uint64 {
	if psize <= 1 || db.ff == nil || db.ff.tree == nil {
		return nil
	}
	end := anchorLoff + psize
	fp := db.ff.tree.PosGet(anchorLoff)
	if !fp.Valid() {
		return nil
	}
	var extras []uint64
	for fp.Valid() {
		ext := &fp.node.Extents[fp.Idx]
		extStart := fp.GetLoff()
		if extStart >= end {
			break
		}
		if extStart > anchorLoff && flexdbTagIsAnchor(ext.Tag()) {
			extras = append(extras, extStart)
			if len(extras) >= 8 {
				break
			}
		}
		fp.ForwardExtent()
	}
	return extras
}

// CheckIntegrity performs a read-only consistency check of the FlexDB.
// It flushes the memtable first, then acquires a read lock on FlexSpace.
//
// Checks performed:
//  1. FlexTree leaf linked list: no cycles, prev/next consistency
//  2. Extent validity: every non-hole extent has poff + len within file bounds
//  3. Extent readability: data at every extent can be read from disk
//  4. Block usage: recomputed from FlexTree matches the block manager's state
//  5. Sparse index: every anchor interval has a matching FlexTree tag
//     and is readable and kv128-decodable
//  6. Sorted keys: keys within each decoded interval are in sorted order
//  7. Anchor coverage: anchor loff+psize spans tile the FlexSpace without gaps/overlaps
//  8. VLOG blake3: for every KV with a VPtr, read the VLOG entry and verify
//     hdrCRC, valCRC, and blake3 checksum of value bytes
//
// Returns nil if no errors found.
func (db *FlexDB) CheckIntegrity() []IntegrityError {
	db.requireReadsAllowed()
	db.topMutRW.Lock()
	const x = true
	defer db.topMutRW.Unlock()

	var errs []IntegrityError
	addErr := func(check, detail string, fatal bool) {
		errs = append(errs, IntegrityError{Check: check, Detail: detail, Fatal: fatal})
	}

	// Flush memtable so FlexSpace has all live data.
	if err := db.writeLockHeldSync(); err != nil {
		addErr("sync_before_integrity", fmt.Sprintf("flush before integrity check failed: %v", err), true)
		return errs
	}

	ff := db.ff
	tree := ff.tree

	// ---- Check 1: File stat ----
	fi, err := ff.fdKV128blocks.Stat()
	if err != nil {
		addErr("file_stat", fmt.Sprintf("cannot stat FLEXSPACE.KV.SLOT_BLOCKS: %v", err), true)
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
	vlogChecked := 0

	// checkVlogBlake3 verifies a single KV's VLOG entry if it has a VPtr.
	checkVlogBlake3 := func(kv *KV, anchorIdx int, anchorKey string) {
		if !kv.HasVPtr() {
			return
		}
		if db.vlog == nil {
			addErr("vlog_blake3",
				fmt.Sprintf("anchor %d (key=%q): KV %q has VPtr but no VLOG file",
					anchorIdx, anchorKey, kv.Key), false)
			return
		}
		// read() verifies hdrCRC, valCRC, and blake3 of the value bytes.
		_, err := db.vlog.read(kv.Vptr, x)
		if err != nil {
			addErr("vlog_blake3",
				fmt.Sprintf("anchor %d (key=%q): KV %q VPtr{Off=%d,Len=%d}: %v",
					anchorIdx, anchorKey, kv.Key, kv.Vptr.Offset, kv.Vptr.Length, err), false)
			return
		}
		vlogChecked++
	}

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

			tag, tagErr := ff.GetTag(anchorLoff)
			if tagErr != nil || !flexdbTagIsAnchor(tag) {
				addErr("anchor_tag",
					fmt.Sprintf("anchor %d (key=%q): loff=%d psize=%d missing FlexTree anchor tag: tag=0x%04x err=%v",
						anchorCount, anchor.key, anchorLoff, psize, tag, tagErr), false)
			}
			if extras := db.extraAnchorTagsInInterval(anchorLoff, psize); len(extras) > 0 {
				addErr("extra_anchor_tag",
					fmt.Sprintf("anchor %d (key=%q): interval loff=%d psize=%d has extra FlexTree anchor tags at loffs=%v",
						anchorCount, anchor.key, anchorLoff, psize, extras), false)
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
						checkVlogBlake3(&kvs[ki], anchorCount, anchor.key)
					}
					src = src[consumed:]
				}
			}

			// All KV.SLOT_BLOCKS data should be slotted page format.
			if len(src) > 0 {
				addErr("unexpected_format",
					fmt.Sprintf("anchor %d (key=%q): %d unexpected non-slotted trailing bytes at byte %d of %d, first bytes: %x",
						anchorCount, anchor.key, len(src), int(psize)-len(src), psize, src[:min(len(src), 16)]), false)
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
func (db *FlexDB) Sync() (err error) {
	db.topMutRW.Lock()
	autoVacuumHandoff := false
	defer func() {
		if err == nil {
			autoVacuumHandoff = db.maybeStartAutoVacuumXLocked()
		}
		if !autoVacuumHandoff {
			db.topMutRW.Unlock()
		}
	}()
	err = db.writeLockHeldSync()
	return err
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
	if total == 0 {
		return // avoid divide by zero
	}
	frac := float64(garbage) / float64(total)
	if frac < threshold {
		//vv("piggyback GC skipped: frac = %0.2f < thresh = %0.2f", frac, threshold)
		return
	}
	//vv("piggyback GC starting: frac = %0.2f", frac)
	start := time.Now()
	db.ff.GC()
	db.piggyGCStats.LastGCTime = time.Now()
	db.piggyGCStats.LastGCDuration = time.Since(start)
	db.piggyGCStats.TotalGCRuns++
	//vv("piggyback GC done in %v. db.piggyGCStats.TotalGCRuns=%v", db.piggyGCStats.LastGCDuration, db.piggyGCStats.TotalGCRuns)
}

func (db *FlexDB) autoVacuumDeletedThresholdBytes() int64 {
	kb := db.cfg.AutoVacuumDeletedAboveKB
	if kb <= 0 {
		kb = 100 * 1024
	}
	return kb * 1024
}

func (db *FlexDB) autoVacuumShouldRunLocked() bool {
	if db.cfg.AutoVacuumPct <= 0 {
		return false
	}
	deleted := atomic.LoadInt64(&db.autoVacuumDeletedBytes)
	if deleted < db.autoVacuumDeletedThresholdBytes() {
		return false
	}
	resident := mustStatFileSize(db.ff.fdKV128blocks)
	if db.vlog != nil {
		resident += db.vlog.size()
	}
	denom := resident + deleted
	if denom <= 0 {
		return false
	}
	return float64(deleted)/float64(denom) >= db.cfg.AutoVacuumPct
}

// maybeStartAutoVacuumXLockedLocked starts a one-shot background vacuum by handing
// off the caller's topMutRW write lock. If it returns true, the caller must
// not unlock topMutRW; the autovacuum goroutine now owns that responsibility.
// The X suffix indicates the topMutRW write lock is held and the caller
// has eXclusive access to the db.
func (db *FlexDB) maybeStartAutoVacuumXLocked() bool {
	if !db.autoVacuumShouldRunLocked() {
		return false
	}
	go db.autoVacuumWorkerLocked()
	return true
}

func (db *FlexDB) writeLockHeldSync() error {
	return db.writeLockHeldSyncR(false)
}

func (db *FlexDB) writeLockHeldSyncCheckpoint() error {
	return db.writeLockHeldSyncR(true)
}

func (db *FlexDB) writeLockHeldSyncR(forceTreeCheckpoint bool) error {
	const x = true
	mtWasEmpty := db.mt.empty.Load()
	cacheDirty := db.cache != nil && db.cache.hasDirtyPages()
	if mtWasEmpty && !forceTreeCheckpoint && !cacheDirty && !db.dirSyncNeeded {
		return nil // nothing to flush
	}

	// Flush WAL bytes before moving the memtable into FlexSpace, but do not
	// force the WAL here. Commit(doFsync=true) already provides per-commit WAL
	// durability. Commit(false) is only promised durable after Sync returns, and
	// by then these KVs have been written through the FlexSpace durable path.
	if !mtWasEmpty {
		if err := db.mt.logFlush(); err != nil {
			return fmt.Errorf("flexdb: Sync flush memwal: %w", err)
		}
		if err := db.flushMemtable(x); err != nil {
			return fmt.Errorf("flexdb: Sync flush memtable: %w", err)
		}
	}
	if err := db.cache.flushDirtyPages(); err != nil {
		return fmt.Errorf("flexdb: Sync flush dirty pages: %w", err)
	}

	// notice that typically we do not sync the db.vlog here;
	// it has already been synced on each large value
	// Put() or Batch.Commit(), so that the VPtr in
	// the MEMWAL will point to something durable...
	// but this has a pretty big (slow) cost for
	// individual Puts. But, db.cfg.OmitMemWalFsync true
	// means we did not actually do the sync in
	// the Put/Batch Put of vlog.appendAndSync, so we
	// have to do it now.
	if db.cfg.OmitMemWalFsync {
		if err := db.vlog.sync(); err != nil {
			return fmt.Errorf("flexdb: Sync VLOG: %w", err)
		}
	}

	db.persistCounters()
	if forceTreeCheckpoint {
		db.ff.SyncCheckpoint()
	} else {
		db.ff.Sync() // fsyncs FLEXSPACE.KV128.BLOCKS
	}
	db.maybePiggybackGC()
	db.verifyAnchorTags()

	// Sync the parent directory so new/renamed files are durable.
	if db.dirSyncNeeded {
		if err := syncDir(db.vfs, db.Path); err != nil {
			return fmt.Errorf("flexdb: sync dir: %w", err)
		}
		db.dirSyncNeeded = false
	}

	ts := uint64(time.Now().UnixNano())
	if err := db.writeLockHeldTruncateMemWALAfterSync(ts, db.ff.tree.PersistentVersion); err != nil {
		return fmt.Errorf("flexdb: Sync truncate memwal: %w", err)
	}

	db.mt.ks.clear(x)
	db.mt.vtypArena = nil
	db.mt.empty.Store(true)
	db.mt.size = 0

	return nil
}

func (db *FlexDB) writeLockHeldTruncateMemWALAfterSync(timestamp, treeVersion uint64) error {
	if db.cfg.OmitMemWalFsync {
		return db.mt.logTruncateWithVersion(timestamp, treeVersion)
	}
	return db.mt.logTruncateSyncWithVersion(timestamp, treeVersion)
}

var ErrKeyEmpty = fmt.Errorf("key cannot be the empty string")

func validateUserKey(key string) error {
	if key == "" {
		return ErrKeyEmpty
	}
	if len(key) > MaxKeySize {
		return fmt.Errorf("flexdb: key too large (max %d bytes)", MaxKeySize)
	}
	return nil
}

func validateKV128RecordSize(kv KV) error {
	if err := validateUserKey(kv.Key); err != nil {
		return err
	}
	return validateKV128RecordSizeAfterUserKey(kv)
}

func validateKV128RecordSizeAfterUserKey(kv KV) error {
	if !kv.isTombstone() && !kv.HasVPtr() && len(kv.Value) > vlogInlineThreshold {
		return fmt.Errorf("flexdb: inline value too large without VLOG (max %d bytes)", vlogInlineThreshold)
	}
	g := GreenMEMWAL_KV{
		WalRecordType: MEMWAL_KV,
		VptrLength:    kv.Vptr.Length,
		VptrOffset:    kv.Vptr.Offset,
		Hlc:           int64(kv.Hlc),
		Key:           kv.Key,
		InlineVal:     kv.Value,
	}
	payloadSize := g.compactPayloadSize()
	size := msgpackByteSliceFrameSize(payloadSize) + msgpackByteSliceFrameSize(8)
	if size >= memtableWalBufCap {
		return fmt.Errorf("flexdb: KV too large for MEMWAL record (size %d, max %d bytes)", size, memtableWalBufCap-1)
	}
	if size := slottedPageHeaderSize + slottedKVEncodedSize(kv, kv.Hlc) + slottedPageCRCSize; size > slottedPageMaxSize {
		return fmt.Errorf("flexdb: KV too large for slotted page (size %d, max %d bytes)", size, slottedPageMaxSize)
	}
	return nil
}

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
// Values of any size are accepted. Values > vlogInlineThreshold (64 bytes) are
// stored in the VLOG file; smaller values are stored inline in
// the FLEXSPACE.KV.SLOT_BLOCKS file with the keys.
//
// Large values are written exactly once: to the VLOG. The WAL stores only
// the VPtr (16 bytes), not the full value.
//
// Puts are not durably on disk until after the user has also
// completed a db.Sync() call. This allows the user to control
// the rate of fsyncs and trade that against their durability
// requirements.
//
// Put requires AllowReads. Before AllowReads, load data through Batch.Set,
// Batch.SetBytes, and Batch.Delete only.
func (db *FlexDB) Put(key string, value []byte, vtyp uint64) (hlc HLC, err error) {
	db.requireReadsAllowed()
	db.topMutRW.Lock()
	autoVacuumHandoff := false
	defer func() {
		if err == nil {
			autoVacuumHandoff = db.maybeStartAutoVacuumXLocked()
		}
		if !autoVacuumHandoff {
			db.topMutRW.Unlock()
		}
	}()
	return db.writeLockHeldPutWithHook(nil, key, value, vtyp, false)
}

func (db *FlexDB) writeLockHeldPutWithHook(beforeWrite func() error, key string, value []byte, vtyp uint64, doDelete bool) (HLC, error) {
	const x = true
	if doDelete && len(value) > 0 {
		return 0, fmt.Errorf("flexdb API use error: cannot supply a value and also delete it's key, this is a contradiction. Do not set a value on delete of a key: '%v'.", key)
	}
	if err := validateUserKey(key); err != nil {
		return 0, err
	}
	atomic.AddInt64(&db.LogicalBytesWritten, int64(len(key)+len(value)))

	// Tick the HLC for this write.
	hlcVal := db.hlc.CreateSendOrLocalEvent()

	// String keys are immutable - no defensive copy needed.
	// Nil and empty values are the same zero-length live value.
	if len(value) == 0 {
		value = nil
	} else {
		// make a copy we own.
		value = append([]byte{}, value...)
	}

	// Build the KV for the memtable. Large values go to VLOG.
	kv := KV{Key: key, Value: value, Hlc: hlcVal}
	// // store vtyp type information
	kv.Vptr.Offset = vtyp
	kv.Vptr.Length = uint64(len(value))

	if doDelete {
		kv.Vptr.Length = rawVlenTombstone
	}

	if db.vlog != nil && value != nil && len(value) > vlogInlineThreshold {
		// Look up old VPtr for blake3 dedup. Check memtable first (cheap),
		// then FlexSpace (loads interval cache). If the old value has the
		// same blake3 checksum, we reuse the old VPtr and skip the VLOG write.
		// See "HLC STALENESS IN VLOG HEADERS" in vlog.go.
		oldVP := db.lookupOldVPtr(key, x)
		vp, _, err := db.vlog.appendDedupAndSync(value, hlcVal, oldVP, db.cfg.OmitMemWalFsync)
		if err != nil {
			return 0, fmt.Errorf("flexdb: vlog append: %w", err)
		}
		kv = KV{Key: key, Vptr: vp, Hlc: hlcVal}
		// type info goes into Value field for large values.
		if vtyp != 0 {
			kv.Value = db.mt.vtypBytes(vtyp)
		}
	}

	if err := validateKV128RecordSize(kv); err != nil {
		return 0, err
	}

	// memtableCap is a billion writes. not really much of a real limit,
	// and giving the user a clear error is a better user experience
	// than crashing on them in a random place because we have
	// run out of memory.
	if db.mt.size >= memtableCap {
		if beforeWrite != nil {
			return 0, fmt.Errorf("flexdb error: write transaction exceeds memtable capacity; split it into smaller transactions. limit is memtableCap=%v", memtableCap)
		}
		// Inline flush when memtable is full.
		if db.mt.size >= memtableCap {
			// Inline flush when memtable is full.
			if err := db.mt.logFlush(); err != nil {
				return 0, fmt.Errorf("flexdb: Put inline flush memwal: %w", err)
			}
			if err := db.flushMemtable(x); err != nil {
				return 0, fmt.Errorf("flexdb: Put inline flush memtable: %w", err)
			}
			if err := db.cache.flushDirtyPages(); err != nil {
				return 0, fmt.Errorf("flexdb: Put inline flush dirty pages: %w", err)
			}
			db.persistCounters()
			db.ff.Sync()

			db.mt.ks.clear(x)
			db.mt.vtypArena = nil
			db.mt.empty.Store(true)
			db.mt.size = 0
			db.flushSeq++
		}
	}
	if beforeWrite != nil {
		if err := beforeWrite(); err != nil {
			return 0, err
		}
	}
	newState := kvToState(kv)

	// WAL stores VPtr metadata for large values. Since LARGE.VLOG was fsynced
	// above, the VPtr is safe to reference on crash recovery.
	if err := db.mt.logAppend(kv); err != nil {
		return 0, fmt.Errorf("flexdb: append memwal key=%q: %w", key, err)
	}

	old, replaced := db.mt.put(kv, x)
	db.mt.empty.Store(false)
	oldState := ksNotExists
	oldKV := old
	oldKVFound := replaced
	if replaced {
		oldState = kvToState(old)
	} else {
		if db.ff.Size() != 0 && db.keyBloomMayExistLocked(key, x) {
			var getErr error
			oldKV, oldKVFound, getErr = db.getPassthroughKV(key, x)
			if getErr == nil && oldKVFound {
				oldState = kvToState(oldKV)
			}
		}
	}
	db.noteAutoVacuumObsoleteKV(oldKV, oldKVFound, kv)
	db.adjustKeyCounters(oldState, newState)
	if oldState == ksNotExists {
		db.rememberNewKeyBloomLocked(key)
	}

	return hlcVal, nil
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
	// no effort to retieve values, only keys. This is
	// useful for very fast full-table scans of just the keys,
	// when the user knows they will not inspect values
	// at all. In contrast, LAZY keeps open the option to
	// look at the values, but will pay some time in overhead.
	SKIP_VALUES SearchModifier = 16

	// LAZY_SMALL requests zero-copy return of inline values.
	// The returned KVcloser.Value aliases interval cache memory.
	// The caller MUST call Close() to release the cache pin.
	// If the result came from a memtable (not yet flushed),
	// then we must do a copy; Value is copied as usual
	// to avoid returning stale/non-linearizable data
	// (best-effort zero-copy).
	// LAZY_SMALL can be | or-ed with Exact, GTE, GT, LTE, or LT.
	LAZY_SMALL SearchModifier = 32

	// LAZY_LARGE means we do not fetch LARGE.VLOG values
	// automatically. The User must call FetchLarge() explicitly
	// when they are desired.
	// LAZY_LARGE can be | or-ed with Exact, GTE, GT, LTE, or LT.
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

// findBuildKV constructs a *KV from the iterator's current position. Returns a
// shallow copy of the internal KV; Key can alias the memtable arena and Value
// can alias cache memory. This is safe only while the caller holds topMutRW.
// API boundaries that return a KV after releasing the lock must clone Key and
// any retained inline Value.
func findBuildKV(it *Iter) *KV {
	if it.pKV == nil {
		return nil
	}
	out := *it.pKV
	return &out
}

// Find allows GTE, GT, LTE, LT, and Exact searches.
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
// If key is the empty string, then GTE and GT return the first key
// in the tree, while LTE and LT return the last key.
//
// Any of the LAZY* set of flag can be bitwise-OR-ed with the smod
// to request that large/small/all values not be returned unless
// and until we decide we want them with an explicit
// FetchLarge() call. For example: Find(Exact|LAZY, "needle")
//
// The returned *KVcloser contains the found key and its value;
// unless laziness was requested.
//
// The returned bool, 'exact', indicates an exact match to the query key.
//
// If the returned kvc *KVcloser is nil, this means that
// the key was not found, or there was an I/O error. The
// caller should always check the returned error (err) first rule out
// I/O error before concluding the key was not found
// from a nil kvc.
//
// A typical call sequence would be:
//
//	kvc, _, err := dbHaystack.Find(Exact, "needle")
//
//	if err != nil {
//	   return err
//	}
//
//	if kvc != nil {
//	  // found exact match! (we know, because Exact was
//	  // requested; if this was a GTE search we would need
//	  // to check the 'exact' bool return to know if we found
//	  // our "needle", or went past it).
//
//	  // Here all value sizes are automatically pulled in, since
//	  // none of the (LAZY_SMALL, LAZY_LARGE, LAZY) smod were requested
//
//	  // For performance, we do not copy kvc.Value for you.
//	  // So you must copy kvc.Value, if you need it later,
//	  // before doing kvc.Close().
//	  processKeyAndValueAtHlcTimestamp(kvc.Key, kvc.Value, kvc.Hlc)
//
//	  kvc.Close() // unpin from internal caches. Allows zero-copy reads.
//	}
//
// Find looks up the first key matching the SearchModifier and returns
// an owned copy of the KV (safe to retain indefinitely). For scanning
// beyond the found key, use Find inside a View or Update transaction.
//
// Goroutine safe. Acquires the read lock internally.
//
// Warning: if kvc != nil, the user must call Close() on the
// returned kvc *KVcloser when done copying any Value out, or
// else memory and resource leaks will ensue.
//
// The kvc.Close() can be skipped if kvc is nil (key not found).
// However it is always fine to do the Close() even then, as
// kvc.Close() is a no-op if kvc is nil.
func (db *FlexDB) Find(smod SearchModifier, key string) (kvc *KVcloser, exact bool, err error) {
	db.requireReadsAllowed()
	db.topMutRW.RLock()
	const x = false
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
		resultKey := strings.Clone(zc.Key)
		vtyp := zc.Vtyp()
		valueFromCache := it.valueNeedsCopy

		// Release iterator state early - we have what we need.
		it.releaseIterState()

		if skipValues {
			kvc = &KVcloser{KV: KV{Key: resultKey, Hlc: zc.Hlc}, db: db, Vtyp: vtyp}
			return
		}

		// LAZY_SMALL path: try zero-copy via cache pinning.
		// Only works for inline values from FlexSpace (not memtable).
		if lazySmall && valueFromCache && !zc.HasVPtr() && len(zc.Value) > 0 {
			kvc, err = db.findBuildKVZeroCopy(resultKey)
			if err != nil {
				return
			}
			if kvc != nil {
				kvc.Key = resultKey
				kvc.Vtyp = vtyp
				return
			}
			// Fallback: key was in memtable or edge case. Copy below.
		}

		// Standard path: copy inline value, auto-fetch large value.
		owned := KV{Vptr: zc.Vptr, Hlc: zc.Hlc}
		owned.Key = resultKey
		if !zc.HasVPtr() && zc.Value != nil {
			owned.Value = append([]byte{}, zc.Value...)
		}
		kvc = &KVcloser{KV: owned, db: db, Vtyp: vtyp}

		// Auto-fetch large value unless LAZY_LARGE was requested
		if !lazyLarge && kvc.HasVPtr() {
			val, _, _, fetchErr := db.resolveVPtr(kvc.KV, x)
			if fetchErr != nil {
				kvc = nil
				err = fetchErr
			} else {
				kvc.Value = val
			}
		}
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
	Vtyp      uint64
	partition *intervalCachePartition // nil when no pin needed
	entry     *intervalCacheEntry     // nil when no pin needed
	db        *FlexDB
	lockHeld  bool
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
// no-op. After Fetch returns nil error, kvc.Value holds the bytes.
// Fetch is only needed when LAZY_LARGE was used in the Find call.
//
// Much more detail: after Find() returns, kvc.Value is always populated
// for inline (small) values, regardless of whether LAZY_SMALL
// was used. Only SKIP_VALUES will result in kvc.Values always
// being nil (even for non-nil Values in the db; SKIP_VALUES
// means we do not retrieve them). What "lazy" means in each case:
//
// LAZY_LARGE: Value is not fetched from VLOG. kvc.Value == nil,
// kvc.Large() == true. You must call Fetch() to get bytes.
//
// LAZY_SMALL: Value is present in kvc.Value, but it's a zero-copy
// alias into cache memory instead of an owned copy. The "lazy"
// here means "lazy about copying", not "lazy about providing the value".
//
// So after Find() with LAZY_SMALL:
//
// - Inline values: kvc.Value points into cache memory (or a copy if it
// fell back). Ready to use.
// - Large values (no LAZY_LARGE): auto-fetched, kvc.Value populated. Ready to use.
// - Large values (with LAZY_LARGE): kvc.Value == nil, need Fetch().
//
// Requiring Fetch() only applies to the VLOG/large-value case. It doesn't
// need a LAZY_SMALL path because the small value is already there, it
// is just borrowed rather than copied.
//
// The only obligation LAZY_SMALL imposes is that you must call Close()
// to release the cache pin (so the entry can be evicted).
//
// Currently without LAZY_SMALL, Close() is a no-op (the value is an
// owned copy); but we reserve the right to alter this, and so require
// that users properly use Close() if kvc != nil.
func (s *KVcloser) Fetch() error {
	if s == nil {
		return nil
	}
	if !s.HasVPtr() {
		return nil // inline value already present
	}
	const x = false
	var val []byte
	var vtyp uint64
	var err error
	if s.lockHeld {
		val, vtyp, _, err = s.db.lockHeldFetchLarge(&s.KV, x)
	} else {
		val, vtyp, _, err = s.db.FetchLarge(&s.KV)
	}
	if err != nil {
		return err
	}
	s.Value = val
	if s.Vtyp != vtyp {
		s.Vtyp = vtyp
	}
	return nil
}

// findBuildKVZeroCopy returns a KVcloser whose Value aliases
// interval cache memory (zero-copy). The cache entry is pinned
// via refcnt; the caller must call Close() to release.
// Caller must hold topMutRW.RLock() or topMutRW.Lock().
//
// Returns nil if the key is not found in FlexSpace (e.g., it
// was in a memtable, or was a tombstone). Caller should fall
// back to the copy path.
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

	idx, exact := intervalCacheEntryFindKeyGE(fce, key)
	if !exact || idx >= fce.count || fce.kvs[idx].Key != key || fce.kvs[idx].isTombstone() {
		partition.releaseEntry(fce)
		return nil, nil
	}
	// Transfer cache entry ownership to KVcloser (don't release).
	// fce.kvs[idx] is a value copy of the KV struct (64B), but
	// the .Value []byte slice header still points into cache memory.
	kvc := &KVcloser{
		KV:        fce.kvs[idx],
		partition: partition,
		entry:     fce,
		db:        db,
	}
	kvc.Vtyp = kvc.KV.Vtyp()
	return kvc, nil
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
// Get returns nil, true for a live key with zero value bytes.
// Get is value size agnostic. It returns large and small values
// immediately. This is tested at, for example, gc_test.go
// Test_GC1K_write_1k_keys_with_large_values.
func (db *FlexDB) Get(key string) (value []byte, found bool, vtyp uint64, hlc HLC, err error) {
	db.requireReadsAllowed()
	db.topMutRW.RLock()
	const x = false
	defer db.topMutRW.RUnlock()
	defer recoverIterIOErr(&err)

	// Check memtable
	if !db.mt.empty.Load() {
		kv, ok := db.mt.get(key, x)
		if ok {
			if kv.isTombstone() {
				return nil, false, 0, 0, nil // tombstone
			}
			val, vtype, _, err := db.resolveVPtr(kv, x)
			if err != nil {
				return nil, false, 0, 0, err
			}
			if len(val) == 0 {
				return nil, true, vtype, kv.Hlc, nil
			}
			out := make([]byte, len(val))
			copy(out, val)
			return out, true, vtype, kv.Hlc, nil
		}
	}

	// Check FlexSpace via sparse index
	return db.getPassthrough(key, x)
}

// someLockHeldGet retrieves the value for key without acquiring topMutRW.
// Caller must already hold topMutRW.Lock() or topMutRW.RLock().
func (db *FlexDB) someLockHeldGet(key string, x bool) (val []byte, found bool, vtyp uint64, hlc HLC, err error) {
	// Check memtable
	if !db.mt.empty.Load() {
		kv, ok := db.mt.get(key, x)
		if ok {
			if kv.isTombstone() {
				return nil, false, 0, 0, nil
			}
			val, vtyp, _, err = db.resolveVPtr(kv, x)
			if err != nil {
				return
			}
			if len(val) == 0 {
				val = nil
				found = true
				hlc = kv.Hlc
				return
			}
			out := make([]byte, len(val))
			copy(out, val)
			return out, true, vtyp, kv.Hlc, nil
		}
	}

	// Check FlexSpace via sparse index
	return db.getPassthrough(key, x)
}

// Delete removes key from the store. Delete requires AllowReads.
func (db *FlexDB) Delete(key string) error {
	db.requireReadsAllowed()
	db.topMutRW.Lock()
	_, err := db.writeLockHeldPutWithHook(nil, key, nil, 0, true)
	autoVacuumHandoff := false
	if err == nil {
		autoVacuumHandoff = db.maybeStartAutoVacuumXLocked()
	}
	if !autoVacuumHandoff {
		db.topMutRW.Unlock()
	}
	return err
}

// DeleteRange deletes all keys in the range [begKey, endKey] with
// configurable inclusivity on each bound. An empty begKey means the lower
// bound is open (start at the first key), and an empty endKey means the upper
// bound is open (continue through the last key). Inclusivity is ignored for an
// open bound.
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
//	DeleteRange(true,  a, z, true,  true)   // [a, z] - both inclusive, include large values
//	DeleteRange(true,  a, z, true,  false)  // [a, z) - half-open, include large values
//	DeleteRange(false, a, z, true,  true)   // [a, z] - both inclusive, skip large values
//	DeleteRange(true, "", "", false, false) // all keys, include large values
//
// Goroutine safe. Concurrent reads and writes are serialized via the
// database write lock. However, when allGone is returned true, all
// previously held iterators, cursors, and references are invalidated.
//
// DeleteRange requires AllowReads.
func (db *FlexDB) DeleteRange(includeLarge bool, begKey, endKey string, begInclusive, endInclusive bool) (n int64, allGone bool, err error) {
	db.requireReadsAllowed()
	db.topMutRW.Lock()
	autoVacuumHandoff := false
	defer func() {
		if err == nil {
			autoVacuumHandoff = db.maybeStartAutoVacuumXLocked()
		}
		if !autoVacuumHandoff {
			db.topMutRW.Unlock()
		}
	}()
	n, allGone, err = db.writeLockHeldDeleteRange(includeLarge, begKey, endKey, begInclusive, endInclusive)
	return n, allGone, err
}

// writeLockHeldDeleteRange is the lock-held body of DeleteRange.
// Caller must hold topMutRW.Lock().
func (db *FlexDB) writeLockHeldDeleteRange(includeLarge bool, begKey, endKey string, begInclusive, endInclusive bool) (n int64, allGone bool, err error) {
	return db.writeLockHeldDeleteRangeWithHook(nil, includeLarge, begKey, endKey, begInclusive, endInclusive)
}

func (db *FlexDB) writeLockHeldDeleteRangeWithHook(beforeWrite func() error, includeLarge bool, begKey, endKey string, begInclusive, endInclusive bool) (n int64, allGone bool, err error) {
	if begKey != "" && endKey != "" && begKey > endKey {
		return 0, false, fmt.Errorf("yogadb: DeleteRange: begKey > endKey")
	}
	// Equal concrete keys with either side exclusive means empty range.
	if begKey != "" && begKey == endKey && (!begInclusive || !endInclusive) {
		return 0, false, nil
	}

	// Fast path: if the range covers every key in the DB and we're
	// including large values, reinitialize instead of iterating.
	// When !includeLarge, large-value keys survive so we can't wipe.
	if includeLarge && db.writeLockHeldCoversAllKeys(begKey, endKey, begInclusive, endInclusive) {
		// Keep full-range DeleteRange fast even inside WriteTx. Like
		// Clear(true), this is not rollbackable; WriteTx.Rollback can only
		// discard later memtable writes in the same WriteTx.
		err := db.writeLockHeldDeleteAll() // only place called
		return 0, true, err
	}

	// Phase 1: Tombstone all non-tombstone keys in range in the memtable.
	if !db.mt.empty.Load() {
		// Collect keys first since writeLockHeldPut mutates the memtable.
		var keys []string
		db.mt.ks.Ascend(KV{Key: begKey}, func(item KV) bool {
			if !deleteRangeInBounds(item.Key, begKey, endKey, begInclusive, endInclusive) {
				// Past endKey - stop iteration.
				if deleteRangePastEnd(item.Key, endKey, endInclusive) {
					return false
				}
				// Before begKey (exclusive match) - skip but continue.
				return true
			}
			if !item.isTombstone() {
				if !includeLarge && item.HasVPtr() {
					return true // skip large-value keys
				}
				keys = append(keys, strings.Clone(item.Key))
			}
			return true
		})
		for _, key := range keys {
			if _, err := db.writeLockHeldPutWithHook(beforeWrite, key, nil, 0, true); err != nil {
				return n, false, err
			}
			n++
		}
	}

	// Phase 2: Walk FlexSpace sparse index directly, decode intervals
	// without cache, and tombstone every non-tombstone key in range.
	n2, err := db.deleteRangeFlexSpace(beforeWrite, begKey, endKey, begInclusive, endInclusive, includeLarge)
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
//
// Clear requires AllowReads.
func (db *FlexDB) Clear(includeLarge bool) (allGone bool, err error) {
	db.requireReadsAllowed()
	db.topMutRW.Lock()
	autoVacuumHandoff := false
	defer func() {
		if err == nil {
			autoVacuumHandoff = db.maybeStartAutoVacuumXLocked()
		}
		if !autoVacuumHandoff {
			db.topMutRW.Unlock()
		}
	}()
	allGone, err = db.writeLockHeldClear(includeLarge)
	return allGone, err
}

// writeLockHeldClear is the lock-held body of Clear.
// Caller must hold topMutRW.Lock().
func (db *FlexDB) writeLockHeldClear(includeLarge bool) (allGone bool, err error) {
	return db.writeLockHeldClearWithHook(nil, includeLarge)
}

func (db *FlexDB) writeLockHeldClearWithHook(beforeWrite func() error, includeLarge bool) (allGone bool, err error) {
	if includeLarge {
		// Keep Clear(true) fast even inside WriteTx. This rewrites database
		// files immediately, so WriteTx.Rollback cannot restore the cleared
		// data; it can only discard later memtable writes in the same WriteTx.
		err := db.writeLockHeldDeleteAll()
		return true, err
	}

	// !includeLarge: must iterate and tombstone only small-value keys.

	// Phase 1: Tombstone small-value keys in the memtable.
	if !db.mt.empty.Load() {
		var keys []string
		db.mt.ks.Scan(func(item KV) bool {
			if !item.isTombstone() && !item.HasVPtr() {
				keys = append(keys, strings.Clone(item.Key))
			}
			return true
		})
		for _, key := range keys {
			if _, err := db.writeLockHeldPutWithHook(beforeWrite, key, nil, 0, true); err != nil {
				return false, err
			}
		}
	}

	// Phase 2: Walk FlexSpace and tombstone small-value keys.
	_, err = db.deleteRangeFlexSpaceClearSmall(beforeWrite)
	return false, err
}

// writeLockHeldCoversAllKeys returns true if the given range covers every
// key in the database (memtable + FlexSpace). When true, the caller can
// use the fast "delete all" path instead of iterating.
//
// The check is conservative: it finds the actual min and max keys across
// all sources and verifies they fall within the range. If the DB is empty,
// returns true (nothing to delete, reinit is a no-op).
//
// Caller must hold topMutRW.Lock().
func (db *FlexDB) writeLockHeldCoversAllKeys(begKey, endKey string, begInclusive, endInclusive bool) bool {
	inBounds := func(key string) bool {
		return deleteRangeInBounds(key, begKey, endKey, begInclusive, endInclusive)
	}

	// Check memtable min/max keys.
	if !db.mt.empty.Load() {
		// Min key (first in ascending order).
		var minKV KV
		var minFound bool
		db.mt.ks.Scan(func(item KV) bool {
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
		db.mt.ks.Reverse(func(item KV) bool {
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

	// Anchor keys are interval lower bounds, and the empty-key sentinel can
	// own the first real interval. Decode non-empty intervals so we check
	// the actual minimum and maximum KV keys.
	var flexMin, flexMax string
	flexFound := false
	for node := t.leafHead; node != nil; node = node.next {
		nh := memSparseIndexTreeHandler{node: node}
		memSparseIndexTreeHandlerInfoUpdate(&nh)
		for i := 0; i < node.count; i++ {
			a := node.anchors[i]
			if a == nil || a.psize == 0 {
				continue
			}
			kvs, err := db.decodeIntervalDirect(a, uint64(a.loff+nh.shift))
			if err != nil {
				return false
			}
			if len(kvs) == 0 {
				continue
			}
			if !flexFound {
				flexMin = kvs[0].Key
				flexFound = true
			}
			flexMax = kvs[len(kvs)-1].Key
		}
	}
	if flexFound && (!inBounds(flexMin) || !inBounds(flexMax)) {
		return false
	}

	return true
}

// writeLockHeldDeleteAll reinitializes the database,
// discarding all data. This is the fast path for DeleteRange
// when the range covers all keys. We close the FlexSpace,
// truncate all data files, and finaly reopen the FlexSpace.
//
// Caller must hold topMutRW.Lock().
func (db *FlexDB) writeLockHeldDeleteAll() error {

	// (1). leave any flush worker that is blocked on topMutRW
	// alone; it can resume when we are done, and that
	// will be just fine.

	// 2. Clear memtable.
	db.mt.reset()

	// 3. Destroy interval cache.
	db.cache.destroyAll()

	// 4. Close FlexSpace (closes KV.SLOT_BLOCKS, REDO.LOG, CoW files).
	db.ff.Close()

	// 5. Remove FlexSpace data files and reopen fresh.
	fs := db.vfs
	path := db.Path
	filesToRemove := []string{
		"FLEXSPACE.KV.SLOT_BLOCKS",
		"FLEXSPACE.KV.SLOT_BLOCKS.vacuum",
		"FLEXSPACE.REDO.LOG",
		"FLEXTREE.PAGES",
		"FLEXTREE.COMMIT",
	}
	for _, name := range filesToRemove {
		if err := fs.Remove(filepath.Join(path, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("yogadb: DeleteAll: remove %s: %w", name, err)
		}
	}

	// Truncate VLOG if present.
	if db.vlog != nil {
		db.vlog.sync()
		db.vlog.close()
		if err := fs.Remove(filepath.Join(path, "LARGE.VLOG")); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("yogadb: DeleteAll: remove LARGE.VLOG: %w", err)
		}
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
	atomic.StoreInt64(&db.autoVacuumDeletedBytes, 0)
	atomic.StoreInt64(&db.autoVacuumVLOGDeletedBytes, 0)
	db.liveKeys = 0
	db.liveBigKeys = 0
	db.liveSmallKeys = 0
	db.persistCounters()
	db.ff.SyncCheckpoint()

	// 8. Truncate WAL file.
	ts := uint64(time.Now().UnixNano())
	if err := db.mt.logTruncateSyncWithVersion(ts, db.ff.tree.PersistentVersion); err != nil {
		return fmt.Errorf("yogadb: DeleteAll: truncate memwal: %w", err)
	}
	if err := syncDir(fs, path); err != nil {
		return fmt.Errorf("yogadb: DeleteAll: sync dir: %w", err)
	}
	db.dirSyncNeeded = false

	// 9. (no longer need to: Restart flush worker; we never killed it).

	return nil
}

// deleteRangeInBounds returns true if key is within the range defined by
// [begKey, endKey] with the given inclusivity flags. Empty bounds are open.
func deleteRangeInBounds(key, begKey, endKey string, begInclusive, endInclusive bool) bool {
	if begKey != "" {
		if begInclusive {
			if key < begKey {
				return false
			}
		} else if key <= begKey {
			return false
		}
	}
	if endKey != "" {
		if endInclusive {
			if key > endKey {
				return false
			}
		} else if key >= endKey {
			return false
		}
	}
	return true
}

// deleteRangePastEnd returns true if key is beyond the end bound.
func deleteRangePastEnd(key, endKey string, endInclusive bool) bool {
	if endKey == "" {
		return false
	}
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
// On memtable flush (detected by flushSeq change), re-seeks from the last
// processed key in the rebuilt sparse index tree.
//
// Caller must hold topMutRW.Lock().
func (db *FlexDB) deleteRangeFlexSpace(beforeWrite func() error, begKey, endKey string, begInclusive, endInclusive, includeLarge bool) (int64, error) {
	const x = true
	var n int64
	target := begKey
	// On first seek, whether we include target depends on begInclusive.
	// After a flush re-seek, we always use strict=true (skip the last processed key).
	seekStrict := begKey != "" && !begInclusive

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
				return n, fmt.Errorf("deleteRangeFlexSpace: decode interval key=%q loff=%d psize=%d: %w",
					anchor.key, uint64(anchor.loff+shift), anchor.psize, err)
			}

			// Process each KV in this interval.
			for _, kv := range kvs {
				// Skip keys before our current seek position.
				if seekStrict {
					if kv.Key <= target {
						continue
					}
				} else {
					if kv.Key < target {
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
				if db.memtableShadowsKeyLocked(kv.Key, x) {
					continue
				}
				if !includeLarge && kv.HasVPtr() {
					continue // skip large-value keys
				}

				// Write tombstone. Track flushSeq to detect inline flush.
				prevSeq := db.flushSeq
				if _, err := db.writeLockHeldPutWithHook(beforeWrite, kv.Key, nil, 0, true); err != nil {
					return n, err
				}
				n++

				if db.flushSeq != prevSeq {
					// Memtable flushed - sparse index tree was rebuilt.
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
func (db *FlexDB) deleteRangeFlexSpaceClearSmall(beforeWrite func() error) (int64, error) {
	const x = true
	var n int64
	var target string
	seekStrict := false
	firstIteration := true

	for {
		t := db.tree
		if t == nil || t.root == nil || t.leafHead == nil {
			return n, nil
		}

		// Start from leafHead (first leaf) or re-seek after flush.
		var node *memSparseIndexTreeNode
		var anchorIdx int
		var shift int64

		if firstIteration && !seekStrict {
			firstIteration = false
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
				return n, fmt.Errorf("deleteRangeFlexSpaceClearSmall: decode interval key=%q loff=%d psize=%d: %w",
					anchor.key, uint64(anchor.loff+shift), anchor.psize, err)
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
				if db.memtableShadowsKeyLocked(kv.Key, x) {
					continue
				}

				prevSeq := db.flushSeq
				if _, err := db.writeLockHeldPutWithHook(beforeWrite, kv.Key, nil, 0, true); err != nil {
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

func (db *FlexDB) memtableShadowsKeyLocked(key string, x bool) bool {
	if db.mt.empty.Load() {
		return false
	}
	_, ok := db.mt.get(key, x)
	return ok
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
		if err != nil {
			return nil, fmt.Errorf("decodeIntervalDirect: slotted page decode loff=%d psize=%d: %w",
				anchorLoff, anchor.psize, err)
		}
		kvs = append(kvs, decoded...)
		src = src[consumed:]
	}
	// All KV.SLOT_BLOCKS data should be slotted page format.
	if len(src) > 0 {
		return nil, fmt.Errorf("decodeIntervalDirect: unexpected non-slotted data at loff=%d, %d trailing bytes, first 16 bytes: %x",
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
// entry for each key. Simpler than intervalCacheDedup since we don't need
// fingerprints or size tracking.
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

// Merge performs an atomic read-modify-write on key. fn is always
// called: when the key exists, oldVal is its current value and
// exists=true; when the key is absent or deleted, oldVal=nil and
// exists=false (allowing conditional creation). Return write=false
// to skip the write, or doDelete=true to delete the key.
//
// This mirrors C's flexdb_merge: it looks up the old value across all
// layers (active memtable, inactive memtable, FlexSpace), applies the
// user function, and writes the result atomically.
//
// Q: When should the callback return doWrite=false, doDelete=false?
// A: This means "do nothing." The main scenarios:
//
//  1. Conditional creation: The callback inspects exists and decides
//     not to create the key. E.g., "only increment if the key
//     already exists" - if exists=false, return a bare return (all zeros = no-op).
//
//  2. Conditional update: The callback inspects the old value and
//     decides no change is needed. E.g., "set to X only if current
//     value isn't already X."
//
//  3. Read-only peek: The callback just wants to see the current
//     value (though Get is simpler for that).
//
// Merge requires AllowReads.
func (db *FlexDB) Merge(key string, fn func(oldVal []byte, exists bool, oldVtyp uint64) (newVal []byte, write bool, doDelete bool, newVtyp uint64)) (err error) {
	db.requireReadsAllowed()
	db.topMutRW.Lock()
	autoVacuumHandoff := false
	defer func() {
		if err == nil {
			autoVacuumHandoff = db.maybeStartAutoVacuumXLocked()
		}
		if !autoVacuumHandoff {
			db.topMutRW.Unlock()
		}
	}()
	return db.writeLockHeldMerge(key, fn)
}

// writeLockHeldMerge is the lock-held body of Merge.
// Caller must hold topMutRW.Lock().
func (db *FlexDB) writeLockHeldMerge(key string, fn func(oldVal []byte, exists bool, oldVtyp uint64) (newVal []byte, write bool, doDelete bool, newVtyp uint64)) error {
	return db.writeLockHeldMergeWithHook(nil, key, fn)
}

func (db *FlexDB) writeLockHeldMergeWithHook(beforeWrite func() error, key string, fn func(oldVal []byte, exists bool, oldVtyp uint64) (newVal []byte, write bool, doDelete bool, newVtyp uint64)) error {
	if err := validateUserKey(key); err != nil {
		return err
	}

	// Phase 1: check memtable.
	var oldVal []byte
	var exists bool
	var oldVtyp uint64
	var shadowedByMemtable bool
	const x = true

	if !db.mt.empty.Load() {
		kv, ok := db.mt.get(key, x)
		if ok {
			shadowedByMemtable = true
			if !kv.isTombstone() {
				oldVtyp = kv.Vptr.Offset
				val, vtyp, _, err := db.resolveVPtr(kv, x)
				if err != nil {
					return err
				}
				oldVal = val
				exists = true
				oldVtyp = vtyp
			}
		}
	}

	if !exists && !shadowedByMemtable {
		// Phase 2: check FlexSpace (getPassthrough already resolves VPtrs).
		val, found, vtyp, _, err := db.getPassthrough(key, x)
		if err != nil {
			return fmt.Errorf("flexdb: merge getPassthrough: %w", err)
		}
		if found {
			oldVal = val
			exists = true
			oldVtyp = vtyp
		}
	}

	// Apply user merge function.
	newVal, write, doDelete, newVtyp := fn(oldVal, exists, oldVtyp)
	if write && doDelete {
		return fmt.Errorf("flexdb: Merge callback returned both doWrite=true and doDelete=true; these are mutually exclusive")
	}
	if !write && !doDelete {
		return nil
	}

	if doDelete {
		_, err := db.writeLockHeldPutWithHook(beforeWrite, key, nil, 0, true)
		return err
	}

	_, err := db.writeLockHeldPutWithHook(beforeWrite, key, newVal, newVtyp, false)
	return err
}

// ====================== Passthrough operations ======================
// These operate directly on FlexSpace + sparse index.
// Caller must hold db.topMutRW. but is RLock sufficient? should be since we change nothing.

func (db *FlexDB) getPassthrough(key string, x bool) (val []byte, found bool, vtyp uint64, hlc HLC, err0 error) {
	var nh memSparseIndexTreeHandler
	db.tree.findAnchorPos(key, &nh)
	anchor := nh.node.anchors[nh.idx]
	anchorLoff := uint64(anchor.loff + nh.shift)
	partition := db.cache.getPartition(anchor)
	fce, err := partition.getEntry(anchor, anchorLoff, db)
	if err != nil {
		partition.releaseEntry(fce)
		return nil, false, 0, 0, err
	}
	defer partition.releaseEntry(fce)

	idx, ok := intervalCacheEntryFindKeyGE(fce, key)
	if !ok {
		return nil, false, 0, 0, nil
	}
	kv := fce.kvs[idx]
	if kv.isTombstone() {
		return nil, false, 0, 0, nil
	}
	val, vtyp, _, err0 = db.resolveVPtr(kv, x)
	if err0 != nil {
		return nil, false, 0, 0, err0
	}
	if len(val) == 0 {
		return nil, true, vtyp, kv.Hlc, nil
	}
	out := make([]byte, len(val))
	copy(out, val)
	return out, true, vtyp, kv.Hlc, nil
}

// getPassthroughKV returns the full KV (including HLC) from the passthrough layer.
func (db *FlexDB) getPassthroughKV(key string, x bool) (KV, bool, error) {
	var nh memSparseIndexTreeHandler
	return db.getPassthroughKVWithHint(key, &nh, x)
}

// getPassthroughKVWithHint is getPassthroughKV with a reusable sparse-index
// cursor. It is profitable when callers probe keys in ascending order.
func (db *FlexDB) getPassthroughKVWithHint(key string, nh *memSparseIndexTreeHandler, x bool) (KV, bool, error) {
	db.tree.treeNodeHandlerNextAnchor(nh, key)
	anchor := nh.node.anchors[nh.idx]
	anchorLoff := uint64(anchor.loff + nh.shift)
	partition := db.cache.getPartition(anchor)
	fce, err := partition.getEntry(anchor, anchorLoff, db)
	if err != nil {
		partition.releaseEntry(fce)
		return KV{}, false, err
	}
	defer partition.releaseEntry(fce)

	idx, ok := intervalCacheEntryFindKeyGE(fce, key)
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
		err = db.putPassthroughInitial(kv, nh, anchor, partition, fce)
	} else {
		err = db.putPassthroughR(kv, nh, anchor, partition, fce)
	}
	if err != nil {
		partition.releaseEntry(fce)
		return err
	}
	if fce.count > flexdbSparseIntervalCount {
		if err := db.treeInsertAnchor(nh, partition, fce); err != nil {
			partition.releaseEntry(fce)
			return err
		}
	}
	db.rememberKeyBloomLocked(kv.Key)
	partition.releaseEntry(fce)
	return nil
}

// putPassthroughInitial handles the first write to an anchor: allocates a
// fixed-size slottedPageMaxSize page via ff.Insert and populates the cache.
func (db *FlexDB) putPassthroughInitial(kv KV, nh *memSparseIndexTreeHandler, anchor *dbAnchor, partition *intervalCachePartition, fce *intervalCacheEntry) error {
	anchorLoff := uint64(anchor.loff + nh.shift)

	idx, eq := intervalCacheEntryFindKeyGE(fce, kv.Key)
	kvs, size := intervalCacheEntryPreviewUpsert(fce, kv, idx, eq)

	// Encode as tight (unpadded) page. Padding to slottedPageMaxSize is deferred
	// until the first dirty flush that needs to grow the page, at which point
	// flushDirtyPages uses ff.Update to resize to slottedPageMaxSize.
	// This avoids ~47% space waste for pages that are never updated after initial flush.
	buf := slottedPageEncode(kvs)
	psize := uint32(len(buf))

	tag := flexdbTagGenerate(true, 0)
	if _, err := db.ff.InsertWTag(buf, anchorLoff, uint64(psize), tag); err != nil {
		return fmt.Errorf("putPassthroughInitial insert anchor key=%q loff=%d psize=%d maxLoff=%d: %w",
			anchor.key, anchorLoff, psize, db.ff.tree.MaxLoff, err)
	}

	partition.replaceEntryContents(fce, kvs, size)
	nh.shiftUpPropagate(int64(psize))
	anchor.psize = psize
	anchor.unsorted = 0

	if nh.node.parent != nil {
		memSparseIndexTreeNodeRebase(nh.node)
	}
	return nil
}

func (db *FlexDB) putPassthroughR(kv KV, nh *memSparseIndexTreeHandler, anchor *dbAnchor, partition *intervalCachePartition, fce *intervalCacheEntry) error {
	idx, eq := intervalCacheEntryFindKeyGE(fce, kv.Key)

	// Check if the new KV would fit. For pages smaller than slottedPageMaxSize
	// (e.g. tight pages from initial flush or post-vacuum), use the full
	// slottedPageMaxSize as the capacity - the page will be grown on flush.
	replaceIdx := -1
	if eq {
		replaceIdx = idx
	}
	fitTarget := int(anchor.psize)
	if fitTarget < slottedPageMaxSize {
		fitTarget = slottedPageMaxSize
	}
	if !intervalCacheEntryWouldFit(fce, kv, replaceIdx, fitTarget) {
		if eq {
			// Replacing an existing key keeps the entry count unchanged.
			// If the page appears too large here, it is often only because
			// this page temporarily mixes old and new HLC bases while a reload
			// or overwrite batch is walking through its keys. Do not physically
			// resize on every key; let the dirty-page flush encode the final
			// settled page once.
			partition.cacheEntryReplace(fce, kv, idx)
			db.putPassthroughMarkDirty(nh, anchor, fce)
			return nil
		}
		// Inserting a new key - page genuinely full. Split.
		kvs, size := intervalCacheEntryPreviewUpsert(fce, kv, idx, eq)
		snap := partition.snapshotEntry(fce)
		partition.replaceEntryContents(fce, kvs, size)
		if err := db.treeInsertAnchor(nh, partition, fce); err != nil {
			partition.restoreEntry(fce, snap)
			return err
		}
		db.putPassthroughMarkDirty(nh, anchor, fce)
		return nil
	}

	// Update cache entry.
	if eq {
		partition.cacheEntryReplace(fce, kv, idx)
	} else {
		partition.cacheEntryInsert(fce, kv, idx)
	}

	// Mark dirty - will be written to disk on Sync or eviction.
	db.putPassthroughMarkDirty(nh, anchor, fce)
	return nil
}

// putPassthroughMarkDirty marks fce as dirty so it will be written to disk
// on Sync or cache eviction. No disk I/O, no CRC computation.
func (db *FlexDB) putPassthroughMarkDirty(nh *memSparseIndexTreeHandler, anchor *dbAnchor, fce *intervalCacheEntry) {
	fce.dirty = true
	fce.dirtyNode = nh.node
	anchor.unsorted = 0
}

func (db *FlexDB) treeInsertAnchor(nh *memSparseIndexTreeHandler, partition *intervalCachePartition, fce *intervalCacheEntry) error {
	anchor := nh.node.anchors[nh.idx]
	anchorLoff := uint64(anchor.loff + nh.shift)

	count := fce.count
	rightCount := count / 2
	leftCount := count - rightCount
	oldPSize := anchor.psize

	// Left half: encode and Update if psize changed.
	var leftBuf []byte
	if db.cfg.PaddedSplits {
		leftBuf = slottedPageEncodePadded(fce.kvs[:leftCount], slottedPageMaxSize)
	} else {
		leftBuf = slottedPageEncode(fce.kvs[:leftCount])
	}
	leftPSize := uint32(len(leftBuf))
	anchor.unsorted = 0
	if leftPSize != oldPSize {
		if _, err := db.updateAnchorPage(anchor, anchorLoff, leftBuf, oldPSize); err != nil {
			return fmt.Errorf("treeInsertAnchor update left anchor key=%q loff=%d oldPSize=%d newPSize=%d maxLoff=%d: %w",
				anchor.key, anchorLoff, oldPSize, leftPSize, db.ff.tree.MaxLoff, err)
		}
	}
	// Left fce will be marked dirty by caller (putPassthroughMarkDirty or
	// putPassthroughR's split path). Content written on flush.

	// Right half: encode and Insert.
	var rightBuf []byte
	if db.cfg.PaddedSplits {
		rightBuf = slottedPageEncodePadded(fce.kvs[leftCount:fce.count], slottedPageMaxSize)
	} else {
		rightBuf = slottedPageEncode(fce.kvs[leftCount:fce.count])
	}
	rightPSize := uint32(len(rightBuf))
	newAnchorLoff := anchorLoff + uint64(leftPSize)
	newAnchorKey := fce.kvs[leftCount].Key
	tag := flexdbTagGenerate(true, 0)
	if _, err := db.ff.InsertWTag(rightBuf, newAnchorLoff, uint64(rightPSize), tag); err != nil {
		return fmt.Errorf("treeInsertAnchor insert right anchor key=%q loff=%d psize=%d maxLoff=%d: %w",
			newAnchorKey, newAnchorLoff, rightPSize, db.ff.tree.MaxLoff, err)
	}

	// Tag both anchors in FlexSpace before advancing sparse-index/cache state.
	if err := db.ff.SetTag(anchorLoff, tag); err != nil {
		return fmt.Errorf("treeInsertAnchor set left tag anchor key=%q loff=%d psize=%d maxLoff=%d: %w",
			anchor.key, anchorLoff, leftPSize, db.ff.tree.MaxLoff, err)
	}
	if err := db.ff.SetTag(newAnchorLoff, tag); err != nil {
		return fmt.Errorf("treeInsertAnchor set right tag anchor key=%q loff=%d psize=%d maxLoff=%d: %w",
			newAnchorKey, newAnchorLoff, rightPSize, db.ff.tree.MaxLoff, err)
	}

	if leftPSize != oldPSize {
		nh.shiftUpPropagate(int64(leftPSize) - int64(oldPSize))
		anchor.psize = leftPSize
	}
	nh.shiftUpPropagate(int64(rightPSize))

	// Compute left/right sizes for cache.
	leftSize := 0
	for i := 0; i < leftCount; i++ {
		leftSize += kvSizeApprox(&fce.kvs[i])
	}

	nh.idx++
	newAnchor := nh.handlerInsert(newAnchorKey, newAnchorLoff, rightPSize)
	nh.idx--

	newPartition := db.cache.getPartition(newAnchor)
	newFce := newPartition.allocEntryForNewAnchor(newAnchor)

	rightSize := fce.size - leftSize
	newFce.kvs = make([]KV, rightCount)
	copy(newFce.kvs, fce.kvs[leftCount:fce.count])
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
	fce.count = leftCount
	fce.size = leftSize

	newPartition.releaseEntry(newFce)

	return nil
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
│ 0      │ Anchor flag - 1 = this extent starts a sparse index interval      │
├────────┼───────────────────────────────────────────────────────────────────┤
│ 1–7    │ Unsorted count - number of unsorted KVs appended to this interval │
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
3. For each extent, calls fh.GetTag() - if flexdbTagIsAnchor(tag) is true:
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
	by the write path itself - not by the tag bit-width.

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
	actually needed. The 7 bits in the tag (max 127) have ample headroom - the operational
	invariant (flexdbUnsortedWriteQuota = 15) is far below the representational
	limit (0x7f = 127).

	The tag bit-width is not the safety mechanism. The quota check in getEntryUnsorted is.
*/
// rebuildAnchorsFromTags walks all FlexTree extents, finds anchor tags,
// and rebuilds the sparse index tree from scratch. Called by recovery()
// on open and by VacuumKV after compaction. Caller must hold topMutRW.
func (db *FlexDB) rebuildAnchorsFromTags(panicOnFailure bool) {
	type anchorInfo struct {
		key      string
		loff     uint64
		unsorted uint8
	}

	ffSize := db.ff.Size()
	if ffSize == 0 {
		db.tree = memSparseIndexTreeCreate()
		return
	}

	// Destroy old anchor tree, create fresh one with sentinel.
	db.tree = memSparseIndexTreeCreate()

	var anchors []anchorInfo
	kvbuf := make([]byte, slottedPageMaxSize)
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
					alwaysPrintf("rebuildAnchorsFromTags: corrupt anchor at loff=%d tag=0x%04x: flexdbReadKVFromHandler could not read first key (anchor would be lost, orphaning all keys in this interval)", loff, tag)
					panicf("rebuildAnchorsFromTags: corrupt anchor at loff=%d tag=0x%04x: flexdbReadKVFromHandler could not read first key (anchor would be lost, orphaning all keys in this interval)", loff, tag)
				} else {
					alwaysPrintf("rebuildAnchorsFromTags: WARNING: skipping unreadable anchor at loff=%d tag=0x%04x during crash recovery (data may have been lost in crash)", loff, tag)
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

	// Set last anchor's psize - the tail fragment from lastAnchorLoff to ffSize.
	// It is inherently variable-sized and may exceed slottedPageMaxSize; that's normal.
	if nh.node != nil && nh.idx < nh.node.count {
		last := nh.node.anchors[nh.idx]
		last.psize = uint32(ffSize - lastAnchorLoff)
	}
}

func (db *FlexDB) recovery() error {
	db.topMutRW.Lock()
	defer db.topMutRW.Unlock()

	ffSize := db.ff.Size()
	needRecount := ffSize > 0
	if ffSize > 0 {
		db.rebuildAnchorsFromTags(false)
	}

	// Replay WAL.
	treeVer := db.ff.tree.PersistentVersion

	// Replay current WAL (FLEXDB.MEMWAL).
	walSize, err := db.mt.memWalSize()
	if err != nil {
		return fmt.Errorf("flexdb: recovery inspect memwal: %w", err)
	}
	walHdr := db.mt.memWalDataOffset()
	skipWal := false
	if db.ff.omitRedoLog {
		v, err := db.mt.logTreeVersion()
		if err != nil {
			return fmt.Errorf("flexdb: recovery read memwal tree version: %w", err)
		}
		if v > 0 && v <= treeVer {
			skipWal = true
		}
	}
	if walSize > walHdr && !skipWal {
		if err := db.logRedo(db.mt.memWalFD, walSize); err != nil {
			return fmt.Errorf("flexdb: recovery: %w", err)
		}
		if err := db.cache.flushDirtyPages(); err != nil {
			return fmt.Errorf("flexdb: recovery flush dirty pages: %w", err)
		}
		needRecount = true
	}
	if needRecount {
		db.recomputeKeyCountsLocked()
		db.persistCounters()
	}

	db.ff.Sync()
	return nil
}

// flexdbReadKVFromHandler reads the first KV (key only needed for anchor)
// from a handler's current position (does NOT advance the handler).
// Handles both slotted page and kv128 formats.
// If panicOnFailure is true, any read/decode failure panics immediately
// (used after VacuumKV where data must be intact). If false, failures
// are logged and the caller handles the ok=false return (used during
// crash recovery where incomplete WAL replay may leave partial extents).
func flexdbReadKVFromHandler(fh FlexSpaceHandler, buf []byte, panicOnFailure bool) (KV, bool) {
	// NOTE: FlexSpaceHandler.Read does NOT advance the handler position.
	// We must call fh.Forward() after each Read to advance past consumed bytes.

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
		// Slotted page format: the header includes the magic, so we read
		// from the start of the extent (no Forward needed - Read is non-advancing
		// and we want to re-read from offset 0 including the magic).
		extentBytes := int(fh.fp.node.Extents[fh.fp.Idx].Len - fh.fp.Diff)
		readLen := min(len(buf), extentBytes)
		needFirstKeyPrefix := slottedPageHeaderSize + 4 + 1
		if readLen < needFirstKeyPrefix {
			alwaysPrintf("flexdbReadKVFromHandler: slotted extent too small at loff=%d readLen=%d need=%d", fh.Loff(), readLen, needFirstKeyPrefix)
			if panicOnFailure {
				panicf("flexdbReadKVFromHandler: slotted extent too small at loff=%d readLen=%d need=%d", fh.Loff(), readLen, needFirstKeyPrefix)
			}
			return KV{}, false
		}
		nr, err2 := fh.Read(buf[:readLen], uint64(readLen))
		if nr < needFirstKeyPrefix || err2 != nil {
			alwaysPrintf("flexdbReadKVFromHandler: slotted first-key read fail at loff=%d nr=%d need=%d err=%v", fh.Loff(), nr, needFirstKeyPrefix, err2)
			if panicOnFailure {
				panicf("flexdbReadKVFromHandler: slotted first-key read fail at loff=%d nr=%d need=%d err=%v", fh.Loff(), nr, needFirstKeyPrefix, err2)
			}
			return KV{}, false
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
		panicf("flexdbReadKVFromHandler: unexpected format at loff=%d magic=%x (expected slotted page, kv128 no longer supported in KV.SLOT_BLOCKS)", fh.Loff(), magic[:])
	}
	return KV{}, false
}

// logRedo replays the current 20-byte-header MEMWAL, applying typed
// GreenMEMWAL records to FlexSpace. Standalone MEMWAL_KV records are treated as
// already committed. KVs bracketed by MEMWAL_BEGIN_TXN/MEMWAL_COMMIT_TXN are
// replayed as a group only if the commit record is present. A torn tail stops
// replay at the last complete, CRC-valid record. Complete malformed records are
// reported as corruption.
func (db *FlexDB) logRedo(fd vfs.File, fileSize int64) error {
	if fileSize == 0 {
		return nil
	}
	if fileSize < memWalHeaderSize {
		return fmt.Errorf("flexdb: logRedo: short WAL header in %s: size=%d, want at least %d", fd.Name(), fileSize, memWalHeaderSize)
	}

	var hdrBuf [memWalHeaderSize]byte
	n, err := fd.ReadAt(hdrBuf[:], 0)
	if err != nil || n != memWalHeaderSize {
		return fmt.Errorf("flexdb: logRedo: read WAL header from %s: n=%d err=%v", fd.Name(), n, err)
	}
	if !memWalHeaderValid(hdrBuf[:]) {
		return fmt.Errorf("flexdb: logRedo: corrupt 20-byte WAL header in %s", fd.Name())
	}

	var nh memSparseIndexTreeHandler
	applyKV := func(kv KV) error {
		if err := validateKV128RecordSize(kv); err != nil {
			return fmt.Errorf("invalid MEMWAL KV key=%q: %w", kv.Key, err)
		}
		return db.putPassthrough(kv, &nh)
	}

	offset := int64(memWalHeaderSize)
	reader := msgp.NewReader(io.NewSectionReader(fd, offset, fileSize-offset))
	inTxn := false
	var pending []KV

	for offset < fileSize {
		g, n, err := LoadMEMWAL(reader)
		if err != nil {
			if greenMEMWALTornTail(err) {
				vv("flexdb: logRedo: torn GreenMEMWAL tail at offset %d: %v", offset, err)
				break
			}
			return fmt.Errorf("flexdb: logRedo: corrupt GreenMEMWAL record at offset %d: %w", offset, err)
		}
		if n <= 0 {
			return fmt.Errorf("flexdb: logRedo: corrupt zero-length GreenMEMWAL record at offset %d", offset)
		}
		offset += int64(n)

		switch g.WalRecordType {
		case MEMWAL_KV:
			var kv KV
			g.toKV(&kv)
			if inTxn {
				pending = append(pending, kv)
				continue
			}
			if err := applyKV(kv); err != nil {
				vv("flexdb: logRedo: putPassthrough error at offset %d: %v", offset-int64(n), err)
				return fmt.Errorf("flexdb: logRedo: replay at offset %d: %w", offset-int64(n), err)
			}
		case MEMWAL_BATCH_KV, MEMWAL_BATCH_KV_HLC, MEMWAL_BATCH_KV_HLC_VALUE_IS_KEY:
			var kvs []KV
			if g.WalRecordType == MEMWAL_BATCH_KV_HLC {
				kvs, err = compactBatchHLCPayloadToKVs(g.InlineVal, kvs)
			} else if g.WalRecordType == MEMWAL_BATCH_KV_HLC_VALUE_IS_KEY {
				kvs, err = compactBatchHLCValueIsKeyPayloadToKVs(g.InlineVal, kvs)
			} else {
				kvs, err = compactBatchPayloadToKVs(g.InlineVal, kvs)
			}
			if err != nil {
				return fmt.Errorf("flexdb: logRedo: decode batch at offset %d: %w", offset-int64(n), err)
			}
			if inTxn {
				pending = append(pending, kvs...)
				continue
			}
			for i := range kvs {
				if err := applyKV(kvs[i]); err != nil {
					vv("flexdb: logRedo: putPassthrough error in batch at offset %d: %v", offset-int64(n), err)
					return fmt.Errorf("flexdb: logRedo: replay batch at offset %d: %w", offset-int64(n), err)
				}
			}
		case MEMWAL_BEGIN_TXN:
			if inTxn {
				return fmt.Errorf("flexdb: logRedo: nested MEMWAL_BEGIN_TXN at offset %d", offset-int64(n))
			}
			inTxn = true
			pending = pending[:0]
		case MEMWAL_COMMIT_TXN:
			if !inTxn {
				return fmt.Errorf("flexdb: logRedo: MEMWAL_COMMIT_TXN without MEMWAL_BEGIN_TXN at offset %d", offset-int64(n))
			}
			for i := range pending {
				if err := applyKV(pending[i]); err != nil {
					vv("flexdb: logRedo: putPassthrough error committing tx at offset %d: %v", offset-int64(n), err)
					return fmt.Errorf("flexdb: logRedo: replay committed transaction ending at offset %d: %w", offset-int64(n), err)
				}
			}
			pending = pending[:0]
			inTxn = false
		default:
			return fmt.Errorf("flexdb: logRedo: unknown GreenMEMWAL record type %d at offset %d", g.WalRecordType, offset-int64(n))
		}
	}
	if inTxn {
		//vv("flexdb: logRedo: discarding incomplete GreenMEMWAL transaction with %d pending KVs", len(pending))
	}
	return nil
}

func greenMEMWALTornTail(err error) bool {
	return errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, msgp.ErrShortBytes) ||
		errors.Is(err, NotEnoughBytes)
}

// ====================== Flush worker ======================

func (db *FlexDB) flushWorker() {

	ticker := time.NewTicker(db.cfg.BackgroundFlushInterval)
	defer func() {
		ticker.Stop()
		db.flushHalt.ReqStop.Close()
		db.flushHalt.Done.Close()
	}()

	for {
		select {
		case <-db.flushHalt.ReqStop.Chan:
			return
		case <-db.flushTrigger:
			db.safeDoFlush()
		case <-ticker.C:
			db.safeDoFlush()
		}
	}
}

// update: just hides bugs. not sure this is a good idea! off for now.
// safeDoFlush wraps doFlush with a recover so that a panic in the
// background flush goroutine does not crash the entire process.
// This is critical for fuzz testing where 48+ worker subprocesses
// share the process; an uncaught goroutine panic kills them all.
func (db *FlexDB) safeDoFlush() {
	if false { // off for now.
		defer func() {
			if r := recover(); r != nil {
				alwaysPrintf("flushWorker: recovered panic in doFlush: %v", r)
			}
		}()
	}
	if err := db.doFlush(); err != nil {
		panicf("flushWorker: doFlush: %v", err)
	}
}

func (db *FlexDB) startFlushWorkerLocked() {
	if db.cfg.DisableBackgroundFlush || db.closed {
		return
	}
	if db.flushWorkerStarted.CompareAndSwap(false, true) {
		go db.flushWorker()
	}
}

func (db *FlexDB) autoVacuumWorkerLocked() {
	defer db.topMutRW.Unlock()
	if err := db.doAutoVacuumLocked(); err != nil {
		alwaysPrintf("autoVacuumWorker: %v", err)
	}
}

func (db *FlexDB) doAutoVacuumLocked() error {
	start := time.Now()

	if db.closed || !db.autoVacuumShouldRunLocked() {
		return nil
	}

	var err error
	if atomic.LoadInt64(&db.autoVacuumVLOGDeletedBytes) > 0 && db.vlog != nil {
		_, err = db.vacuumVLOGXLocked()
		if err != nil {
			db.autoVacuumLastErr = err.Error()
			return fmt.Errorf("autovacuum VacuumVLOG: %w", err)
		}
	}
	_, err = db.vacuumKVLocked()
	if err != nil {
		db.autoVacuumLastErr = err.Error()
		return fmt.Errorf("autovacuum VacuumKV: %w", err)
	}

	atomic.StoreInt64(&db.autoVacuumDeletedBytes, 0)
	atomic.StoreInt64(&db.autoVacuumVLOGDeletedBytes, 0)
	atomic.StoreInt64(&db.autoVacuumLastDurMs, time.Since(start).Milliseconds())
	atomic.AddInt64(&db.autoVacuumRuns, 1)
	db.autoVacuumLastErr = ""
	return nil
}

// only called by the flushWorker goroutine.
func (db *FlexDB) doFlush() (err error) {
	db.topMutRW.Lock()
	x := true
	autoVacuumHandoff := false
	defer func() {
		if false {
			vv("end of doFlush: sessionMetrics() = '%v'", db.writeLockHeldSessionMetrics())
		}
		if err == nil {
			autoVacuumHandoff = db.maybeStartAutoVacuumXLocked()
		}
		if !autoVacuumHandoff {
			x = false
			db.topMutRW.Unlock()
		}
	}()

	if db.mt.empty.Load() {
		return nil
	}
	if !db.allowReads.Load() && db.mt.bulk.count > 0 {
		// During the read-disabled bulk-load phase, background flushes can turn
		// a pristine initial load into repeated reload merges. Keep this phase
		// under explicit user control: Sync flushes it, and AllowReads performs
		// the required transition.
		return nil
	}

	// Flush WAL to disk
	if err := db.mt.logFlush(); err != nil {
		return fmt.Errorf("doFlush flush memwal: %w", err)
	}
	if err := db.mt.memWalFD.Sync(); err != nil {
		return fmt.Errorf("doFlush sync memwal: %w", err)
	}
	atomic.AddInt64(&db.MemWALFsyncs, 1)

	// Flush memtable to FlexSpace
	if err := db.flushMemtable(x); err != nil {
		return fmt.Errorf("doFlush flush memtable: %w", err)
	}
	if err := db.cache.flushDirtyPages(); err != nil {
		return fmt.Errorf("doFlush flush dirty pages: %w", err)
	}
	db.persistCounters()
	db.ff.Sync()
	db.maybePiggybackGC()

	// Truncate WAL (always use 20-byte versioned header for consistent disk format)
	ts := uint64(time.Now().UnixNano())
	if err := db.mt.logTruncateSyncWithVersion(ts, db.ff.tree.PersistentVersion); err != nil {
		return fmt.Errorf("doFlush truncate memwal: %w", err)
	}

	// Clear the memtable
	db.mt.ks.clear(x)
	db.mt.vtypArena = nil
	db.mt.empty.Store(true)
	db.mt.size = 0
	db.flushSeq++
	return nil
}

func (db *FlexDB) flushMemtable(x bool) error {
	m := &db.mt
	if !db.allowReads.Load() && m.bulk.count > 0 && db.ff.Size() > 0 {
		return db.mergeReloadBulkXLocked()
	}
	if ok, err := db.flushMemtableBulkInitial(m); ok || err != nil {
		return err
	}
	if m.bulk.count > 0 {
		return fmt.Errorf("flexdb: unmaterialized initial bulk data reached normal memtable flush; call AllowReads before general writes")
	}
	var nh memSparseIndexTreeHandler
	batch := make([]KV, 0, memtableFlushBatch)
	var err error

	m.ks.Ascend(KV{}, func(item KV) bool {
		batch = append(batch, item)
		if len(batch) >= memtableFlushBatch {
			for _, kv := range batch {
				if err = db.putPassthrough(kv, &nh); err != nil {
					err = fmt.Errorf("putPassthrough key=%q: %w", kv.Key, err)
					return false
				}
			}
			batch = batch[:0]
		}
		return true
	})
	if err != nil {
		return err
	}
	for _, kv := range batch {
		if err = db.putPassthrough(kv, &nh); err != nil {
			return fmt.Errorf("putPassthrough key=%q: %w", kv.Key, err)
		}
	}
	return nil
}

func (db *FlexDB) flushMemtableBulkInitial(m *memtable) (bool, error) {
	if !db.bulkInitialFastPathEligibleLocked(m) || db.tree == nil || db.tree.root != db.tree.leafHead ||
		!db.tree.root.isLeaf || db.tree.root.count != 1 ||
		db.tree.root.anchors[0] == nil || db.tree.root.anchors[0].key != "" ||
		db.tree.root.anchors[0].psize != 0 {
		return false, nil
	}

	tag := flexdbTagGenerate(true, 0)
	bulkPageCount := flexdbSparseIntervalCount
	if bulkPageCount < 1 {
		bulkPageCount = 1
	}
	allSmallInlineZeroVtyp := m.bulk.count > 0 && m.bulk.allSmallInlineZeroVtyp
	allValuesAliasKeys := allSmallInlineZeroVtyp && m.bulk.allValuesAliasKeys
	page := make([]KV, 0, 128)
	var pageBase HLC
	pageSmallInlineZeroVtyp := true
	pageValuesAliasKeys := allValuesAliasKeys
	pageSize := 0
	pageApproxSize := 0
	cacheOwnsPageOnFlush := false
	var nh memSparseIndexTreeHandler
	nh.node = db.tree.root
	nh.idx = db.tree.root.count

	flushPageItems := func(pageItems []KV, itemsBase HLC, itemsSize int, itemsApproxSize int, cacheOwnsItems bool, itemsValuesAliasKeys bool) error {
		if len(pageItems) == 0 {
			return nil
		}
		if itemsSize > slottedPageMaxSize {
			return fmt.Errorf("bulk initial flush built overlarge slotted page: size=%d max=%d count=%d firstKey=%q",
				itemsSize, slottedPageMaxSize, len(pageItems), pageItems[0].Key)
		}
		ff := db.ff
		ff.gc.writeBetweenStages = true
		ff.globalEpoch++
		if !ff.bm.blockFit(uint64(itemsSize)) {
			ff.bm.nextBlock(false)
		}
		loff := ff.Size()
		poff := ff.bm.offset()
		start := ff.bm.blkoff
		dst := ff.bm.buf[start:start]
		var buf []byte
		if itemsValuesAliasKeys {
			buf = slottedPageEncodeKnownSizeSmallInlineZeroVtypValueIsKey(dst, pageItems, itemsBase, itemsSize)
		} else if pageSmallInlineZeroVtyp {
			buf = slottedPageEncodeKnownSizeSmallInlineZeroVtyp(dst, pageItems, itemsBase, itemsSize)
		} else {
			buf = slottedPageEncodeKnownSize(dst, pageItems, itemsBase, itemsSize)
		}
		if len(buf) != itemsSize {
			return fmt.Errorf("bulk initial flush encoded size mismatch: got=%d want=%d count=%d firstKey=%q",
				len(buf), itemsSize, len(pageItems), pageItems[0].Key)
		}
		ff.bm.blkoff += uint64(itemsSize)
		ff.bm.updateBlkUsage(ff.bm.blkid, int32(itemsSize))
		if ff.bm.blkoff == FLEXSPACE_BLOCK_SIZE {
			ff.bm.nextBlock(false)
		}
		atomic.AddInt64(&ff.insertCount, 1)
		atomic.AddInt64(&ff.insertBytes, int64(itemsSize))
		if r := ff.tree.InsertWTagAppend(poff, uint32(itemsSize), tag); r != 0 {
			return fmt.Errorf("bulk initial flush tree append loff=%d poff=%d psize=%d firstKey=%q failed",
				loff, poff, itemsSize, pageItems[0].Key)
		}
		if !ff.omitRedoLog {
			ff.logWrite(flexOpTreeInsert, loff, poff, uint64(itemsSize))
			if tag != 0 {
				ff.logWrite(flexOpSetTag, loff, uint64(tag), 0)
			}
			if ff.logFull() {
				ff.Sync()
			}
		}
		var anchor *dbAnchor
		if loff == 0 {
			anchor = db.tree.root.anchors[0]
			anchor.psize = uint32(len(buf))
			anchor.unsorted = 0
			nh.node = db.tree.root
			nh.idx = db.tree.root.count
			nh.shift = 0
		} else {
			anchor = nh.handlerAppend(pageItems[0].Key, loff, uint32(len(buf)))
			if anchor == nil {
				return fmt.Errorf("bulk initial flush append anchor returned nil loff=%d psize=%d firstKey=%q",
					loff, len(buf), pageItems[0].Key)
			}
		}
		if anchor != nil && db.cache != nil {
			if cacheOwnsItems {
				db.cache.getPartition(anchor).installCleanEntryOwnedWithSize(anchor, pageItems, itemsBase, len(buf), itemsApproxSize)
			} else {
				db.cache.getPartition(anchor).installCleanEntryWithSize(anchor, pageItems, itemsBase, len(buf), itemsApproxSize)
			}
		}
		return nil
	}

	flushPage := func() error {
		if err := flushPageItems(page, pageBase, pageSize, pageApproxSize, cacheOwnsPageOnFlush, pageValuesAliasKeys); err != nil {
			return err
		}
		if cacheOwnsPageOnFlush {
			page = make([]KV, 0, 128)
		} else {
			page = page[:0]
		}
		pageSize = 0
		pageApproxSize = 0
		pageBase = 0
		pageSmallInlineZeroVtyp = true
		pageValuesAliasKeys = allValuesAliasKeys
		return nil
	}

	var err error
	consumeItem := func(item KV) bool {
		itemSmallInlineZeroVtyp := allSmallInlineZeroVtyp
		if item.isTombstone() {
			itemSmallInlineZeroVtyp = false
		} else if !allSmallInlineZeroVtyp {
			itemSmallInlineZeroVtyp = slottedKVSmallInlineZeroVtyp(item)
		}
		itemValuesAliasKeys := itemSmallInlineZeroVtyp && allValuesAliasKeys
		if len(page) == 0 {
			pageBase = item.Hlc
			pageSize = slottedPageHeaderSize + slottedPageCRCSize
			pageSmallInlineZeroVtyp = true
			pageValuesAliasKeys = itemValuesAliasKeys
		}
		if len(page) > 0 && len(page) >= bulkPageCount {
			if err = flushPage(); err != nil {
				return false
			}
			pageBase = item.Hlc
			pageSize = slottedPageHeaderSize + slottedPageCRCSize
			pageSmallInlineZeroVtyp = true
			pageValuesAliasKeys = itemValuesAliasKeys
		}
		itemSize := 0
		if itemValuesAliasKeys {
			itemSize = slottedKVEncodedSizeSmallInlineZeroVtypValueIsKey(item, pageBase)
		} else if itemSmallInlineZeroVtyp {
			itemSize = slottedKVEncodedSizeSmallInlineZeroVtypKnown(item, pageBase)
		} else {
			itemSize = slottedKVEncodedSize(item, pageBase)
		}
		if len(page) > 0 && item.Hlc < pageBase {
			newBase := item.Hlc
			newPageSize := 0
			if pageValuesAliasKeys {
				newPageSize = intervalCacheEntrySlottedKVsSizeSmallInlineZeroVtypValueIsKey(page, newBase)
			} else if pageSmallInlineZeroVtyp {
				newPageSize = intervalCacheEntrySlottedKVsSizeSmallInlineZeroVtyp(page, newBase)
			} else {
				newPageSize = intervalCacheEntrySlottedKVsSize(page, newBase)
			}
			if itemValuesAliasKeys {
				itemSize = slottedKVEncodedSizeSmallInlineZeroVtypValueIsKey(item, newBase)
			} else if itemSmallInlineZeroVtyp {
				itemSize = slottedKVEncodedSizeSmallInlineZeroVtypKnown(item, newBase)
			} else {
				itemSize = slottedKVEncodedSize(item, newBase)
			}
			if newPageSize+itemSize > slottedPageMaxSize {
				if err = flushPage(); err != nil {
					return false
				}
				pageBase = item.Hlc
				pageSize = slottedPageHeaderSize + slottedPageCRCSize
				pageSmallInlineZeroVtyp = true
				pageValuesAliasKeys = itemValuesAliasKeys
				if itemValuesAliasKeys {
					itemSize = slottedKVEncodedSizeSmallInlineZeroVtypValueIsKey(item, pageBase)
				} else if itemSmallInlineZeroVtyp {
					itemSize = slottedKVEncodedSizeSmallInlineZeroVtypKnown(item, pageBase)
				} else {
					itemSize = slottedKVEncodedSize(item, pageBase)
				}
			} else {
				pageBase = newBase
				pageSize = newPageSize
			}
		} else if len(page) > 0 && pageSize+itemSize > slottedPageMaxSize {
			if err = flushPage(); err != nil {
				return false
			}
			pageBase = item.Hlc
			pageSize = slottedPageHeaderSize + slottedPageCRCSize
			pageSmallInlineZeroVtyp = true
			pageValuesAliasKeys = itemValuesAliasKeys
			if itemValuesAliasKeys {
				itemSize = slottedKVEncodedSizeSmallInlineZeroVtypValueIsKey(item, pageBase)
			} else if itemSmallInlineZeroVtyp {
				itemSize = slottedKVEncodedSizeSmallInlineZeroVtypKnown(item, pageBase)
			} else {
				itemSize = slottedKVEncodedSize(item, pageBase)
			}
		}
		page = append(page, item)
		if !itemSmallInlineZeroVtyp {
			pageSmallInlineZeroVtyp = false
		}
		if !itemValuesAliasKeys {
			pageValuesAliasKeys = false
		}
		pageSize += itemSize
		if itemValuesAliasKeys {
			pageApproxSize += 24 + len(item.Key)
		} else {
			pageApproxSize += kvSizeApprox(&item)
		}
		return true
	}
	consumeAliasKey := func(key string, hlc HLC) bool {
		if len(page) == 0 {
			pageBase = hlc
			pageSize = slottedPageHeaderSize + slottedPageCRCSize
			pageSmallInlineZeroVtyp = true
			pageValuesAliasKeys = true
		}
		if len(page) > 0 && len(page) >= bulkPageCount {
			if err = flushPage(); err != nil {
				return false
			}
			pageBase = hlc
			pageSize = slottedPageHeaderSize + slottedPageCRCSize
			pageSmallInlineZeroVtyp = true
			pageValuesAliasKeys = true
		}
		itemSize := 4 + uvarintLen64(uint64(hlc-pageBase)) + len(key)
		if len(page) > 0 && hlc < pageBase {
			newBase := hlc
			newPageSize := intervalCacheEntrySlottedKVsSizeSmallInlineZeroVtypValueIsKey(page, newBase)
			itemSize = 4 + uvarintLen64(uint64(hlc-newBase)) + len(key)
			if newPageSize+itemSize > slottedPageMaxSize {
				if err = flushPage(); err != nil {
					return false
				}
				pageBase = hlc
				pageSize = slottedPageHeaderSize + slottedPageCRCSize
				pageSmallInlineZeroVtyp = true
				pageValuesAliasKeys = true
				itemSize = 4 + uvarintLen64(uint64(hlc-pageBase)) + len(key)
			} else {
				pageBase = newBase
				pageSize = newPageSize
			}
		} else if len(page) > 0 && pageSize+itemSize > slottedPageMaxSize {
			if err = flushPage(); err != nil {
				return false
			}
			pageBase = hlc
			pageSize = slottedPageHeaderSize + slottedPageCRCSize
			pageSmallInlineZeroVtyp = true
			pageValuesAliasKeys = true
			itemSize = 4 + uvarintLen64(uint64(hlc-pageBase)) + len(key)
		}
		page = append(page, valueIsKeyKV(key, hlc))
		pageSize += itemSize
		pageApproxSize += 24 + len(key)
		return true
	}
	if m.bulk.count > 0 {
		if m.bulk.sorted && !m.bulk.sortedHasDuplicates && allSmallInlineZeroVtyp {
			pageSmallInlineZeroVtyp = true
			for si := range m.bulk.segments {
				seg := &m.bulk.segments[si]
				for start, segLen := 0, seg.len(); start < segLen; {
					first := seg.kv(start)
					chunkBase := first.Hlc
					chunkSize := slottedPageHeaderSize + slottedPageCRCSize
					chunkApproxSize := 0
					end := start
					for end < segLen && end-start < bulkPageCount {
						item := seg.kv(end)
						if end > start && item.Hlc < chunkBase {
							break
						}
						itemSize := 0
						if allValuesAliasKeys {
							itemSize = slottedKVEncodedSizeSmallInlineZeroVtypValueIsKey(item, chunkBase)
						} else {
							itemSize = slottedKVEncodedSizeSmallInlineZeroVtypKnown(item, chunkBase)
						}
						if end > start && chunkSize+itemSize > slottedPageMaxSize {
							break
						}
						chunkSize += itemSize
						chunkApproxSize += kvSizeApprox(&item)
						end++
					}
					if end == start {
						item := first
						if allValuesAliasKeys {
							chunkSize += slottedKVEncodedSizeSmallInlineZeroVtypValueIsKey(item, chunkBase)
						} else {
							chunkSize += slottedKVEncodedSizeSmallInlineZeroVtypKnown(item, chunkBase)
						}
						chunkApproxSize += kvSizeApprox(&item)
						end++
					}
					var chunk []KV
					if len(seg.aliasKeys) > 0 {
						chunk = make([]KV, end-start)
						for i := range chunk {
							chunk[i] = seg.kv(start + i)
						}
					} else {
						chunk = seg.kvs[start:end:end]
					}
					if err = flushPageItems(chunk, chunkBase, chunkSize, chunkApproxSize, true, allValuesAliasKeys); err != nil {
						break
					}
					start = end
				}
				if err != nil {
					break
				}
			}
		} else if m.bulk.sorted {
			var best KV
			haveBest := false
			flushBest := func() bool {
				if !haveBest {
					return true
				}
				return consumeItem(best)
			}
			for si := range m.bulk.segments {
				seg := &m.bulk.segments[si]
				for ki, segLen := 0, seg.len(); ki < segLen; ki++ {
					item := seg.kv(ki)
					if !haveBest {
						best = item
						haveBest = true
						continue
					}
					if best.Key == item.Key {
						if item.Hlc >= best.Hlc {
							best = item
						}
						continue
					}
					if !flushBest() {
						break
					}
					best = item
				}
				if err != nil {
					break
				}
			}
			if err == nil && !flushBest() {
				// consumeItem records the real error in err.
			}
		} else {
			order := m.bulk.buildOrder()
			keys := m.bulk.keys
			cacheOwnsPageOnFlush = allValuesAliasKeys
			if fixedKeyLen := m.bulk.fixedKeyLen; fixedKeyLen > 0 {
				last := fixedKeyLen - 1
				for i := 0; i < len(order); {
					best := order[i]
					j := i + 1
					firstKey := keys[i]
					for j < len(order) && firstKey[last] == keys[j][last] && firstKey == keys[j] {
						cand := order[j]
						if m.bulk.hlc(cand) >= m.bulk.hlc(best) {
							best = cand
						}
						j++
					}
					if allValuesAliasKeys {
						if !consumeAliasKey(firstKey, m.bulk.hlc(best)) {
							break
						}
					} else if !consumeItem(m.bulk.kv(best)) {
						break
					}
					i = j
				}
			} else {
				for i := 0; i < len(order); {
					best := order[i]
					j := i + 1
					firstKey := keys[i]
					for j < len(order) && bulkIngestKeysEqual(firstKey, keys[j]) {
						cand := order[j]
						if m.bulk.hlc(cand) >= m.bulk.hlc(best) {
							best = cand
						}
						j++
					}
					if allValuesAliasKeys {
						if !consumeAliasKey(firstKey, m.bulk.hlc(best)) {
							break
						}
					} else if !consumeItem(m.bulk.kv(best)) {
						break
					}
					i = j
				}
			}
		}
	} else {
		m.ks.Ascend(KV{}, consumeItem)
	}
	if err != nil {
		return true, err
	}
	if err := flushPage(); err != nil {
		return true, err
	}
	m.bulk.reset()
	db.recomputeKeyCountsLocked()
	return true, nil
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

// AllowReads transitions the database from its read-disabled load phase to
// general-purpose reads and writes. The first effective call flushes and syncs
// any pending batch load while reads are still disabled, which lets empty
// databases use the optimized direct bulk-to-FlexSpace path. For a reopened
// non-empty database, pre-AllowReads batches are kept as a sorted reload run
// and merged into the existing FlexSpace at this transition.
//
// Before AllowReads, the only supported data-loading sequence is:
//
//	b := db.NewBatch()
//	b.Set(key, value, vtyp) // or b.SetBytes(...)
//	b.Delete(key)
//	b.Commit(doFsync)
//
// Additional batches and explicit db.Sync calls are also allowed before
// AllowReads, but callers do not need to call Sync before AllowReads. All
// reads, transactions, single-key Put/Delete, DeleteRange, Clear, Merge,
// vacuum, and integrity operations require AllowReads first and will panic if
// used during the initial load phase.
//
// Idempotent. The second call is ignored.
func (db *FlexDB) AllowReads() {
	if db.allowReads.Load() {
		// already done
		return
	}

	// Must grab the write lock because the first AllowReads call is the
	// transition from read-disabled bulk loading into normal readable mode.
	db.topMutRW.Lock()
	defer db.topMutRW.Unlock()

	if db.allowReads.Load() {
		return
	}
	if err := db.writeLockHeldSyncCheckpoint(); err != nil {
		panicf("db.AllowReads(): %v", err)
	}
	db.allowReads.Store(true)
	db.startFlushWorkerLocked()
}
