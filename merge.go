package yogadb

import (
	"fmt"
	"sync/atomic"
	"time"
)

// MergeStats summarizes a DB-into-DB merge.
type MergeStats struct {
	SourceKeys          int64
	SourceTombstones    int64
	TouchedIntervals    int64
	ReusedExtents       int64
	ReusedBytes         int64
	RewrittenPages      int64
	RewrittenBytes      int64
	ResultRecords       int64
	ResultTombstones    int64
	ResultLiveKeys      int64
	ResultLiveBigKeys   int64
	ResultLiveSmallKeys int64
	OldLiveKeysRemoved  int64
	OldLiveBigRemoved   int64
	OldLiveSmallRemoved int64
	SourceWinners       int64
	DestinationWinners  int64
	MaxMergedSourceHLC  HLC
	MaxMergedResultHLC  HLC
}

// MergeOptions controls conflict resolution for DB-into-DB merges.
type MergeOptions struct {
	// TiesToDestination keeps the destination record when source and destination
	// contain the same key with exactly the same HLC. By default, equal-HLC ties
	// go to the source.
	TiesToDestination bool
}

type mergeCursor interface {
	peek() (KV, bool, error)
	next() (KV, bool, error)
}

type sliceMergeCursor struct {
	kvs  []KV
	idx  int
	have bool
	cur  KV
}

func newSliceMergeCursor(kvs []KV) *sliceMergeCursor {
	return &sliceMergeCursor{kvs: kvs}
}

func (c *sliceMergeCursor) peek() (KV, bool, error) {
	if c.have {
		return c.cur, true, nil
	}
	if c.idx >= len(c.kvs) {
		return KV{}, false, nil
	}
	c.cur = c.kvs[c.idx]
	c.have = true
	return c.cur, true, nil
}

func (c *sliceMergeCursor) next() (KV, bool, error) {
	kv, ok, err := c.peek()
	if !ok || err != nil {
		return kv, ok, err
	}
	c.idx++
	c.have = false
	return kv, true, nil
}

type dbRawMergeCursor struct {
	db        *FlexDB
	node      *memSparseIndexTreeNode
	anchorIdx int
	shift     int64
	kvs       []KV
	kvIdx     int
	have      bool
	cur       KV
}

func newDBRawMergeCursor(db *FlexDB) *dbRawMergeCursor {
	c := &dbRawMergeCursor{db: db}
	if db.tree != nil {
		c.node = db.tree.leafHead
		if c.node != nil {
			nh := memSparseIndexTreeHandler{node: c.node}
			memSparseIndexTreeHandlerInfoUpdate(&nh)
			c.shift = nh.shift
		}
	}
	return c
}

func (c *dbRawMergeCursor) peek() (KV, bool, error) {
	if c.have {
		return c.cur, true, nil
	}
	for {
		if c.kvIdx < len(c.kvs) {
			c.cur = c.kvs[c.kvIdx]
			c.have = true
			return c.cur, true, nil
		}
		if c.node == nil {
			return KV{}, false, nil
		}
		if c.anchorIdx >= c.node.count {
			c.node = c.node.next
			c.anchorIdx = 0
			c.kvs = nil
			c.kvIdx = 0
			if c.node == nil {
				return KV{}, false, nil
			}
			nh := memSparseIndexTreeHandler{node: c.node}
			memSparseIndexTreeHandlerInfoUpdate(&nh)
			c.shift = nh.shift
			continue
		}
		anchor := c.node.anchors[c.anchorIdx]
		c.anchorIdx++
		c.kvs = nil
		c.kvIdx = 0
		if anchor == nil || anchor.psize == 0 {
			continue
		}
		kvs, err := c.db.decodeIntervalDirect(anchor, uint64(anchor.loff+c.shift))
		if err != nil {
			return KV{}, false, err
		}
		c.kvs = kvs
	}
}

func (c *dbRawMergeCursor) next() (KV, bool, error) {
	kv, ok, err := c.peek()
	if !ok || err != nil {
		return kv, ok, err
	}
	c.kvIdx++
	c.have = false
	return kv, true, nil
}

type mergeAnchorSpan struct {
	key     string
	nextKey string
	loff    uint64
	psize   uint32
	unsort  uint8
}

type mergeOutputBuilder struct {
	db           *FlexDB
	tree         *FlexTree
	stats        *MergeStats
	appendOffset uint64
	page         []KV
	pageBase     HLC
	pageSize     int
	liveBig      int64
	liveSmall    int64
}

func newMergeOutputBuilder(db *FlexDB, tree *FlexTree, appendOffset uint64, stats *MergeStats) *mergeOutputBuilder {
	return &mergeOutputBuilder{
		db:           db,
		tree:         tree,
		stats:        stats,
		appendOffset: appendOffset,
		page:         make([]KV, 0, 128),
		liveBig:      db.liveBigKeys,
		liveSmall:    db.liveSmallKeys,
	}
}

func (w *mergeOutputBuilder) emitKV(kv KV) error {
	if err := validateKV128RecordSize(kv); err != nil {
		return err
	}
	pageCount := flexdbSparseIntervalCount
	if pageCount < 1 {
		pageCount = 1
	}
	if len(w.page) == 0 {
		w.pageBase = kv.Hlc
		w.pageSize = slottedPageHeaderSize + slottedPageCRCSize
	}
	if len(w.page) > 0 && len(w.page) >= pageCount {
		if err := w.flushPage(); err != nil {
			return err
		}
		w.pageBase = kv.Hlc
		w.pageSize = slottedPageHeaderSize + slottedPageCRCSize
	}

	itemSize := slottedKVEncodedSize(kv, w.pageBase)
	if len(w.page) > 0 && kv.Hlc < w.pageBase {
		newBase := kv.Hlc
		newPageSize := intervalCacheEntrySlottedKVsSize(w.page, newBase)
		itemSize = slottedKVEncodedSize(kv, newBase)
		if newPageSize+itemSize > slottedPageMaxSize {
			if err := w.flushPage(); err != nil {
				return err
			}
			w.pageBase = kv.Hlc
			w.pageSize = slottedPageHeaderSize + slottedPageCRCSize
			itemSize = slottedKVEncodedSize(kv, w.pageBase)
		} else {
			w.pageBase = newBase
			w.pageSize = newPageSize
		}
	} else if len(w.page) > 0 && w.pageSize+itemSize > slottedPageMaxSize {
		if err := w.flushPage(); err != nil {
			return err
		}
		w.pageBase = kv.Hlc
		w.pageSize = slottedPageHeaderSize + slottedPageCRCSize
		itemSize = slottedKVEncodedSize(kv, w.pageBase)
	}
	if w.pageSize+itemSize > slottedPageMaxSize {
		return fmt.Errorf("flexdb merge: KV too large for slotted page key=%q size=%d max=%d",
			kv.Key, w.pageSize+itemSize, slottedPageMaxSize)
	}

	w.page = append(w.page, kv)
	w.pageSize += itemSize
	w.stats.ResultRecords++
	if kv.Hlc > w.stats.MaxMergedResultHLC {
		w.stats.MaxMergedResultHLC = kv.Hlc
	}
	if kv.isTombstone() {
		w.stats.ResultTombstones++
		return nil
	}
	if kv.HasVPtr() {
		w.liveBig++
	} else {
		w.liveSmall++
	}
	return nil
}

func (w *mergeOutputBuilder) flushPage() error {
	if len(w.page) == 0 {
		return nil
	}
	buf := slottedPageEncode(w.page)
	if len(buf) > slottedPageMaxSize {
		return fmt.Errorf("flexdb merge: encoded overlarge slotted page size=%d max=%d firstKey=%q",
			len(buf), slottedPageMaxSize, w.page[0].Key)
	}
	poff, err := w.appendBytes(buf)
	if err != nil {
		return err
	}
	tag := flexdbTagGenerate(true, 0)
	if r := w.tree.InsertWTagAppend(poff, uint32(len(buf)), tag); r != 0 {
		return fmt.Errorf("flexdb merge: append FlexTree extent poff=%d len=%d firstKey=%q failed",
			poff, len(buf), w.page[0].Key)
	}
	w.stats.RewrittenPages++
	w.stats.RewrittenBytes += int64(len(buf))
	w.page = w.page[:0]
	w.pageBase = 0
	w.pageSize = 0
	return nil
}

func (w *mergeOutputBuilder) appendBytes(buf []byte) (uint64, error) {
	if len(buf) == 0 {
		return w.appendOffset, nil
	}
	blkEnd := ((w.appendOffset >> FLEXSPACE_BLOCK_BITS) + 1) << FLEXSPACE_BLOCK_BITS
	if w.appendOffset+uint64(len(buf)) > blkEnd {
		w.appendOffset = blkEnd
	}
	poff := w.appendOffset
	if err := writeAtFull(w.db.ff.fdKV128blocks, buf, int64(poff), "flexdb merge append page"); err != nil {
		return 0, err
	}
	atomic.AddInt64(&w.db.ff.KV128BytesWritten, int64(len(buf)))
	w.appendOffset += uint64(len(buf))
	return poff, nil
}

func (w *mergeOutputBuilder) reuseOldInterval(oldTree *FlexTree, span mergeAnchorSpan) error {
	if err := w.flushPage(); err != nil {
		return err
	}
	if span.psize == 0 {
		return nil
	}
	pos := oldTree.PosGet(span.loff)
	if !pos.Valid() {
		return fmt.Errorf("flexdb merge: old interval key=%q loff=%d psize=%d has no FlexTree extent",
			span.key, span.loff, span.psize)
	}
	remaining := uint64(span.psize)
	first := true
	for remaining > 0 {
		ext := &pos.node.Extents[pos.Idx]
		step := uint64(ext.Len - pos.Diff)
		if step > remaining {
			step = remaining
		}
		tag := uint16(0)
		if first {
			tag = flexdbTagGenerate(true, span.unsort)
		}
		poff := ext.Poff() + uint64(pos.Diff)
		if r := w.tree.InsertWTagAppend(poff, uint32(step), tag); r != 0 {
			return fmt.Errorf("flexdb merge: reuse FlexTree extent key=%q poff=%d len=%d failed",
				span.key, poff, step)
		}
		w.stats.ReusedExtents++
		w.stats.ReusedBytes += int64(step)
		first = false
		remaining -= step
		pos.Forward(step)
	}
	return nil
}

func (w *mergeOutputBuilder) finish() error {
	if err := w.flushPage(); err != nil {
		return err
	}
	w.stats.ResultLiveBigKeys = w.liveBig
	w.stats.ResultLiveSmallKeys = w.liveSmall
	w.stats.ResultLiveKeys = w.liveBig + w.liveSmall
	return nil
}

func (db *FlexDB) mergeAnchorSpansLocked() []mergeAnchorSpan {
	if db.tree == nil || db.tree.leafHead == nil {
		return nil
	}
	var spans []mergeAnchorSpan
	for node := db.tree.leafHead; node != nil; node = node.next {
		nh := memSparseIndexTreeHandler{node: node}
		memSparseIndexTreeHandlerInfoUpdate(&nh)
		for i := 0; i < node.count; i++ {
			anchor := node.anchors[i]
			if anchor == nil {
				continue
			}
			spans = append(spans, mergeAnchorSpan{
				key:    anchor.key,
				loff:   uint64(anchor.loff + nh.shift),
				psize:  anchor.psize,
				unsort: anchor.unsorted,
			})
		}
	}
	for i := 0; i+1 < len(spans); i++ {
		spans[i].nextKey = spans[i+1].key
	}
	return spans
}

func countLiveKVs(kvs []KV) (big, small int64) {
	for i := range kvs {
		if kvs[i].isTombstone() {
			continue
		}
		if kvs[i].HasVPtr() {
			big++
		} else {
			small++
		}
	}
	return big, small
}

func cloneFlexTreeForMerge(oldTree *FlexTree) *FlexTree {
	newTree := NewFlexTree(oldTree.fs)
	newTree.Path = oldTree.Path
	newTree.MaxExtentSize = oldTree.MaxExtentSize
	newTree.cowEnabled = oldTree.cowEnabled
	newTree.metaFD = oldTree.metaFD
	newTree.nodeFD = oldTree.nodeFD
	newTree.maxSlotID = oldTree.maxSlotID
	newTree.nodesFileCap = oldTree.nodesFileCap
	newTree.metaNextOff = oldTree.metaNextOff
	newTree.metaFileCap = oldTree.metaFileCap
	newTree.totalLogicalBytesWrit = oldTree.totalLogicalBytesWrit
	newTree.totalPhysicalBytesWrit = oldTree.totalPhysicalBytesWrit
	newTree.PersistentVersion = oldTree.PersistentVersion
	newTree.MaxHLC = oldTree.MaxHLC
	newTree.liveKeys = oldTree.liveKeys
	newTree.liveBigKeys = oldTree.liveBigKeys
	newTree.liveSmallKeys = oldTree.liveSmallKeys
	newTree.FlexTreePagesBytesWritten = atomic.LoadInt64(&oldTree.FlexTreePagesBytesWritten)

	root := newTree.AllocLeaf()
	root.Dirty = true
	newTree.NodeCount++
	newTree.Root = root.NodeID
	newTree.LeafHead = root.NodeID
	return newTree
}

func sortedBulkIngestKVs(b *bulkIngestBuilder) []KV {
	if b.count == 0 {
		return nil
	}
	out := make([]KV, 0, b.count)
	consume := func(kv KV) {
		if len(out) == 0 || out[len(out)-1].Key != kv.Key {
			out = append(out, kv)
			return
		}
		if kv.Hlc >= out[len(out)-1].Hlc {
			out[len(out)-1] = kv
		}
	}
	if b.sorted {
		for si := range b.segments {
			seg := &b.segments[si]
			for i, n := 0, seg.len(); i < n; i++ {
				consume(seg.kv(i))
			}
		}
		return out
	}
	order := b.buildOrder()
	for _, ref := range order {
		consume(b.kv(ref))
	}
	return out
}

func (db *FlexDB) resetBlockManagerFromTreeLocked() {
	ff := db.ff
	for i := range ff.bm.blkusage {
		ff.bm.blkusage[i] = 0
	}
	for i := range ff.bm.blkdist {
		ff.bm.blkdist[i] = 0
	}
	ff.bm.freeBlocks = 0
	for i := range ff.bm.buf {
		ff.bm.buf[i] = 0
	}
	bmInit(ff.bm, ff.tree, ff.fdKV128blocks)
}

func (db *FlexDB) installMergedTreeLocked(newTree *FlexTree, appendEnd uint64, stats *MergeStats) error {
	db.ff.tree = newTree
	db.liveBigKeys = stats.ResultLiveBigKeys
	db.liveSmallKeys = stats.ResultLiveSmallKeys
	db.liveKeys = stats.ResultLiveKeys
	db.cache.destroyAll()

	if err := db.ff.fdKV128blocks.Truncate(int64(appendEnd)); err != nil {
		return fmt.Errorf("flexdb merge: truncate data file: %w", err)
	}
	if err := db.ff.fdKV128blocks.Sync(); err != nil {
		return fmt.Errorf("flexdb merge: sync data file: %w", err)
	}
	atomic.AddInt64(&db.ff.KV128Fsyncs, 1)

	db.resetBlockManagerFromTreeLocked()
	db.ff.globalEpoch++
	if stats.MaxMergedResultHLC > 0 {
		db.hlc.ReceiveMessageWithHLC(stats.MaxMergedResultHLC)
	}
	db.persistCounters()
	if err := db.ff.tree.SyncCoW(); err != nil {
		return fmt.Errorf("flexdb merge: sync cow: %w", err)
	}
	db.ff.logTruncate()
	db.ff.writeLogVersion()
	db.ff.redoLogFlushAndSync()
	ts := uint64(time.Now().UnixNano())
	if err := db.mt.logTruncateWithVersion(ts, db.ff.tree.PersistentVersion); err != nil {
		return fmt.Errorf("flexdb merge: truncate memwal: %w", err)
	}
	db.rebuildAnchorsFromTags(true)
	db.mt.reset()
	db.flushSeq++
	return nil
}

func (db *FlexDB) mergeFromCursorLocked(src mergeCursor, sourceMaxHLC HLC, opts MergeOptions, prepareSourceWinner func(KV) (KV, error)) (*MergeStats, error) {
	stats := &MergeStats{MaxMergedSourceHLC: sourceMaxHLC}
	if src == nil {
		return stats, nil
	}
	if err := db.cache.flushDirtyPages(); err != nil {
		return stats, fmt.Errorf("flexdb merge: flush dirty destination pages: %w", err)
	}
	db.ff.Sync()

	oldTree := db.ff.tree
	newTree := cloneFlexTreeForMerge(oldTree)
	fileInfo, err := db.ff.fdKV128blocks.Stat()
	if err != nil {
		return stats, fmt.Errorf("flexdb merge: stat destination data file: %w", err)
	}
	out := newMergeOutputBuilder(db, newTree, uint64(fileInfo.Size()), stats)
	spans := db.mergeAnchorSpansLocked()

	emitSource := func(kv KV) error {
		stats.SourceKeys++
		if kv.isTombstone() {
			stats.SourceTombstones++
		}
		if kv.Hlc > stats.MaxMergedSourceHLC {
			stats.MaxMergedSourceHLC = kv.Hlc
		}
		prepared, err := prepareSourceWinner(kv)
		if err != nil {
			return err
		}
		stats.SourceWinners++
		return out.emitKV(prepared)
	}

	for _, span := range spans {
		next, hasNext, err := src.peek()
		if err != nil {
			return stats, err
		}
		if span.psize == 0 {
			for hasNext && span.nextKey != "" && next.Key < span.nextKey {
				kv, _, err := src.next()
				if err != nil {
					return stats, err
				}
				if err := emitSource(kv); err != nil {
					return stats, err
				}
				next, hasNext, err = src.peek()
				if err != nil {
					return stats, err
				}
			}
			continue
		}
		touched := hasNext && (span.nextKey == "" || next.Key < span.nextKey)
		if !touched {
			if err := out.reuseOldInterval(oldTree, span); err != nil {
				return stats, err
			}
			continue
		}

		stats.TouchedIntervals++
		oldKVs, err := db.decodeIntervalDirect(&dbAnchor{key: span.key, loff: int64(span.loff), psize: span.psize, unsorted: span.unsort}, span.loff)
		if err != nil {
			return stats, fmt.Errorf("flexdb merge: decode touched destination interval key=%q loff=%d psize=%d: %w",
				span.key, span.loff, span.psize, err)
		}
		oldBig, oldSmall := countLiveKVs(oldKVs)
		out.liveBig -= oldBig
		out.liveSmall -= oldSmall
		stats.OldLiveBigRemoved += oldBig
		stats.OldLiveSmallRemoved += oldSmall
		stats.OldLiveKeysRemoved += oldBig + oldSmall

		var newKVs []KV
		for hasNext && (span.nextKey == "" || next.Key < span.nextKey) {
			kv, _, err := src.next()
			if err != nil {
				return stats, err
			}
			stats.SourceKeys++
			if kv.isTombstone() {
				stats.SourceTombstones++
			}
			if kv.Hlc > stats.MaxMergedSourceHLC {
				stats.MaxMergedSourceHLC = kv.Hlc
			}
			newKVs = append(newKVs, kv)
			next, hasNext, err = src.peek()
			if err != nil {
				return stats, err
			}
		}
		if err := db.mergeKVSlicesIntoOutput(oldKVs, newKVs, opts, prepareSourceWinner, out, stats); err != nil {
			return stats, err
		}
	}

	for {
		kv, ok, err := src.next()
		if err != nil {
			return stats, err
		}
		if !ok {
			break
		}
		if err := emitSource(kv); err != nil {
			return stats, err
		}
	}

	if err := out.finish(); err != nil {
		return stats, err
	}
	if stats.MaxMergedSourceHLC > stats.MaxMergedResultHLC {
		stats.MaxMergedResultHLC = stats.MaxMergedSourceHLC
	}
	if stats.SourceKeys == 0 {
		return stats, nil
	}
	if err := db.installMergedTreeLocked(newTree, out.appendOffset, stats); err != nil {
		return stats, err
	}
	return stats, nil
}

func mergeSourceWinsConflict(oldKV, newKV KV, opts MergeOptions) bool {
	if newKV.Hlc > oldKV.Hlc {
		return true
	}
	if newKV.Hlc < oldKV.Hlc {
		return false
	}
	return !opts.TiesToDestination
}

func (db *FlexDB) mergeKVSlicesIntoOutput(oldKVs, newKVs []KV, opts MergeOptions, prepareSourceWinner func(KV) (KV, error), out *mergeOutputBuilder, stats *MergeStats) error {
	i, j := 0, 0
	for i < len(oldKVs) || j < len(newKVs) {
		if i >= len(oldKVs) {
			prepared, err := prepareSourceWinner(newKVs[j])
			if err != nil {
				return err
			}
			stats.SourceWinners++
			if err := out.emitKV(prepared); err != nil {
				return err
			}
			j++
			continue
		}
		if j >= len(newKVs) {
			stats.DestinationWinners++
			if err := out.emitKV(oldKVs[i]); err != nil {
				return err
			}
			i++
			continue
		}
		oldKV := oldKVs[i]
		newKV := newKVs[j]
		switch {
		case oldKV.Key < newKV.Key:
			stats.DestinationWinners++
			if err := out.emitKV(oldKV); err != nil {
				return err
			}
			i++
		case oldKV.Key > newKV.Key:
			prepared, err := prepareSourceWinner(newKV)
			if err != nil {
				return err
			}
			stats.SourceWinners++
			if err := out.emitKV(prepared); err != nil {
				return err
			}
			j++
		default:
			if mergeSourceWinsConflict(oldKV, newKV, opts) {
				prepared, err := prepareSourceWinner(newKV)
				if err != nil {
					return err
				}
				stats.SourceWinners++
				if err := out.emitKV(prepared); err != nil {
					return err
				}
			} else {
				stats.DestinationWinners++
				if err := out.emitKV(oldKV); err != nil {
					return err
				}
			}
			i++
			j++
		}
	}
	return nil
}

func (db *FlexDB) mergeReloadBulkLocked() error {
	if db.mt.bulk.count == 0 {
		return nil
	}
	kvs := sortedBulkIngestKVs(&db.mt.bulk)
	cursor := newSliceMergeCursor(kvs)
	_, err := db.mergeFromCursorLocked(cursor, 0, MergeOptions{}, func(kv KV) (KV, error) {
		return kv, nil
	})
	return err
}

// MergeFrom merges every raw KV record from src into db. If both databases
// contain the same key, the higher HLC wins; on equal HLC, src wins. Tombstones
// are ordinary timestamped records: a tombstone deletes another record only
// when the tombstone wins the HLC conflict.
func (db *FlexDB) MergeFrom(src *FlexDB) (*MergeStats, error) {
	return db.MergeFromWithOptions(src, MergeOptions{})
}

// MergeFromWithOptions is MergeFrom with explicit conflict-resolution options.
func (db *FlexDB) MergeFromWithOptions(src *FlexDB, opts MergeOptions) (*MergeStats, error) {
	db.requireReadsAllowed()
	src.requireReadsAllowed()
	if db == src {
		return nil, fmt.Errorf("flexdb: cannot MergeFrom self")
	}

	src.topMutRW.Lock()
	defer src.topMutRW.Unlock()
	if !src.mt.empty {
		if err := src.writeLockHeldSync(); err != nil {
			return nil, fmt.Errorf("flexdb: sync source before merge: %w", err)
		}
	}

	db.topMutRW.Lock()
	defer db.topMutRW.Unlock()
	if !db.mt.empty {
		if err := db.writeLockHeldSync(); err != nil {
			return nil, fmt.Errorf("flexdb: sync destination before merge: %w", err)
		}
	}

	cursor := newDBRawMergeCursor(src)
	stats, err := db.mergeFromCursorLocked(cursor, HLC(src.ff.tree.MaxHLC), opts, func(kv KV) (KV, error) {
		if !kv.HasVPtr() {
			return kv, nil
		}
		val, vtyp, hlc, err := src.lockHeldFetchLarge(&kv)
		if err != nil {
			return KV{}, err
		}
		vp, _, err := db.vlog.appendDedupAndSync(val, hlc, db.lookupOldVPtr(kv.Key), db.cfg.OmitMemWalFsync)
		if err != nil {
			return KV{}, err
		}
		out := KV{Key: kv.Key, Vptr: vp, Hlc: kv.Hlc}
		if vtyp != 0 {
			out.Value = make([]byte, 8)
			putUint64(out.Value, vtyp)
		}
		return out, nil
	})
	return stats, err
}
