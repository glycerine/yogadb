package yogadb

import (
	"fmt"
	"sync/atomic"
	"time"
)

// HLC is a hybrid logical/physical clock, see the
// literature references in the README for citations.
//
// Goroutine safety: all HLC routines are goroutine
// safe except for the argument m to ReceiveMessageWithHLC(m)
// by default, since m is typically from a network
// message that is exclusive owned. If m could be shared,
// read its HLC with Aload() first before passing it to
// ReceiveMessageWithHLC().
//
// Warning about 32-bit CPU systems:
// this package uses atomic.LoadInt64()
// which is safe and fine on 64-bit systems but
// which can be buggy on 32-bit systems if the compiler
// happens to not 64-bit align your data in memory.
//
// So: if you need this package on a 32-bit system,
// ensure your HLCs are always the very first
// field(s) in your struct, so they are 64-bit
// aligned.
//
// See https://pkg.go.dev/sync/atomic#pkg-note-BUG
// where it discusses 32-bit systems (i.e. ARM not ARM64):
//
// "On ARM, 386, and 32-bit MIPS, it is the caller's
// responsibility to arrange for 64-bit alignment of
// 64-bit words accessed atomically via the primitive
// atomic functions (types atomic.Int64 and atomic.Uint64 are
// automatically aligned). The first word in an
// allocated struct, array, or slice; in a global variable;
// or in a local variable (because on 32-bit
// architectures, the subject of 64-bit atomic operations
// will escape to the heap) can be relied upon to be 64-bit
// aligned."
type HLC int64

const getCount HLC = HLC(1<<16) - 1 // low 16 bits are 1
const getLC HLC = ^getCount         // low 16 bits are 0

// LC atomically reads the hlc and returns the upper 48 bits.
// The naming reflects the terminology in the original paper.
func (hlc *HLC) LC() int64 {
	r := HLC(atomic.LoadInt64((*int64)(hlc)))
	return int64(r & getLC)
}

// Count atomically reads the hlc and returns the lower 16 bits.
// The naming reflects the terminology in the original paper.
func (hlc *HLC) Count() int64 {
	r := HLC(atomic.LoadInt64((*int64)(hlc)))
	return int64(r & getCount)
}

// Aload does an atomic load of hlc and returns it.
// Callers of ReceiveMessageWithHLC, for instance,
// should use Aload() to read their HLC atomically
// (for the argument m) before calling ReceiveMessageWithHLC(m),
// if the possibility of data races exists (if more
// than one goroutine can see m).
func (hlc *HLC) Aload() (r HLC) {
	r = HLC(atomic.LoadInt64((*int64)(hlc)))
	return
}

func (hlc *HLC) String() string {
	r := HLC(atomic.LoadInt64((*int64)(hlc)))

	lc := int64(r & getLC)
	count := int64(r & getCount)
	return fmt.Sprintf("HLC{Count: %v, LC:%v (%v)}",
		count, lc, time.Unix(0, lc).Format(rfc3339MsecTz0))
}

//const rfc3339MsecTz0 = "2006-01-02T15:04:05.000Z07:00"

// AssembleHLC does the simple addition,
// but takes care of the type conversation too.
// For safety, it masks off the low 16 bits
// of lc that should always be 0 anyway before
// doing the addition.
func AssembleHLC(lc int64, count int64) HLC {
	return HLC(lc)&getLC + HLC(count)
}

// Here we use a bit-manipulation trick.
// By adding mask (all 1s in lower 16 bits),
// we increment the 16th bit if any lower
// bits were set. We then mask away the
// lower 16 bits.
//func roundUpTo16Bits(pt int64) int64 {
//	return (pt + getCount) & getLC
//}

// PhysicalTime48 rounds up to the 16th
// bit the UnixNano() of the current time,
// as requested by the Hybrid-Logical-Clock
// algorithm. The low order 16 bits are
// used for a logical counter rather than
// nanoseconds. The low 16 bits are always zero
// on return from this function.
func PhysicalTime48() HLC {
	pt := time.Now().UnixNano()

	// hybrid-logical-clocks (HLC) wants to
	// round up at the 48th bit.
	return (HLC(pt) + getCount) & getLC
}

// CreateSendOrLocalEvent
// updates the local hybrid clock j
// based on PhysicalTime48.
// POST: r == *hlc
func (hlc *HLC) CreateSendOrLocalEvent() (r HLC) {

	for {
		j := HLC(atomic.LoadInt64((*int64)(hlc)))
		// equivalent to: j := *hlc

		ptj := PhysicalTime48()
		jLC := j & getLC
		jCount := j & getCount

		jLC1 := jLC
		if ptj > jLC {
			jLC = ptj
		}
		if jLC == jLC1 {
			jCount++
		} else {
			jCount = 0
		}
		r = (jLC + jCount)

		// equivalent to: *hlc = r
		if atomic.CompareAndSwapInt64((*int64)(hlc), int64(j), int64(r)) {
			return
		}
		// collision with another writer, try again.
	}
	return
}

// CreateAndNow is the same as CreateSendOrLocalEvent
// but also returns the raw time.Time before
// hlc conversion of the low 16 bits.
func (hlc *HLC) CreateAndNow() (r HLC, now time.Time) {

	for {
		j := HLC(atomic.LoadInt64((*int64)(hlc)))

		//inlined ptj := PhysicalTime48()
		now = time.Now()
		pt := now.UnixNano()
		ptj := (HLC(pt) + getCount) & getLC

		jLC := j & getLC
		jCount := j & getCount

		jLC1 := jLC
		if ptj > jLC {
			jLC = ptj
		}
		if jLC == jLC1 {
			jCount++
		} else {
			jCount = 0
		}
		r = (jLC + jCount)

		if atomic.CompareAndSwapInt64((*int64)(hlc), int64(j), int64(r)) {
			return
		}
		// collision with another writer, try again.
	}
	return
}

// ReceiveMessageWithHLC
// updates the local hybrid clock hlc based on the
// received message m's hybrid clock.
//
// PRE: to avoid data races, m should be
// owned exclusively by the
// calling goroutine, or if shared, m should
// be the result of an atomic load with m := sourceHLC.Aload().
//
// POST: r == *hlc
func (hlc *HLC) ReceiveMessageWithHLC(m HLC) (r HLC) {

	for {
		j := HLC(atomic.LoadInt64((*int64)(hlc)))

		jLC := j & getLC
		jCount := j & getCount
		jlcOrig := jLC

		mLC := m & getLC
		mCount := m & getCount

		ptj := PhysicalTime48()
		if ptj > jLC {
			jLC = ptj
		}
		if mLC > jLC {
			jLC = mLC
		}
		if jLC == jlcOrig && jlcOrig == mLC {
			jCount = max(jCount, mCount) + 1
		} else if jLC == jlcOrig {
			jCount++
		} else if jLC == mLC {
			jCount = mCount + 1
		} else {
			jCount = 0
		}
		r = (jLC + jCount)

		if atomic.CompareAndSwapInt64((*int64)(hlc), int64(j), int64(r)) {
			return
		}
		// collision with another writer, try again.
	}
	return
}

// ToTime returns the LC and Count as the nanoseconds since Unix epoch
// using the time.Unix() call directly.
func (hlc *HLC) ToTime() time.Time {
	j := atomic.LoadInt64((*int64)(hlc))
	return time.Unix(0, j)
}

// ToTime48 returns only the LC in the upper 48 bits
// of hlc; the lower 16 bits of r.UnixNano() will be all 0.
// See ToTime to include the Count as well.
func (hlc *HLC) ToTime48() (r time.Time) {

	j := HLC(atomic.LoadInt64((*int64)(hlc)))

	lc := int64(j & getLC)
	r = time.Unix(0, int64(lc))
	return
}
