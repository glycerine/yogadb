package main

// zipfian.go — Zipfian distribution generator matching C's rgen_new_zipfian.
//
// This is a faithful port of the YCSB-style Zipfian from lib.c with theta=0.99.
// Also implements clustered Zipfian (zipfuni) and latest distribution.

import (
	"math"
	"math/rand"
	"sync/atomic"
)

const zipfianConstant = 0.99

// ZipfianGen generates Zipfian-distributed random numbers in [base, base+mod).
type ZipfianGen struct {
	rng    *rand.Rand
	base   uint64
	mod    uint64
	modD   float64
	theta  float64
	alpha  float64
	zetan  float64
	eta    float64
	quick1 float64
	quick2 float64
}

// NewZipfian creates a Zipfian generator for the range [min, max].
func NewZipfian(rng *rand.Rand, min, max uint64) *ZipfianGen {
	mod := max - min + 1
	modD := float64(mod)
	theta := zipfianConstant
	quick1 := 1.0 + math.Pow(0.5, theta)
	zeta2theta := zetaRange(0, 2, theta)
	alpha := 1.0 / (1.0 - theta)
	zetan := zetaFull(mod, theta)
	eta := (1.0 - math.Pow(2.0/modD, 1.0-theta)) / (1.0 - (zeta2theta / zetan))
	quick2 := 1.0 - eta

	return &ZipfianGen{
		rng:    rng,
		base:   min,
		mod:    mod,
		modD:   modD,
		theta:  theta,
		alpha:  alpha,
		zetan:  zetan,
		eta:    eta,
		quick1: quick1,
		quick2: quick2,
	}
}

// Next returns the next Zipfian-distributed value.
func (z *ZipfianGen) Next() uint64 {
	u := z.rng.Float64()
	uz := u * z.zetan
	if uz < 1.0 {
		return z.base
	} else if uz < z.quick1 {
		return z.base + 1
	}
	x := z.modD * math.Pow((z.eta*u)+z.quick2, z.alpha)
	return z.base + uint64(x)
}

// zetaRange computes sum(1/(i+1)^theta for i in [start, start+count)).
func zetaRange(start, count uint64, theta float64) float64 {
	sum := 0.0
	for i := uint64(0); i < count; i++ {
		sum += 1.0 / math.Pow(float64(start+i+1), theta)
	}
	return sum
}

// Pre-computed zeta values for theta=0.99, matching C's zetalist[].
// Each entry covers zetalist_step = 0x10000000000 (1<<40) items.
var zetalist = [17]float64{
	0,
	math.Float64frombits(0x4040437dd948c1d9),
	math.Float64frombits(0x4040b8f8009bce85),
	math.Float64frombits(0x4040fe1121e564d6),
	math.Float64frombits(0x40412f435698cdf5),
	math.Float64frombits(0x404155852507a510),
	math.Float64frombits(0x404174d7818477a7),
	math.Float64frombits(0x40418f5e593bd5a9),
	math.Float64frombits(0x4041a6614fb930fd),
	math.Float64frombits(0x4041bab40ad5ec98),
	math.Float64frombits(0x4041cce73d363e24),
	math.Float64frombits(0x4041dd6239ebabc3),
	math.Float64frombits(0x4041ec715f5c47be),
	math.Float64frombits(0x4041fa4eba083897),
	math.Float64frombits(0x4042072772fe12bd),
	math.Float64frombits(0x4042131f5e380b72),
	math.Float64frombits(0x40421e53630da013),
}

const zetalistStep = uint64(0x10000000000)  // 1<<40
const zetalistCount = uint64(16)

// zetaFull uses the pre-computed table for large n, matching C's zeta().
func zetaFull(n uint64, theta float64) float64 {
	zlid0 := n / zetalistStep
	zlid := zlid0
	if zlid > zetalistCount {
		zlid = zetalistCount
	}
	sum0 := zetalist[zlid]
	start := zlid * zetalistStep
	count := n - start
	sum1 := zetaRange(start, count, theta)
	return sum0 + sum1
}

// ClusteredZipfianGen generates clustered Zipfian (zipfuni) distribution.
// This creates aggregated hot spots: z * usize + uniform(usize).
type ClusteredZipfianGen struct {
	zipf  *ZipfianGen
	rng   *rand.Rand
	base  uint64
	usize uint64
}

// NewClusteredZipfian creates a clustered Zipfian generator matching C's rgen_new_zipfuni.
// ufactor controls cluster size (default 1000 in the C benchmarks).
func NewClusteredZipfian(rng *rand.Rand, min, max, ufactor uint64) *ClusteredZipfianGen {
	nr := max - min + 1
	if ufactor == 1 {
		return &ClusteredZipfianGen{
			zipf:  NewZipfian(rng, min, max),
			rng:   rng,
			base:  min,
			usize: 1,
		}
	}
	znr := nr / ufactor
	return &ClusteredZipfianGen{
		zipf:  NewZipfian(rng, 0, znr-1),
		rng:   rng,
		base:  min,
		usize: ufactor,
	}
}

// Next returns the next clustered Zipfian value.
func (c *ClusteredZipfianGen) Next() uint64 {
	z := c.zipf.Next() * c.usize
	u := uint64(c.rng.Int63n(int64(c.usize)))
	return c.base + z + u
}

// LatestGen generates the "latest" distribution: reads follow recently-written keys.
// Write returns an atomically incrementing counter.
// Read returns head - zipfian_offset.
type LatestGen struct {
	zipf *ZipfianGen
	head atomic.Uint64
}

// NewLatest creates a latest distribution generator matching C's rgen_new_latest.
func NewLatest(rng *rand.Rand, zipfRange uint64) *LatestGen {
	if zipfRange == 0 {
		zipfRange = 1
	}
	return &LatestGen{
		zipf: NewZipfian(rng, 1, zipfRange),
	}
}

// NextRead returns the next key to read (near the head).
func (l *LatestGen) NextRead() uint64 {
	z := l.zipf.Next()
	head := l.head.Load()
	if head > z {
		return head - z
	}
	return 0
}

// NextWrite returns the next key to write (incrementing).
func (l *LatestGen) NextWrite() uint64 {
	return l.head.Add(1) - 1
}

// Distribution is a unified interface for key distribution generators.
type Distribution interface {
	Next() uint64
}

// SeqGen generates sequential keys from 0 to max.
type SeqGen struct {
	cur uint64
	max uint64
}

func NewSeqGen(max uint64) *SeqGen {
	return &SeqGen{max: max}
}

func (s *SeqGen) Next() uint64 {
	v := s.cur
	s.cur++
	if s.cur > s.max {
		s.cur = 0
	}
	return v
}

// UniformGen wraps rand for uniform distribution in [min, max].
type UniformGen struct {
	rng *rand.Rand
	min uint64
	mod uint64
}

func NewUniformGen(rng *rand.Rand, min, max uint64) *UniformGen {
	return &UniformGen{rng: rng, min: min, mod: max - min + 1}
}

func (u *UniformGen) Next() uint64 {
	return u.min + uint64(u.rng.Int63n(int64(u.mod)))
}
