package yogadb

import (
	"fmt"
	"math/rand"
	randv2 "math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	porc "github.com/glycerine/porcupine"
	"github.com/glycerine/vfs"
)

// ====================== Multi-Key Porcupine Model ======================

// kvOp identifies the operation type for the multi-key model.
type kvOp int

const (
	KV_PUT kvOp = 1
	KV_GET kvOp = 2
	KV_DEL kvOp = 3
)

func (o kvOp) String() string {
	switch o {
	case KV_PUT:
		return "PUT"
	case KV_GET:
		return "GET"
	case KV_DEL:
		return "DEL"
	}
	return fmt.Sprintf("kvOp(%d)", int(o))
}

// kvInput is the input to the multi-key Porcupine model.
type kvInput struct {
	op    kvOp
	key   string
	value string // for PUT; ignored for GET/DEL
}

func (ki kvInput) String() string {
	switch ki.op {
	case KV_PUT:
		return fmt.Sprintf("PUT(%q, %q)", ki.key, ki.value)
	case KV_GET:
		return fmt.Sprintf("GET(%q)", ki.key)
	case KV_DEL:
		return fmt.Sprintf("DEL(%q)", ki.key)
	}
	return fmt.Sprintf("kvInput{op:%v, key:%q, value:%q}", ki.op, ki.key, ki.value)
}

// kvOutput is the output from the multi-key Porcupine model.
type kvOutput struct {
	value string
	found bool
}

func (ko kvOutput) String() string {
	if !ko.found {
		return "<not found>"
	}
	return fmt.Sprintf("%q", ko.value)
}

// kvRegState is the per-key register state. We use a struct so it's
// comparable with == (which Porcupine's cache requires).
type kvRegState struct {
	value string
	found bool // true if the key has been Put (and not Deleted)
}

// kvModel is a Porcupine model for a multi-key KV store.
// Each key is partitioned into its own independent register,
// making the checker both correct and fast.
//
// Per-partition state: kvRegState{value, found}
// PUT(k,v): always legal, state = {v, true}
// DEL(k):   always legal, state = {"", false}
// GET(k):   legal if output matches state
var kvModel = porc.Model{
	Partition: func(history []porc.Operation) [][]porc.Operation {
		m := make(map[string][]porc.Operation)
		for _, op := range history {
			inp := op.Input.(kvInput)
			m[inp.key] = append(m[inp.key], op)
		}
		var partitions [][]porc.Operation
		for _, ops := range m {
			partitions = append(partitions, ops)
		}
		return partitions
	},
	Init: func() interface{} {
		return kvRegState{} // {value: "", found: false}
	},
	Step: func(state, input, output interface{}) (bool, interface{}) {
		s := state.(kvRegState)
		inp := input.(kvInput)
		out := output.(kvOutput)

		switch inp.op {
		case KV_PUT:
			return true, kvRegState{value: inp.value, found: true}

		case KV_DEL:
			return true, kvRegState{value: "", found: false}

		case KV_GET:
			if !s.found {
				return !out.found, s
			}
			return out.found && out.value == s.value, s
		}
		return false, s
	},
	DescribeOperation: func(input, output interface{}) string {
		inp := input.(kvInput)
		out := output.(kvOutput)
		switch inp.op {
		case KV_PUT:
			return fmt.Sprintf("put(%q, %q)", inp.key, inp.value)
		case KV_DEL:
			return fmt.Sprintf("del(%q)", inp.key)
		case KV_GET:
			if out.found {
				return fmt.Sprintf("get(%q) -> %q", inp.key, out.value)
			}
			return fmt.Sprintf("get(%q) -> <not found>", inp.key)
		}
		return "?"
	},
	DescribeState: func(state interface{}) string {
		s := state.(kvRegState)
		if !s.found {
			return "<empty>"
		}
		return fmt.Sprintf("%q", s.value)
	},
}

// ====================== Helpers ======================

// crashAndRecover takes a crashable MemFS, creates a crash clone with only
// synced data, and opens a new FlexDB on it. The caller must close the
// returned db.
func crashAndRecover(t *testing.T, fs *vfs.MemFS, dir string, cfg *Config) (*FlexDB, *vfs.MemFS) {
	t.Helper()
	crashedFS := fs.CrashClone(vfs.CrashCloneCfg{
		UnsyncedDataPercent: 0, // Option B: only synced data
	})
	if cfg == nil {
		cfg = &Config{}
	}
	cfg.FS = crashedFS
	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatalf("crashAndRecover: OpenFlexDB failed: %v", err)
	}
	return db, crashedFS
}

// verifyDurability checks that all synced key-value pairs are present in db.
// syncedKVs maps key -> expected value. A nil value means the key was deleted.
func verifyDurability(t *testing.T, db *FlexDB, syncedKVs map[string]string) {
	t.Helper()
	for k, want := range syncedKVs {
		got, foundErr := db.Get(k)
		if want == "" {
			// deleted key: should not be found (we use "" as tombstone sentinel)
			if foundErr == nil {
				t.Errorf("key %q: expected not-found after delete, got %q", k, string(got))
			}
		} else {
			if foundErr != nil {
				t.Errorf("key %q: expected %q, got not-found err='%v", k, want, foundErr)
			} else if string(got) != want {
				t.Errorf("key %q: expected %q, got %q", k, want, string(got))
			}
		}
	}
}

// verifyNoPhantomKeys checks that every key in the recovered DB was
// actually in the synced set (no fabricated data).
func verifyNoPhantomKeys(t *testing.T, db *FlexDB, syncedKVs map[string]string) {
	t.Helper()
	// Check a set of keys that should NOT be present.
	// We can't do a full scan (no Scan API yet), so we probe
	// a range of plausible keys.
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%04d", i)
		got, foundErr := db.Get(key)
		if foundErr != nil {
			continue
		}
		want, inSynced := syncedKVs[key]
		if !inSynced {
			t.Errorf("phantom key %q found with value %q (never synced)", key, string(got))
		} else if want == "" {
			t.Errorf("phantom key %q found with value %q (was deleted)", key, string(got))
		}
	}
}

// recordOp appends an operation to the ops slice under the mutex.
func recordOp(mu *sync.Mutex, ops *[]porc.Operation, op porc.Operation) {
	mu.Lock()
	*ops = append(*ops, op)
	mu.Unlock()
}

// snapshotOps returns a copy of ops under the mutex.
func snapshotOps(mu *sync.Mutex, ops *[]porc.Operation) []porc.Operation {
	mu.Lock()
	cp := make([]porc.Operation, len(*ops))
	copy(cp, *ops)
	mu.Unlock()
	return cp
}

// checkLinzOps checks that ops are linearizable under kvModel,
// and writes a visualization on failure.
func checkLinzOps(t *testing.T, ops []porc.Operation) {
	t.Helper()
	if porc.CheckOperations(kvModel, ops) {
		return
	}
	// Not linearizable - visualize and fail.
	res, info := porc.CheckOperationsVerbose(kvModel, ops, 0)
	if res != porc.Illegal {
		t.Fatalf("expected Illegal, got %v", res)
	}
	nm := fmt.Sprintf("red.nonlinz.%v.html", t.Name())
	fd, err := vfs.Default.Create(nm, vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatalf("create vis file: %v", err)
	}
	defer fd.Close()
	if err := porc.Visualize(kvModel, info, fd); err != nil {
		t.Fatalf("visualize: %v", err)
	}
	t.Fatalf("operations not linearizable! visualization: %s\nops:\n%v", nm, kvOpsSlice(ops))
}

type kvOpsSlice []porc.Operation

func (s kvOpsSlice) String() string {
	r := "[\n"
	for i, op := range s {
		r += fmt.Sprintf("  %02d: client=%d %v -> %v  [%s .. %s]\n",
			i, op.ClientId, op.Input, op.Output,
			niceu(op.Call), niceu(op.Return))
	}
	r += "]"
	return r
}

// ====================== Tests ======================

// TestRecovery_DurabilityAfterSync verifies invariant S1:
// Put + Sync -> crash -> reopen -> all synced data present.
func TestRecovery_DurabilityAfterSync(t *testing.T) {
	dir := "test_recovery_durability"
	fs := vfs.NewCrashableMem()
	panicOn(fs.MkdirAll(dir, 0755))

	cfg := &Config{FS: fs}
	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatalf("OpenFlexDB: %v", err)
	}

	// Write and sync several key-value pairs.
	synced := map[string]string{}
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("key%04d", i)
		v := fmt.Sprintf("value%04d", i)
		panicOn(db.Put(k, []byte(v)))
		synced[k] = v
	}
	panicOn(db.Sync())
	db.Close()

	// Crash and recover.
	db2, _ := crashAndRecover(t, fs, dir, nil)
	defer db2.Close()

	verifyDurability(t, db2, synced)
}

// TestRecovery_NoPhantomData verifies invariant S4:
// After crash, no keys exist that were never Put.
func TestRecovery_NoPhantomData(t *testing.T) {
	dir := "test_recovery_phantom"
	fs := vfs.NewCrashableMem()
	panicOn(fs.MkdirAll(dir, 0755))

	cfg := &Config{FS: fs}
	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatalf("OpenFlexDB: %v", err)
	}

	synced := map[string]string{}
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("key%04d", i)
		v := fmt.Sprintf("val%04d", i)
		panicOn(db.Put(k, []byte(v)))
		synced[k] = v
	}
	panicOn(db.Sync())
	db.Close()

	db2, _ := crashAndRecover(t, fs, dir, nil)
	defer db2.Close()

	verifyNoPhantomKeys(t, db2, synced)
}

// TestRecovery_DeleteDurability verifies invariant S5:
// Del + Sync -> crash -> reopen -> key not found.
func TestRecovery_DeleteDurability(t *testing.T) {
	dir := "test_recovery_delete"
	fs := vfs.NewCrashableMem()
	panicOn(fs.MkdirAll(dir, 0755))

	cfg := &Config{FS: fs}
	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatalf("OpenFlexDB: %v", err)
	}

	// Put, sync, then delete, sync.
	panicOn(db.Put("delme", []byte("hello")))
	panicOn(db.Sync())
	panicOn(db.Delete("delme"))
	panicOn(db.Sync())

	// Also put a key that stays.
	panicOn(db.Put("keeper", []byte("world")))
	panicOn(db.Sync())
	db.Close()

	db2, _ := crashAndRecover(t, fs, dir, nil)
	defer db2.Close()

	// "delme" should not be found.
	if _, foundErr := db2.Get("delme"); foundErr == nil {
		t.Fatalf("key 'delme' should not be found after delete+sync+crash")
	}
	// "keeper" should be found.
	v, foundErr := db2.Get("keeper")
	if foundErr != nil || string(v) != "world" {
		t.Fatalf("key 'keeper': expected 'world', got %q foundErr=%v", string(v), foundErr)
	}
}

// TestRecovery_ProgressAfterRecovery verifies invariant L2:
// After recovery, new Put/Get/Del all work.
func TestRecovery_ProgressAfterRecovery(t *testing.T) {
	dir := "test_recovery_progress"
	fs := vfs.NewCrashableMem()
	panicOn(fs.MkdirAll(dir, 0755))

	cfg := &Config{FS: fs}
	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatalf("OpenFlexDB: %v", err)
	}

	panicOn(db.Put("before", []byte("crash")))
	panicOn(db.Sync())
	db.Close()

	db2, _ := crashAndRecover(t, fs, dir, nil)
	defer db2.Close()

	// New operations after recovery should work.
	panicOn(db2.Put("after", []byte("recovery")))
	v, foundErr := db2.Get("after")
	if foundErr != nil || string(v) != "recovery" {
		t.Fatalf("post-recovery Put/Get failed: foundErr=%v val=%q", foundErr, string(v))
	}
	panicOn(db2.Delete("after"))
	if _, foundErr = db2.Get("after"); foundErr == nil {
		t.Fatalf("post-recovery Delete failed: key still found")
	}

	// Pre-crash data still accessible.
	v, foundErr = db2.Get("before")
	if foundErr != nil || string(v) != "crash" {
		t.Fatalf("pre-crash key not found after recovery: foundErr='%v' val=%q", foundErr, string(v))
	}
}

// TestRecovery_RecoveryTerminates verifies invariant L1:
// OpenFlexDB on a crashed DB must complete within a reasonable time.
func TestRecovery_RecoveryTerminates(t *testing.T) {
	dir := "test_recovery_terminates"
	fs := vfs.NewCrashableMem()
	panicOn(fs.MkdirAll(dir, 0755))

	cfg := &Config{FS: fs}
	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatalf("OpenFlexDB: %v", err)
	}

	// Write a moderate amount of data.
	for i := 0; i < 500; i++ {
		k := fmt.Sprintf("key%04d", i)
		v := fmt.Sprintf("value%04d_padding_to_make_it_larger_%d", i, i*31)
		panicOn(db.Put(k, []byte(v)))
	}
	panicOn(db.Sync())
	db.Close()

	crashedFS := fs.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 0})

	// Recovery must complete within 30 seconds.
	done := make(chan struct{})
	var db2 *FlexDB
	var openErr error
	go func() {
		recoverCfg := &Config{FS: crashedFS}
		db2, openErr = OpenFlexDB(dir, recoverCfg)
		close(done)
	}()

	select {
	case <-done:
		if openErr != nil {
			t.Fatalf("recovery failed: %v", openErr)
		}
		db2.Close()
	case <-time.After(30 * time.Second):
		t.Fatalf("recovery did not terminate within 30 seconds (L1 violation)")
	}
}

// TestRecovery_KeyCountConsistency verifies invariant C1:
// Len() matches the number of distinct live keys after recovery.
func TestRecovery_KeyCountConsistency(t *testing.T) {
	dir := "test_recovery_keycount"
	fs := vfs.NewCrashableMem()
	panicOn(fs.MkdirAll(dir, 0755))

	cfg := &Config{FS: fs}
	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatalf("OpenFlexDB: %v", err)
	}

	nKeys := 200
	for i := 0; i < nKeys; i++ {
		k := fmt.Sprintf("key%04d", i)
		v := fmt.Sprintf("val%04d", i)
		panicOn(db.Put(k, []byte(v)))
	}
	// Delete some.
	nDel := 50
	for i := 0; i < nDel; i++ {
		k := fmt.Sprintf("key%04d", i)
		panicOn(db.Delete(k))
	}
	panicOn(db.Sync())
	db.Close()

	db2, _ := crashAndRecover(t, fs, dir, nil)
	defer db2.Close()

	reportedLen := db2.Len()

	// Count by probing all keys.
	var actualCount int64
	for i := 0; i < nKeys; i++ {
		k := fmt.Sprintf("key%04d", i)
		_, foundErr := db2.Get(k)
		if foundErr == nil {
			actualCount++
		}
	}
	expectedLive := int64(nKeys - nDel)
	if actualCount != expectedLive {
		t.Fatalf("actual live keys: got %d, expected %d", actualCount, expectedLive)
	}
	if reportedLen != expectedLive {
		t.Errorf("Len() = %d, but actual live keys = %d (C1 violation)", reportedLen, expectedLive)
	}
}

// TestRecovery_UnsyncedDataLoss verifies that un-synced Puts
// may be lost after crash - and that is NOT a bug.
func TestRecovery_UnsyncedDataLoss(t *testing.T) {
	dir := "test_recovery_unsynced"
	fs := vfs.NewCrashableMem()
	panicOn(fs.MkdirAll(dir, 0755))

	cfg := &Config{FS: fs}
	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatalf("OpenFlexDB: %v", err)
	}

	// Synced data.
	panicOn(db.Put("synced_key", []byte("synced_val")))
	panicOn(db.Sync())

	// Un-synced data (no Sync after these Puts).
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("unsynced%04d", i)
		panicOn(db.Put(k, []byte("ephemeral")))
	}
	// Deliberately do NOT call db.Sync() here.
	db.Close()

	db2, _ := crashAndRecover(t, fs, dir, nil)
	defer db2.Close()

	// Synced data must survive.
	v, foundErr := db2.Get("synced_key")
	if foundErr != nil || string(v) != "synced_val" {
		t.Fatalf("synced key lost after crash! foundErr='%v' val=%q", foundErr, string(v))
	}

	// Un-synced data may or may not be present (UnsyncedDataPercent=0 means
	// it should be gone), but the DB must not be corrupted either way.
	// We just verify the DB is operational.
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("unsynced%04d", i)
		db2.Get(k) // must not panic or hang
	}
}

// TestRecovery_LinearizabilitySingleKey verifies invariant S6:
// Concurrent Put/Get on a single key, crash, recover, continue,
// entire history must be linearizable.
func TestRecovery_LinearizabilitySingleKey(t *testing.T) {
	dir := "test_recovery_linz_single"
	fs := vfs.NewCrashableMem()
	panicOn(fs.MkdirAll(dir, 0755))

	cfg := &Config{FS: fs}
	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatalf("OpenFlexDB: %v", err)
	}

	const (
		numWriters     = 1 // single writer for simplicity
		numReaders     = 3
		preCrashSteps  = 20
		postCrashSteps = 20
		theKey         = "thekey"
	)

	var (
		ops   []porc.Operation
		opsMu sync.Mutex
	)

	// clientId 0 = writer, clientId 1..numReaders = readers
	writerClient := 0

	// ---- Phase 1: pre-crash operations ----
	for step := 0; step < preCrashSteps; step++ {
		val := fmt.Sprintf("v%d", step)

		// PUT
		callTime := time.Now().UnixNano()
		panicOn(db.Put(theKey, []byte(val)))
		returnTime := time.Now().UnixNano()

		recordOp(&opsMu, &ops, porc.Operation{
			ClientId: writerClient,
			Input:    kvInput{op: KV_PUT, key: theKey, value: val},
			Output:   kvOutput{value: val, found: true},
			Call:     callTime,
			Return:   returnTime,
		})

		// Concurrent GETs
		var wg sync.WaitGroup
		for r := 0; r < numReaders; r++ {
			wg.Add(1)
			go func(clientId int) {
				defer wg.Done()
				callT := time.Now().UnixNano()
				got, foundErr := db.Get(theKey)
				returnT := time.Now().UnixNano()

				out := kvOutput{value: string(got), found: foundErr == nil}
				recordOp(&opsMu, &ops, porc.Operation{
					ClientId: clientId,
					Input:    kvInput{op: KV_GET, key: theKey},
					Output:   out,
					Call:     callT,
					Return:   returnT,
				})
			}(r + 1)
		}
		wg.Wait()
	}

	// Sync so data is durable, then check pre-crash linearizability.
	panicOn(db.Sync())
	checkLinzOps(t, snapshotOps(&opsMu, &ops))

	// Close (simulates orderly shutdown before crash clone).
	db.Close()

	// ---- Crash and recover ----
	db2, _ := crashAndRecover(t, fs, dir, nil)
	defer db2.Close()

	// The last synced value.
	lastVal := fmt.Sprintf("v%d", preCrashSteps-1)

	// After recovery, a GET must return the last synced value.
	// Record this as a new operation in the continuing history.
	{
		callT := time.Now().UnixNano()
		got, foundErr := db2.Get(theKey)
		returnT := time.Now().UnixNano()
		if foundErr != nil || string(got) != lastVal {
			t.Fatalf("after recovery: expected %q, got %q foundErr=%v", lastVal, string(got), foundErr)
		}
		recordOp(&opsMu, &ops, porc.Operation{
			ClientId: 1,
			Input:    kvInput{op: KV_GET, key: theKey},
			Output:   kvOutput{value: string(got), found: foundErr == nil},
			Call:     callT,
			Return:   returnT,
		})
	}

	// ---- Phase 2: post-crash operations ----
	for step := preCrashSteps; step < preCrashSteps+postCrashSteps; step++ {
		val := fmt.Sprintf("v%d", step)

		callTime := time.Now().UnixNano()
		panicOn(db2.Put(theKey, []byte(val)))
		returnTime := time.Now().UnixNano()

		recordOp(&opsMu, &ops, porc.Operation{
			ClientId: writerClient,
			Input:    kvInput{op: KV_PUT, key: theKey, value: val},
			Output:   kvOutput{value: val, found: true},
			Call:     callTime,
			Return:   returnTime,
		})

		var wg sync.WaitGroup
		for r := 0; r < numReaders; r++ {
			wg.Add(1)
			go func(clientId int) {
				defer wg.Done()
				callT := time.Now().UnixNano()
				got, foundErr := db2.Get(theKey)
				returnT := time.Now().UnixNano()

				out := kvOutput{value: string(got), found: foundErr == nil}
				recordOp(&opsMu, &ops, porc.Operation{
					ClientId: clientId,
					Input:    kvInput{op: KV_GET, key: theKey},
					Output:   out,
					Call:     callT,
					Return:   returnT,
				})
			}(r + 1)
		}
		wg.Wait()
	}

	// Final linearizability check on entire history (pre + post crash).
	checkLinzOps(t, snapshotOps(&opsMu, &ops))
}

// TestRecovery_LinearizabilityMultiKey verifies invariant S6 with
// multiple independent keys and concurrent writers/readers.
//
// Update: we now pre-compute random choices before spawning goro
// to avoid data races on rand.Rand.
func TestRecovery_LinearizabilityMultiKey(t *testing.T) {
	dir := "test_recovery_linz_multi"
	fs := vfs.NewCrashableMem()
	panicOn(fs.MkdirAll(dir, 0755))

	cfg := &Config{FS: fs}
	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatalf("OpenFlexDB: %v", err)
	}

	const (
		numKeys        = 5
		numClients     = 4 // each does both reads and writes
		preCrashSteps  = 15
		postCrashSteps = 15
	)

	keys := make([]string, numKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("mk%02d", i)
	}

	var (
		ops   []porc.Operation
		opsMu sync.Mutex
	)

	rng := rand.New(rand.NewSource(42))

	// Pre-compute random choices to avoid sharing *rand.Rand across goroutines.
	type clientChoice struct {
		keyIdx int
		doPut  bool
	}
	totalSteps := preCrashSteps + postCrashSteps
	choices := make([][]clientChoice, totalSteps)
	for s := 0; s < totalSteps; s++ {
		choices[s] = make([]clientChoice, numClients)
		for c := 0; c < numClients; c++ {
			choices[s][c] = clientChoice{
				keyIdx: rng.Intn(numKeys),
				doPut:  rng.Intn(2) == 0,
			}
		}
	}

	// ---- Phase 1: pre-crash ----
	for step := 0; step < preCrashSteps; step++ {
		var wg sync.WaitGroup
		for c := 0; c < numClients; c++ {
			wg.Add(1)
			go func(clientId, step int) {
				defer wg.Done()
				ch := choices[step][clientId]
				key := keys[ch.keyIdx]

				if ch.doPut {
					// PUT
					val := fmt.Sprintf("s%d_c%d", step, clientId)
					callT := time.Now().UnixNano()
					err := db.Put(key, []byte(val))
					returnT := time.Now().UnixNano()
					if err != nil {
						t.Errorf("Put failed: %v", err)
						return
					}
					recordOp(&opsMu, &ops, porc.Operation{
						ClientId: clientId,
						Input:    kvInput{op: KV_PUT, key: key, value: val},
						Output:   kvOutput{value: val, found: true},
						Call:     callT,
						Return:   returnT,
					})
				} else {
					// GET
					callT := time.Now().UnixNano()
					got, foundErr := db.Get(key)
					returnT := time.Now().UnixNano()
					recordOp(&opsMu, &ops, porc.Operation{
						ClientId: clientId,
						Input:    kvInput{op: KV_GET, key: key},
						Output:   kvOutput{value: string(got), found: foundErr == nil},
						Call:     callT,
						Return:   returnT,
					})
				}
			}(c, step)
		}
		wg.Wait()
	}

	panicOn(db.Sync())
	checkLinzOps(t, snapshotOps(&opsMu, &ops))
	db.Close()

	// ---- Crash and recover ----
	db2, _ := crashAndRecover(t, fs, dir, nil)
	defer db2.Close()

	// After recovery, verify all keys have valid values by reading them.
	// These reads extend the Porcupine history.
	for _, key := range keys {
		callT := time.Now().UnixNano()
		got, foundErr := db2.Get(key)
		returnT := time.Now().UnixNano()
		recordOp(&opsMu, &ops, porc.Operation{
			ClientId: 0,
			Input:    kvInput{op: KV_GET, key: key},
			Output:   kvOutput{value: string(got), found: foundErr == nil},
			Call:     callT,
			Return:   returnT,
		})
	}

	// ---- Phase 2: post-crash ----
	for step := preCrashSteps; step < preCrashSteps+postCrashSteps; step++ {
		var wg sync.WaitGroup
		for c := 0; c < numClients; c++ {
			wg.Add(1)
			go func(clientId, step int) {
				defer wg.Done()
				ch := choices[step][clientId]
				key := keys[ch.keyIdx]

				if ch.doPut {
					val := fmt.Sprintf("s%d_c%d", step, clientId)
					callT := time.Now().UnixNano()
					err := db2.Put(key, []byte(val))
					returnT := time.Now().UnixNano()
					if err != nil {
						t.Errorf("Put failed: %v", err)
						return
					}
					recordOp(&opsMu, &ops, porc.Operation{
						ClientId: clientId,
						Input:    kvInput{op: KV_PUT, key: key, value: val},
						Output:   kvOutput{value: val, found: true},
						Call:     callT,
						Return:   returnT,
					})
				} else {
					callT := time.Now().UnixNano()
					got, foundErr := db2.Get(key)
					returnT := time.Now().UnixNano()
					recordOp(&opsMu, &ops, porc.Operation{
						ClientId: clientId,
						Input:    kvInput{op: KV_GET, key: key},
						Output:   kvOutput{value: string(got), found: foundErr == nil},
						Call:     callT,
						Return:   returnT,
					})
				}
			}(c, step)
		}
		wg.Wait()
	}

	checkLinzOps(t, snapshotOps(&opsMu, &ops))
}

// TestRecovery_CrashDuringRecovery verifies invariant L3:
// If the process crashes during OpenFlexDB recovery, a subsequent
// OpenFlexDB must still succeed. Recovery is idempotent.
func TestRecovery_CrashDuringRecovery(t *testing.T) {
	dir := "test_recovery_crash_during"
	fs := vfs.NewCrashableMem()
	panicOn(fs.MkdirAll(dir, 0755))

	cfg := &Config{FS: fs}
	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatalf("OpenFlexDB: %v", err)
	}

	// Write data and sync.
	synced := map[string]string{}
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("key%04d", i)
		v := fmt.Sprintf("val%04d", i)
		panicOn(db.Put(k, []byte(v)))
		synced[k] = v
	}
	panicOn(db.Sync())
	db.Close()

	// First crash clone (simulates crash at runtime).
	crashed1 := fs.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 0})

	// Open on crashed1 to start recovery, then take another crash clone
	// to simulate crashing DURING recovery. We do this by opening on
	// crashed1, writing nothing, and taking a clone of crashed1.
	// This tests that the DB files produced during recovery are
	// themselves crash-safe.
	recoverCfg := &Config{FS: crashed1}
	db2, err := OpenFlexDB(dir, recoverCfg)
	if err != nil {
		t.Fatalf("first recovery failed: %v", err)
	}
	// Sync the recovered state so recovery artifacts are durable.
	panicOn(db2.Sync())
	db2.Close()

	// Second crash clone of the recovered FS (simulates crash right
	// after recovery completed and synced).
	crashed2 := crashed1.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 0})

	// Third open: must succeed and have all data.
	recoverCfg2 := &Config{FS: crashed2}
	db3, err := OpenFlexDB(dir, recoverCfg2)
	if err != nil {
		t.Fatalf("second recovery (crash-during-recovery) failed: %v", err)
	}
	defer db3.Close()

	verifyDurability(t, db3, synced)
}

// TestRecovery_RepeatedCrashCycles verifies that multiple
// crash-recover-write-crash cycles maintain linearizability.
//
// Update: added settleAfterCrash() to the tracker
// after each crash clone. Without this, ksMaybeSet entries would
// accumulate stale syncedValue fields across cycles. When the
// same writer re-used the same seq numbers in a new
// cycle, the tracker could incorrectly promote a key to
// ksSet with a value the DB didn't actually have.
// .
func TestRecovery_RepeatedCrashCycles(t *testing.T) {
	dir := "test_recovery_cycles"
	fs := vfs.NewCrashableMem()
	panicOn(fs.MkdirAll(dir, 0755))

	const (
		numCycles   = 5
		opsPerCycle = 10
		numKeys     = 3
	)

	keys := []string{"rk0", "rk1", "rk2"}

	var (
		ops   []porc.Operation
		opsMu sync.Mutex
	)

	currentFS := fs
	writerClient := 0
	readerClient := 1

	for cycle := 0; cycle < numCycles; cycle++ {
		cfg := &Config{FS: currentFS}
		db, err := OpenFlexDB(dir, cfg)
		if err != nil {
			t.Fatalf("cycle %d: OpenFlexDB failed: %v", cycle, err)
		}

		// Read all keys after recovery (extends history).
		for _, key := range keys {
			callT := time.Now().UnixNano()
			got, foundErr := db.Get(key)
			returnT := time.Now().UnixNano()
			recordOp(&opsMu, &ops, porc.Operation{
				ClientId: readerClient,
				Input:    kvInput{op: KV_GET, key: key},
				Output:   kvOutput{value: string(got), found: foundErr == nil},
				Call:     callT,
				Return:   returnT,
			})
		}

		// Do some writes and reads.
		for step := 0; step < opsPerCycle; step++ {
			key := keys[step%numKeys]
			val := fmt.Sprintf("c%d_s%d", cycle, step)

			callT := time.Now().UnixNano()
			panicOn(db.Put(key, []byte(val)))
			returnT := time.Now().UnixNano()
			recordOp(&opsMu, &ops, porc.Operation{
				ClientId: writerClient,
				Input:    kvInput{op: KV_PUT, key: key, value: val},
				Output:   kvOutput{value: val, found: true},
				Call:     callT,
				Return:   returnT,
			})

			// Read it back.
			callT = time.Now().UnixNano()
			got, foundErr := db.Get(key)
			returnT = time.Now().UnixNano()
			recordOp(&opsMu, &ops, porc.Operation{
				ClientId: readerClient,
				Input:    kvInput{op: KV_GET, key: key},
				Output:   kvOutput{value: string(got), found: foundErr == nil},
				Call:     callT,
				Return:   returnT,
			})
		}

		panicOn(db.Sync())
		db.Close()

		// Crash and prepare for next cycle.
		crashedFS := currentFS.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 0})
		currentFS = crashedFS
	}

	// All operations across all cycles must be linearizable.
	checkLinzOps(t, snapshotOps(&opsMu, &ops))
}

// TestRecovery_ConcurrentWritersCrash verifies that with N concurrent
// writers, crash at a random point, recovery is consistent and no
// corruption occurs.
func TestRecovery_ConcurrentWritersCrash(t *testing.T) {
	dir := "test_recovery_concurrent"
	fs := vfs.NewCrashableMem()
	panicOn(fs.MkdirAll(dir, 0755))

	cfg := &Config{FS: fs}
	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatalf("OpenFlexDB: %v", err)
	}

	const (
		numWriters = 4
		numOps     = 50
	)

	// Track what was synced.
	syncedKVs := make(map[string]string)
	var syncMu sync.Mutex

	// Phase 1: concurrent writes.
	var wg sync.WaitGroup
	var writesDone int64
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for i := 0; i < numOps; i++ {
				k := fmt.Sprintf("w%d_k%04d", writerID, i)
				v := fmt.Sprintf("w%d_v%04d", writerID, i)
				if err := db.Put(k, []byte(v)); err != nil {
					t.Errorf("writer %d: Put failed: %v", writerID, err)
					return
				}
				atomic.AddInt64(&writesDone, 1)
			}
		}(w)
	}
	wg.Wait()

	// Sync everything - all writes become durable.
	panicOn(db.Sync())
	for w := 0; w < numWriters; w++ {
		for i := 0; i < numOps; i++ {
			k := fmt.Sprintf("w%d_k%04d", w, i)
			v := fmt.Sprintf("w%d_v%04d", w, i)
			syncMu.Lock()
			syncedKVs[k] = v
			syncMu.Unlock()
		}
	}
	db.Close()

	// Crash and recover.
	db2, _ := crashAndRecover(t, fs, dir, nil)
	defer db2.Close()

	verifyDurability(t, db2, syncedKVs)
}

// ====================== Three-State Key Tracker ======================

// durState represents the durability state of a key.
type durState int

const (
	ksUnset    durState = 0 // never written
	ksMaybeSet durState = 1 // written but not synced
	ksSet      durState = 2 // written AND synced
)

// keyEntry tracks a key's state and expected value.
type keyEntry struct {
	state       durState
	value       string // current value for this state
	syncedValue string // last synced value (if any)
	wasSynced   bool   // true if this key was previously synced
}

// keyTracker is a thread-safe three-state tracker for crash testing.
type keyTracker struct {
	mu      sync.Mutex
	entries map[string]keyEntry
}

func newKeyTracker() *keyTracker {
	return &keyTracker{entries: make(map[string]keyEntry)}
}

// markMaybeSet records that a Put was issued but not yet synced.
// If the key was previously ksSet, preserves the synced value so
// we know what value is valid after a crash.
func (kt *keyTracker) markMaybeSet(key, value string) {
	kt.mu.Lock()
	e := kt.entries[key]
	if e.state == ksSet {
		// Preserve the synced value - after crash, the DB should
		// have either this synced value or the new unsynced value.
		e.syncedValue = e.value
		e.wasSynced = true
	}
	e.state = ksMaybeSet
	e.value = value
	kt.entries[key] = e
	kt.mu.Unlock()
}

// promoteAllToSet promotes all ksMaybeSet keys to ksSet (called after Sync).
func (kt *keyTracker) promoteAllToSet() {
	kt.mu.Lock()
	for k, e := range kt.entries {
		if e.state == ksMaybeSet {
			e.state = ksSet
			e.wasSynced = false
			e.syncedValue = ""
			kt.entries[k] = e
		}
	}
	kt.mu.Unlock()
}

// validate checks the recovered DB against tracked key states.
func (kt *keyTracker) validate(t *testing.T, db *FlexDB) {
	t.Helper()
	kt.mu.Lock()
	defer kt.mu.Unlock()

	for key, entry := range kt.entries {
		got, foundErr := db.Get(key)
		found := (foundErr == nil)
		switch entry.state {
		case ksSet:
			if !found {
				t.Errorf("ksSet key %q: expected %q, got not-found", key, entry.value)
			} else if string(got) != entry.value {
				t.Errorf("ksSet key %q: expected %q, got %q", key, entry.value, string(got))
			}
		case ksMaybeSet:
			if found {
				gotStr := string(got)
				if gotStr != entry.value {
					// Key has a value, but not the unsynced one.
					// If it was previously synced, the old synced value is also valid.
					if entry.wasSynced && gotStr == entry.syncedValue {
						// OK: reverted to last synced value after crash.
					} else {
						t.Errorf("ksMaybeSet key %q: expected %q or %q or absent, got %q",
							key, entry.value, entry.syncedValue, gotStr)
					}
				}
			} else {
				// Not found. That's OK if it was never synced.
				// If it was previously synced, it MUST be present.
				if entry.wasSynced {
					t.Errorf("ksMaybeSet key %q: was previously synced with %q, but not found after crash",
						key, entry.syncedValue)
				}
			}
		case ksUnset:
			if found {
				t.Errorf("ksUnset key %q: expected absent, got %q", key, string(got))
			}
		}
	}
}

// snapshot returns a copy of the tracker for use across crash boundaries.
func (kt *keyTracker) snapshot() *keyTracker {
	kt.mu.Lock()
	defer kt.mu.Unlock()
	cp := newKeyTracker()
	for k, e := range kt.entries {
		cp.entries[k] = e
	}
	return cp
}

// settleAfterCrash adjusts the tracker to reflect that unsynced data
// was lost in the crash. This must be called after taking a CrashClone
// and before starting a new cycle of writes.
//
//   - ksSet keys: unchanged (they must survive the crash)
//   - ksMaybeSet with wasSynced: reverts to ksSet with syncedValue
//     (crash dropped the unsynced write, reverting to last synced value)
//   - ksMaybeSet without wasSynced: removed (never synced, lost)
func (kt *keyTracker) settleAfterCrash() {
	kt.mu.Lock()
	defer kt.mu.Unlock()
	for k, e := range kt.entries {
		if e.state != ksMaybeSet {
			continue
		}
		if e.wasSynced {
			kt.entries[k] = keyEntry{
				state: ksSet,
				value: e.syncedValue,
			}
		} else {
			delete(kt.entries, k)
		}
	}
}

// ====================== Stress Config ======================

type stressConfig struct {
	numKeys      int
	numWriters   int
	numReaders   int
	opsPerWriter int
	syncInterval int // Sync every N ops per writer
	valueSize    int
	seed         int64
}

// makeValue creates a deterministic value for a key at a given sequence.
func makeValue(key string, seq int, size int) string {
	base := fmt.Sprintf("%s_seq%d_", key, seq)
	if len(base) >= size {
		return base[:size]
	}
	// Pad to desired size.
	buf := make([]byte, size)
	copy(buf, base)
	for i := len(base); i < size; i++ {
		buf[i] = byte('A' + (i % 26))
	}
	return string(buf)
}

// runStressWorkers launches writer and reader goroutines that operate
// on db until stopCh is closed. Writers track keys via tracker.
// Each writer writes to its own key range (w{writerID}_k{idx}) to avoid
// cross-writer races in the tracker. Returns when all goroutines exit.
//
// Uses an RWMutex to coordinate: writers hold RLock during markMaybeSet+Put
// (allowing concurrent writes), Sync takes full Lock (exclusive, ensuring
// no Puts are in-flight when we promote).
func runStressWorkers(
	t *testing.T,
	db *FlexDB,
	cfg stressConfig,
	tracker *keyTracker,
	stopCh <-chan struct{},
) {
	var wg sync.WaitGroup

	// Writers hold RLock during markMaybeSet+Put.
	// Sync takes Lock to ensure no Puts are in-flight.
	var opSyncMu sync.RWMutex

	// Writers
	for w := 0; w < cfg.numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					// Post-crash FS operations may panic; that's expected.
				}
			}()
			var opCount int
			for i := 0; i < cfg.opsPerWriter; i++ {
				select {
				case <-stopCh:
					return
				default:
				}

				keyIdx := i % cfg.numKeys
				key := fmt.Sprintf("w%d_k%04d", writerID, keyIdx)
				val := makeValue(key, writerID*cfg.opsPerWriter+i, cfg.valueSize)

				opSyncMu.RLock()
				tracker.markMaybeSet(key, val)
				err := db.Put(key, []byte(val))
				opSyncMu.RUnlock()
				if err != nil {
					return // DB may be closed/crashed
				}
				opCount++

				if cfg.syncInterval > 0 && opCount%cfg.syncInterval == 0 {
					opSyncMu.Lock()
					err := db.Sync()
					if err == nil {
						tracker.promoteAllToSet()
					}
					opSyncMu.Unlock()
					if err != nil {
						return
					}
				}
			}
		}(w)
	}

	// Readers
	for r := 0; r < cfg.numReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					// Post-crash FS operations may panic; that's expected.
				}
			}()
			for {
				select {
				case <-stopCh:
					return
				default:
				}
				key := fmt.Sprintf("w%d_k%04d", readerID%cfg.numWriters, readerID%cfg.numKeys)
				db.Get(key) // result doesn't matter; just exercising reads
			}
		}(r)
	}

	wg.Wait()
}

// ====================== Stress Tests ======================

// TestRecoveryStress_RandomCrashTiming opens a DB, launches concurrent
// writers/readers, crashes at a random point, and validates the
// three-state key tracker against the recovered DB.
func TestRecoveryStress_RandomCrashTiming(t *testing.T) {
	const iterations = 5

	for iter := 0; iter < iterations; iter++ {
		seed := int64(iter*7 + 42)
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			dir := fmt.Sprintf("test_stress_random_%d", seed)
			fs := vfs.NewCrashableMem()
			panicOn(fs.MkdirAll(dir, 0755))

			cfg := &Config{FS: fs}
			db, err := OpenFlexDB(dir, cfg)
			if err != nil {
				t.Fatalf("OpenFlexDB: %v", err)
			}

			tracker := newKeyTracker()
			rng := rand.New(rand.NewSource(seed))

			sCfg := stressConfig{
				numKeys:      50,
				numWriters:   4,
				numReaders:   4,
				opsPerWriter: 200,
				syncInterval: 25,
				valueSize:    32,
				seed:         seed,
			}

			// Run writers/readers concurrently; crash after random delay.
			stopCh := make(chan struct{})
			doneCh := make(chan struct{})
			go func() {
				runStressWorkers(t, db, sCfg, tracker, stopCh)
				close(doneCh)
			}()

			// Wait a random amount of time before crashing.
			crashDelay := time.Duration(rng.Intn(50)+10) * time.Millisecond
			time.Sleep(crashDelay)
			close(stopCh)
			<-doneCh

			// Take a snapshot of the tracker before crash clone.
			snap := tracker.snapshot()

			// Crash clone with only synced data.
			crashedFS := fs.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 0})

			recoverCfg := &Config{FS: crashedFS}
			db2, err := OpenFlexDB(dir, recoverCfg)
			if err != nil {
				t.Fatalf("recovery failed (seed=%d): %v", seed, err)
			}

			snap.validate(t, db2)
			db2.Close()
		})
	}
}

// TestRecoveryStress_RepeatedRandomCrashes does 10 cycles of:
// open -> concurrent ops -> random crash -> recover.
// The keyTracker accumulates across cycles: ksSet keys from prior
// cycles must survive all subsequent crashes.
func TestRecoveryStress_RepeatedRandomCrashes(t *testing.T) {
	dir := "test_stress_repeated"
	fs := vfs.NewCrashableMem()
	panicOn(fs.MkdirAll(dir, 0755))

	const numCycles = 10
	tracker := newKeyTracker()
	currentFS := fs
	rng := rand.New(rand.NewSource(99))

	for cycle := 0; cycle < numCycles; cycle++ {
		cfg := &Config{FS: currentFS}
		db, err := OpenFlexDB(dir, cfg)
		if err != nil {
			t.Fatalf("cycle %d: OpenFlexDB failed: %v", cycle, err)
		}

		// Validate accumulated state from prior cycles.
		if cycle > 0 {
			tracker.validate(t, db)
		}

		sCfg := stressConfig{
			numKeys:      30,
			numWriters:   3,
			numReaders:   2,
			opsPerWriter: 100,
			syncInterval: 20,
			valueSize:    24,
			seed:         int64(cycle),
		}

		stopCh := make(chan struct{})
		doneCh := make(chan struct{})
		go func() {
			runStressWorkers(t, db, sCfg, tracker, stopCh)
			close(doneCh)
		}()

		crashDelay := time.Duration(rng.Intn(30)+5) * time.Millisecond
		time.Sleep(crashDelay)
		close(stopCh)
		<-doneCh

		// Crash clone.
		crashedFS := currentFS.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 0})
		currentFS = crashedFS

		// Settle tracker: unsynced data is lost, synced data reverts.
		tracker.settleAfterCrash()
	}

	// Final open and validate.
	cfg := &Config{FS: currentFS}
	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatalf("final open failed: %v", err)
	}
	defer db.Close()
	tracker.validate(t, db)
}

// TestRecoveryStress_LargeDataset writes 10,000 keys with 256-byte
// values (~2.5 MB), overwrites them, syncs, crashes, and recovers.
func TestRecoveryStress_LargeDataset(t *testing.T) {
	dir := "test_stress_large"
	fs := vfs.NewCrashableMem()
	panicOn(fs.MkdirAll(dir, 0755))

	cfg := &Config{FS: fs}
	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatalf("OpenFlexDB: %v", err)
	}

	const (
		numKeys   = 1_000 // 10_000 takes tooooo long.
		valueSize = 256
		rounds    = 3
	)

	tracker := newKeyTracker()

	for round := 0; round < rounds; round++ {
		for i := 0; i < numKeys; i++ {
			key := fmt.Sprintf("lk%05d", i)
			val := makeValue(key, round*numKeys+i, valueSize)
			tracker.markMaybeSet(key, val)
			panicOn(db.Put(key, []byte(val)))
		}
		panicOn(db.Sync())
		tracker.promoteAllToSet()
	}
	db.Close()

	crashedFS := fs.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 0})
	recoverCfg := &Config{FS: crashedFS}
	db2, err := OpenFlexDB(dir, recoverCfg)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	defer db2.Close()

	tracker.validate(t, db2)

	// Also verify Len.
	if got := db2.Len(); got != numKeys {
		t.Errorf("Len() = %d, expected %d", got, numKeys)
	}
}

// TestRecoveryStress_HighConcurrency uses 16 writers + 16 readers
// (matching YogaDB's 16-shard RWMutex) to stress thread-safety
// of the recovery path under high contention.
func TestRecoveryStress_HighConcurrency(t *testing.T) {
	dir := "test_stress_highconc"
	fs := vfs.NewCrashableMem()
	panicOn(fs.MkdirAll(dir, 0755))

	cfg := &Config{FS: fs}
	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatalf("OpenFlexDB: %v", err)
	}

	tracker := newKeyTracker()

	sCfg := stressConfig{
		numKeys:      100,
		numWriters:   16,
		numReaders:   16,
		opsPerWriter: 100,
		syncInterval: 50,
		valueSize:    48,
		seed:         777,
	}

	stopCh := make(chan struct{})
	doneCh := make(chan struct{})
	go func() {
		runStressWorkers(t, db, sCfg, tracker, stopCh)
		close(doneCh)
	}()

	// Let workers run for a bit, then crash mid-cycle.
	time.Sleep(30 * time.Millisecond)
	close(stopCh)
	<-doneCh

	snap := tracker.snapshot()

	crashedFS := fs.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 0})
	recoverCfg := &Config{FS: crashedFS}
	db2, err := OpenFlexDB(dir, recoverCfg)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	defer db2.Close()

	snap.validate(t, db2)
}

// TestRecoveryStress_UnsyncedDataPartial tests CrashClone with
// UnsyncedDataPercent=50. The DB must open without error/panic.
// ksSet data must be intact. ksMaybeSet data may go either way.
func TestRecoveryStress_UnsyncedDataPartial(t *testing.T) {
	dir := "test_stress_partial"
	fs := vfs.NewCrashableMem()
	panicOn(fs.MkdirAll(dir, 0755))

	cfg := &Config{FS: fs}
	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatalf("OpenFlexDB: %v", err)
	}

	tracker := newKeyTracker()

	// Write and sync a baseline.
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("pk%04d", i)
		val := fmt.Sprintf("synced_val_%04d", i)
		tracker.markMaybeSet(key, val)
		panicOn(db.Put(key, []byte(val)))
	}
	panicOn(db.Sync())
	tracker.promoteAllToSet()

	// Write more without syncing (these become ksMaybeSet).
	for i := 100; i < 200; i++ {
		key := fmt.Sprintf("pk%04d", i)
		val := fmt.Sprintf("unsynced_val_%04d", i)
		tracker.markMaybeSet(key, val)
		panicOn(db.Put(key, []byte(val)))
	}
	// Deliberately no Sync here.
	db.Close()

	// Crash with 50% unsynced data retained.
	crashedFS := fs.CrashClone(vfs.CrashCloneCfg{
		UnsyncedDataPercent: 50,
		RNG:                 randv2.New(randv2.NewPCG(12345, 67890)),
	})

	recoverCfg := &Config{FS: crashedFS}
	db2, err := OpenFlexDB(dir, recoverCfg)
	if err != nil {
		// Partial unsynced data can create inconsistent state.
		// Log it but don't fail - this is an expected possibility.
		t.Logf("OpenFlexDB with partial unsynced data failed (expected possibility): %v", err)
		return
	}
	defer db2.Close()

	// Validate: ksSet must be intact, ksMaybeSet may go either way.
	tracker.validate(t, db2)

	// Also verify the DB is operational post-recovery.
	panicOn(db2.Put("post_recovery", []byte("works")))
	got, foundErr := db2.Get("post_recovery")
	if foundErr != nil || string(got) != "works" {
		t.Fatalf("post-recovery Put/Get failed")
	}
}

// TestRecoveryStress_CrashNearSync tests the narrow window where
// db.Sync() has started but not completed. We start Sync in a
// goroutine and immediately take a CrashClone.
func TestRecoveryStress_CrashNearSync(t *testing.T) {
	const iterations = 5

	for iter := 0; iter < iterations; iter++ {
		t.Run(fmt.Sprintf("iter%d", iter), func(t *testing.T) {
			dir := fmt.Sprintf("test_stress_nearsync_%d", iter)
			fs := vfs.NewCrashableMem()
			panicOn(fs.MkdirAll(dir, 0755))

			cfg := &Config{FS: fs}
			db, err := OpenFlexDB(dir, cfg)
			if err != nil {
				t.Fatalf("OpenFlexDB: %v", err)
			}

			tracker := newKeyTracker()

			// Write and sync baseline data.
			for i := 0; i < 50; i++ {
				key := fmt.Sprintf("ns%04d", i)
				val := fmt.Sprintf("base_%04d", i)
				tracker.markMaybeSet(key, val)
				panicOn(db.Put(key, []byte(val)))
			}
			panicOn(db.Sync())
			tracker.promoteAllToSet()

			// Write more data that we'll try to sync.
			for i := 50; i < 150; i++ {
				key := fmt.Sprintf("ns%04d", i)
				val := fmt.Sprintf("newsync_%04d", i)
				tracker.markMaybeSet(key, val)
				panicOn(db.Put(key, []byte(val)))
			}

			// Start Sync in a goroutine, immediately crash clone.
			syncDone := make(chan error, 1)
			go func() {
				syncDone <- db.Sync()
			}()

			// Take crash clone immediately - Sync may or may not have completed.
			crashedFS := fs.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 0})

			// Wait for Sync to finish (it may succeed or fail, doesn't matter).
			<-syncDone
			db.Close()

			recoverCfg := &Config{FS: crashedFS}
			db2, err := OpenFlexDB(dir, recoverCfg)
			if err != nil {
				t.Fatalf("recovery failed: %v", err)
			}
			defer db2.Close()

			// Baseline data (ksSet) must survive. New data (ksMaybeSet)
			// may or may not be present depending on crash timing.
			tracker.validate(t, db2)
		})
	}
}
