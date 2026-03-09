package yogadb

import (
	//"context"
	"fmt"
	//"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glycerine/idem"
	porc "github.com/glycerine/porcupine"
	"github.com/glycerine/vfs"
)

func Test016_linz(t *testing.T) {

	defer func() {
		vv("test 016 wrapping up.")
	}()

	var int64seed int64

	// number of steps to take.
	nSteps := 10

	numReaders := 3
	numWriters := 3
	_ = numWriters

	var ops []porc.Operation
	var opsMut sync.Mutex
	done := make(chan bool)
	wgcount := int64(numReaders * nSteps)

	dbPath := "linz016.db"
	cfg := &Config{
		//OmitFlexSpaceOpsRedoLog: true,
		//OmitMemWalFsync: true, // would be dangerous here, eh?
	}
	db, err := OpenFlexDB(dbPath, cfg)
	panicOn(err)

	fin := make(map[string]bool)
	var finmut sync.Mutex

	readers := newFuzzUsers(t, db, numReaders, int64seed)

	// establish a point for all writes to initially end,
	// that is a ways out, so we can add to ops before
	// it actually finishes. Trying to resolve the race
	// when readers get the new value before we've added
	// it to the ops log.
	farout := time.Now().Add(time.Hour)

	workerFunc := func(i, jnode int, endtmWrite time.Time) {
		defer func() {
			res := atomic.AddInt64(&wgcount, -1)
			if res == 0 {
				close(done)
			}
		}()

		// select a key
		//ikey := rnd(5)
		//skey := fmt.Sprintf("%v", ikey)

		skey := "a" // start simple
		//vv("i=%v, jnode=%v, about to Read", i, jnode)

		begtmRead := time.Now()
		val, found := readers[jnode].db.Get(skey)
		endtmRead := time.Now()

		finmut.Lock()
		delete(fin, fmt.Sprintf("i=%v:%v node", i, jnode))
		finmut.Unlock()

		//vv("i=%v, jnode=%v, back from Read", i, jnode)
		var i2 int
		var err error
		if found {
			i2, err = strconv.Atoi(string(val))
			panicOn(err)
		}

		opsMut.Lock()
		ops = append(ops, porc.Operation{
			ClientId: jnode,
			Input:    registerInput{op: REGISTER_GET},
			Call:     begtmRead.UnixNano(), // invocation timestamp
			Output:   i2,
			Return:   endtmRead.UnixNano(), // response timestamp
		})

		// grab a snapshot of the ops until now to check online,
		// so we don't go very far past any non-linz point.
		oview := append([]porc.Operation{}, ops...)

		opsMut.Unlock()

		linz := porc.CheckOperations(registerModel, oview)
		if !linz {
			writeToDiskNonLinz(t, oview)
			t.Fatalf("error: expected operations to be linearizable! ops='%v'", opsSlice(ops))
		}

		vv("jnode=%v, i=%v passed linearizability checker", jnode, i)
	} // end func workerFunc

	//wg.Add(n * N)
	for i := range nSteps {
		for j := range numReaders {
			fin[fmt.Sprintf("i=%v:%v node", i, j)] = true
		}
	}

	for i := range nSteps {

		begtmWrite := time.Now()

		opsMut.Lock()
		pos := len(ops)
		ops = append(ops, porc.Operation{
			ClientId: 0,
			Input:    registerInput{op: REGISTER_PUT, value: i},
			Call:     begtmWrite.UnixNano(), // invocation timestamp
			Output:   i,
			// farout should be far enough out that
			// all reads should overlap it.
			Return: farout.UnixNano(),

			// later update to this when we have it:
			//Return:   endtmWrite.UnixNano(), // response timestamp
		})
		opsMut.Unlock()

		v := []byte(fmt.Sprintf("%v", i))
		vv("about to write at i=%v: '%v'", i, string(v))

		// WRITE

		// pick a writer, key, and value.
		w := 0
		key := "a"
		err := readers[w].db.Put(key, v)
		panicOn(err)
		err = readers[w].db.Sync()
		panicOn(err)
		vv("back from db.Sync after Put of key '%v' -> '%v' value", key, string(v))

		endtmWrite := time.Now()

		opsMut.Lock()
		ops[pos].Return = endtmWrite.UnixNano() // response timestamp
		opsMut.Unlock()

		for jnode := range numReaders {
			go workerFunc(i, jnode, endtmWrite)
		}
	}

	vv("016 end of i loop")

	select {
	case <-done:
	case <-time.After(time.Second):

		finmut.Lock()
		notfin := len(fin)
		vv("not finished: %v '%#v'", notfin, fin)
		finmut.Unlock()

		if notfin > 0 {
			vv("gonna panic bc not finished: %v '%#v'", notfin, fin)
			panic("where is lost read?")
		}
	}
	vv("016 good: nothing Waiting")

	opsMut.Lock()
	defer opsMut.Unlock()

	readers[0].checkLinz(ops)
}

func opsViewUpTo(ops []porc.Operation, endtmWrite int64) (r []porc.Operation) {
	for i := range ops {
		if ops[i].Return <= endtmWrite {
			r = append(r, ops[i])
		}
	}
	return
}

type registerOp int

const porcTombstoneValue = "<deletion-tombstone>"

const (
	REGISTER_UNK registerOp = 0
	REGISTER_PUT registerOp = 1 // put a porcTombstoneValue to delete.
	REGISTER_GET registerOp = 2
)

func (o registerOp) String() string {
	switch o {
	case REGISTER_PUT:
		return "REGISTER_PUT"
	case REGISTER_GET:
		return "REGISTER_GET"
	}
	panic(fmt.Sprintf("unknown registerOp: %v", int(o)))
}

type registerInput struct {
	op    registerOp // false = put, true = get
	value int
}

func (ri registerInput) String() string {
	if ri.op == REGISTER_GET {
		return fmt.Sprintf("registerInput{op: %v}", ri.op)
	}
	return fmt.Sprintf("registerInput{op: %v, value: %v}", ri.op, ri.value)
}

type opsSlice []porc.Operation

func opstring(e porc.Operation) string {
	return fmt.Sprintf(`porc.Operation{
    ClientId: %v,
       Input: %v,
      Output: %v,
      Call: %v  (%v),
    Return: %v  (%v),
}`, e.ClientId, e.Input, e.Output,
		niceu(e.Call), e.Call,
		niceu(e.Return), e.Return)
}

func niceu(u int64) string {
	return nice(time.Unix(0, u))
}

func (s opsSlice) String() (r string) {
	r = "opsSlice{\n"
	for i, e := range s {
		r += fmt.Sprintf("%02d: %v,\n", i, opstring(e))
	}
	r += "}"
	return
}

// a sequential specification of a register
var registerModel = porc.Model{
	Init: func() interface{} {
		return 0
	},
	// step function: takes a state, input, and output, and returns whether it
	// was a legal operation, along with a new state
	Step: func(state, input, output interface{}) (legal bool, newState interface{}) {
		regInput := input.(registerInput)

		switch regInput.op {
		case REGISTER_PUT:
			legal = true // always ok to execute a put
			newState = regInput.value

		case REGISTER_GET:
			newState = state // state is unchanged by GET

			if output == state {
				legal = true
			}
		}
		return
	},
	DescribeOperation: func(input, output interface{}) string {
		inp := input.(registerInput)
		switch inp.op {
		case REGISTER_GET:
			return fmt.Sprintf("get() -> '%d'", output.(int))
		case REGISTER_PUT:
			return fmt.Sprintf("put('%d')", inp.value)
		}
		panic(fmt.Sprintf("invalid inp.op! '%v'", int(inp.op)))
		return "<invalid>" // unreachable
	},
}

// user tries to get read/write work done.
type fuzzUser struct {
	t      *testing.T
	seed   int64
	name   string
	userid int

	rnd func(nChoices int64) (r int64)

	db   *FlexDB
	halt *idem.Halter
}

func newFuzzUsers(t *testing.T, db *FlexDB, numUsers int, int64seed int64) (users []*fuzzUser) {

	seedBytes := int64SeedToBytes(int64seed)
	rng := newPRNG(seedBytes)
	rnd := rng.pseudoRandNonNegInt64Range

	for userNum := 0; userNum < numUsers; userNum++ {
		user := &fuzzUser{
			db:     db,
			t:      t,
			seed:   int64seed,
			name:   fmt.Sprintf("user%v", userNum),
			userid: userNum,
			rnd:    rnd,
			halt:   idem.NewHalterNamed(fmt.Sprintf("fuzzUser_%v", userNum)),
		}
		users = append(users, user)
	}
	return
}

func (s *fuzzUser) checkLinz(ops []porc.Operation) error {

	/* how to construct:
	op := porc.Operation{
		ClientId: e.ClientId,
		Input:    e.Value,
		Call:     e.Ts,
		Output:   ret.Value,
		Return:   ret.Ts,
	}
	ops = append(ops, op)
	*/

	//vv("linzCheck: about to porc.CheckEvents on %v evs", len(evs))
	linz := porc.CheckOperations(registerModel, ops)
	if !linz {

		alwaysPrintf("error: user %v: expected operations to be linearizable! seed='%v'; ops='%v'", s.name, s.seed, opsSlice(ops))

		writeToDiskNonLinz(s.t, ops)
		return fmt.Errorf("error: user %v: expected operations to be linearizable! seed='%v'", s.name, s.seed)
	}

	//vv("user %v: len(ops)=%v passed linearizability checker.", s.name, len(ops))

	// nothing much to see really.
	//writeToDiskOkOperations(s.t, s.name, ops)
	return nil
}

func writeToDiskNonLinz(t *testing.T, ops []porc.Operation) {

	res, info := porc.CheckOperationsVerbose(registerModel, ops, 0)
	if res != porc.Illegal {
		t.Fatalf("expected output %v, got output %v", porc.Illegal, res)
	}
	nm := fmt.Sprintf("red.nonlinz.%v.%03d.html", t.Name(), 0)
	for i := 1; fileExists(vfs.Default, nm) && i < 1000; i++ {
		nm = fmt.Sprintf("red.nonlinz.%v.%03d.html", t.Name(), i)
	}
	vv("writing out non-linearizable ops history '%v'", nm)
	fd, err := vfs.Default.Create(nm, vfs.WriteCategoryUnspecified)
	panicOn(err)
	defer fd.Close()

	err = porc.Visualize(registerModel, info, fd)
	if err != nil {
		t.Fatalf("ops visualization failed")
	}
	t.Logf("wrote ops visualization to %s", fd.Name())
}

// only sets up to the first 8 bytes.
func int64SeedToBytes(int64seed int64) (b [32]byte) {
	n := uint64(int64seed)
	// not-sure-what-endian fill, but it works with monotone increase check.
	for i := range 8 {
		// this fill order is needed so that the
		// check in Reseed for monotone increasing
		// succeeds.
		b[i] = byte(n >> ((7 - i) * 8))
	}
	return
}

func writeToDiskOkOperations(fs vfs.FS, t *testing.T, user string, ops []porc.Operation) {

	res, info := porc.CheckOperationsVerbose(registerModel, ops, 0)
	if res == porc.Illegal {
		t.Fatalf("expected output %v, got output %v", porc.Ok, res)
	}
	nm := fmt.Sprintf("green.linz.%v.%03d.user_%v.html", t.Name(), 0, user)
	for i := 1; fileExists(vfs.Default, nm) && i < 1000; i++ {
		nm = fmt.Sprintf("green.linz.%v.%03d.user_%v.html", t.Name(), i, user)
	}
	vv("writing out linearizable ops history '%v', len %v", nm, len(ops))
	fd, err := vfs.Default.Create(nm, vfs.WriteCategoryUnspecified)
	panicOn(err)
	defer fd.Close()

	err = porc.Visualize(registerModel, info, fd)
	if err != nil {
		t.Fatalf("ops visualization failed")
	}
	t.Logf("wrote ops visualization to %s", fd.Name())
}
