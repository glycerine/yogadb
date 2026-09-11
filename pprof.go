package yogadb

import (
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
)

func WriteMemProfiles(fn string) {
	if !strings.HasSuffix(fn, ".") {
		fn += "."
	}
	h, err := os.Create(fn + "heap")
	panicOn(err)
	defer h.Close()
	a, err := os.Create(fn + "allocs")
	panicOn(err)
	defer a.Close()
	//g, err := os.Create(fn + "goroutine")
	//panicOn(err)
	//defer g.Close()

	runtime.GC() // get up-to-date statistics

	hp := pprof.Lookup("heap")
	ap := pprof.Lookup("allocs")
	//gp := pprof.Lookup("goroutine")

	panicOn(hp.WriteTo(h, 0)) // 1=> text format, human readable. 0=>gzipped protobuf.
	panicOn(ap.WriteTo(a, 0)) // go tool pprof needs 0
	//panicOn(gp.WriteTo(g, 2)) // 2=> goroutine stacks printed in same form as SIGQUIT crash
}
