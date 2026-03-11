package yogadb

import (
	"bytes"
	"fmt"
)

func FirstDiff(dbA, dbB *FlexDB) string {

	nA := dbA.Len()
	nB := dbB.Len()
	smaller := min(nA, nB)

	if nA == 0 && nB == 0 {
		return "identical databases, but no keys in either."
	}
	if nA == 0 {
		return fmt.Sprintf("A has zero keys. B has %v keys.", nB)
	}
	if nB == 0 {
		return fmt.Sprintf("A has %v keys. B has zero keys.", nA)
	}

	roA := dbA.BeginView()
	defer roA.Close()
	roB := dbB.BeginView()
	defer roB.Close()

	itA := roA.NewIter()
	defer itA.Close()
	itA.SeekToFirst()

	itB := roB.NewIter()
	defer itB.Close()
	itB.SeekToFirst()

	var i int64
	for ; i < smaller; i++ {

		va := itA.Valid()
		vb := itB.Valid()

		switch {
		case !va && !vb:
			return fmt.Sprintf("no diff seen after %v keys of (nA: %v, nB: %v)", i, nA, nB)

		case va && !vb:
			return fmt.Sprintf("no more data in B('%v') at i = %v", dbB.Path, i)

		case !va && vb:
			return fmt.Sprintf("no more data in A('%v') at i = %v", dbA.Path, i)

		case va && vb:
			// both valid, compare them
			ka, va, foundA, errA := itA.GetAnySize()
			panicOn(errA)
			if !foundA {
				panicf("how can itA be valid but return not found?")
			}
			kb, vb, foundB, errB := itB.GetAnySize()
			panicOn(errB)
			if !foundB {
				panicf("how can itB be valid but return not found?")
			}
			if ka != kb {
				return fmt.Sprintf("key diff at i=%v, keyA='%v' but keyB='%v'", i, ka, kb)
			}
			cmp := bytes.Compare(va, vb)
			if cmp != 0 {
				return fmt.Sprintf("value diff at i=%v, keyA==keyB:'%v' but va=\n%v\n vb=\n%v\n", i, ka, string(va), string(vb))
			}
		}
		itA.Next()
		itB.Next()
	}
	return fmt.Sprintf("no diff seen after %v keys of (nA: %v, nB: %v)", i, nA, nB)
}
