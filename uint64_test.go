package yogadb

import (
	"testing"
)

func TestPutUint64(t *testing.T) {

	for _, want := range []uint64{0, 1, 2, 3, ^uint64(0)} {
		b := make([]byte, 8)
		putUint64(b, want)
		got := getUint64(b)
		if got != want {
			t.Errorf("expected %v, got %v", want, got)
		}
	}
}
