package yogadb

import (
	"bytes"
	"sort"
)

// keyStable is a place to keep your keys
// when you think of them like horses.
//
// INVAR: if i is the index into stable for a given key (the i-th key we added):
// key i=0 is stored in s.keys[0:s.stable[i]]
// key i>0 is stored in s.keys[s.stable[i-1], s.stable[i]]
type keyStable struct {
	keys   []byte // arena with a copy of all keys, stacked end to end.
	stable []int  // stable store, this never changes, is only appeneded to. says where to find the string
	sorted []int  // sorted indexes of stable in ascending key order.
	tomb   []int  // index in stable of all deleted keys
}

func newKeyStable(n int) *keyStable {
	return &keyStable{
		keys:   make([]byte, 0, 8<<20),
		stable: make([]int, 0, 256<<10),
		sorted: make([]int, 0, 256<<10),
	}
}

func (s *keyStable) clear() {
	s.keys = s.keys[:0]
	s.stable = s.stable[:0]
	s.sorted = s.sorted[:0]
	s.tomb = s.tomb[:0]
}

func (s *keyStable) addKey(key []byte) (whereInStable int) {

	// INVAR: string i=0 is stored in s.keys[0:s.stable[i]]
	//        string i>0 is stored in s.keys[s.stable[i-1], s.stable[i]]

	// already present?
	w, found := s.findKey(key)
	if found {
		return s.sorted[w]
	}
	// not present, add it
	whereInStable = len(s.stable)
	s.stable = append(s.stable, len(s.keys)+len(key))
	s.sorted = append(s.sorted, whereInStable)
	s.keys = append(s.keys, key...)
	if whereInStable > 0 {
		sort.Sort(s)
	}
	return
}

func (s *keyStable) Less(i, j int) bool {
	if i == j {
		return false
	}
	ib := s.at(i)
	jb := s.at(j)
	return bytes.Compare(ib, jb) < 0
}

func (s *keyStable) at(i int) []byte {
	if i == 0 {
		return s.keys[:s.stable[0]]
	}
	return s.keys[s.stable[i-1]:s.stable[i]]
}

func (s *keyStable) Swap(i, j int) {
	s.sorted[i], s.sorted[j] = s.sorted[j], s.sorted[i]
}

func (s *keyStable) Len() int {
	return len(s.sorted)
}

func (s *keyStable) findKey(needle []byte) (where int, found bool) {
	return sort.Find(len(s.stable), func(i int) int {
		return bytes.Compare(needle, s.at(i))
	})
}

func (s *keyStable) delKey(needle []byte) (found bool) {
	var w int // where the needle lives in s.sorted
	w, found = s.findKey(needle)
	if !found {
		return
	}
	s.tomb = append(s.tomb, s.sorted[w])
	sort.Sort((*stableTombType)(s))

	last := len(s.sorted) - 1
	if w < last {
		copy(s.sorted[w:], s.sorted[w+1:])
	}
	s.sorted = s.sorted[:last]
	return
}

type stableTombType keyStable

func (s *stableTombType) Less(i, j int) bool {
	if i == j {
		return false
	}
	ib := ((*keyStable)(s)).at(s.tomb[i])
	jb := ((*keyStable)(s)).at(s.tomb[j])
	return bytes.Compare(ib, jb) < 0
}

func (s *stableTombType) Swap(i, j int) {
	s.tomb[i], s.tomb[j] = s.tomb[j], s.tomb[i]
}

func (s *stableTombType) Len() int {
	return len(s.tomb)
}
