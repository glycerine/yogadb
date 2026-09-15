package yogabluge

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"

	bluge "github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/index"
	segment "github.com/blugelabs/bluge_segment_api"
	"github.com/glycerine/yogadb"
)

const (
	defaultNamespace = "default"
	keyRoot          = "bluge:v1:"
	idWidth          = 16
)

var (
	// ErrReadOnly is returned when Bluge attempts to mutate a directory opened
	// for reader-only use.
	ErrReadOnly = errors.New("yogabluge: directory is read-only")

	// ErrNilDB is returned by Setup when the directory has no backing FlexDB.
	ErrNilDB = errors.New("yogabluge: nil *yogadb.FlexDB")

	processLocks sync.Map
)

// Directory stores Bluge segment and snapshot items in a YogaDB FlexDB.
//
// The FlexDB is client-owned: Directory does not open, AllowReads, Sync on
// setup, or Close it. Callers should open the FlexDB and call AllowReads before
// giving it to Bluge.
type Directory struct {
	db     *yogadb.FlexDB
	ns     string
	prefix string

	lock *sync.Mutex

	mu       sync.Mutex
	readOnly bool
	locked   bool
}

var _ index.Directory = (*Directory)(nil)

// NewDirectory returns a Bluge directory backed by db and isolated by namespace.
func NewDirectory(db *yogadb.FlexDB, namespace string) *Directory {
	ns := normalizeNamespace(namespace)
	d := &Directory{
		db:     db,
		ns:     ns,
		prefix: namespacePrefix(ns),
	}
	d.lock = sharedProcessLock(db, ns)
	return d
}

// NewConfig returns a Bluge config whose directory factory stores items in db.
func NewConfig(db *yogadb.FlexDB, namespace string) bluge.Config {
	ns := normalizeNamespace(namespace)
	return bluge.DefaultConfigWithDirectory(func() index.Directory {
		return NewDirectory(db, ns)
	})
}

// Setup records the requested mode. The backing FlexDB must already be open and
// readable; Directory deliberately leaves FlexDB lifecycle ownership to callers.
func (d *Directory) Setup(readOnly bool) error {
	if d.db == nil {
		return ErrNilDB
	}
	d.mu.Lock()
	d.readOnly = readOnly
	d.mu.Unlock()
	return nil
}

// List returns all ids of the requested Bluge item kind in descending order.
func (d *Directory) List(kind string) ([]uint64, error) {
	if err := d.ready(); err != nil {
		return nil, err
	}
	prefix := d.kindPrefix(kind)
	ids := make([]uint64, 0)
	err := d.db.View(func(ro *yogadb.ReadOnlyTx) error {
		it := ro.NewIter()
		defer it.Close()

		for it.Seek(prefix); it.Valid(); it.Next() {
			key := it.Key()
			if !strings.HasPrefix(key, prefix) {
				break
			}
			suffix := key[len(prefix):]
			if len(suffix) != idWidth {
				return fmt.Errorf("yogabluge: malformed item key %q", key)
			}
			id, err := strconv.ParseUint(suffix, 16, 64)
			if err != nil {
				return fmt.Errorf("yogabluge: malformed item id in key %q: %w", key, err)
			}
			ids = append(ids, id)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })
	return ids, nil
}

// Load returns the bytes for a Bluge item.
func (d *Directory) Load(kind string, id uint64) (*segment.Data, io.Closer, error) {
	if err := d.ready(); err != nil {
		return nil, nil, err
	}
	value, found, _, _, err := d.db.Get(d.itemKey(kind, id))
	if err != nil {
		return nil, nil, err
	}
	if !found {
		return nil, nil, fmt.Errorf("yogabluge: %s item %016x not found", kind, id)
	}
	return segment.NewDataBytes(value), nil, nil
}

// Persist stores or replaces a Bluge item.
func (d *Directory) Persist(kind string, id uint64, w index.WriterTo, closeCh chan struct{}) error {
	if err := d.readyWritable(); err != nil {
		return err
	}
	if err := checkClosed(closeCh); err != nil {
		return err
	}

	var buf bytes.Buffer
	if _, err := w.WriteTo(&buf, closeCh); err != nil {
		return err
	}
	if err := checkClosed(closeCh); err != nil {
		return err
	}

	key := d.itemKey(kind, id)
	value := append([]byte(nil), buf.Bytes()...)
	return d.db.Update(func(rw *yogadb.WriteTx) error {
		_, err := rw.Put(key, value, 0)
		return err
	})
}

// Remove marks a Bluge item absent in YogaDB.
func (d *Directory) Remove(kind string, id uint64) error {
	if err := d.readyWritable(); err != nil {
		return err
	}
	return d.db.Update(func(rw *yogadb.WriteTx) error {
		return rw.Delete(d.itemKey(kind, id))
	})
}

// Stats returns the number of live Bluge items and their total value bytes.
func (d *Directory) Stats() (numItems uint64, numBytes uint64) {
	if d == nil || d.db == nil {
		return 0, 0
	}
	_ = d.db.View(func(ro *yogadb.ReadOnlyTx) error {
		it := ro.NewIter()
		defer it.Close()

		for it.Seek(d.prefix); it.Valid(); it.Next() {
			key := it.Key()
			if !strings.HasPrefix(key, d.prefix) {
				break
			}
			value, found, _, _, err := ro.Get(key)
			if err != nil {
				return err
			}
			if found {
				numItems++
				numBytes += uint64(len(value))
			}
		}
		return nil
	})
	return numItems, numBytes
}

// Sync flushes the backing FlexDB.
func (d *Directory) Sync() error {
	if err := d.ready(); err != nil {
		return err
	}
	return d.db.Sync()
}

// Lock enforces Bluge's single-writer rule inside this Go process.
func (d *Directory) Lock() error {
	if err := d.readyWritable(); err != nil {
		return err
	}
	d.lock.Lock()
	d.mu.Lock()
	d.locked = true
	d.mu.Unlock()
	return nil
}

// Unlock releases the in-process writer lock.
func (d *Directory) Unlock() error {
	if err := d.ready(); err != nil {
		return err
	}
	d.mu.Lock()
	locked := d.locked
	d.locked = false
	d.mu.Unlock()
	if locked {
		d.lock.Unlock()
	}
	return nil
}

func (d *Directory) ready() error {
	if d == nil || d.db == nil {
		return ErrNilDB
	}
	return nil
}

func (d *Directory) readyWritable() error {
	if err := d.ready(); err != nil {
		return err
	}
	d.mu.Lock()
	readOnly := d.readOnly
	d.mu.Unlock()
	if readOnly {
		return ErrReadOnly
	}
	return nil
}

func (d *Directory) kindPrefix(kind string) string {
	return d.prefix + hex.EncodeToString([]byte(kind)) + ":"
}

func (d *Directory) itemKey(kind string, id uint64) string {
	return d.kindPrefix(kind) + fmt.Sprintf("%016x", id)
}

func normalizeNamespace(namespace string) string {
	if namespace == "" {
		return defaultNamespace
	}
	return namespace
}

func namespacePrefix(namespace string) string {
	return keyRoot + hex.EncodeToString([]byte(namespace)) + ":"
}

func sharedProcessLock(db *yogadb.FlexDB, namespace string) *sync.Mutex {
	key := lockKey{db: db, namespace: namespace}
	actual, _ := processLocks.LoadOrStore(key, &sync.Mutex{})
	return actual.(*sync.Mutex)
}

type lockKey struct {
	db        *yogadb.FlexDB
	namespace string
}

func checkClosed(closeCh chan struct{}) error {
	if closeCh == nil {
		return nil
	}
	select {
	case <-closeCh:
		return segment.ErrClosed
	default:
		return nil
	}
}
