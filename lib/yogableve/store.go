package yogableve

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/blevesearch/bleve/v2/registry"
	store "github.com/blevesearch/upsidedown_store_api"
	"github.com/glycerine/yogadb"
)

const (
	// Name is the Bleve registry name for the YogaDB-backed upside_down store.
	Name = "yogadb"

	defaultNamespace = "default"
	keyRoot          = "yogableve:v1:"
)

var (
	ErrNilDB            = errors.New("yogableve: nil *yogadb.FlexDB")
	ErrClosed           = errors.New("yogableve: store is closed")
	ErrReadOnly         = errors.New("yogableve: store is read-only")
	ErrWrongBatch       = errors.New("yogableve: batch was not created by this store")
	ErrMergeUnsupported = errors.New("yogableve: merge called without a merge operator")
)

// Store implements Bleve's upside_down KVStore API on top of a FlexDB.
//
// Stores created with NewStore do not own the FlexDB lifecycle. Stores created
// through New, the Bleve registry constructor, open and close their own FlexDB.
type Store struct {
	db       *yogadb.FlexDB
	mo       store.MergeOperator
	ns       string
	prefix   string
	ownDB    bool
	readOnly bool

	mu     sync.Mutex
	closed bool
}

var _ store.KVStore = (*Store)(nil)
var _ store.KVStoreStats = (*Store)(nil)

// NewStore returns a Bleve upside_down KVStore backed by a caller-owned FlexDB.
func NewStore(db *yogadb.FlexDB, namespace string, mo store.MergeOperator) *Store {
	ns := normalizeNamespace(namespace)
	return &Store{
		db:     db,
		mo:     mo,
		ns:     ns,
		prefix: namespacePrefix(ns),
	}
}

// New is the Bleve registry constructor for the "yogadb" KV store.
func New(mo store.MergeOperator, config map[string]interface{}) (store.KVStore, error) {
	if config == nil {
		config = map[string]interface{}{}
	}

	path, ok := config["path"].(string)
	if !ok {
		return nil, fmt.Errorf("yogableve: must specify path")
	}

	cfg := &yogadb.Config{
		NoDisk:                 boolConfig(config, "noDisk", path == ""),
		DisableBackgroundFlush: boolConfig(config, "disableBackgroundFlush", false),
		OmitMemWalFsync:        boolConfig(config, "omitMemWalFsync", false),
	}
	if cacheMB, ok := uint64Config(config, "cacheMB"); ok {
		cfg.CacheMB = cacheMB
	}
	if cfg.NoDisk && path == "" {
		path = "yogableve-memory"
	}

	db, err := yogadb.OpenFlexDB(path, cfg)
	if err != nil {
		return nil, err
	}
	db.AllowReads()

	ns, _ := config["namespace"].(string)
	s := NewStore(db, ns, mo)
	s.ownDB = true
	s.readOnly = boolConfig(config, "read_only", false)
	return s, nil
}

// Writer returns a writer for atomic batch mutations.
func (s *Store) Writer() (store.KVWriter, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if s.readOnly {
		return nil, ErrReadOnly
	}
	return &Writer{s: s}, nil
}

// Reader returns an isolated reader. This initial adapter materializes the
// namespace into an in-memory snapshot so Bleve can keep reading while writers
// continue mutating the backing FlexDB.
func (s *Store) Reader() (store.KVReader, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if s.db == nil {
		return nil, ErrNilDB
	}
	r := &Reader{
		s:      s,
		values: make(map[string][]byte),
	}
	err := s.db.View(func(ro *yogadb.ReadOnlyTx) error {
		it := ro.NewIter()
		defer it.Close()
		for it.Seek(s.prefix); it.Valid(); it.Next() {
			dbKey, val, _, _, found, err := it.GetAnySize()
			if err != nil {
				return err
			}
			if !found {
				continue
			}
			raw, ok := s.rawKey(dbKey)
			if !ok {
				break
			}
			keyCopy := append([]byte(nil), raw...)
			valCopy := append([]byte(nil), val...)
			r.entries = append(r.entries, snapshotEntry{key: keyCopy, val: valCopy})
			r.values[string(keyCopy)] = valCopy
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return r, nil
}

// Close marks the store closed. Registry-created stores also close their FlexDB.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	ownDB := s.ownDB
	db := s.db
	s.mu.Unlock()

	if ownDB && db != nil {
		db.Close()
	}
	return nil
}

// Stats returns a JSON-serializable snapshot of store-level metrics.
func (s *Store) Stats() json.Marshaler {
	return statsJSON(s.StatsMap())
}

// StatsMap reports basic live-key and byte counts for this store namespace.
func (s *Store) StatsMap() map[string]interface{} {
	stats := map[string]interface{}{
		"name":      Name,
		"namespace": "",
		"items":     uint64(0),
		"bytes":     uint64(0),
	}
	if s == nil || s.db == nil {
		return stats
	}
	stats["namespace"] = s.ns

	err := s.db.View(func(ro *yogadb.ReadOnlyTx) error {
		it := ro.NewIter()
		defer it.Close()
		for it.Seek(s.prefix); it.Valid(); it.Next() {
			key := it.Key()
			if !strings.HasPrefix(key, s.prefix) {
				break
			}
			_, val, _, _, found, err := it.GetAnySize()
			if err != nil {
				return err
			}
			if found {
				stats["items"] = stats["items"].(uint64) + 1
				stats["bytes"] = stats["bytes"].(uint64) + uint64(len(val))
			}
		}
		return nil
	})
	if err != nil {
		stats["error"] = err.Error()
	}
	return stats
}

func (s *Store) checkOpen() error {
	if s == nil || s.db == nil {
		return ErrNilDB
	}
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return ErrClosed
	}
	return nil
}

func (s *Store) dbKey(key []byte) string {
	return s.prefix + string(key)
}

func (s *Store) rawKey(dbKey string) ([]byte, bool) {
	if !strings.HasPrefix(dbKey, s.prefix) {
		return nil, false
	}
	return []byte(dbKey[len(s.prefix):]), true
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

func boolConfig(config map[string]interface{}, name string, fallback bool) bool {
	v, ok := config[name]
	if !ok {
		return fallback
	}
	b, ok := v.(bool)
	if !ok {
		return fallback
	}
	return b
}

func uint64Config(config map[string]interface{}, name string) (uint64, bool) {
	switch v := config[name].(type) {
	case uint64:
		return v, true
	case uint:
		return uint64(v), true
	case int:
		if v >= 0 {
			return uint64(v), true
		}
	case int64:
		if v >= 0 {
			return uint64(v), true
		}
	case float64:
		if v >= 0 {
			return uint64(v), true
		}
	case json.Number:
		n, err := v.Int64()
		if err == nil && n >= 0 {
			return uint64(n), true
		}
	}
	return 0, false
}

type statsJSON map[string]interface{}

func (s statsJSON) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}(s))
}

func init() {
	err := registry.RegisterKVStore(Name, New)
	if err != nil {
		panic(err)
	}
}
