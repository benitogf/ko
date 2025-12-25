package ko

import (
	"sort"
	"strings"
	"sync"

	"github.com/benitogf/ooo/key"
	"github.com/benitogf/ooo/meta"
	"github.com/benitogf/ooo/storage"
	"github.com/syndtr/goleveldb/leveldb"
	errorsLeveldb "github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// EmbeddedStorage implements storage.EmbeddedLayer using LevelDB
// This is a pure persistence layer without in-memory caching
type EmbeddedStorage struct {
	Path   string
	client *leveldb.DB
	mutex  sync.RWMutex
	active bool
}

// NewEmbeddedStorage creates a new embedded storage
func NewEmbeddedStorage(path string) *EmbeddedStorage {
	return &EmbeddedStorage{
		Path: path,
	}
}

// Active returns whether the storage is active
func (e *EmbeddedStorage) Active() bool {
	e.mutex.RLock()
	defer e.mutex.RUnlock()
	return e.active
}

func (e *EmbeddedStorage) recover() error {
	var err error
	e.client, err = leveldb.RecoverFile(e.Path, &opt.Options{})
	return err
}

// Start initializes the embedded storage
func (e *EmbeddedStorage) Start(layerOpt storage.LayerOptions) error {
	var err error
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if e.Path == "" {
		e.Path = "data/db"
	}

	e.client, err = leveldb.OpenFile(e.Path, &opt.Options{})
	if errorsLeveldb.IsCorrupted(err) {
		err = e.recover()
		if err != nil {
			return err
		}
	}

	if err != nil {
		return err
	}

	e.active = true
	return nil
}

// Close shuts down the embedded storage
func (e *EmbeddedStorage) Close() {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	e.active = false
	if e.client != nil {
		e.client.Close()
	}
}

// Get retrieves a single value by exact key
func (e *EmbeddedStorage) Get(k string) (meta.Object, error) {
	data, err := e.client.Get([]byte(k), nil)
	if err != nil {
		if err.Error() == "leveldb: not found" {
			return meta.Object{}, storage.ErrNotFound
		}
		return meta.Object{}, err
	}

	obj, err := meta.Decode(data)
	if err != nil {
		return meta.Object{}, err
	}
	obj.Path = k

	return obj, nil
}

// GetList retrieves all values matching a glob pattern
func (e *EmbeddedStorage) GetList(path string) ([]meta.Object, error) {
	if !key.HasGlob(path) {
		return nil, storage.ErrInvalidPattern
	}

	res := []meta.Object{}

	// Use prefix scan for efficiency
	globPrefixKey := strings.Split(path, "*")[0]
	rangeKey := util.BytesPrefix([]byte(globPrefixKey))
	if globPrefixKey == "" || globPrefixKey == "*" {
		rangeKey = nil
	}

	iter := e.client.NewIterator(rangeKey, &opt.ReadOptions{
		DontFillCache: true,
	})
	defer iter.Release()

	for iter.Next() {
		k := string(iter.Key())
		if !key.Match(path, k) {
			continue
		}

		obj, err := meta.Decode(iter.Value())
		if err != nil {
			continue
		}
		obj.Path = k
		res = append(res, obj)
	}

	if err := iter.Error(); err != nil {
		return nil, err
	}

	return res, nil
}

// Set stores a value
func (e *EmbeddedStorage) Set(k string, obj *meta.Object) error {
	encoded := meta.New(obj)
	return e.client.Put([]byte(k), encoded, nil)
}

// Del deletes a key
func (e *EmbeddedStorage) Del(k string) error {
	if !key.HasGlob(k) {
		_, err := e.client.Get([]byte(k), nil)
		if err != nil {
			if err.Error() == "leveldb: not found" {
				return storage.ErrNotFound
			}
			return err
		}
		return e.client.Delete([]byte(k), nil)
	}

	// Glob delete
	globPrefixKey := strings.Split(k, "*")[0]
	rangeKey := util.BytesPrefix([]byte(globPrefixKey))
	if globPrefixKey == "" || globPrefixKey == "*" {
		rangeKey = nil
	}

	iter := e.client.NewIterator(rangeKey, nil)
	defer iter.Release()

	for iter.Next() {
		if key.Match(k, string(iter.Key())) {
			if err := e.client.Delete(iter.Key(), nil); err != nil {
				return err
			}
		}
	}

	return iter.Error()
}

// Keys returns all keys
func (e *EmbeddedStorage) Keys() ([]string, error) {
	keys := []string{}

	iter := e.client.NewIterator(nil, &opt.ReadOptions{
		DontFillCache: true,
	})
	defer iter.Release()

	for iter.Next() {
		keys = append(keys, string(iter.Key()))
	}

	if err := iter.Error(); err != nil {
		return nil, err
	}

	sort.Slice(keys, func(i, j int) bool {
		return strings.ToLower(keys[i]) < strings.ToLower(keys[j])
	})

	return keys, nil
}

// Clear removes all data
func (e *EmbeddedStorage) Clear() {
	iter := e.client.NewIterator(nil, nil)
	defer iter.Release()

	for iter.Next() {
		_ = e.client.Delete(iter.Key(), nil)
	}
}

// Load reads all data from persistent storage
func (e *EmbeddedStorage) Load() (map[string]*meta.Object, error) {
	data := make(map[string]*meta.Object)

	iter := e.client.NewIterator(nil, &opt.ReadOptions{
		DontFillCache: true,
	})
	defer iter.Release()

	for iter.Next() {
		k := string(iter.Key())
		obj, err := meta.Decode(iter.Value())
		if err != nil {
			continue
		}
		obj.Path = k
		data[k] = &obj
	}

	if err := iter.Error(); err != nil {
		return nil, err
	}

	return data, nil
}

// Verify EmbeddedStorage implements storage.EmbeddedLayer
var _ storage.EmbeddedLayer = (*EmbeddedStorage)(nil)
