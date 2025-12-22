package ko

import (
	"encoding/json"
	"errors"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/key"
	"github.com/benitogf/ooo/merge"
	"github.com/benitogf/ooo/meta"
	"github.com/syndtr/goleveldb/leveldb"
	errorsLeveldb "github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// Storage composition of Database interface
type Storage struct {
	Path            string
	mem             sync.Map
	memMutex        sync.Map
	noBroadcastKeys []string
	client          *leveldb.DB
	mutex           sync.RWMutex
	watcher         ooo.StorageChan
	storage         *ooo.Storage
	beforeRead      func(key string)
}

// Active provides access to the status of the storage client
func (db *Storage) Active() bool {
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	return db.storage.Active
}

func (db *Storage) recover() error {
	var err error
	db.client, err = leveldb.RecoverFile(db.Path, &opt.Options{})

	// recover failed
	if err != nil {
		return err
	}

	return nil
}

// Start the storage client
func (db *Storage) Start(storageOpt ooo.StorageOpt) error {
	var err error
	db.mutex.Lock()
	defer db.mutex.Unlock()
	if db.storage == nil {
		db.storage = &ooo.Storage{}
	}

	if db.Path == "" {
		db.Path = "data/db"
	}

	if db.watcher == nil {
		db.watcher = make(ooo.StorageChan)
	}

	db.client, err = leveldb.OpenFile(db.Path, &opt.Options{})

	if errorsLeveldb.IsCorrupted(err) {
		log.Println("db is corrupted, attempting recover", err)
		err = db.recover()
		if err != nil {
			log.Println("failed to recover db", err)
			return err
		}
	}

	// load db snapshot into db
	err = db.load()
	if err != nil {
		log.Println("failed to load db snapshot", err)
		return err
	}

	db.storage.Active = true
	db.noBroadcastKeys = storageOpt.NoBroadcastKeys
	db.beforeRead = storageOpt.BeforeRead
	return nil
}

func (db *Storage) load() error {
	iter := db.client.NewIterator(nil, &opt.ReadOptions{
		DontFillCache: true,
	})

	for iter.Next() {
		path := string(iter.Key())
		value := iter.Value()
		// Decode JSON to meta.Object and store pointer directly
		obj, err := meta.Decode(value)
		if err != nil {
			continue // Skip corrupted entries
		}
		obj.Path = path
		db.mem.Store(path, &obj)
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		return err
	}

	return nil
}

// Close the storage client
func (db *Storage) Close() {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	db.storage.Active = false
	db.client.Close()
	if db.watcher != nil {
		close(db.watcher)
		db.watcher = nil
	}
}

func (db *Storage) _getLock(path string) *sync.Mutex {
	newLock := sync.Mutex{}
	lock, _ := db.memMutex.LoadOrStore(path, &newLock)
	return lock.(*sync.Mutex)
}

func (db *Storage) _loadLock(path string) (*sync.Mutex, error) {
	lock, found := db.memMutex.Load(path)
	if !found {
		return nil, errors.New("ooo: lock not found can't unlock")
	}
	return lock.(*sync.Mutex), nil
}

// Clear all keys in the storage
func (db *Storage) Clear() {
	db.mem.Range(func(key interface{}, value interface{}) bool {
		db.mem.Delete(key)
		return true
	})
	iter := db.client.NewIterator(nil, nil)
	for iter.Next() {
		_ = db.client.Delete(iter.Key(), nil)
	}
	iter.Release()
}

// Keys list all the keys in the storage
func (db *Storage) Keys() ([]string, error) {
	keys := []string{}
	db.mem.Range(func(k interface{}, value interface{}) bool {
		keys = append(keys, k.(string))
		return true
	})

	sort.Slice(keys, func(i, j int) bool {
		return strings.ToLower(keys[i]) < strings.ToLower(keys[j])
	})

	return keys, nil
}

// KeysRange list keys in a path and time range
func (db *Storage) KeysRange(path string, from, to int64) ([]string, error) {
	keys := []string{}
	if !strings.Contains(path, "*") {
		return keys, ooo.ErrInvalidPath
	}

	if to < from {
		return keys, errors.New("katamari: invalid range")
	}

	db.mem.Range(func(k interface{}, value interface{}) bool {
		current := k.(string)
		if !key.Match(path, current) {
			return true
		}
		paths := strings.Split(current, "/")
		created := key.Decode(paths[len(paths)-1])
		if created < from || created > to {
			return true
		}
		keys = append(keys, current)
		return true
	})

	return keys, nil
}

// GetNRange get last N elements of a path related value(s) for a given time range
func (db *Storage) GetNRange(path string, limit int, from, to int64) ([]meta.Object, error) {
	if db.beforeRead != nil {
		db.beforeRead(path)
	}
	res := []meta.Object{}
	if !strings.Contains(path, "*") {
		return res, ooo.ErrInvalidPath
	}

	if limit <= 0 {
		return res, errors.New("katamari: invalid limit")
	}

	db.mem.Range(func(k interface{}, value interface{}) bool {
		if !key.Match(path, k.(string)) {
			return true
		}

		current := k.(string)
		paths := strings.Split(current, "/")
		created := key.Decode(paths[len(paths)-1])
		if created < from || created > to {
			return true
		}

		res = append(res, *value.(*meta.Object))
		return true
	})

	sort.Slice(res, meta.SortDesc(res))

	if len(res) > limit {
		return res[:limit], nil
	}

	return res, nil
}

func (db *Storage) getN(path string, limit int, order string) ([]meta.Object, error) {
	res := []meta.Object{}
	if !strings.Contains(path, "*") {
		return res, ooo.ErrInvalidPath
	}

	if limit <= 0 {
		return res, errors.New("katamari: invalid limit")
	}

	db.mem.Range(func(k interface{}, value interface{}) bool {
		if !key.Match(path, k.(string)) {
			return true
		}

		res = append(res, *value.(*meta.Object))
		return true
	})

	if order == "asc" {
		sort.Slice(res, meta.SortAsc(res))
	} else {
		sort.Slice(res, meta.SortDesc(res))
	}

	if len(res) > limit {
		return res[:limit], nil
	}

	return res, nil
}

// GetN get last N elements of a path related value(s)
func (db *Storage) GetN(path string, limit int) ([]meta.Object, error) {
	if db.beforeRead != nil {
		db.beforeRead(path)
	}
	return db.getN(path, limit, "desc")
}

func (db *Storage) GetNAscending(path string, limit int) ([]meta.Object, error) {
	if db.beforeRead != nil {
		db.beforeRead(path)
	}
	return db.getN(path, limit, "asc")
}

func (db *Storage) getList(path string, order string) ([]meta.Object, error) {
	res := []meta.Object{}
	if !strings.Contains(path, "*") {
		return res, ooo.ErrInvalidPath
	}

	db.mem.Range(func(k interface{}, value interface{}) bool {
		if !key.Match(path, k.(string)) {
			return true
		}

		res = append(res, *value.(*meta.Object))
		return true
	})

	if order == "desc" {
		sort.Slice(res, meta.SortDesc(res))
	} else {
		sort.Slice(res, meta.SortAsc(res))
	}

	return res, nil
}

// Get retrieves a single value by exact key (non-glob).
func (db *Storage) Get(path string) (meta.Object, error) {
	if db.beforeRead != nil {
		db.beforeRead(path)
	}
	if strings.Contains(path, "*") {
		return meta.Object{}, ooo.ErrGlobNotAllowed
	}
	data, found := db.mem.Load(path)
	if !found {
		return meta.Object{}, ooo.ErrNotFound
	}
	return *data.(*meta.Object), nil
}

// GetList retrieves list of values matching a glob pattern (ascending order).
func (db *Storage) GetList(path string) ([]meta.Object, error) {
	if db.beforeRead != nil {
		db.beforeRead(path)
	}
	return db.getList(path, "asc")
}

// GetListDescending retrieves list of values matching a glob pattern (descending order).
func (db *Storage) GetListDescending(path string) ([]meta.Object, error) {
	if db.beforeRead != nil {
		db.beforeRead(path)
	}
	return db.getList(path, "desc")
}

// GetAndLock retrieves a single value and locks the key mutex.
func (db *Storage) GetAndLock(path string) (meta.Object, error) {
	if strings.Contains(path, "*") {
		return meta.Object{}, ooo.ErrCantLockGlob
	}
	if db.beforeRead != nil {
		db.beforeRead(path)
	}
	lock := db._getLock(path)
	lock.Lock()
	data, found := db.mem.Load(path)
	if !found {
		lock.Unlock()
		return meta.Object{}, ooo.ErrNotFound
	}
	return *data.(*meta.Object), nil
}

func (db *Storage) SetAndUnlock(path string, data json.RawMessage) (string, error) {
	if strings.Contains(path, "*") {
		return "", errors.New("ooo: can't lock a glob pattern path")
	}
	lock, err := db._loadLock(path)
	if err != nil {
		return "", err
	}
	res, err := db.Set(path, data)
	lock.Unlock()
	return res, err
}

func (db *Storage) Unlock(path string) error {
	lock, found := db.memMutex.Load(path)
	if !found {
		return errors.New("ooo: lock not found can't unlock")
	}
	lock.(*sync.Mutex).Unlock()
	return nil
}

// Peek a value timestamps
func (db *Storage) Peek(path string, now int64) (int64, int64) {
	previous, found := db.mem.Load(path)
	if !found {
		return now, 0
	}

	return previous.(*meta.Object).Created, now
}

func (db *Storage) _set(path string, data json.RawMessage, now int64) (string, error) {
	index := key.LastIndex(path)
	created, updated := db.Peek(path, now)
	obj := &meta.Object{
		Created: created,
		Updated: updated,
		Index:   index,
		Path:    path,
		Data:    data,
	}

	db.mem.Store(path, obj)
	// Encode to JSON for leveldb persistence
	encoded := meta.New(obj)
	err := db.client.Put([]byte(path), encoded, nil)

	if err != nil {
		return "", err
	}

	return index, nil
}

// Push stores data under a new key generated from a glob pattern path.
// The path must end with /* (glob pattern). Returns the generated index.
func (db *Storage) Push(path string, data json.RawMessage) (string, error) {
	if !key.IsValid(path) {
		return "", ooo.ErrInvalidPath
	}
	if len(data) == 0 {
		return "", ooo.ErrInvalidStorageData
	}

	if !key.IsGlob(path) {
		return "", ooo.ErrGlobRequired
	}

	// Generate new key from glob pattern
	newPath := key.Build(path)
	index := key.LastIndex(newPath)
	now := time.Now().UTC().UnixNano()

	obj := &meta.Object{
		Created: now,
		Updated: now,
		Index:   index,
		Path:    newPath,
		Data:    data,
	}

	db.mem.Store(newPath, obj)
	// Encode to JSON for leveldb persistence
	encoded := meta.New(obj)
	err := db.client.Put([]byte(newPath), encoded, nil)
	if err != nil {
		return "", err
	}

	if !key.Contains(db.noBroadcastKeys, newPath) && db.Active() {
		db.watcher <- ooo.StorageEvent{Key: newPath, Operation: "set"}
	}

	return index, nil
}

// Set a value
func (db *Storage) Set(path string, data json.RawMessage) (string, error) {
	if !key.IsValid(path) {
		return path, ooo.ErrInvalidPath
	}
	if len(data) == 0 {
		return path, errors.New("ooo: invalid storage data (empty)")
	}
	now := time.Now().UTC().UnixNano()

	if !strings.Contains(path, "*") {
		index, err := db._set(path, data, now)
		if err != nil {
			return path, err
		}

		if !key.Contains(db.noBroadcastKeys, path) {
			db.watcher <- ooo.StorageEvent{Key: path, Operation: "set"}
		}

		return index, nil
	}

	keys := []string{}
	db.mem.Range(func(_key interface{}, value interface{}) bool {
		current := _key.(string)
		if !key.Match(path, current) {
			return true
		}
		keys = append(keys, current)
		return true
	})

	// batch set
	for _, key := range keys {
		_, err := db._set(key, data, now)
		if err != nil {
			return path, err
		}
	}

	if !key.Contains(db.noBroadcastKeys, path) && db.Active() {
		db.watcher <- ooo.StorageEvent{Key: path, Operation: "set"}
	}

	return path, nil
}

func (db *Storage) _patch(path string, data json.RawMessage, now int64) (string, error) {
	raw, found := db.mem.Load(path)
	if !found {
		return path, ooo.ErrNotFound
	}

	obj := raw.(*meta.Object)
	merged, info, err := merge.MergeBytes(obj.Data, data)
	if err != nil {
		return path, err
	}

	if len(info.Replaced) == 0 {
		return path, ooo.ErrNoop
	}

	index := key.LastIndex(path)
	created, updated := db.Peek(path, now)
	newObj := &meta.Object{
		Created: created,
		Updated: updated,
		Index:   index,
		Path:    path,
		Data:    merged,
	}

	db.mem.Store(path, newObj)
	// Encode to JSON for leveldb persistence
	encoded := meta.New(newObj)
	err = db.client.Put([]byte(path), encoded, nil)

	return path, err
}

// Set a value to matching keys
func (db *Storage) Patch(path string, data json.RawMessage) (string, error) {
	if !key.IsValid(path) {
		return path, ooo.ErrInvalidPath
	}
	if len(data) == 0 {
		return path, errors.New("ooo: invalid storage data (empty)")
	}

	now := time.Now().UTC().UnixNano()
	if !strings.Contains(path, "*") {
		index, err := db._patch(path, data, now)
		if err != nil {
			return path, err
		}

		if !key.Contains(db.noBroadcastKeys, path) && db.Active() {
			db.watcher <- ooo.StorageEvent{Key: path, Operation: "set"}
		}
		return index, nil
	}

	keys := []string{}
	db.mem.Range(func(_key interface{}, value interface{}) bool {
		current := _key.(string)
		if !key.Match(path, current) {
			return true
		}
		keys = append(keys, current)
		return true
	})

	// batch patch
	for _, key := range keys {
		_, err := db._patch(key, data, now)
		if err != nil {
			return path, err
		}
	}

	if !key.Contains(db.noBroadcastKeys, path) && db.Active() {
		db.watcher <- ooo.StorageEvent{Key: path, Operation: "set"}
	}

	return path, nil
}

// SetWithMeta set entries with metadata created/updated values
func (db *Storage) SetWithMeta(path string, data json.RawMessage, created int64, updated int64) (string, error) {
	index := key.LastIndex(path)
	obj := &meta.Object{
		Created: created,
		Updated: updated,
		Index:   index,
		Path:    path,
		Data:    data,
	}

	db.mem.Store(path, obj)
	// Encode to JSON for leveldb persistence
	encoded := meta.New(obj)
	err := db.client.Put([]byte(path), encoded, nil)

	if err != nil {
		return "", err
	}

	if !key.Contains(db.noBroadcastKeys, path) && db.Active() {
		db.watcher <- ooo.StorageEvent{Key: path, Operation: "set"}
	}

	return index, nil
}

// Del a key/pattern value(s)
func (db *Storage) Del(path string) error {
	var err error
	if !strings.Contains(path, "*") {
		_, found := db.mem.Load(path)
		if !found {
			return ooo.ErrNotFound
		}
		db.mem.Delete(path)

		_, err = db.client.Get([]byte(path), nil)
		if err != nil && err.Error() == "leveldb: not found" {
			return ooo.ErrNotFound
		}

		if err != nil {
			return err
		}

		err = db.client.Delete([]byte(path), nil)
		if err != nil {
			return err
		}

		if !key.Contains(db.noBroadcastKeys, path) {
			db.watcher <- ooo.StorageEvent{Key: path, Operation: "del"}
		}
		return nil
	}

	db.mem.Range(func(k interface{}, value interface{}) bool {
		if key.Match(path, k.(string)) {
			db.mem.Delete(k.(string))
		}
		return true
	})

	globPrefixKey := strings.Split(path, "*")[0]
	rangeKey := util.BytesPrefix([]byte(globPrefixKey + ""))
	if globPrefixKey == "" || globPrefixKey == "*" {
		rangeKey = nil
	}
	iter := db.client.NewIterator(rangeKey, nil)
	for iter.Next() {
		if key.Match(path, string(iter.Key())) {
			err = db.client.Delete(iter.Key(), nil)
			if err != nil {
				break
			}
		}
	}
	if err != nil {
		return err
	}
	iter.Release()
	err = iter.Error()
	if err != nil {
		return err
	}

	if !key.Contains(db.noBroadcastKeys, path) {
		db.watcher <- ooo.StorageEvent{Key: path, Operation: "del"}
	}
	return nil
}

// Watch the storage set/del events
func (db *Storage) Watch() ooo.StorageChan {
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	return db.watcher
}
