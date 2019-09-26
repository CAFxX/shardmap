package shardmap

import (
	"runtime"
	"sync"
	"unsafe"

	"github.com/cespare/xxhash"
	"github.com/tidwall/rhh"
)

// Map is a hashmap. Like map[string]interface{}, but sharded and thread-safe.
type Map struct {
	init   sync.Once
	cap    int
	shards int
	mask   uint64
	s      []paddedShard
}

type shard struct {
	sync.RWMutex
	rhh.Map
}

type paddedShard struct {
	shard
	_ [64-(unsafe.Sizeof(shard{})%64)]byte
}

// New returns a new hashmap with the specified capacity. This function is only
// needed when you must define a minimum capacity, otherwise just use:
//    var m shardmap.Map
func New(cap int) *Map {
	return &Map{cap: cap}
}

// Set assigns a value to a key.
// Returns the previous value, or false when no value was assigned.
func (m *Map) Set(key string, value interface{}) (prev interface{}, replaced bool) {
	m.initDo()
	shard := &m.s[m.choose(key)]
	shard.Lock()
	prev, replaced = shard.Set(key, value)
	shard.Unlock()
	return prev, replaced
}

// Get returns a value for a key.
// Returns false when no value has been assign for key.
func (m *Map) Get(key string) (value interface{}, ok bool) {
	m.initDo()
	shard := &m.s[m.choose(key)]
	shard.RLock()
	value, ok = shard.Get(key)
	shard.RUnlock()
	return value, ok
}

// Delete deletes a value for a key.
// Returns the deleted value, or false when no value was assigned.
func (m *Map) Delete(key string) (prev interface{}, deleted bool) {
	m.initDo()
	shard := &m.s[m.choose(key)]
	shard.Lock()
	prev, deleted = shard.Delete(key)
	shard.Unlock()
	return prev, deleted
}

// Len returns the number of values in map.
func (m *Map) Len() int {
	m.initDo()
	var len int
	for i := range m.s {
		shard := &m.s[i]
		shard.Lock()
		len += shard.Len()
		shard.Unlock()
	}
	return len
}

// Range iterates over all key/values.
// It's not safe to call or Set or Delete while ranging.
func (m *Map) Range(iter func(key string, value interface{}) bool) {
	m.initDo()
	var done bool
	for i := range m.s {
		func() {
			shard := &m.s[i]
			shard.RLock()
			defer shard.RUnlock()
			shard.Range(func(key string, value interface{}) bool {
				if !iter(key, value) {
					done = true
					return false
				}
				return true
			})
		}()
		if done {
			break
		}
	}
}

func (m *Map) choose(key string) int {
	return int(xxhash.Sum64String(key) & m.mask)
}

func (m *Map) initDo() {
	m.init.Do(func() {
		m.shards = 1
		for m.shards < runtime.NumCPU()*16 {
			m.shards *= 2
		}
		m.mask = uint64(m.shards - 1)
		scap := (m.cap + m.shards - 1) / m.shards
		m.s = make([]paddedShard, m.shards)
		for i := 0; i < len(m.s); i++ {
			m.s[i].Map = *rhh.New(scap)
		}
	})
}
