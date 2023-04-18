package store

import (
	"fmt"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

const cacheSize = 1000 // Adjust this value to set the maximum number of cache entries

type State int

const (
	StateEmpty State = iota
	StateRebuilding
	StateReady
)

type Store struct {
	data      map[string]interface{}
	hashData  map[string]map[string]interface{}
	cache     *lru.Cache[string, interface{}]
	mutex     sync.RWMutex
	hashMutex sync.RWMutex
	state     State
}

func NewStore() *Store {
	cache, err := lru.New[string, interface{}](cacheSize)
	if err != nil {
		panic(err)
	}

	store := &Store{
		data:     make(map[string]interface{}),
		hashData: make(map[string]map[string]interface{}),
		cache:    cache,
		state:    StateEmpty,
	}

	// Load data from the log file
	store.state = StateRebuilding
	if err := store.loadLog(); err != nil {
		panic(err)
	}
	store.state = StateReady

	// Set up periodic log compaction
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for range ticker.C {
			store.mutex.Lock()
			if err := store.compactLog(); err != nil {
				fmt.Printf("Error compacting log: %v\n", err)
			}
			store.mutex.Unlock()
		}
	}()

	return store
}

func (s *Store) Set(key, value string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.data[key] = value
	s.cache.Add(key, value)

	if s.state != StateRebuilding {
		// Append the operation to the log
		s.appendLogEntry("SET " + key + " " + fmt.Sprintf("%v", value))
	}
}

func (s *Store) Get(key string) (interface{}, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	// First, check the cache
	value, found := s.cache.Get(key)
	if found {
		return value, found
	}

	// If not found in cache, check the main data storage
	value, found = s.data[key]
	if found {
		// Update the cache with the found value
		s.cache.Add(key, value)
	}

	return value, found
}

func (s *Store) Del(key string) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	_, ok := s.data[key]
	if ok {
		delete(s.data, key)
		s.cache.Remove(key)

		if s.state != StateRebuilding {
			// Append the operation to the log
			s.appendLogEntry("DEL " + key)
		}
	}
	return ok
}

// HSet adds or updates a field-value pair in a hash stored at key
func (s *Store) HSet(key, field string, value interface{}) {
	s.hashMutex.Lock()
	defer s.hashMutex.Unlock()

	data, ok := s.hashData[key]
	if !ok {
		data = make(map[string]interface{})
		s.hashData[key] = data
	}
	data[field] = value

	if s.state != StateRebuilding {
		// Append the operation to the log
		s.appendLogEntry("HSET " + key + " " + fmt.Sprintf("%v %v", field, value))
	}
}
