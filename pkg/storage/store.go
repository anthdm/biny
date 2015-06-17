package storage

import (
	"bytes"
	"errors"
	"math/rand"
	"sync"
)

const (
	defaultCachSize int = 2048
	tableHeight         = 12
	tableOffset         = 4
	magicBytes          = 0xcafebabe
)

const (
	nKV = iota
	nKey
	nVal
	nHeight
	nNext
)

var (
	errKeyNotFound = errors.New("key not found")
	errMissingKey  = errors.New("missing key parameter")
)

// TODO
// for now config is just a placeholder for constructing NewStore()
type Config struct {
	Capacity int
	// ...
}

type store struct {
	txCount   int64
	rnd       *rand.Rand
	cacheSize int

	sync.RWMutex
	cache     []byte
	entries   []int
	prevEntry [tableHeight]int
	mHeight   int
}

func (s *store) Write(key, val []byte) error {
	if len(key) == 0 {
		return errMissingKey
	}
	s.Lock()
	defer s.Unlock()
	return s.writeLock(key, val)
}

func (s *store) Read(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errMissingKey
	}
	s.RLock()
	defer s.RUnlock()
	return s.readLock(key)
}

func (s *store) writeLock(key, value []byte) error {
	kLen := len(key)
	vLen := len(value)

	if entry, match := s.scanEntries(key, true); match {
		cacheSize := len(s.cache)
		s.cache = append(s.cache, key...)
		s.cache = append(s.cache, value...)
		s.entries[entry] = cacheSize
		m := s.entries[entry+nVal]
		s.entries[entry+nVal] = len(value)
		s.cacheSize += len(value) - m
		return nil
	}

	height := s.randomTableHeight()
	if height > s.mHeight {
		for i := s.mHeight; i < height; i++ {
			s.prevEntry[i] = 0
		}
		s.mHeight = height
	}

	size := len(s.cache)
	buf := make([]byte, kLen+vLen)
	copy(buf, key)
	copy(buf[kLen:], value)
	s.cache = append(s.cache, buf...)

	entry := len(s.entries)
	s.entries = append(s.entries, size, kLen, vLen, height)
	for i, n := range s.prevEntry[:height] {
		m := n + i + nNext
		s.entries = append(s.entries, s.entries[m])
		s.entries[m] = entry
	}
	s.cacheSize += kLen + vLen
	s.txCount++
	return nil
}

// scanEntries scans all the keys in the cache table til it finds a possible match
// it will return true if the keys are identical.
func (s *store) scanEntries(key []byte, prevEntry bool) (int, bool) {
	entry := 0
	h := s.mHeight - 1
	for {
		nextEntry := s.entries[entry+nNext+h]
		match := 1
		if nextEntry != 0 {
			o := s.entries[nextEntry]
			match = bytes.Compare(s.cache[o:o+s.entries[nextEntry+nKey]], key)
			//log.Println(string(s.cache[o : o+s.entries[nextEntry+nKey]]))
		}
		if match < 0 {
			entry = nextEntry
		} else {
			if prevEntry {
				s.prevEntry[h] = entry
			}
			if match == 0 {
				return nextEntry, true
			}
			if h == 0 {
				return nextEntry, match == 0
			}
			h--
		}
	}
}

// deleteLock deletes the value of the given key from the table with RLock holded
func (s *store) deleteLock(key []byte) error {
	s.Lock()
	defer s.Unlock()

	entry, match := s.scanEntries(key, true)
	if !match {
		return errKeyNotFound
	}
	height := s.entries[entry+nHeight]
	for i, n := range s.prevEntry[:height] {
		m := i + tableOffset + n
		s.entries[m] = s.entries[s.entries[m]+nNext+i]
	}
	s.cacheSize -= s.entries[entry+nKey] + s.entries[entry+nVal]
	s.txCount--
	return nil
}

func (s *store) readLock(key []byte) ([]byte, error) {
	entry, match := s.scanEntries(key, false)
	if match {
		offset := s.entries[entry] + s.entries[entry+nKey]
		return s.cache[offset : offset+s.entries[entry+nVal]], nil
	}
	return nil, errKeyNotFound
}

// has returns true if the given key exist in the table, false otherwise
func (s *store) has(key []byte) bool {
	s.RLock()
	defer s.RUnlock()
	_, exist := s.scanEntries(key, false)
	return exist
}

func (s *store) randomTableHeight() int {
	i := 1
	for i < tableHeight && s.rnd.Int()%4 == 0 {
		i++
	}
	return i
}

// Cap return the table available buffer
func (s *store) Available() int {
	s.RLock()
	defer s.RUnlock()
	return cap(s.cache) - len(s.cache)
}

// CacheSize return the total size of the cache
func (s *store) CacheSize() int {
	s.RLock()
	defer s.RUnlock()
	return s.cacheSize
}

// EntryCount returns the number of entries in the table
func (s *store) EntryCount() int64 {
	s.RLock()
	defer s.RUnlock()
	return s.txCount
}

// creates a new store with given capacity
func newStore(capacity int) *store {
	s := &store{
		rnd:     rand.New(rand.NewSource(magicBytes)),
		mHeight: 1,
		cache:   make([]byte, 0, capacity),
		entries: make([]int, 4+tableHeight),
	}
	s.entries[nHeight] = tableHeight
	return s
}

func NewStore(cfg *Config) *store {
	return newStore(cfg.Capacity)
}
