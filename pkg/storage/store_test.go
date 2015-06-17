package storage

import (
	"bytes"
	"testing"
)

func TestCacheSize(t *testing.T) {
	s := newStore(0)
	if err := s.Write([]byte("a"), []byte("b")); err != nil {
		t.Fatal(err)
	}
	if s.CacheSize() != 2 {
		t.Fatalf("expected cachSize %d got %d", 2, s.CacheSize())
	}
	if err := s.Write([]byte("f"), []byte("i")); err != nil {
		t.Fatal(err)
	}
	if s.CacheSize() != 4 {
		t.Fatalf("expected cachSize %d got %d", 4, s.CacheSize())
	}
	if err := s.deleteLock([]byte("f")); err != nil {
		t.Fatal(err)
	}
	// len(s.cache) is still 4 but in the transaction perspective 2
	if s.CacheSize() != 2 {
		t.Fatalf("expected cachSize %d got %d", 2, s.CacheSize())
	}
}

func TestWriteReadLock(t *testing.T) {
	keyCount := 100
	s := newStore(0)
	keys := generateKeys(keyCount)
	for i := 0; i < len(keys); i++ {
		value := generateValue(32)
		key := keys[i%len(keys)]
		if err := s.writeLock(key, value); err != nil {
			t.Fatal(err)
		}
		res, err := s.readLock(key)
		if err != nil {
			t.Fatal(err)
		}
		equalBytes(t, value, res)
	}
	if s.EntryCount() != int64(keyCount) {
		t.Fatalf("expexted %d entries got %d", keyCount, s.EntryCount())
	}
}

func TestDeleteLock(t *testing.T) {
	s := newStore(0)
	key, val := []byte("foo"), []byte("bar")
	if err := s.Write(key, val); err != nil {
		t.Fatal(err)
	}
	if err := s.deleteLock(key); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Read(key); err == nil {
		t.Fatal("Expected error got nil")
	}
}

func TestUpdateKeyWorks(t *testing.T) {
	s := newStore(0)
	key, val := []byte("foo"), []byte("bar")
	if err := s.Write(key, val); err != nil {
		t.Fatal(err)
	}
	newVal := []byte("fighters")
	if err := s.Write(key, newVal); err != nil {
		t.Fatal(err)
	}
	res, err := s.Read(key)
	if err != nil {
		t.Fatal(err)
	}
	equalBytes(t, newVal, res)
}

func equalBytes(t *testing.T, expect, result []byte) {
	if bytes.Compare(expect, result) != 0 {
		t.Fatalf("expected %s to be equal %s", expect, result)
	}
}
