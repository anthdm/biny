package storage

import (
	"fmt"
	"math/rand"
	"testing"
)

const keyCount = 100000

func generateValue(size int) []byte {
	val := make([]byte, size)
	for i := 0; i < size; i++ {
		// gives a .. z
		val[i] = uint8(rand.Int()%26) + 97
	}
	return val
}

func generateKeys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("%d", i))
	}
	return keys
}

func (s *store) fill(keys [][]byte, value []byte) {
	for _, key := range keys {
		s.writeLock(key, value)
	}
}

func benchWrite(b *testing.B, size int) {
	b.StopTimer()
	s := newStore(1024)

	keys := generateKeys(keyCount)
	val := generateValue(size)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		s.Write(keys[i%len(keys)], val)
	}
	b.StopTimer()
}

func benchRead(b *testing.B, size int) {
	b.SetBytes(int64(size))
	b.StopTimer()
	s := newStore(1024)

	keys := generateKeys(keyCount)
	s.fill(keys, generateValue(size))
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		s.Read(keys[i%len(keys)])
	}
	b.StopTimer()
}

func BenchmarkWrite32b(b *testing.B) {
	benchWrite(b, 32)
}

func BenchmarkRead32b(b *testing.B) {
	benchRead(b, 32)
}
