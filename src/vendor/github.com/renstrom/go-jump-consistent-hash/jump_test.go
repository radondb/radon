package jump

import (
	"fmt"
	"hash"
	"strconv"
	"testing"
)

var jumpTestVectors = []struct {
	key      uint64
	buckets  int32
	expected int32
}{
	{1, 1, 0},
	{42, 57, 43},
	{0xDEAD10CC, 1, 0},
	{0xDEAD10CC, 666, 361},
	{256, 1024, 520},
	// Test negative values
	{0, -10, 0},
	{0xDEAD10CC, -666, 0},
}

func TestJumpHash(t *testing.T) {
	for _, v := range jumpTestVectors {
		h := Hash(v.key, v.buckets)
		if h != v.expected {
			t.Errorf("expected bucket for key=%d to be %d, got %d",
				v.key, v.expected, h)
		}
	}
}

var jumpStringTestVectors = []struct {
	key      string
	buckets  int32
	hasher   hash.Hash64
	expected int32
}{
	{"localhost", 10, CRC32, 9},
	{"ёлка", 10, CRC64, 6},
	{"ветер", 10, FNV1, 3},
	{"中国", 10, FNV1a, 5},
	{"日本", 10, CRC64, 6},
}

func TestJumpHashString(t *testing.T) {
	for _, v := range jumpStringTestVectors {
		h := HashString(v.key, v.buckets, v.hasher)
		if h != v.expected {
			t.Errorf("expected bucket for key=%s to be %d, got %d",
				strconv.Quote(v.key), v.expected, h)
		}
	}
}

func TestHasher(t *testing.T) {
	for _, v := range jumpStringTestVectors {
		hasher := New(int(v.buckets), v.hasher)
		h := hasher.Hash(v.key)
		if int32(h) != v.expected {
			t.Errorf("expected bucket for key=%s to be %d, got %d",
				strconv.Quote(v.key), v.expected, h)
		}
	}
}

func ExampleHash() {
	fmt.Print(Hash(256, 1024))
	// Output: 520
}

func ExampleHashString() {
	fmt.Print(HashString("127.0.0.1", 8, CRC64))
	// Output: 7
}

func BenchmarkHash(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Hash(uint64(i), int32(i))
	}
}

func BenchmarkHashStringCRC32(b *testing.B) {
	s := "Lorem ipsum dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh euismod tincidunt ut laoreet dolore magna aliquam erat volutpat."
	for i := 0; i < b.N; i++ {
		HashString(s, int32(i), CRC32)
	}
}

func BenchmarkHashStringCRC64(b *testing.B) {
	s := "Lorem ipsum dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh euismod tincidunt ut laoreet dolore magna aliquam erat volutpat."
	for i := 0; i < b.N; i++ {
		HashString(s, int32(i), CRC64)
	}
}

func BenchmarkHashStringFNV1(b *testing.B) {
	s := "Lorem ipsum dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh euismod tincidunt ut laoreet dolore magna aliquam erat volutpat."
	for i := 0; i < b.N; i++ {
		HashString(s, int32(i), FNV1)
	}
}

func BenchmarkHashStringFNV1a(b *testing.B) {
	s := "Lorem ipsum dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh euismod tincidunt ut laoreet dolore magna aliquam erat volutpat."
	for i := 0; i < b.N; i++ {
		HashString(s, int32(i), FNV1a)
	}
}
