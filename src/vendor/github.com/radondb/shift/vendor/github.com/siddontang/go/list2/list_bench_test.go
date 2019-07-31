package list2

import (
	"container/list"
	"testing"
)

func BenchmarkGoList(b *testing.B) {
	l := list.New()

	n := 10000

	for j := 0; j < b.N; j++ {
		for i := 0; i < n; i++ {
			l.PushBack(i)
		}

		for i := 0; i < n/2; i++ {
			f := l.Front()
			l.Remove(f)
		}

		for i := 0; i < n/2; i++ {
			l.PushFront(i)
		}

	}
}

func BenchmarkList(b *testing.B) {
	l := NewSize(10240)

	b.ResetTimer()
	n := 10000

	for j := 0; j < b.N; j++ {
		for i := 0; i < n; i++ {
			l.PushBack(i)
		}

		for i := 0; i < n/2; i++ {
			f := l.Front()
			l.Remove(f)
		}

		for i := 0; i < n/2; i++ {
			l.PushFront(i)
		}

	}
}
