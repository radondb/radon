package ring

import (
	"errors"
)

var (
	ErrRingLenNotEnough = errors.New("ring has not enough items for pop n")
	ErrRingCapNotEnough = errors.New("ring has not enough space for push n")
)

type Ring struct {
	items   []interface{}
	head    int
	tail    int
	size    int
	maxSize int
}

func NewRing(maxSize int) *Ring {
	r := new(Ring)

	r.size = maxSize
	r.head = 0
	r.tail = 0

	//for a empty item
	r.maxSize = r.size + 1

	r.items = make([]interface{}, r.maxSize)

	return r
}

func (r *Ring) Len() int {
	if r.head == r.tail {
		return 0
	} else if r.tail > r.head {
		return r.tail - r.head
	} else {
		return r.tail + r.maxSize - r.head
	}
}

func (r *Ring) Cap() int {
	return r.size - r.Len()
}

func (r *Ring) MPop(n int) ([]interface{}, error) {
	if r.Len() < n {
		return nil, ErrRingLenNotEnough
	}

	items := make([]interface{}, n)

	for i := 0; i < n; i++ {
		head := (r.head + i) % r.maxSize
		items[i] = r.items[head]
		r.items[head] = nil
	}

	r.head = (r.head + n) % r.maxSize
	return items, nil
}

func (r *Ring) Pop() (interface{}, error) {
	if items, err := r.MPop(1); err != nil {
		return nil, err
	} else {
		return items[0], nil
	}
}

func (r *Ring) MPush(items []interface{}) error {
	n := len(items)

	if r.Cap() < n {
		return ErrRingCapNotEnough
	}

	for i := 0; i < n; i++ {
		tail := (r.tail + i) % r.maxSize
		r.items[tail] = items[i]
	}

	r.tail = (r.tail + n) % r.maxSize
	return nil
}

func (r *Ring) Push(item interface{}) error {
	items := []interface{}{item}
	return r.MPush(items)
}

func (r *Ring) Full() bool {
	return r.Cap() == 0
}

func (r *Ring) Empty() bool {
	return r.Len() == 0
}

func (r *Ring) Gets(n int) []interface{} {
	if r.Len() < n {
		n = r.Len()
	}
	result := make([]interface{}, n)
	for i := 0; i < n; i++ {
		result[i] = r.items[(r.head+i)%r.maxSize]
	}
	return result
}

func (r *Ring) Get() interface{} {
	if r.Empty() {
		return ErrRingLenNotEnough
	}
	return r.items[r.head]
}

func (r *Ring) GetAll() []interface{} {
	return r.Gets(r.Len())
}
