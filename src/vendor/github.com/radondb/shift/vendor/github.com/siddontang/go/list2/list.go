package list2

const defaultSize = 1024

type Element struct {
	Value interface{}

	prev int
	next int

	index int

	list *List
}

func (e *Element) Next() *Element {
	if e.next == 0 || e.prev == -1 || e.list == nil {
		return nil
	}

	return e.list.elems[e.next]
}

func (e *Element) Prev() *Element {
	if e.prev == 0 || e.prev == -1 || e.list == nil {
		return nil
	}

	return e.list.elems[e.prev]
}

//static linked list based array
type List struct {
	elems []*Element

	num int

	root *Element

	free int
}

func New() *List {
	return NewSize(defaultSize)
}

func NewSize(size int) *List {
	if size < defaultSize {
		size = defaultSize
	}

	l := new(List)

	l.elems = make([]*Element, size)
	for i := range l.elems {
		e := new(Element)

		e.list = l
		e.index = i
		e.next = i + 1
		if i+1 == len(l.elems) {
			e.next = -1
		}

		l.elems[i] = e
	}

	//use first for root
	l.root = l.elems[0]
	l.root.next = 0
	l.root.prev = 0

	l.free = 1
	l.num = 0

	return l
}

func (l *List) Len() int {
	return l.num
}

func (l *List) Front() *Element {
	if l.root.next == 0 {
		return nil
	} else {
		return l.elems[l.root.next]
	}
}

func (l *List) Back() *Element {
	if l.root.prev == 0 {
		return nil
	} else {
		return l.elems[l.root.prev]
	}
}

func (l *List) remove(e *Element) *Element {
	next := e.next
	prev := e.prev

	l.elems[prev].next = next
	l.elems[next].prev = prev

	e.next = -1
	e.prev = -1

	l.num--

	return e
}

func (l *List) Remove(e *Element) interface{} {
	if e.list != l {
		return nil
	}

	if e.prev == -1 {
		return e.Value
	}

	l.remove(e)

	v := e.Value
	e.Value = nil

	e.next = l.free
	l.free = e.index

	return v
}

func (l *List) getFreeElem() *Element {
	if l.free == -1 {
		//no free elements, create
		num := len(l.elems)

		newElems := make([]*Element, 2*num)
		for i := num; i < 2*num; i++ {
			e := new(Element)
			e.list = l
			e.index = i
			e.next = i + 1
			if i+1 == 2*num {
				e.next = -1
			}

			newElems[i] = e
		}

		l.free = num

		copy(newElems, l.elems)

		l.elems = newElems
	}

	n := l.free
	l.free = l.elems[n].next

	return l.elems[n]
}

func (l *List) insert(e *Element, index int) *Element {
	at := l.elems[index]
	n := at.next
	at.next = e.index
	e.prev = at.index
	e.next = n
	l.elems[n].prev = e.index
	e.list = l
	l.num++
	return e
}

func (l *List) insertValue(v interface{}, index int) *Element {
	e := l.getFreeElem()
	e.Value = v

	return l.insert(e, index)
}

func (l *List) PushFront(v interface{}) *Element {
	return l.insertValue(v, l.root.index)
}

func (l *List) PushBack(v interface{}) *Element {
	return l.insertValue(v, l.root.prev)
}

func (l *List) InsertBefore(v interface{}, mark *Element) *Element {
	if mark.list != l {
		return nil
	}

	return l.insertValue(v, mark.prev)
}

func (l *List) InsertAfter(v interface{}, mark *Element) *Element {
	if mark.list != l {
		return nil
	}

	return l.insertValue(v, mark.index)
}

func (l *List) MoveToFront(e *Element) {
	if e.list != l || l.root.next == e.index {
		return
	}

	l.insert(l.remove(e), l.root.index)
}

func (l *List) MoveToBack(e *Element) {
	if e.list != l || l.root.prev == e.index {
		return
	}
	l.insert(l.remove(e), l.root.prev)
}

func (l *List) MoveBefore(e, mark *Element) {
	if e.list != l || e == mark {
		return
	}

	l.insert(l.remove(e), mark.prev)
}

func (l *List) MoveAfter(e, mark *Element) {
	if e.list != l || e == mark {
		return
	}
	l.insert(l.remove(e), mark.index)
}

func (l *List) PushBackList(other *List) {
	for i, e := other.Len(), other.Front(); i > 0; i, e = i-1, e.Next() {
		l.insertValue(e.Value, l.root.prev)
	}
}

func (l *List) PushFrontList(other *List) {
	for i, e := other.Len(), other.Back(); i > 0; i, e = i-1, e.Prev() {
		l.insertValue(e.Value, l.root.index)
	}
}
