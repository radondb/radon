package ring

import (
	"testing"
)

func TestRing(t *testing.T) {
	size := 5

	r := NewRing(size)

	if r.Len() != 0 {
		t.Fatal("len not:", 0)
	}

	if r.Cap() != size {
		t.Fatal("cap not:", size)
	}

	var err error

	items := []interface{}{1, 2, 3, 4}
	err = r.MPush(items)

	if err != nil {
		t.Fatal(err)
	}

	if r.Len() != 4 {
		t.Fatal("invalid len", r.Len())
	}

	if r.Cap() != 1 {
		t.Fatal("invalid cap", r.Cap())
	}

	items, err = r.MPop(2)

	if err != nil {
		t.Fatal(err)
	}

	if v, ok := items[0].(int); ok {
		if v != 1 {
			t.Fatal("invalid value", v)
		}
	} else {
		t.Fatal("invalid data", items[0])
	}

	if v, ok := items[1].(int); ok {
		if v != 2 {
			t.Fatal("invalid value", v)
		}
	} else {
		t.Fatal("invalid data", items[1])
	}

	items = []interface{}{5, 6, 7}
	err = r.MPush(items)

	if err != nil {
		t.Fatal(err)
	}

	if r.Len() != size {
		t.Fatal("invalid size", r.Len())
	}

	if r.Cap() != 0 {
		t.Fatal("invalid cap", r.Cap())
	}

	items, err = r.MPop(3)

	if err != nil {
		t.Fatal(err)
	}

	if v, ok := items[0].(int); ok {
		if v != 3 {
			t.Fatal("invalid value", v)
		}
	} else {
		t.Fatal("invalid data", items[0])
	}

	if v, ok := items[1].(int); ok {
		if v != 4 {
			t.Fatal("invalid value", v)
		}
	} else {
		t.Fatal("invalid data", items[1])
	}

	if v, ok := items[2].(int); ok {
		if v != 5 {
			t.Fatal("invalid value", v)
		}
	} else {
		t.Fatal("invalid data", items[2])
	}

	if r.Len() != 2 {
		t.Fatal("invalid len", r.Len())
	}

	if r.Cap() != 3 {
		t.Fatal("invalid cap", r.Cap())
	}

}

func TestRingGet(t *testing.T) {
	r := NewRing(5)
	if !r.Empty() {
		t.Fatal(" invalid len", r.Len())
	}
	err := r.MPush([]interface{}{1, 2, 3, 4, 5})
	if err != nil {
		t.Fatal(err.Error())
	}
	if !r.Full() {
		t.Fatal(" invalid cap", r.Cap())
	}

	err = r.Push(1)
	if err == nil {
		t.Fatal("should return a error")
	}

	result := r.GetAll()
	if len(result) != 5 {
		t.Fatal("invalid len", len(result))
	}

	value, _ := r.Pop()
	v, _ := value.(int)
	if v != 1 {
		t.Fatal("invalid value", v)
	}

	result = r.Gets(3)

	if len(result) != 3 {
		t.Fatal("invalid len", len(result))
	}

	value, _ = result[0].(int)
	v, _ = value.(int)

	if v != 2 {
		t.Fatal("invalid value", v)
	}

	value, _ = result[2].(int)
	v, _ = value.(int)

	if v != 4 {
		t.Fatal("invalid value", v)
	}
}
