/*
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package common

import (
	"bytes"
	crand "crypto/rand"
	"testing"
)

func TestHashKey(t *testing.T) {
	a := []byte("asdf")
	b := []byte("asdf")
	c := []byte("csfd")
	if !bytes.Equal(a, b) {
		t.Error("a != b")
	}
	if hash64a(a) != hash64a(b) {
		t.Error("hash64a(a) != hash64a(b)")
	}
	if bytes.Equal(a, c) {
		t.Error("a == c")
	}
	if hash64a(a) == hash64a(c) {
		t.Error("hash64a(a) == hash64a(c)")
	}
}

func randSlice(length int) []byte {
	slice := make([]byte, length)
	if _, err := crand.Read(slice); err != nil {
		panic(err)
	}
	return slice
}

func TestPutHasGetRemove(t *testing.T) {

	type record struct {
		key []byte
		val []byte
	}

	ranrec := func() *record {
		return &record{
			randSlice(20),
			randSlice(20),
		}
	}

	table := NewHashTable()
	records := make([]*record, 400)
	var i int
	for i = range records {
		r := ranrec()
		records[i] = r
		table.Put(r.key, []byte(""))
		table.Put(r.key, r.val)

		if table.Size() != 2*(i+1) {
			t.Error("size was wrong", table.Size(), i+1)
		}
	}

	for _, r := range records {
		if has, val := table.Get(r.key); !has {
			t.Error(table, "Missing key")
		} else if !bytes.Equal(val[1].([]byte), r.val) {
			t.Error("wrong value")
		}
		if has, _ := table.Get(randSlice(12)); has {
			t.Error("Table has extra key")
		}
	}
}

func TestIterate(t *testing.T) {
	table := NewHashTable()
	t.Logf("%T", table)
	for k, v, next := table.Next()(); next != nil; k, v, next = next() {
		t.Errorf("Should never reach here %v %v %v", k, v, next)
	}
	records := make(map[string][]byte)
	for i := 0; i < 100; i++ {
		v := randSlice(8)
		keySlice := []byte{0x01}
		keySlice = append(keySlice, v...)
		keySlice = append(keySlice, 0x02)
		k := BytesToString(keySlice)
		records[k] = v
		table.Put(v, k)
		if table.Size() != (i + 1) {
			t.Error("size was wrong", table.Size(), i+1)
		}
	}
	count := 0
	for k, v, next := table.Next()(); next != nil; k, v, next = next() {
		if v1, has := records[v[0].(string)]; !has {
			t.Error("bad key in table")
		} else if !bytes.Equal(k, v1) {
			t.Error("values don't equal")
		}
		count++
	}
	if len(records) != count {
		t.Error("iterate missed records")
	}
}
