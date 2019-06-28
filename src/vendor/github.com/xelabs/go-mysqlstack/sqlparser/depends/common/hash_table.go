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
	"hash/fnv"
)

// hash64a used to get bucket.
func hash64a(data []byte) uint64 {
	h := fnv.New64a()
	h.Write(data)
	return h.Sum64()
}

type entry struct {
	// key slice.
	key []byte
	// value interface.
	value []interface{}
	// point to the next entry.
	next *entry
}

func (e *entry) put(key []byte, value interface{}) *entry {
	if e == nil {
		return &entry{key, []interface{}{value}, nil}
	}
	if bytes.Equal(e.key, key) {
		e.value = append(e.value, value)
		return e
	}

	e.next = e.next.put(key, value)
	return e
}

func (e *entry) get(key []byte) (bool, []interface{}) {
	if e == nil {
		return false, nil
	} else if bytes.Equal(e.key, key) {
		return true, e.value
	} else {
		return e.next.get(key)
	}
}

// HashTable the hash table.
type HashTable struct {
	// stores value for a given key.
	hashEntry []*entry
	// k: bucket. v: index in the hashEntry.
	hashMap map[uint64]int
	// size of entrys.
	size int
}

// NewHashTable create hash table.
func NewHashTable() *HashTable {
	return &HashTable{
		hashMap: make(map[uint64]int),
		size:    0,
	}
}

// Size used to get the hashtable size.
func (h *HashTable) Size() int {
	return h.size
}

// Put puts the key/value pairs to the HashTable.
func (h *HashTable) Put(key []byte, value interface{}) {
	var table *entry
	bucket := hash64a(key)
	index, ok := h.hashMap[bucket]
	if !ok {
		table = &entry{key, []interface{}{value}, nil}
		h.hashMap[bucket] = len(h.hashEntry)
		h.hashEntry = append(h.hashEntry, table)
	} else {
		h.hashEntry[index] = h.hashEntry[index].put(key, value)
	}
	h.size++
}

// Get gets the values of the "key".
func (h *HashTable) Get(key []byte) (bool, []interface{}) {
	bucket := hash64a(key)
	index, ok := h.hashMap[bucket]
	if !ok {
		return false, nil
	}
	return h.hashEntry[index].get(key)
}

// Iterator used to iterate the HashTable.
type Iterator func() (key []byte, value []interface{}, next Iterator)

// Next used to iterate the HashTable.
func (h *HashTable) Next() Iterator {
	var e *entry
	var iter Iterator
	table := h.hashEntry
	i := -1
	iter = func() (key []byte, val []interface{}, next Iterator) {
		for e == nil {
			i++
			if i >= len(table) {
				return nil, nil, nil
			}
			e = table[i]
		}
		key = e.key
		val = e.value
		e = e.next
		return key, val, iter
	}
	return iter
}
