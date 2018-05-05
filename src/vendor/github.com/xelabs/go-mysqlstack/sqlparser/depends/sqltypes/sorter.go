// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
//
// Copyright (c) XeLabs
// BohuTANG

package sqltypes

import (
	"bytes"
	"fmt"
	"sort"
)

// Len is part of sort.Interface.
func (result *Result) Len() int {
	return len(result.Rows)
}

// Swap is part of sort.Interface.
func (result *Result) Swap(i, j int) {
	result.Rows[i], result.Rows[j] = result.Rows[j], result.Rows[i]
}

// Less is part of sort.Interface. It is implemented by looping along the
// less functions until it finds a comparison that is either Less or
// !Less. Note that it can call the less functions twice per call. We
// could change the functions to return -1, 0, 1 and reduce the
// number of calls for greater efficiency: an exercise for the reader.
func (result *Result) Less(i, j int) bool {
	p, q := result.Rows[i], result.Rows[j]
	// Try all but the last comparison.
	var k int
	for k = 0; k < len(result.sorters)-1; k++ {
		ser := result.sorters[k]
		switch {
		case ser.less(ser.idx, p, q):
			// p < q, so we have a decision.
			return true
		case ser.less(ser.idx, q, p):
			// p > q, so we have a decision.
			return false
		}
		// p == q; try the next comparison.
	}
	// All comparisons to here said "equal", so just return whatever
	// the final comparison reports.
	ser := result.sorters[k]
	return ser.less(ser.idx, p, q)
}

// LessFunc implements the Less function of sorter interface.
type LessFunc func(idx int, v1, v2 []Value) bool
type sorter struct {
	idx  int
	less LessFunc
}

func lessAscFn(idx int, v1, v2 []Value) bool {
	vn1 := v1[idx].ToNative()
	vn2 := v2[idx].ToNative()
	switch vn1.(type) {
	case int64:
		return vn1.(int64) < vn2.(int64)
	case uint64:
		return vn1.(uint64) < vn2.(uint64)
	case float64:
		return vn1.(float64) < vn2.(float64)
	case []byte:
		return bytes.Compare(vn1.([]byte), vn2.([]byte)) < 0
	case nil:
		return false
	default:
		panic(fmt.Sprintf("unsupported.orderby.type:%T", vn1))
	}
}

// OrderedByAsc adds a 'order by asc' operator to the result.
func (result *Result) OrderedByAsc(fields ...string) error {
	for _, field := range fields {
		idx := -1
		for k, f := range result.Fields {
			if f.Name == field {
				idx = k
				break
			}
		}
		if idx == -1 {
			return fmt.Errorf("can.not.find.the.orderby.field[%s].direction.asc", field)
		}
		ser := &sorter{idx: idx, less: lessAscFn}
		result.sorters = append(result.sorters, ser)
	}
	return nil
}

func lessDescFn(idx int, v1, v2 []Value) bool {
	vn1 := v1[idx].ToNative()
	vn2 := v2[idx].ToNative()
	switch vn1.(type) {
	case int64:
		return vn2.(int64) < vn1.(int64)
	case uint64:
		return vn2.(uint64) < vn1.(uint64)
	case float64:
		return vn2.(float64) < vn1.(float64)
	case []byte:
		return bytes.Compare(vn2.([]byte), vn1.([]byte)) < 0
	case nil:
		return false
	default:
		panic(fmt.Sprintf("unsupported.orderby.type:%T", vn1))
	}
}

// OrderedByDesc adds a 'order by desc' operator to the result.
func (result *Result) OrderedByDesc(fields ...string) error {
	for _, field := range fields {
		idx := -1
		for k, f := range result.Fields {
			if f.Name == field {
				idx = k
				break
			}
		}
		if idx == -1 {
			return fmt.Errorf("can.not.find.the.orderby.field[%s].direction.desc", field)
		}
		ser := &sorter{idx: idx, less: lessDescFn}
		result.sorters = append(result.sorters, ser)
	}
	return nil
}

// Sort sorts the argument slice according to the less functions passed to OrderedBy.
func (result *Result) Sort() {
	if len(result.sorters) == 0 {
		return
	}
	sort.Sort(result)
}
