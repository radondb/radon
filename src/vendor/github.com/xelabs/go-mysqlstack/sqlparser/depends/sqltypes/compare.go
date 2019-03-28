// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqltypes

import (
	"bytes"
	"fmt"
)

// Compare used to compare two values.
// if return 0, v1=v2;if -1 v1<v2;if 0 v1>v2.
func Compare(v1, v2 Value) int {
	switch v1.Type() {
	case Decimal:
		v1 = MakeTrusted(Float64, v1.Raw())
	}

	switch v2.Type() {
	case Decimal:
		v2 = MakeTrusted(Float64, v2.Raw())
	}

	if v1.IsNull() {
		if v2.IsNull() {
			return 0
		}
		return -1
	}
	if v2.IsNull() {
		return 1
	}

	var err error
	var out float64
	vn1 := v1.ToNative()
	vn2 := v2.ToNative()
	switch vn1.(type) {
	case int64:
		switch vn2.(type) {
		case int64:
			return CompareInt64(vn1.(int64), vn2.(int64))
		case uint64:
			if vn1.(int64) < 0 {
				return -1
			}
			return CompareUint64(uint64(vn1.(int64)), vn2.(uint64))
		case float64:
			return CompareFloat64(float64(vn1.(int64)), vn2.(float64))
		case []byte:
			if out, err = v2.ParseFloat64(); err != nil {
				return CompareInt64(vn1.(int64), 0)
			}
			return CompareFloat64(float64(vn1.(int64)), out)
		default:
			panic(fmt.Sprintf("unsupported.value.type:%T", vn1))
		}
	case uint64:
		switch vn2.(type) {
		case int64:
			if vn2.(int64) < 0 {
				return 1
			}
			return CompareUint64(vn1.(uint64), uint64(vn2.(int64)))
		case uint64:
			return CompareUint64(vn1.(uint64), vn2.(uint64))
		case float64:
			return CompareFloat64(float64(vn1.(uint64)), vn2.(float64))
		case []byte:
			if out, err = v2.ParseFloat64(); err != nil {
				return CompareUint64(vn1.(uint64), 0)
			}
			return CompareFloat64(float64(vn1.(uint64)), out)
		default:
			panic(fmt.Sprintf("unsupported.value.type:%T", vn1))
		}
	case float64:
		switch vn2.(type) {
		case int64:
			return CompareFloat64(vn1.(float64), float64(vn2.(int64)))
		case uint64:
			return CompareFloat64(vn1.(float64), float64(vn2.(uint64)))
		case float64:
			return CompareFloat64(vn1.(float64), vn2.(float64))
		case []byte:
			if out, err = v2.ParseFloat64(); err != nil {
				return CompareFloat64(vn1.(float64), 0)
			}
			return CompareFloat64(vn1.(float64), out)
		default:
			panic(fmt.Sprintf("unsupported.value.type:%T", vn1))
		}
	case []byte:
		switch vn2.(type) {
		case int64:
			if out, err = v1.ParseFloat64(); err != nil {
				return CompareInt64(0, vn2.(int64))
			}
			return CompareFloat64(out, float64(vn2.(int64)))
		case uint64:
			if out, err = v1.ParseFloat64(); err != nil {
				return CompareUint64(0, vn2.(uint64))
			}
			return CompareFloat64(out, float64(vn2.(uint64)))
		case float64:
			if out, err = v1.ParseFloat64(); err != nil {
				return CompareFloat64(0, vn2.(float64))
			}
			return CompareFloat64(out, vn2.(float64))
		case []byte:
			return bytes.Compare(vn1.([]byte), vn2.([]byte))
		default:
			panic(fmt.Sprintf("unsupported.value.type:%T", vn1))
		}
	}
	panic(fmt.Sprintf("unsupported.value.type:%T", vn1))
}

// CompareInt64 returns an integer comparing the int64 x to y.
func CompareInt64(x, y int64) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

// CompareUint64 returns an integer comparing the uint64 x to y.
func CompareUint64(x, y uint64) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

// CompareFloat64 returns an integer comparing the float64 x to y.
func CompareFloat64(x, y float64) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

// CompareString returns an integer comparing the string x to y.
func CompareString(x, y string) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}
