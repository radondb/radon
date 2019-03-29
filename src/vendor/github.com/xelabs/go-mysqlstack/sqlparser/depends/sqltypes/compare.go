// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqltypes

import (
	"bytes"
	"fmt"
	"strconv"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
)

// numeric represents a numeric value extracted from
// a Value, used for arithmetic operations.
type numeric struct {
	typ  querypb.Type
	ival int64
	uval uint64
	fval float64
}

// NullsafeCompare returns 0 if v1==v2, -1 if v1<v2, and 1 if v1>v2.
// NULL is the lowest value. If any value is numeric, then a numeric
// comparison is performed after necessary conversions. If none are
// numeric, then it's a simple binary comparison.
func NullsafeCompare(v1, v2 Value) int {
	if v1.IsNull() {
		if v2.IsNull() {
			return 0
		}
		return -1
	}
	if v2.IsNull() {
		return 1
	}

	if isNumber(v1.Type()) || isNumber(v2.Type()) {
		lv1, err := newNumeric(v1)
		if err != nil {
			panic(err)
		}
		lv2, err := newNumeric(v2)
		if err != nil {
			panic(err)
		}
		return compareNumeric(lv1, lv2)
	}

	if v1.Type() == Tuple || v2.Type() == Tuple {
		panic(fmt.Sprintf("unsupported.value.type:%v.vs.%v", v1.Type(), v2.Type()))
	}

	return bytes.Compare(v1.val, v2.val)
}

// newNumeric parses a value and produces an Int64, Uint64 or Float64.
func newNumeric(v Value) (numeric, error) {
	str := v.String()
	switch {
	case v.IsSigned():
		ival, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return numeric{}, err
		}
		return numeric{ival: ival, typ: Int64}, nil
	case v.IsUnsigned():
		uval, err := strconv.ParseUint(str, 10, 64)
		if err != nil {
			return numeric{}, err
		}
		return numeric{uval: uval, typ: Uint64}, nil
	case v.IsFloat():
		fval, err := strconv.ParseFloat(str, 64)
		if err != nil {
			return numeric{}, err
		}
		return numeric{fval: fval, typ: Float64}, nil
	}

	// For other types, do best effort.
	if ival, err := strconv.ParseInt(str, 10, 64); err == nil {
		return numeric{ival: ival, typ: Int64}, nil
	}
	if fval, err := strconv.ParseFloat(str, 64); err == nil {
		return numeric{fval: fval, typ: Float64}, nil
	}
	return numeric{ival: 0, typ: Int64}, nil
}

func compareNumeric(v1, v2 numeric) int {
	// Equalize the types.
	switch v1.typ {
	case Int64:
		switch v2.typ {
		case Uint64:
			if v1.ival < 0 {
				return -1
			}
			v1 = numeric{typ: Uint64, uval: uint64(v1.ival)}
		case Float64:
			v1 = numeric{typ: Float64, fval: float64(v1.ival)}
		}
	case Uint64:
		switch v2.typ {
		case Int64:
			if v2.ival < 0 {
				return 1
			}
			v2 = numeric{typ: Uint64, uval: uint64(v2.ival)}
		case Float64:
			v1 = numeric{typ: Float64, fval: float64(v1.uval)}
		}
	case Float64:
		switch v2.typ {
		case Int64:
			v2 = numeric{typ: Float64, fval: float64(v2.ival)}
		case Uint64:
			v2 = numeric{typ: Float64, fval: float64(v2.uval)}
		}
	}

	// Both values are of the same type.
	switch v1.typ {
	case Int64:
		return CompareInt64(v1.ival, v2.ival)
	case Uint64:
		return CompareUint64(v1.uval, v2.uval)
	case Float64:
		return CompareFloat64(v1.fval, v2.fval)
	}

	return 0
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
