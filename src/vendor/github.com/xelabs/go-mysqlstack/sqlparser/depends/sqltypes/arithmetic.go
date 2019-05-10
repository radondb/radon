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

// NullsafeAdd adds two Values in a null-safe manner. A null value
// is treated as 0. If both values are null, then a null is returned.
// If both values are not null, a numeric value is built
// from each input: Signed->int64, Unsigned->uint64, Float->float64.
// Otherwise the 'best type fit' is chosen for the number: int64 or float64.
// Addition is performed by upgrading types as needed, or in case
// of overflow: int64->uint64, int64->float64, uint64->float64.
// Unsigned ints can only be added to positive ints. After the
// addition, if one of the input types was Decimal, then
// a Decimal is built. Otherwise, the final type of the
// result is preserved.
func NullsafeAdd(v1, v2 Value, resultType querypb.Type, prec int) (Value, error) {
	if v1.IsNull() {
		return v2, nil
	}
	if v2.IsNull() {
		return v1, nil
	}

	lv1, err := newNumeric(v1)
	if err != nil {
		return NULL, err
	}
	lv2, err := newNumeric(v2)
	if err != nil {
		return NULL, err
	}
	lresult, err := addNumeric(lv1, lv2)
	if err != nil {
		return NULL, err
	}
	return castFromNumeric(lresult, resultType, prec)
}

// NullsafeDiv used to divide two Values in a null-safe manner.
func NullsafeDiv(v1, v2 Value, resultType querypb.Type, prec int) (Value, error) {
	if v1.IsNull() || v2.IsNull() {
		return NULL, nil
	}

	lv1, err := newNumericFloat(v1)
	if err != nil {
		return NULL, err
	}
	lv2, err := newNumericFloat(v2)
	if err != nil {
		return NULL, err
	}

	if lv2.fval == 0 {
		return NULL, nil
	}

	lresult := numeric{typ: Float64, fval: lv1.fval / lv2.fval}
	return castFromNumeric(lresult, resultType, prec)
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

// Min returns the minimum of v1 and v2. If one of the
// values is NULL, it returns the other value. If both
// are NULL, it returns NULL.
func Min(v1, v2 Value) Value {
	return minmax(v1, v2, true)
}

// Max returns the maximum of v1 and v2. If one of the
// values is NULL, it returns the other value. If both
// are NULL, it returns NULL.
func Max(v1, v2 Value) Value {
	return minmax(v1, v2, false)
}

func minmax(v1, v2 Value, min bool) Value {
	if v1.IsNull() {
		return v2
	}
	if v2.IsNull() {
		return v1
	}

	n := NullsafeCompare(v1, v2)

	// XNOR construct. See tests.
	v1isSmaller := n < 0
	if min == v1isSmaller {
		return v1
	}
	return v2
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

// newNumeric parses a value and produces an Int64, Uint64 or Float64.
func newNumericFloat(v Value) (numeric, error) {
	str := v.String()
	var fval float64
	switch {
	case v.IsSigned():
		ival, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return numeric{}, err
		}
		fval = float64(ival)
	case v.IsUnsigned():
		uval, err := strconv.ParseUint(str, 10, 64)
		if err != nil {
			return numeric{}, err
		}
		fval = float64(uval)
	case v.IsFloat():
		val, err := strconv.ParseFloat(str, 64)
		if err != nil {
			return numeric{}, err
		}
		fval = val
	default:
		// For other types, do best effort.
		if ival, err := strconv.ParseInt(str, 10, 64); err == nil {
			fval = float64(ival)
		} else {
			if val, err := strconv.ParseFloat(str, 64); err == nil {
				fval = val
			}
		}
	}

	return numeric{fval: fval, typ: Float64}, nil
}

func addNumeric(v1, v2 numeric) (numeric, error) {
	v1, v2 = prioritize(v1, v2)
	switch v1.typ {
	case Int64:
		return intPlusInt(v1.ival, v2.ival), nil
	case Uint64:
		switch v2.typ {
		case Int64:
			return uintPlusInt(v1.uval, v2.ival)
		case Uint64:
			return uintPlusUint(v1.uval, v2.uval), nil
		}
	case Float64:
		return floatPlusAny(v1.fval, v2), nil
	}
	panic("unreachable")
}

func divNumeric(v1, v2 numeric) (numeric, error) {
	switch v1.typ {
	case Int64:
		return intPlusInt(v1.ival, v2.ival), nil
	case Uint64:
		switch v2.typ {
		case Int64:
			return uintPlusInt(v1.uval, v2.ival)
		case Uint64:
			return uintPlusUint(v1.uval, v2.uval), nil
		}
	case Float64:
		return floatPlusAny(v1.fval, v2), nil
	}
	panic("unreachable")
}

// prioritize reorders the input parameters
// to be Float64, Uint64, Int64.
func prioritize(v1, v2 numeric) (altv1, altv2 numeric) {
	switch v1.typ {
	case Int64:
		if v2.typ == Uint64 || v2.typ == Float64 {
			return v2, v1
		}
	case Uint64:
		if v2.typ == Float64 {
			return v2, v1
		}
	}
	return v1, v2
}

func intPlusInt(v1, v2 int64) numeric {
	result := v1 + v2
	if v1 > 0 && v2 > 0 && result < 0 {
		goto overflow
	}
	if v1 < 0 && v2 < 0 && result > 0 {
		goto overflow
	}
	return numeric{typ: Int64, ival: result}

overflow:
	return numeric{typ: Float64, fval: float64(v1) + float64(v2)}
}

func uintPlusInt(v1 uint64, v2 int64) (numeric, error) {
	if v2 < 0 {
		return numeric{}, fmt.Errorf("cannot add a negative number to an unsigned integer: %d, %d", v1, v2)
	}
	return uintPlusUint(v1, uint64(v2)), nil
}

func uintPlusUint(v1, v2 uint64) numeric {
	result := v1 + v2
	if result < v2 {
		return numeric{typ: Float64, fval: float64(v1) + float64(v2)}
	}
	return numeric{typ: Uint64, uval: result}
}

func floatPlusAny(v1 float64, v2 numeric) numeric {
	switch v2.typ {
	case Int64:
		v2.fval = float64(v2.ival)
	case Uint64:
		v2.fval = float64(v2.uval)
	}
	return numeric{typ: Float64, fval: v1 + v2.fval}
}

func castFromNumeric(v numeric, resultType querypb.Type, prec int) (Value, error) {
	switch {
	case IsSigned(resultType):
		switch v.typ {
		case Int64:
			return MakeTrusted(resultType, strconv.AppendInt(nil, v.ival, 10)), nil
		case Uint64, Float64:
			return NULL, fmt.Errorf("unexpected type conversion: %v to %v", v.typ, resultType)
		}
	case IsUnsigned(resultType):
		switch v.typ {
		case Uint64:
			return MakeTrusted(resultType, strconv.AppendUint(nil, v.uval, 10)), nil
		case Int64, Float64:
			return NULL, fmt.Errorf("unexpected type conversion: %v to %v", v.typ, resultType)
		}
	case IsFloat(resultType) || resultType == Decimal:
		switch v.typ {
		case Int64:
			return MakeTrusted(resultType, strconv.AppendInt(nil, v.ival, 10)), nil
		case Uint64:
			return MakeTrusted(resultType, strconv.AppendUint(nil, v.uval, 10)), nil
		case Float64:
			format := byte('g')
			if resultType == Decimal {
				format = 'f'
			}
			return MakeTrusted(resultType, strconv.AppendFloat(nil, v.fval, format, prec, 64)), nil
		}
	}
	return NULL, fmt.Errorf("unexpected type conversion to non-numeric: %v", resultType)
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

// Cast converts a Value to the target type.
func Cast(v Value, typ querypb.Type) (Value, error) {
	if v.Type() == typ || v.IsNull() {
		return v, nil
	}
	if IsSigned(typ) && v.IsSigned() {
		return MakeTrusted(typ, v.val), nil
	}
	if IsUnsigned(typ) && v.IsUnsigned() {
		return MakeTrusted(typ, v.val), nil
	}
	if (IsFloat(typ) || typ == Decimal) && (v.IsIntegral() || v.IsFloat() || v.Type() == Decimal) {
		return MakeTrusted(typ, v.val), nil
	}
	if IsQuoted(typ) && (v.IsIntegral() || v.IsFloat() || v.Type() == Decimal || v.IsQuoted()) {
		return MakeTrusted(typ, v.val), nil
	}

	// Explicitly disallow Expression.
	if v.Type() == Expression {
		return NULL, fmt.Errorf("%v cannot be cast to %v", v, typ)
	}

	// If the above fast-paths were not possible,
	// go through full validation.
	return NewValue(typ, v.val)
}
