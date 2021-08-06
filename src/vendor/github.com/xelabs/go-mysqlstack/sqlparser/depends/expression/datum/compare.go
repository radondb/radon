/*
 * Radon
 *
 * Copyright 2020 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package datum

import (
	"math"
	"strings"
)

// NullsafeCompare returns 0 if v1==v2, -1 if v1<v2, and 1 if v1>v2.
// NULL is the lowest value.
func NullsafeCompare(x, y Datum, cmpFunc CompareFunc) int64 {
	if CheckNull(x) {
		if CheckNull(y) {
			return 0
		}
		return -1
	}
	if CheckNull(y) {
		return 1
	}
	return cmpFunc(x, y)
}

// GetCmpFunc get the CompareFunc based on the left and right.
func GetCmpFunc(left, right *IField) CompareFunc {
	leftResTyp, rightResTyp := left.Type, right.Type
	leftConst, rightConst := left.IsConstant, right.IsConstant

	if leftResTyp == IntResult && rightResTyp == IntResult {
		return CompareInt
	}
	if leftResTyp == DurationResult && rightResTyp == DurationResult {
		return CompareDuration
	}
	if (leftResTyp == IntResult || leftResTyp == DecimalResult) && (rightResTyp == IntResult || rightResTyp == DecimalResult) {
		return CompareDecimal
	}
	if (leftResTyp == DecimalResult && !leftConst && IsStringType(rightResTyp) && rightConst) ||
		(rightResTyp == DecimalResult && !rightConst && IsStringType(leftResTyp) && leftConst) {
		return CompareDecimal
	}
	if leftResTyp == TimeResult && rightConst || rightResTyp == TimeResult && leftConst {
		return CompareDatetime
	}
	if leftResTyp == DurationResult && rightConst || rightResTyp == DurationResult && leftConst {
		return CompareDuration
	}
	if IsStringType(leftResTyp) && IsStringType(rightResTyp) {
		if leftResTyp == TimeResult || rightResTyp == TimeResult {
			return CompareDatetime
		}
		return CompareString
	}
	return CompareFloat64
}

// CompareFunc defines the compare function prototype.
type CompareFunc = func(x, y Datum) int64

// CompareInt returns an integer comparing the int64 x to y.
func CompareInt(x, y Datum) int64 {
	a, flag1 := x.ValInt()
	b, flag2 := y.ValInt()

	if !flag1 && !flag2 {
		if a == b {
			return 0
		}
		if a < b {
			return -1
		}
		return 1
	}

	if !flag2 {
		if b < 0 || uint64(a) > math.MaxInt64 {
			return 1
		}
	}

	if !flag1 {
		if a < 0 || uint64(b) > math.MaxInt64 {
			return -1
		}
	}

	if uint64(a) == uint64(b) {
		return 0
	}
	if uint64(a) < uint64(b) {
		return -1
	}
	return 1
}

// CompareFloat64 returns an integer comparing the float64 x to y.
func CompareFloat64(x, y Datum) int64 {
	a, b := x.ValReal(), y.ValReal()
	if a == b {
		return 0
	}
	if a < b {
		return -1
	}
	return 1
}

// CompareDecimal returns an integer comparing the decimal x to y.
func CompareDecimal(x, y Datum) int64 {
	return int64(x.ValDecimal().Cmp(y.ValDecimal()))
}

// CompareString returns an integer comparing the string x to y.
func CompareString(x, y Datum) int64 {
	str1, str2 := x.ValStr(), y.ValStr()
	if ignoreCase(x) && ignoreCase(y) {
		str1, str2 = strings.ToLower(str1), strings.ToLower(str2)
	}
	return int64(strings.Compare(str1, str2))
}

// CompareDatetime returns an integer comparing the DTime x to y.
func CompareDatetime(x, y Datum) int64 {
	f1, f2 := getFsp(x), getFsp(y)
	fsp := TernaryOpt(f1 > f2, f1, f2).(int)
	if fsp < 0 {
		fsp = 6
	}
	t1, err1 := CastToDatetime(x, fsp)
	t2, err2 := CastToDatetime(y, fsp)
	if err1 == nil && err2 == nil {
		vd := datetimeToInt64(t1)
		vo := datetimeToInt64(t2)
		switch {
		case vd < vo:
			return -1
		case vd > vo:
			return 1
		}

		switch {
		case t1.microsecond < t2.microsecond:
			return -1
		case t1.microsecond > t2.microsecond:
			return 1
		}
		return 0
	}
	return CompareString(x, y)
}

// CompareDuration returns an integer comparing the Duration x to y.
func CompareDuration(x, y Datum) int64 {
	f1, f2 := getFsp(x), getFsp(y)
	fsp := TernaryOpt(f1 > f2, f1, f2).(int)
	if fsp < 0 {
		fsp = 6
	}
	d1, err1 := CastToDuration(x, fsp)
	d2, err2 := CastToDuration(y, fsp)
	if err1 == nil && err2 == nil {
		if d1.duration > d2.duration {
			return 1
		} else if d1.duration == d2.duration {
			return 0
		} else {
			return -1
		}
	}
	return CompareString(x, y)
}

// AreEqual checks the equality of the given values,
// returning false if the types don't match.
func AreEqual(d1, d2 Datum) bool {
	if CheckNull(d1) {
		if CheckNull(d2) {
			return true
		}
		return false
	}

	if CheckNull(d2) {
		return false
	}

	if d1.Type() != d2.Type() {
		return false
	}

	var res int64
	switch d1.Type() {
	case TypeInt:
		res = CompareInt(d1, d2)
	case TypeFloat:
		res = CompareFloat64(d1, d2)
	case TypeDecimal:
		res = CompareDecimal(d1, d2)
	case TypeDuration:
		res = CompareDuration(d1, d2)
	case TypeTime:
		res = CompareDatetime(d1, d2)
	default:
		res = CompareString(d1, d2)
	}
	return res == 0
}
