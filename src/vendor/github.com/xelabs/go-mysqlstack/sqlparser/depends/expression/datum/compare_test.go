/*
 * Radon
 *
 * Copyright 2020 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package datum

import (
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

func TestNullsafeCompare(t *testing.T) {
	tcases := []struct {
		v1      Datum
		v2      Datum
		cmpFunc CompareFunc
		res     int64
		equal   bool
	}{
		{
			v1:      NewDNull(true),
			v2:      NewDNull(true),
			cmpFunc: CompareInt,
			res:     0,
			equal:   true,
		},
		{
			v1:      NewDNull(true),
			v2:      NewDString("2", 10, false),
			cmpFunc: CompareInt,
			res:     -1,
		},
		{
			v1:      NewDString("2", 10, false),
			v2:      NewDNull(true),
			cmpFunc: CompareInt,
			res:     1,
		},
		{
			v1:      NewDInt(2, false),
			v2:      NewDInt(1, false),
			cmpFunc: CompareInt,
			res:     1,
		},
		{
			v1:      NewDInt(1, false),
			v2:      NewDInt(1, false),
			cmpFunc: CompareInt,
			res:     0,
			equal:   true,
		},
		{
			v1:      NewDInt(1, false),
			v2:      NewDInt(2, false),
			cmpFunc: CompareInt,
			res:     -1,
		},
		{
			v1:      NewDInt(2, true),
			v2:      NewDInt(1, true),
			cmpFunc: CompareInt,
			res:     1,
		},
		{
			v1:      NewDInt(1, true),
			v2:      NewDInt(1, true),
			cmpFunc: CompareInt,
			res:     0,
			equal:   true,
		},
		{
			v1:      NewDInt(1, true),
			v2:      NewDInt(2, true),
			cmpFunc: CompareInt,
			res:     -1,
		},
		{
			v1:      NewDInt(-1, false),
			v2:      NewDInt(2, true),
			cmpFunc: CompareInt,
			res:     -1,
		},
		{
			v1:      NewDInt(-1, true),
			v2:      NewDInt(2, false),
			cmpFunc: CompareInt,
			res:     1,
		},
		{
			v1:      NewDString("luoyang", 10, false),
			v2:      NewDString("luohe", 10, false),
			cmpFunc: CompareString,
			res:     1,
		},
		{
			v1:      NewDString("luoyang", 10, false),
			v2:      NewDString("luohe", 10, false),
			cmpFunc: CompareString,
			res:     1,
		},
		{
			v1:      NewDString("ABCD", 10, false),
			v2:      NewDString("abcd", 10, false),
			cmpFunc: CompareString,
			res:     0,
			equal:   true,
		},
		{
			v1:      NewDFloat(2.33),
			v2:      NewDInt(2, false),
			cmpFunc: CompareFloat64,
			res:     1,
		},
		{
			v1:      NewDFloat(2.33),
			v2:      NewDString("2.33", 10, false),
			cmpFunc: CompareFloat64,
			res:     0,
		},
		{
			v1:      NewDFloat(2.33),
			v2:      NewDDecimal(decimal.NewFromFloat(3.23)),
			cmpFunc: CompareFloat64,
			res:     -1,
		},
		{
			v1:      NewDFloat(2.33),
			v2:      NewDFloat(2.333),
			cmpFunc: CompareFloat64,
			res:     -1,
		},
		{
			v1:      NewDInt(3, false),
			v2:      NewDDecimal(decimal.NewFromFloat(3.23)),
			cmpFunc: CompareDecimal,
			res:     -1,
		},
		{
			v1:      NewDDecimal(decimal.NewFromFloatWithExponent(14530529080000.2333, -4)),
			v2:      NewDTime(sqltypes.Datetime, 4, 1453, 5, 29, 9, 0, 0, 233300),
			cmpFunc: CompareDatetime,
			res:     -1,
		},
		{
			v1:      NewDDecimal(decimal.NewFromFloatWithExponent(14530529080000.2333, -4)),
			v2:      NewDTime(sqltypes.Datetime, 4, 1453, 5, 29, 8, 0, 0, 223300),
			cmpFunc: CompareDatetime,
			res:     1,
		},
		{
			v1:      NewDTime(sqltypes.Datetime, 4, 1453, 5, 29, 8, 0, 0, 223300),
			v2:      NewDTime(sqltypes.Datetime, 4, 1453, 5, 29, 8, 0, 0, 223300),
			cmpFunc: CompareDatetime,
			res:     0,
			equal:   true,
		},
		{
			v1:      NewDInt(14530529080000, true),
			v2:      NewDTime(sqltypes.Datetime, 4, 1453, 5, 29, 8, 0, 0, 233300),
			cmpFunc: CompareDatetime,
			res:     -1,
		},
		{
			v1:      NewDDecimal(decimal.NewFromFloatWithExponent(14530529080000.2333, -4)),
			v2:      NewDDecimal(decimal.NewFromFloatWithExponent(14530529080000.2333, -4)),
			cmpFunc: CompareDatetime,
			res:     0,
			equal:   true,
		},
		{
			v1:      NewDTime(sqltypes.Datetime, 4, 1453, 5, 29, 8, 0, 0, 233300),
			v2:      NewDInt(100, false),
			cmpFunc: CompareDatetime,
			res:     1,
		},
		{
			v1: NewDTime(sqltypes.Datetime, 4, 1453, 5, 29, 8, 0, 0, 233300),
			v2: &Duration{
				duration: time.Duration(8 * 3600),
				fsp:      0,
			},
			cmpFunc: CompareDatetime,
			res:     -1,
		},
		{
			v1: NewDTime(sqltypes.Datetime, 4, 1453, 5, 29, 8, 0, 0, 233300),
			v2: &Duration{
				duration: time.Duration(8*3600) * time.Second,
				fsp:      0,
			},
			cmpFunc: CompareDuration,
			res:     1,
		},
		{
			v1: NewDInt(80000, false),
			v2: &Duration{
				duration: time.Duration(8*3600) * time.Second,
				fsp:      0,
			},
			cmpFunc: CompareDuration,
			res:     0,
		},
		{
			v1: NewDString("70000", 10, false),
			v2: &Duration{
				duration: time.Duration(8*3600) * time.Second,
				fsp:      0,
			},
			cmpFunc: CompareDuration,
			res:     -1,
		},
		{
			v1: &Duration{
				duration: time.Duration(8*3600) * time.Second,
				fsp:      0,
			},
			v2: &Duration{
				duration: time.Duration(8*3600) * time.Second,
				fsp:      0,
			},
			cmpFunc: CompareDuration,
			res:     0,
			equal:   true,
		},
		{
			v1:      NewDString("1T08:00:00", 10, false),
			v2:      NewDString("1 08:00:00", 10, false),
			cmpFunc: CompareDuration,
			res:     1,
		},
	}
	for _, tcase := range tcases {
		res := NullsafeCompare(tcase.v1, tcase.v2, tcase.cmpFunc)
		assert.Equal(t, tcase.res, res)

		equal := AreEqual(tcase.v1, tcase.v2)
		assert.Equal(t, tcase.equal, equal)
	}
}

func TestGetCmpFunc(t *testing.T) {
	tcases := []struct {
		left  *IField
		right *IField
		res   string
	}{
		{
			left:  mockField(IntResult, 0, false, true, false),
			right: mockField(IntResult, 0, false, true, false),
			res:   "expression/datum.CompareInt",
		},
		{
			left:  mockField(DurationResult, 0, false, true, false),
			right: mockField(DurationResult, 0, false, true, false),
			res:   "expression/datum.CompareDuration",
		},
		{
			left:  mockField(DecimalResult, 0, false, true, false),
			right: mockField(IntResult, 0, false, true, false),
			res:   "expression/datum.CompareDecimal",
		},
		{
			left:  mockField(IntResult, 0, false, true, false),
			right: mockField(DecimalResult, 0, false, true, false),
			res:   "expression/datum.CompareDecimal",
		},
		{
			left:  mockField(DecimalResult, 0, false, true, false),
			right: mockField(StringResult, 0, false, false, true),
			res:   "expression/datum.CompareDecimal",
		},
		{
			left:  mockField(TimeResult, 0, false, true, true),
			right: mockField(DecimalResult, 0, false, true, false),
			res:   "expression/datum.CompareDecimal",
		},
		{
			left:  mockField(TimeResult, 0, false, true, false),
			right: mockField(IntResult, 0, false, true, true),
			res:   "expression/datum.CompareDatetime",
		},
		{
			left:  mockField(StringResult, 0, false, false, true),
			right: mockField(DurationResult, 0, false, true, false),
			res:   "expression/datum.CompareDuration",
		},
		{
			left:  mockField(StringResult, 0, false, false, false),
			right: mockField(StringResult, 0, false, false, false),
			res:   "expression/datum.CompareString",
		},
		{
			left:  mockField(StringResult, 0, false, false, false),
			right: mockField(TimeResult, 0, false, true, false),
			res:   "expression/datum.CompareDatetime",
		},
		{
			left:  mockField(RealResult, 0, false, true, false),
			right: mockField(StringResult, 0, false, false, false),
			res:   "expression/datum.CompareFloat64",
		},
	}
	for _, tcase := range tcases {
		res := GetCmpFunc(tcase.left, tcase.right)
		assert.Equal(t, tcase.res, getFunctionName(res))
	}
}

func getFunctionName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}
