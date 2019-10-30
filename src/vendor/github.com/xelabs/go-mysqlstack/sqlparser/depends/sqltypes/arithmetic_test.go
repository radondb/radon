// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
//
// Copyright (c) XeLabs
// BohuTANG

package sqltypes

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/shopspring/decimal"

	"github.com/stretchr/testify/assert"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
)

func TestArithmetic(t *testing.T) {
	// uint64.
	{
		v1 := testVal(Uint24, "3")
		v2 := testVal(Uint24, "5")
		v3 := testVal(Uint24, "4")

		cmp := NullsafeCompare(v1, v2)
		assert.Equal(t, -1, cmp)

		cmp = NullsafeCompare(v2, v3)
		assert.Equal(t, 1, cmp)

		cmp = NullsafeCompare(v1, v1)
		assert.Equal(t, 0, cmp)
	}
	// int64.
	{
		v1 := testVal(Int64, "3")
		v2 := testVal(Int64, "5")
		v3 := testVal(Int64, "4")

		cmp := NullsafeCompare(v1, v2)
		assert.Equal(t, -1, cmp)

		cmp = NullsafeCompare(v2, v3)
		assert.Equal(t, 1, cmp)

		cmp = NullsafeCompare(v1, v1)
		assert.Equal(t, 0, cmp)
	}
	// float64.
	{
		v1 := testVal(Decimal, "3.14159")
		v2 := testVal(Decimal, "3.142")
		v3 := testVal(Decimal, "3.1416")

		cmp := NullsafeCompare(v1, v2)
		assert.Equal(t, -1, cmp)

		cmp = NullsafeCompare(v2, v3)
		assert.Equal(t, 1, cmp)

		cmp = NullsafeCompare(v1, v1)
		assert.Equal(t, 0, cmp)
	}
	// []byte.
	{
		v1 := testVal(VarChar, "pra")
		v2 := testVal(VarChar, "sci")
		v3 := testVal(VarChar, "qq")

		cmp := NullsafeCompare(v1, v2)
		assert.Equal(t, -1, cmp)

		cmp = NullsafeCompare(v2, v3)
		assert.Equal(t, 1, cmp)

		cmp = NullsafeCompare(v1, v1)
		assert.Equal(t, 0, cmp)
	}
	// different type.
	{
		v1 := testVal(Int64, "3")
		v2 := testVal(Decimal, "3.0")
		v3 := testVal(VarChar, "3")

		cmp := NullsafeCompare(v1, v2)
		assert.Equal(t, 0, cmp)

		cmp = NullsafeCompare(v1, v3)
		assert.Equal(t, 0, cmp)

		cmp = NullsafeCompare(v2, v3)
		assert.Equal(t, 0, cmp)
	}
	// different type.
	{
		v1 := testVal(Float64, "2.5")
		v2 := testVal(Decimal, "2.5")

		cmp := NullsafeCompare(v1, v2)
		assert.Equal(t, 0, cmp)
	}
	// v1 is null.
	{
		v1 := NULL
		v2 := testVal(VarChar, "qq")

		cmp := NullsafeCompare(v1, v2)
		assert.Equal(t, -1, cmp)
	}
	// v2 is null.
	{
		v1 := testVal(VarChar, "qq")
		v2 := NULL

		cmp := NullsafeCompare(v1, v2)
		assert.Equal(t, 1, cmp)
	}
	// v1 v2 are null.
	{
		v1 := NULL
		v2 := NULL

		cmp := NullsafeCompare(v1, v2)
		assert.Equal(t, 0, cmp)
	}

	// uint64.
	{
		v1 := testVal(Uint24, "5")
		v2 := testVal(Float64, "-5.1")
		v3 := testVal(VarChar, "a")

		cmp := NullsafeCompare(v1, v2)
		assert.Equal(t, 1, cmp)

		cmp = NullsafeCompare(v2, v3)
		assert.Equal(t, -1, cmp)

		cmp = NullsafeCompare(v1, v3)
		assert.Equal(t, 1, cmp)
	}
}

func TestAdd(t *testing.T) {
	tcases := []struct {
		v1, v2 Value
		out    Value
		typ    querypb.Type
		err    string
	}{{
		// All nulls.
		v1:  NULL,
		v2:  NULL,
		out: NULL,
	}, {
		// First value null.
		v1:  NewInt64(1),
		v2:  NULL,
		typ: Int64,
		out: NewInt64(1),
	}, {
		// Second value null.
		v1:  NULL,
		v2:  NewInt64(1),
		typ: Int64,
		out: NewInt64(1),
	}, {
		// Make sure underlying error is returned for LHS.
		v1:  testVal(Int64, "1.2"),
		v2:  NewInt64(2),
		typ: Int64,
		err: "strconv.ParseInt: parsing \"1.2\": invalid syntax",
	}, {
		// Make sure underlying error is returned for RHS.
		v1:  NewInt64(2),
		v2:  testVal(Int64, "1.2"),
		typ: Int64,
		err: "strconv.ParseInt: parsing \"1.2\": invalid syntax",
	}, {
		// Make sure underlying error is returned while adding.
		v1:  NewInt64(-2),
		v2:  NewUint64(1),
		typ: Uint64,
		err: "BIGINT.UNSIGNED.value.is.out.of.range.in: '1 + -2'",
	}, {
		v1:  testVal(Decimal, "1.797693134862315708145274237317043567981e+308"),
		v2:  testVal(Decimal, "1.797693134862315708145274237317043567981e+308"),
		typ: Decimal,
		err: "DOUBLE.value.is.out.of.range",
	}, {
		v1:  NewInt64(3),
		v2:  NewFloat64(2.2),
		typ: Float64,
		out: NewFloat64(5.2),
	}, {
		v1:  NewUint64(3),
		v2:  NewFloat64(2),
		typ: Float64,
		out: NewFloat64(5),
	}, {
		v1:  testVal(Decimal, "3"),
		v2:  NewFloat64(2),
		typ: Float64,
		out: NewFloat64(5),
	}, {
		v1:  NewFloat64(1),
		v2:  NewFloat64(2),
		typ: Float64,
		out: NewFloat64(3),
	}, {
		v1:  testVal(Decimal, "1.2"),
		v2:  testVal(Decimal, "2.1"),
		typ: Decimal,
		out: testVal(Decimal, "3.3"),
	}, {
		v1:  testVal(Decimal, "1.2"),
		v2:  NewFloat64(2.1),
		typ: Float64,
		out: testVal(Float64, "3.3"),
	}, {
		v1:  testVal(Decimal, "1.2"),
		v2:  NewUint64(1),
		typ: Decimal,
		out: testVal(Decimal, "2.2"),
	}, {
		v1:  testVal(Decimal, "1.2"),
		v2:  NewInt64(-1),
		typ: Decimal,
		out: testVal(Decimal, "0.2"),
	}, {
		v1:  NewInt64(3),
		v2:  NewInt64(2),
		typ: Int64,
		out: NewInt64(5),
	}, {
		v1:  NewUint64(3),
		v2:  NewInt64(2),
		typ: Uint64,
		out: NewUint64(5),
	}, {
		v1:  testVal(Decimal, "3"),
		v2:  NewInt64(2),
		typ: Decimal,
		out: testVal(Decimal, "5"),
	}, {
		v1:  NewFloat64(3),
		v2:  NewInt64(2),
		typ: Float64,
		out: NewFloat64(5),
	}, {
		v1:  NewInt64(3),
		v2:  NewUint64(2),
		typ: Uint64,
		out: NewUint64(5),
	}, {
		v1:  NewUint64(3),
		v2:  NewUint64(2),
		typ: Uint64,
		out: NewUint64(5),
	}, {
		v1:  testVal(Decimal, "3"),
		v2:  NewUint64(2),
		typ: Decimal,
		out: testVal(Decimal, "5"),
	}, {
		v1:  NewFloat64(3),
		v2:  NewUint64(2),
		typ: Float64,
		out: NewFloat64(5),
	}, {
		v1:  testVal(Datetime, "1000-01-01 00:00:00.00"),
		v2:  NewUint64(200),
		typ: Decimal,
		out: testVal(Decimal, "10000101000200"),
	}, {
		v1:  testVal(Date, "1000-01-01"),
		v2:  NewUint64(200),
		typ: Uint64,
		out: NewUint64(10000301),
	}, {
		v1:  NewInt64(9223372036854775807),
		v2:  NewInt64(9223372036854775807),
		typ: Int64,
		err: "BIGINT.value.is.out.of.range.in: '9223372036854775807 + 9223372036854775807'",
	}}
	for _, tcase := range tcases {
		got, errs := NullsafeAdd(tcase.v1, tcase.v2, tcase.typ, -1)
		if errs != nil {
			assert.Equal(t, tcase.err, errs.Error())
			continue
		}
		assert.Equal(t, tcase.out, got)
	}
}

func TestSum(t *testing.T) {
	tcases := []struct {
		v1, v2 Value
		out    Value
		typ    querypb.Type
		err    string
	}{{
		// First value null.
		v1:  NewInt64(1),
		v2:  NULL,
		typ: Decimal,
		out: NewInt64(1),
	}, {
		// Second value null.
		v1:  NULL,
		v2:  NewInt64(1),
		typ: Decimal,
		out: NewInt64(1),
	}, {
		v1:  NewInt64(9223372036854775807),
		v2:  NewInt64(9223372036854775807),
		typ: Decimal,
		out: testVal(Decimal, "18446744073709552000"),
	}, {
		// Make sure underlying error is returned for LHS.
		v1:  testVal(Int64, "1.2"),
		v2:  NewInt64(2),
		typ: Decimal,
		err: "strconv.ParseInt: parsing \"1.2\": invalid syntax",
	}, {
		// Make sure underlying error is returned for RHS.
		v1:  NewInt64(2),
		v2:  testVal(Int64, "1.2"),
		typ: Decimal,
		err: "strconv.ParseInt: parsing \"1.2\": invalid syntax",
	}, {
		// Make sure underlying error is returned while adding.
		v1:  NewFloat64(1.797693134862315708145274237317043567981e+308),
		v2:  NewFloat64(1.797693134862315708145274237317043567981e+308),
		typ: Float64,
		err: "DOUBLE.value.is.out.of.range.in: '1.7976931348623157e+308 + 1.7976931348623157e+308'",
	}, {
		v1:  testVal(Decimal, "1.797693134862315708145274237317043567981e+308"),
		v2:  testVal(Decimal, "1.797693134862315708145274237317043567981e+308"),
		typ: Decimal,
		err: "DOUBLE.value.is.out.of.range",
	}, {
		v1:  NewInt64(3),
		v2:  NewFloat64(2),
		typ: Float64,
		out: testVal(Float64, "5"),
	}, {
		v1:  NewUint64(3),
		v2:  NewFloat64(2),
		typ: Float64,
		out: testVal(Float64, "5"),
	}, {
		v1:  testVal(Decimal, "3"),
		v2:  NewFloat64(2),
		typ: Float64,
		out: testVal(Float64, "5"),
	}, {
		v1:  NewFloat64(3),
		v2:  NewFloat64(2),
		typ: Float64,
		out: testVal(Float64, "5"),
	}, {
		v1:  NewInt64(3),
		v2:  testVal(Decimal, "2"),
		typ: Decimal,
		out: testVal(Decimal, "5"),
	}, {
		v1:  NewUint64(3),
		v2:  testVal(Decimal, "2"),
		typ: Decimal,
		out: testVal(Decimal, "5"),
	}, {
		v1:  testVal(Decimal, "3"),
		v2:  testVal(Decimal, "2"),
		typ: Decimal,
		out: testVal(Decimal, "5"),
	}, {
		v1:  NewFloat64(3),
		v2:  testVal(Decimal, "2"),
		typ: Float64,
		out: testVal(Float64, "5"),
	}, {
		v1:  NewInt64(3),
		v2:  NewInt64(2),
		typ: Decimal,
		out: testVal(Decimal, "5"),
	}, {
		v1:  NewUint64(3),
		v2:  NewInt64(2),
		typ: Decimal,
		out: testVal(Decimal, "5"),
	}, {
		v1:  testVal(Decimal, "3"),
		v2:  NewInt64(2),
		typ: Decimal,
		out: testVal(Decimal, "5"),
	}, {
		v1:  NewFloat64(3),
		v2:  NewInt64(2),
		typ: Float64,
		out: testVal(Float64, "5"),
	}, {
		v1:  NewInt64(3),
		v2:  NewUint64(2),
		typ: Decimal,
		out: testVal(Decimal, "5"),
	}, {
		v1:  NewUint64(3),
		v2:  NewUint64(2),
		typ: Decimal,
		out: testVal(Decimal, "5"),
	}, {
		v1:  testVal(Decimal, "3"),
		v2:  NewUint64(2),
		typ: Decimal,
		out: testVal(Decimal, "5"),
	}, {
		v1:  NewFloat64(3),
		v2:  NewUint64(2),
		typ: Float64,
		out: testVal(Float64, "5"),
	}, {
		v1:  testVal(Datetime, "1000-01-01 00:00:00.00"),
		v2:  NewUint64(200),
		typ: Decimal,
		out: testVal(Decimal, "10000101000200"),
	}, {
		v1:  testVal(Date, "1000-01-01"),
		v2:  NewUint64(200),
		typ: Decimal,
		out: testVal(Decimal, "10000301"),
	}}
	for _, tcase := range tcases {
		got, errs := NullsafeSum(tcase.v1, tcase.v2, tcase.typ, -1)
		if errs != nil {
			assert.Equal(t, tcase.err, errs.Error())
			continue
		}
		assert.Equal(t, tcase.out, got)
	}
}

func TestDiv(t *testing.T) {
	tcases := []struct {
		v1, v2 Value
		out    Value
		typ    querypb.Type
		err    string
	}{{
		// First value null.
		v1:  NewInt64(1),
		v2:  NULL,
		typ: Decimal,
		out: NULL,
	}, {
		// Second value null.
		v1:  NULL,
		v2:  NewInt64(1),
		typ: Decimal,
		out: NULL,
	}, {
		// 0.
		v1:  NewInt64(1),
		v2:  NewInt64(0),
		typ: Decimal,
		out: NULL,
	}, {
		// Make sure underlying error is returned for LHS.
		v1:  testVal(Int64, "1.2"),
		v2:  NewInt64(2),
		typ: Decimal,
		err: "strconv.ParseInt: parsing \"1.2\": invalid syntax",
	}, {
		// Make sure underlying error is returned for RHS.
		v1:  NewInt64(2),
		v2:  testVal(Int64, "1.2"),
		typ: Decimal,
		err: "strconv.ParseInt: parsing \"1.2\": invalid syntax",
	}, {
		// Make sure underlying error is returned while adding.
		v1:  NewFloat64(1.797693134862315708145274237317043567981e+308),
		v2:  NewFloat64(0.2),
		typ: Float64,
		err: "DOUBLE.value.is.out.of.range.in: '1.7976931348623157e+308 / 0.2'",
	}, {
		v1:  testVal(Decimal, "1.797693134862315708145274237317043567981e+308"),
		v2:  testVal(Decimal, "0.2"),
		typ: Decimal,
		err: "DOUBLE.value.is.out.of.range",
	}, {
		v1:  NewInt64(3),
		v2:  NewFloat64(2),
		typ: Float64,
		out: testVal(Float64, "1.5000"),
	}, {
		v1:  NewUint64(3),
		v2:  NewFloat64(2),
		typ: Float64,
		out: testVal(Float64, "1.5000"),
	}, {
		v1:  testVal(Decimal, "3"),
		v2:  NewFloat64(2),
		typ: Float64,
		out: testVal(Float64, "1.5000"),
	}, {
		v1:  NewFloat64(3),
		v2:  NewFloat64(2),
		typ: Float64,
		out: testVal(Float64, "1.5000"),
	}, {
		v1:  NewInt64(3),
		v2:  testVal(Decimal, "2"),
		typ: Decimal,
		out: testVal(Decimal, "1.5000"),
	}, {
		v1:  NewUint64(3),
		v2:  testVal(Decimal, "2"),
		typ: Decimal,
		out: testVal(Decimal, "1.5000"),
	}, {
		v1:  testVal(Decimal, "3"),
		v2:  testVal(Decimal, "2"),
		typ: Decimal,
		out: testVal(Decimal, "1.5000"),
	}, {
		v1:  NewFloat64(3),
		v2:  testVal(Decimal, "2"),
		typ: Float64,
		out: testVal(Float64, "1.5000"),
	}, {
		v1:  NewInt64(3),
		v2:  NewInt64(2),
		typ: Decimal,
		out: testVal(Decimal, "1.5000"),
	}, {
		v1:  NewUint64(3),
		v2:  NewInt64(2),
		typ: Decimal,
		out: testVal(Decimal, "1.5000"),
	}, {
		v1:  testVal(Decimal, "3"),
		v2:  NewInt64(2),
		typ: Decimal,
		out: testVal(Decimal, "1.5000"),
	}, {
		v1:  NewFloat64(3),
		v2:  NewInt64(2),
		typ: Float64,
		out: testVal(Float64, "1.5000"),
	}, {
		v1:  NewInt64(3),
		v2:  NewUint64(2),
		typ: Decimal,
		out: testVal(Decimal, "1.5000"),
	}, {
		v1:  NewUint64(3),
		v2:  NewUint64(2),
		typ: Decimal,
		out: testVal(Decimal, "1.5000"),
	}, {
		v1:  testVal(Decimal, "3"),
		v2:  NewUint64(2),
		typ: Decimal,
		out: testVal(Decimal, "1.5000"),
	}, {
		v1:  NewFloat64(3),
		v2:  NewUint64(2),
		typ: Float64,
		out: testVal(Float64, "1.5000"),
	}, {
		v1:  testVal(Datetime, "1000-01-01 00:00:00.00"),
		v2:  NewUint64(200),
		typ: Decimal,
		out: testVal(Decimal, "50000505000.0000"),
	}, {
		v1:  testVal(Date, "1000-01-01"),
		v2:  NewUint64(200),
		typ: Decimal,
		out: testVal(Decimal, "50000.5050"),
	}}
	for _, tcase := range tcases {
		got, errs := NullsafeDiv(tcase.v1, tcase.v2, tcase.typ, 4)
		if errs != nil {
			assert.Equal(t, tcase.err, errs.Error())
			continue
		}
		assert.Equal(t, tcase.out, got)
	}
}

func TestNullsafeCompare(t *testing.T) {
	tcases := []struct {
		v1, v2 Value
		out    int
		err    string
	}{{
		// All nulls.
		v1:  NULL,
		v2:  NULL,
		out: 0,
	}, {
		// LHS null.
		v1:  NULL,
		v2:  NewInt64(1),
		out: -1,
	}, {
		// RHS null.
		v1:  NewInt64(1),
		v2:  NULL,
		out: 1,
	}, {
		// Numeric equal.
		v1:  NewInt64(1),
		v2:  NewUint64(1),
		out: 0,
	}, {
		// Numeric unequal.
		v1:  NewInt64(1),
		v2:  NewUint64(2),
		out: -1,
	}, {
		v1:  NewInt64(1),
		v2:  testVal(Decimal, "1.0"),
		out: 0,
	}, {
		v1:  testVal(Decimal, "1.0"),
		v2:  NewUint64(2),
		out: -1,
	}, {
		v1:  testVal(Decimal, "1.0"),
		v2:  NewFloat64(2),
		out: -1,
	}, {
		// Non-numeric equal
		v1:  testVal(VarBinary, "abcd"),
		v2:  testVal(Binary, "abcd"),
		out: 0,
	}, {
		// Non-numeric unequal
		v1:  testVal(VarBinary, "abcd"),
		v2:  testVal(Binary, "bcde"),
		out: -1,
	}, {
		// Date/Time types
		v1:  testVal(Datetime, "1000-01-01 00:00:00"),
		v2:  testVal(Binary, "1000-01-01 00:00:00"),
		out: 0,
	}, {
		// Date/Time types
		v1:  testVal(Datetime, "2000-01-01 00:00:00"),
		v2:  testVal(Binary, "1000-01-01 00:00:00"),
		out: 1,
	}, {
		// Date/Time types
		v1:  testVal(Datetime, "1000-01-01 00:00:00"),
		v2:  testVal(Binary, "2000-01-01 00:00:00"),
		out: -1,
	}, {
		// Date/Time types
		v1:  testVal(Datetime, "1000-01-01 00:00:00.00"),
		v2:  testVal(Decimal, "10000101000000.00"),
		out: 0,
	}, {
		// Date/Time types
		v1:  testVal(Date, "1000-01-01"),
		v2:  testVal(Int64, "10000101"),
		out: 0,
	}}
	for _, tcase := range tcases {
		got := NullsafeCompare(tcase.v1, tcase.v2)
		assert.Equal(t, tcase.out, got)
	}
}

func TestCast(t *testing.T) {
	tcases := []struct {
		typ querypb.Type
		v   Value
		out Value
		err string
	}{{
		typ: VarChar,
		v:   NULL,
		out: NULL,
	}, {
		typ: VarChar,
		v:   testVal(VarChar, "exact types"),
		out: testVal(VarChar, "exact types"),
	}, {
		typ: Int64,
		v:   testVal(Int32, "32"),
		out: testVal(Int64, "32"),
	}, {
		typ: Int24,
		v:   testVal(Uint64, "64"),
		out: testVal(Int24, "64"),
	}, {
		typ: Int24,
		v:   testVal(VarChar, "bad int"),
		err: `strconv.ParseInt: parsing "bad int": invalid syntax`,
	}, {
		typ: Uint64,
		v:   testVal(Uint32, "32"),
		out: testVal(Uint64, "32"),
	}, {
		typ: Uint24,
		v:   testVal(Int64, "64"),
		out: testVal(Uint24, "64"),
	}, {
		typ: Uint24,
		v:   testVal(Int64, "-1"),
		err: `strconv.ParseUint: parsing "-1": invalid syntax`,
	}, {
		typ: Float64,
		v:   testVal(Int64, "64"),
		out: testVal(Float64, "64"),
	}, {
		typ: Float32,
		v:   testVal(Float64, "64"),
		out: testVal(Float32, "64"),
	}, {
		typ: Float32,
		v:   testVal(Decimal, "1.24"),
		out: testVal(Float32, "1.24"),
	}, {
		typ: Decimal,
		v:   testVal(Float32, "1.24"),
		out: testVal(Decimal, "1.24"),
	}, {
		typ: Float64,
		v:   testVal(VarChar, "1.25"),
		out: testVal(Float64, "1.25"),
	}, {
		typ: Float64,
		v:   testVal(VarChar, "bad float"),
		err: `strconv.ParseFloat: parsing "bad float": invalid syntax`,
	}, {
		typ: VarChar,
		v:   testVal(Int64, "64"),
		out: testVal(VarChar, "64"),
	}, {
		typ: VarBinary,
		v:   testVal(Float64, "64"),
		out: testVal(VarBinary, "64"),
	}, {
		typ: VarBinary,
		v:   testVal(Decimal, "1.24"),
		out: testVal(VarBinary, "1.24"),
	}, {
		typ: VarBinary,
		v:   testVal(VarChar, "1.25"),
		out: testVal(VarBinary, "1.25"),
	}, {
		typ: VarChar,
		v:   testVal(VarBinary, "valid string"),
		out: testVal(VarChar, "valid string"),
	}, {
		typ: VarChar,
		v:   testVal(Expression, "bad string"),
		err: "bad string cannot be cast to VARCHAR",
	}}
	for _, tcase := range tcases {
		got, errs := Cast(tcase.v, tcase.typ)
		if errs != nil {
			assert.Equal(t, tcase.err, errs.Error())
			continue
		}
		assert.Equal(t, tcase.out, got)
	}
}

func TestNewNumeric(t *testing.T) {
	tcases := []struct {
		v   Value
		out numeric
		err string
	}{{
		v:   NewInt64(1),
		out: numeric{typ: Int64, ival: 1},
	}, {
		v:   NewUint64(1),
		out: numeric{typ: Uint64, uval: 1},
	}, {
		v:   NewFloat64(1),
		out: numeric{typ: Float64, fval: 1},
	}, {
		v:   testVal(Decimal, "1.2"),
		out: numeric{typ: Decimal, dval: decimal.NewFromFloat(1.2)},
	}, {
		// For non-number type, Int64 is the default.
		v:   testVal(VarChar, "1"),
		out: numeric{typ: Float64, fval: 1},
	}, {
		// If Int64 can't work, we use Float64.
		v:   testVal(VarChar, "1.2"),
		out: numeric{typ: Float64, fval: 1.2},
	}, {
		// Only valid Int64 allowed if type is Int64.
		v:   testVal(Int64, "1.2"),
		err: "strconv.ParseInt: parsing \"1.2\": invalid syntax",
	}, {
		// Only valid Uint64 allowed if type is Uint64.
		v:   testVal(Uint64, "1.2"),
		err: "strconv.ParseUint: parsing \"1.2\": invalid syntax",
	}, {
		// Only valid Float64 allowed if type is Float64.
		v:   testVal(Float64, "abcd"),
		err: "strconv.ParseFloat: parsing \"abcd\": invalid syntax",
	}, {
		v:   testVal(VarChar, "abcd"),
		out: numeric{typ: Int64, ival: 0},
	}}
	for _, tcase := range tcases {
		got, errs := newNumeric(tcase.v)
		if errs != nil {
			assert.Equal(t, tcase.err, errs.Error())
			continue
		}
		assert.Equal(t, tcase.out, got)
	}
}

func TestAddNumeric(t *testing.T) {
	tcases := []struct {
		v1, v2 numeric
		out    numeric
		err    error
	}{{
		v1:  numeric{typ: Int64, ival: 1},
		v2:  numeric{typ: Int64, ival: 2},
		out: numeric{typ: Int64, ival: 3},
	}, {
		v1:  numeric{typ: Int64, ival: 1},
		v2:  numeric{typ: Uint64, uval: 2},
		out: numeric{typ: Uint64, uval: 3},
	}, {
		v1:  numeric{typ: Int64, ival: 1},
		v2:  numeric{typ: Float64, fval: 2},
		out: numeric{typ: Float64, fval: 3},
	}, {
		v1:  numeric{typ: Uint64, uval: 1},
		v2:  numeric{typ: Uint64, uval: 2},
		out: numeric{typ: Uint64, uval: 3},
	}, {
		v1:  numeric{typ: Uint64, uval: 1},
		v2:  numeric{typ: Float64, fval: 2},
		out: numeric{typ: Float64, fval: 3},
	}, {
		v1:  numeric{typ: Float64, fval: 1},
		v2:  numeric{typ: Float64, fval: 2},
		out: numeric{typ: Float64, fval: 3},
	}, {
		v1:  numeric{typ: Int64, ival: 1},
		v2:  numeric{typ: Decimal, dval: decimal.NewFromFloat(2)},
		out: numeric{typ: Decimal, dval: decimal.NewFromFloat(3)},
	}, {
		v1:  numeric{typ: Uint64, uval: 1},
		v2:  numeric{typ: Decimal, dval: decimal.NewFromFloat(2)},
		out: numeric{typ: Decimal, dval: decimal.NewFromFloat(3)},
	}, {
		v1:  numeric{typ: Decimal, dval: decimal.NewFromFloat(1)},
		v2:  numeric{typ: Decimal, dval: decimal.NewFromFloat(2)},
		out: numeric{typ: Decimal, dval: decimal.NewFromFloat(3)},
	}, {
		v1:  numeric{typ: Float64, fval: 1},
		v2:  numeric{typ: Decimal, dval: decimal.NewFromFloat(2)},
		out: numeric{typ: Float64, fval: 3},
	}, {
		// Int64 overflow.
		v1:  numeric{typ: Int64, ival: 9223372036854775807},
		v2:  numeric{typ: Int64, ival: 2},
		err: fmt.Errorf("BIGINT.value.is.out.of.range.in: '9223372036854775807 + 2'"),
	}, {
		// Int64 underflow.
		v1:  numeric{typ: Int64, ival: -9223372036854775807},
		v2:  numeric{typ: Int64, ival: -2},
		err: fmt.Errorf("BIGINT.value.is.out.of.range.in: '-9223372036854775807 + -2'"),
	}, {
		v1:  numeric{typ: Int64, ival: -1},
		v2:  numeric{typ: Uint64, uval: 2},
		out: numeric{typ: Uint64, uval: 1},
	}, {
		// Uint64 overflow.
		v1:  numeric{typ: Uint64, uval: 18446744073709551615},
		v2:  numeric{typ: Uint64, uval: 2},
		err: fmt.Errorf("BIGINT.UNSIGNED.value.is.out.of.range.in: '18446744073709551615 + 2'"),
	}, {
		// Float64 overflow.
		v1:  numeric{typ: Float64, fval: 1.797693134862315708145274237317043567981e+308},
		v2:  numeric{typ: Float64, fval: 1.797693134862315708145274237317043567981e+308},
		err: fmt.Errorf("DOUBLE.value.is.out.of.range.in: '1.7976931348623157e+308 + 1.7976931348623157e+308'"),
	}}
	for _, tcase := range tcases {
		got, err := addNumeric(tcase.v1, tcase.v2)
		if err != nil {
			assert.Equal(t, tcase.err, err)
			continue
		}
		assert.Equal(t, tcase.out, got)
	}
}

func TestPrioritize(t *testing.T) {
	ival := numeric{typ: Int64}
	uval := numeric{typ: Uint64}
	fval := numeric{typ: Float64}
	dval := numeric{typ: Decimal}

	tcases := []struct {
		v1, v2     numeric
		out1, out2 numeric
	}{{
		v1:   ival,
		v2:   uval,
		out1: uval,
		out2: ival,
	}, {
		v1:   ival,
		v2:   fval,
		out1: fval,
		out2: ival,
	}, {
		v1:   uval,
		v2:   ival,
		out1: uval,
		out2: ival,
	}, {
		v1:   uval,
		v2:   fval,
		out1: fval,
		out2: uval,
	}, {
		v1:   fval,
		v2:   ival,
		out1: fval,
		out2: ival,
	}, {
		v1:   fval,
		v2:   uval,
		out1: fval,
		out2: uval,
	}, {
		v1:   dval,
		v2:   fval,
		out1: fval,
		out2: dval,
	}, {
		v1:   ival,
		v2:   dval,
		out1: dval,
		out2: ival,
	}, {
		v1:   uval,
		v2:   dval,
		out1: dval,
		out2: uval,
	}}
	for _, tcase := range tcases {
		got1, got2 := prioritize(tcase.v1, tcase.v2)
		if got1 != tcase.out1 || got2 != tcase.out2 {
			t.Errorf("prioritize(%v, %v): (%v, %v) , want (%v, %v)", tcase.v1.typ, tcase.v2.typ, got1.typ, got2.typ, tcase.out1.typ, tcase.out2.typ)
		}
	}
}

func TestCastFromNumeric(t *testing.T) {
	tcases := []struct {
		typ querypb.Type
		v   numeric
		out Value
		err error
	}{{
		typ: Int64,
		v:   numeric{typ: Int64, ival: 1},
		out: NewInt64(1),
	}, {
		typ: Int64,
		v:   numeric{typ: Uint64, uval: 1},
		err: fmt.Errorf("unexpected type conversion: UINT64 to INT64"),
	}, {
		typ: Int64,
		v:   numeric{typ: Float64, fval: 1.2e-16},
		err: fmt.Errorf("unexpected type conversion: FLOAT64 to INT64"),
	}, {
		typ: Uint64,
		v:   numeric{typ: Int64, ival: 1},
		err: fmt.Errorf("unexpected type conversion: INT64 to UINT64"),
	}, {
		typ: Uint64,
		v:   numeric{typ: Uint64, uval: 1},
		out: NewUint64(1),
	}, {
		typ: Uint64,
		v:   numeric{typ: Float64, fval: 1.2e-16},
		err: fmt.Errorf("unexpected type conversion: FLOAT64 to UINT64"),
	}, {
		typ: Float64,
		v:   numeric{typ: Int64, ival: 1},
		out: testVal(Float64, "1"),
	}, {
		typ: Float64,
		v:   numeric{typ: Uint64, uval: 1},
		out: testVal(Float64, "1"),
	}, {
		typ: Float64,
		v:   numeric{typ: Float64, fval: 1.2e-16},
		out: testVal(Float64, "1.2e-16"),
	}, {
		typ: Decimal,
		v:   numeric{typ: Int64, ival: 1},
		out: testVal(Decimal, "1"),
	}, {
		typ: Decimal,
		v:   numeric{typ: Uint64, uval: 1},
		out: testVal(Decimal, "1"),
	}, {
		// For float, we should not use scientific notation.
		typ: Decimal,
		v:   numeric{typ: Float64, fval: 1.2e-16},
		out: testVal(Decimal, "0.00000000000000012"),
	}, {
		typ: VarBinary,
		v:   numeric{typ: Int64, ival: 1},
		err: fmt.Errorf("unexpected type conversion to non-numeric: VARBINARY"),
	}}
	for _, tcase := range tcases {
		got, err := castFromNumeric(tcase.v, tcase.typ, -1)
		if err != nil {
			assert.Equal(t, tcase.err, err)
			continue
		}
		assert.Equal(t, tcase.out, got)
	}
}

func TestCompareNumeric(t *testing.T) {
	tcases := []struct {
		v1, v2 numeric
		out    int
	}{{
		v1:  numeric{typ: Int64, ival: 1},
		v2:  numeric{typ: Int64, ival: 1},
		out: 0,
	}, {
		v1:  numeric{typ: Int64, ival: 1},
		v2:  numeric{typ: Int64, ival: 2},
		out: -1,
	}, {
		v1:  numeric{typ: Int64, ival: 2},
		v2:  numeric{typ: Int64, ival: 1},
		out: 1,
	}, {
		// Special case.
		v1:  numeric{typ: Int64, ival: -1},
		v2:  numeric{typ: Uint64, uval: 1},
		out: -1,
	}, {
		v1:  numeric{typ: Int64, ival: 1},
		v2:  numeric{typ: Uint64, uval: 1},
		out: 0,
	}, {
		v1:  numeric{typ: Int64, ival: 1},
		v2:  numeric{typ: Uint64, uval: 2},
		out: -1,
	}, {
		v1:  numeric{typ: Int64, ival: 2},
		v2:  numeric{typ: Uint64, uval: 1},
		out: 1,
	}, {
		v1:  numeric{typ: Int64, ival: 1},
		v2:  numeric{typ: Float64, fval: 1},
		out: 0,
	}, {
		v1:  numeric{typ: Int64, ival: 1},
		v2:  numeric{typ: Float64, fval: 2},
		out: -1,
	}, {
		v1:  numeric{typ: Int64, ival: 2},
		v2:  numeric{typ: Float64, fval: 1},
		out: 1,
	}, {
		// Special case.
		v1:  numeric{typ: Uint64, uval: 1},
		v2:  numeric{typ: Int64, ival: -1},
		out: 1,
	}, {
		v1:  numeric{typ: Uint64, uval: 1},
		v2:  numeric{typ: Int64, ival: 1},
		out: 0,
	}, {
		v1:  numeric{typ: Uint64, uval: 1},
		v2:  numeric{typ: Int64, ival: 2},
		out: -1,
	}, {
		v1:  numeric{typ: Uint64, uval: 2},
		v2:  numeric{typ: Int64, ival: 1},
		out: 1,
	}, {
		v1:  numeric{typ: Uint64, uval: 1},
		v2:  numeric{typ: Uint64, uval: 1},
		out: 0,
	}, {
		v1:  numeric{typ: Uint64, uval: 1},
		v2:  numeric{typ: Uint64, uval: 2},
		out: -1,
	}, {
		v1:  numeric{typ: Uint64, uval: 2},
		v2:  numeric{typ: Uint64, uval: 1},
		out: 1,
	}, {
		v1:  numeric{typ: Uint64, uval: 1},
		v2:  numeric{typ: Float64, fval: 1},
		out: 0,
	}, {
		v1:  numeric{typ: Uint64, uval: 1},
		v2:  numeric{typ: Float64, fval: 2},
		out: -1,
	}, {
		v1:  numeric{typ: Uint64, uval: 2},
		v2:  numeric{typ: Float64, fval: 1},
		out: 1,
	}, {
		v1:  numeric{typ: Float64, fval: 1},
		v2:  numeric{typ: Int64, ival: 1},
		out: 0,
	}, {
		v1:  numeric{typ: Float64, fval: 1},
		v2:  numeric{typ: Int64, ival: 2},
		out: -1,
	}, {
		v1:  numeric{typ: Float64, fval: 2},
		v2:  numeric{typ: Int64, ival: 1},
		out: 1,
	}, {
		v1:  numeric{typ: Float64, fval: 1},
		v2:  numeric{typ: Uint64, uval: 1},
		out: 0,
	}, {
		v1:  numeric{typ: Float64, fval: 1},
		v2:  numeric{typ: Uint64, uval: 2},
		out: -1,
	}, {
		v1:  numeric{typ: Float64, fval: 2},
		v2:  numeric{typ: Uint64, uval: 1},
		out: 1,
	}, {
		v1:  numeric{typ: Float64, fval: 1},
		v2:  numeric{typ: Float64, fval: 1},
		out: 0,
	}, {
		v1:  numeric{typ: Float64, fval: 1},
		v2:  numeric{typ: Float64, fval: 2},
		out: -1,
	}, {
		v1:  numeric{typ: Float64, fval: 2},
		v2:  numeric{typ: Float64, fval: 1},
		out: 1,
	}}
	for _, tcase := range tcases {
		got := compareNumeric(tcase.v1, tcase.v2)
		if got != tcase.out {
			t.Errorf("equalNumeric(%v, %v): %v, want %v", tcase.v1, tcase.v2, got, tcase.out)
		}
	}
}

func TestMin(t *testing.T) {
	tcases := []struct {
		v1, v2 Value
		min    Value
	}{{
		v1:  NULL,
		v2:  NULL,
		min: NULL,
	}, {
		v1:  NewInt64(1),
		v2:  NULL,
		min: NewInt64(1),
	}, {
		v1:  NULL,
		v2:  NewInt64(1),
		min: NewInt64(1),
	}, {
		v1:  NewInt64(1),
		v2:  NewInt64(2),
		min: NewInt64(1),
	}, {
		v1:  NewInt64(2),
		v2:  NewInt64(1),
		min: NewInt64(1),
	}, {
		v1:  NewInt64(1),
		v2:  NewInt64(1),
		min: NewInt64(1),
	}, {
		v1:  testVal(VarChar, "aa"),
		v2:  testVal(VarChar, "aa"),
		min: testVal(VarChar, "aa"),
	}}
	for _, tcase := range tcases {
		v := Min(tcase.v1, tcase.v2)

		if !reflect.DeepEqual(v, tcase.min) {
			t.Errorf("Min(%v, %v): %v, want %v", tcase.v1, tcase.v2, v, tcase.min)
		}
	}
}

func TestMax(t *testing.T) {
	tcases := []struct {
		v1, v2 Value
		max    Value
	}{{
		v1:  NULL,
		v2:  NULL,
		max: NULL,
	}, {
		v1:  NewInt64(1),
		v2:  NULL,
		max: NewInt64(1),
	}, {
		v1:  NULL,
		v2:  NewInt64(1),
		max: NewInt64(1),
	}, {
		v1:  NewInt64(1),
		v2:  NewInt64(2),
		max: NewInt64(2),
	}, {
		v1:  NewInt64(2),
		v2:  NewInt64(1),
		max: NewInt64(2),
	}, {
		v1:  NewInt64(1),
		v2:  NewInt64(1),
		max: NewInt64(1),
	}, {
		v1:  testVal(VarChar, "aa"),
		v2:  testVal(VarChar, "aa"),
		max: testVal(VarChar, "aa"),
	}}
	for _, tcase := range tcases {
		v := Max(tcase.v1, tcase.v2)

		if !reflect.DeepEqual(v, tcase.max) {
			t.Errorf("Max(%v, %v): %v, want %v", tcase.v1, tcase.v2, v, tcase.max)
		}
	}
}

func TestCastToBool(t *testing.T) {
	tcases := []struct {
		v   Value
		out bool
	}{{
		v:   NewInt64(12),
		out: true,
	}, {
		v:   NewInt64(0),
		out: false,
	}, {
		v:   NewUint64(1),
		out: true,
	}, {
		v:   NewUint64(0),
		out: false,
	}, {
		v:   NewFloat64(1),
		out: true,
	}, {
		v:   NewFloat64(0),
		out: false,
	}, {
		v:   testVal(Decimal, "0"),
		out: false,
	}, {
		v:   testVal(Decimal, "1.2"),
		out: true,
	}, {
		v:   testVal(VarChar, "1"),
		out: true,
	}, {
		v:   testVal(VarChar, "0"),
		out: false,
	}, {
		v:   testVal(VarChar, "1.2"),
		out: true,
	}, {
		v:   testVal(VarChar, "abcd"),
		out: false,
	}, {
		v:   NULL,
		out: false,
	}, {
		v:   testVal(Uint64, "1.2"),
		out: false,
	}}

	for _, tcase := range tcases {
		got := CastToBool(tcase.v)
		assert.Equal(t, tcase.out, got)
	}
}
