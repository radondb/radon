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
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func mockField(typ ResultType, scale int, isUnsigned, isBinary, isConstant bool) *IField {
	return &IField{
		Type:       typ,
		Scale:      scale,
		Length:     -1,
		IsUnsigned: isUnsigned,
		IsBinary:   isBinary,
		IsConstant: isConstant,
	}
}

func TestAdd(t *testing.T) {
	tcases := []struct {
		v1     Datum
		v2     Datum
		field  *IField
		resTyp Type
		resStr string
		err    string
	}{
		{
			v1:     NewDNull(true),
			v2:     NewDInt(1, false),
			field:  mockField(IntResult, 0, false, true, false),
			resTyp: TypeNull,
			resStr: "NULL",
		},
		{
			v1:     NewDInt(1, false),
			v2:     NewDInt(1, false),
			field:  mockField(IntResult, 0, false, true, false),
			resTyp: TypeInt,
			resStr: "2",
		},
		{
			// int64(math.UInt64).
			v1:    NewDInt(-1, true),
			v2:    NewDInt(2, true),
			field: mockField(IntResult, 0, true, true, false),
			err:   "BIGINT.UNSIGNED.value.is.out.of.range.in: '18446744073709551615' + '2'",
		},
		{
			v1:    NewDInt(-2, false),
			v2:    NewDInt(1, true),
			field: mockField(IntResult, 0, true, true, false),
			err:   "BIGINT.UNSIGNED.value.is.out.of.range.in: '-2' + '1'",
		},
		{
			v1:    NewDInt(1, true),
			v2:    NewDInt(-2, false),
			field: mockField(IntResult, 0, true, true, false),
			err:   "BIGINT.UNSIGNED.value.is.out.of.range.in: '1' + '-2'",
		},
		{
			// int64(math.UInt64).
			v1:    NewDInt(-1, true),
			v2:    NewDInt(2, false),
			field: mockField(IntResult, 0, true, true, false),
			err:   "BIGINT.UNSIGNED.value.is.out.of.range.in: '18446744073709551615' + '2'",
		},
		{
			v1:    NewDInt(2, false),
			v2:    NewDInt(-1, true),
			field: mockField(IntResult, 0, true, true, false),
			err:   "BIGINT.UNSIGNED.value.is.out.of.range.in: '2' + '18446744073709551615'",
		},
		{
			v1:    NewDInt(math.MaxInt64, false),
			v2:    NewDInt(1, false),
			field: mockField(IntResult, 0, false, true, false),
			err:   "BIGINT.value.is.out.of.range.in: '9223372036854775807' + '1'",
		},
		{
			v1:     NewDDecimal(decimal.NewFromFloat(1.23)),
			v2:     NewDDecimal(decimal.NewFromFloat(2.77)),
			field:  mockField(DecimalResult, 2, false, true, false),
			resTyp: TypeDecimal,
			resStr: "4",
		},
		{
			v1:    NewDDecimal(decimal.NewFromFloat(math.MaxFloat64)),
			v2:    NewDDecimal(decimal.NewFromFloat(math.MaxFloat64)),
			field: mockField(DecimalResult, 2, false, true, false),
			err:   "DOUBLE.value.is.out.of.range.in: '179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000' + '179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000'",
		},
		{
			v1:     NewDFloat(1.23),
			v2:     NewDDecimal(decimal.NewFromFloat(2.77)),
			field:  mockField(RealResult, NotFixedDec, false, true, false),
			resTyp: TypeFloat,
			resStr: "4",
		},
		{
			v1:    NewDFloat(math.MaxFloat64),
			v2:    NewDDecimal(decimal.NewFromFloat(math.MaxFloat64)),
			field: mockField(RealResult, NotFixedDec, false, true, false),
			err:   "DOUBLE.value.is.out.of.range.in: '1.7976931348623157e+308' + '1.7976931348623157e+308'",
		},
	}

	for _, tcase := range tcases {
		res, err := Add(tcase.v1, tcase.v2, tcase.field)
		if err != nil {
			assert.Equal(t, tcase.err, err.Error())
		} else {
			assert.Equal(t, tcase.resTyp, res.Type())
			assert.Equal(t, tcase.resStr, res.ValStr())
		}
	}
}

func TestSub(t *testing.T) {
	tcases := []struct {
		v1     Datum
		v2     Datum
		field  *IField
		resTyp Type
		resStr string
		err    string
	}{
		{
			v1:     NewDNull(true),
			v2:     NewDInt(1, false),
			field:  mockField(IntResult, 0, false, true, false),
			resTyp: TypeNull,
			resStr: "NULL",
		},
		{
			v1:     NewDInt(2, false),
			v2:     NewDInt(1, false),
			field:  mockField(IntResult, 0, false, true, false),
			resTyp: TypeInt,
			resStr: "1",
		},
		{
			// int64(math.UInt64).
			v1:    NewDInt(1, true),
			v2:    NewDInt(2, true),
			field: mockField(IntResult, 0, true, true, false),
			err:   "BIGINT.UNSIGNED.value.is.out.of.range.in: '1' - '2'",
		},
		{
			v1: NewDInt(1, false),
			// int64(math.UInt64).
			v2:    NewDInt(-1, true),
			field: mockField(IntResult, 0, true, true, false),
			err:   "BIGINT.UNSIGNED.value.is.out.of.range.in: '1' - '18446744073709551615'",
		},
		{
			v1:    NewDInt(1, true),
			v2:    NewDInt(2, false),
			field: mockField(IntResult, 0, true, true, false),
			err:   "BIGINT.UNSIGNED.value.is.out.of.range.in: '1' - '2'",
		},
		{
			// int64(math.UInt64).
			v1:    NewDInt(-1, true),
			v2:    NewDInt(-1, false),
			field: mockField(IntResult, 0, true, true, false),
			err:   "BIGINT.UNSIGNED.value.is.out.of.range.in: '18446744073709551615' - '-1'",
		},
		{
			v1:    NewDInt(1, false),
			v2:    NewDInt(math.MinInt64, false),
			field: mockField(IntResult, 0, false, true, false),
			err:   "BIGINT.value.is.out.of.range.in: '1' - '-9223372036854775808'",
		},
		{
			v1:    NewDInt(-2, false),
			v2:    NewDInt(math.MaxInt64, false),
			field: mockField(IntResult, 0, false, true, false),
			err:   "BIGINT.value.is.out.of.range.in: '-2' - '9223372036854775807'",
		},
		{
			v1:     NewDDecimal(decimal.NewFromFloat(1.23)),
			v2:     NewDDecimal(decimal.NewFromFloat(2.77)),
			field:  mockField(DecimalResult, 2, false, true, false),
			resTyp: TypeDecimal,
			resStr: "-1.54",
		},
		{
			v1:    NewDDecimal(decimal.NewFromFloat(math.MaxFloat64)),
			v2:    NewDDecimal(decimal.NewFromFloat(-math.MaxFloat64)),
			field: mockField(DecimalResult, 2, false, true, false),
			err:   "DOUBLE.value.is.out.of.range.in: '179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000' - '-179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000'",
		},
		{
			v1:     NewDFloat(1.23),
			v2:     NewDDecimal(decimal.NewFromFloat(2.77)),
			field:  mockField(RealResult, NotFixedDec, false, true, false),
			resTyp: TypeFloat,
			resStr: "-1.54",
		},
		{
			v1:    NewDFloat(math.MaxFloat64),
			v2:    NewDDecimal(decimal.NewFromFloat(-math.MaxFloat64)),
			field: mockField(RealResult, NotFixedDec, false, true, false),
			err:   "DOUBLE.value.is.out.of.range.in: '1.7976931348623157e+308' - '-1.7976931348623157e+308'",
		},
	}

	for _, tcase := range tcases {
		res, err := Sub(tcase.v1, tcase.v2, tcase.field)
		if err != nil {
			assert.Equal(t, tcase.err, err.Error())
		} else {
			assert.Equal(t, tcase.resTyp, res.Type())
			assert.Equal(t, tcase.resStr, res.ValStr())
		}
	}
}

func TestMul(t *testing.T) {
	tcases := []struct {
		v1     Datum
		v2     Datum
		field  *IField
		resTyp Type
		resStr string
		err    string
	}{
		{
			v1:     NewDNull(true),
			v2:     NewDInt(1, false),
			field:  mockField(IntResult, 0, false, true, false),
			resTyp: TypeNull,
			resStr: "NULL",
		},
		{
			v1:     NewDInt(2, false),
			v2:     NewDInt(1, false),
			field:  mockField(IntResult, 0, false, true, false),
			resTyp: TypeInt,
			resStr: "2",
		},
		{
			v1:     NewDInt(2, true),
			v2:     NewDInt(1, false),
			field:  mockField(IntResult, 0, true, true, false),
			resTyp: TypeInt,
			resStr: "2",
		},
		{
			v1:     NewDInt(1, true),
			v2:     NewDInt(2, true),
			field:  mockField(IntResult, 0, true, true, false),
			resTyp: TypeInt,
			resStr: "2",
		},
		{
			v1:    NewDInt(math.MaxInt64, true),
			v2:    NewDInt(3, false),
			field: mockField(IntResult, 0, true, true, false),
			err:   "BIGINT.UNSIGNED.value.is.out.of.range.in: '9223372036854775807' * '3'",
		},
		{
			v1:    NewDInt(math.MaxInt64, false),
			v2:    NewDInt(3, false),
			field: mockField(IntResult, 0, false, true, false),
			err:   "BIGINT.value.is.out.of.range.in: '9223372036854775807' * '3'",
		},

		{
			v1:     NewDDecimal(decimal.NewFromFloat(1.23)),
			v2:     NewDDecimal(decimal.NewFromFloat(2.77)),
			field:  mockField(DecimalResult, 4, false, true, false),
			resTyp: TypeDecimal,
			resStr: "3.4071",
		},
		{
			// int64(math.UInt64).
			v1:    NewDInt(2, true),
			v2:    NewDDecimal(decimal.NewFromFloat(math.MaxFloat64)),
			field: mockField(DecimalResult, 0, false, true, false),
			err:   "DOUBLE.value.is.out.of.range.in: '2' * '179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000'",
		},
		{
			v1:     NewDFloat(1.23),
			v2:     NewDDecimal(decimal.NewFromFloat(2.77)),
			field:  mockField(RealResult, NotFixedDec, false, true, false),
			resTyp: TypeFloat,
			resStr: "3.4071",
		},
		{
			v1:    NewDFloat(math.MaxFloat64),
			v2:    NewDString("2", 10, false),
			field: mockField(RealResult, NotFixedDec, false, true, false),
			err:   "DOUBLE.value.is.out.of.range.in: '1.7976931348623157e+308' * '2'",
		},
	}

	for _, tcase := range tcases {
		res, err := Mul(tcase.v1, tcase.v2, tcase.field)
		if err != nil {
			assert.Equal(t, tcase.err, err.Error())
		} else {
			assert.Equal(t, tcase.resTyp, res.Type())
			assert.Equal(t, tcase.resStr, res.ValStr())
		}
	}
}

func TestDiv(t *testing.T) {
	tcases := []struct {
		v1     Datum
		v2     Datum
		field  *IField
		resTyp Type
		resStr string
		err    string
	}{
		{
			v1:     NewDNull(true),
			v2:     NewDInt(1, false),
			field:  mockField(IntResult, 0, false, true, false),
			resTyp: TypeNull,
			resStr: "NULL",
		},
		{
			v1:     NewDInt(2, false),
			v2:     NewDInt(1, false),
			field:  mockField(DecimalResult, 4, false, true, false),
			resTyp: TypeDecimal,
			resStr: "2",
		},
		{
			v1:     NewDInt(2, true),
			v2:     NewDInt(0, false),
			field:  mockField(DecimalResult, 0, true, true, false),
			resTyp: TypeNull,
			resStr: "NULL",
		},
		{
			v1:     NewDFloat(1),
			v2:     NewDInt(0, false),
			field:  mockField(RealResult, NotFixedDec, false, true, false),
			resTyp: TypeNull,
			resStr: "NULL",
		},
		{
			v1:     NewDFloat(1),
			v2:     NewDInt(2, false),
			field:  mockField(RealResult, NotFixedDec, false, true, false),
			resTyp: TypeFloat,
			resStr: "0.5",
		},
		{
			v1:    NewDDecimal(decimal.NewFromFloat(math.MaxFloat64)),
			v2:    NewDDecimal(decimal.NewFromFloat(0.5)),
			field: mockField(DecimalResult, 0, false, true, false),
			err:   "DOUBLE.value.is.out.of.range.in: '179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000' / '0.5'",
		},
		{
			v1:    NewDFloat(math.MaxFloat64),
			v2:    NewDString("0.5", 10, false),
			field: mockField(RealResult, 0, true, true, false),
			err:   "DOUBLE.value.is.out.of.range.in: '1.7976931348623157e+308' / '0.5'",
		},
	}

	for _, tcase := range tcases {
		res, err := Div(tcase.v1, tcase.v2, tcase.field)
		if err != nil {
			assert.Equal(t, tcase.err, err.Error())
		} else {
			assert.Equal(t, tcase.resTyp, res.Type())
			assert.Equal(t, tcase.resStr, res.ValStr())
		}
	}
}

func TestIntDiv(t *testing.T) {
	tcases := []struct {
		v1     Datum
		v2     Datum
		field  *IField
		resTyp Type
		resStr string
		err    string
	}{
		{
			v1:     NewDNull(true),
			v2:     NewDInt(1, false),
			field:  mockField(IntResult, 0, false, true, false),
			resTyp: TypeNull,
			resStr: "NULL",
		},
		{
			v1:     NewDInt(-1, true),
			v2:     NewDInt(1, false),
			field:  mockField(IntResult, 0, true, true, false),
			resTyp: TypeInt,
			resStr: "9223372036854775808",
		},
		{
			v1:     NewDInt(2, true),
			v2:     NewDInt(0, false),
			field:  mockField(IntResult, 0, true, true, false),
			resTyp: TypeNull,
			resStr: "NULL",
		},
		{
			v1:     NewDFloat(1),
			v2:     NewDInt(2, false),
			field:  mockField(IntResult, 0, false, true, false),
			resTyp: TypeInt,
			resStr: "0",
		},
		{
			v1:    NewDDecimal(decimal.NewFromFloat(math.MaxFloat64)),
			v2:    NewDDecimal(decimal.NewFromFloat(0.5)),
			field: mockField(IntResult, 0, false, true, false),
			err:   "BIGINT.value.is.out.of.range.in: '1.7976931348623157e+308' div '0.5'",
		},
		{
			v1:    NewDFloat(math.MaxFloat64),
			v2:    NewDString("0.5", 10, false),
			field: mockField(IntResult, 0, true, true, false),
			err:   "BIGINT.UNSIGNED.value.is.out.of.range.in: '1.7976931348623157e+308' div '0.5'",
		},
	}

	for _, tcase := range tcases {
		res, err := IntDiv(tcase.v1, tcase.v2, tcase.field)
		if err != nil {
			assert.Equal(t, tcase.err, err.Error())
		} else {
			assert.Equal(t, tcase.resTyp, res.Type())
			assert.Equal(t, tcase.resStr, res.ValStr())
		}
	}
}
