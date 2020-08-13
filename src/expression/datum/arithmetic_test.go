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
			field:  &IField{IntResult, 0, false, false},
			resTyp: TypeNull,
			resStr: "NULL",
		},
		{
			v1:     NewDInt(1, false),
			v2:     NewDInt(1, false),
			field:  &IField{IntResult, 0, false, false},
			resTyp: TypeInt,
			resStr: "2",
		},
		{
			// int64(math.UInt64).
			v1:    NewDInt(-1, true),
			v2:    NewDInt(2, true),
			field: &IField{IntResult, 0, true, false},
			err:   "BIGINT.UNSIGNED.value.is.out.of.range.in: '18446744073709551615' + '2'",
		},
		{
			v1:    NewDInt(-2, false),
			v2:    NewDInt(1, true),
			field: &IField{IntResult, 0, true, false},
			err:   "BIGINT.UNSIGNED.value.is.out.of.range.in: '-2' + '1'",
		},
		{
			v1:    NewDInt(1, true),
			v2:    NewDInt(-2, false),
			field: &IField{IntResult, 0, true, false},
			err:   "BIGINT.UNSIGNED.value.is.out.of.range.in: '1' + '-2'",
		},
		{
			// int64(math.UInt64).
			v1:    NewDInt(-1, true),
			v2:    NewDInt(2, false),
			field: &IField{IntResult, 0, true, false},
			err:   "BIGINT.UNSIGNED.value.is.out.of.range.in: '18446744073709551615' + '2'",
		},
		{
			v1:    NewDInt(2, false),
			v2:    NewDInt(-1, true),
			field: &IField{IntResult, 0, true, false},
			err:   "BIGINT.UNSIGNED.value.is.out.of.range.in: '2' + '18446744073709551615'",
		},
		{
			v1:    NewDInt(math.MaxInt64, false),
			v2:    NewDInt(1, false),
			field: &IField{IntResult, 0, false, false},
			err:   "BIGINT.value.is.out.of.range.in: '9223372036854775807' + '1'",
		},
		{
			v1:     NewDDecimal(decimal.NewFromFloat(1.23)),
			v2:     NewDDecimal(decimal.NewFromFloat(2.77)),
			field:  &IField{DecimalResult, 2, false, false},
			resTyp: TypeDecimal,
			resStr: "4",
		},
		{
			v1:    NewDDecimal(decimal.NewFromFloat(math.MaxFloat64)),
			v2:    NewDDecimal(decimal.NewFromFloat(math.MaxFloat64)),
			field: &IField{DecimalResult, 2, false, false},
			err:   "DOUBLE.value.is.out.of.range.in: '179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000' + '179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000'",
		},
		{
			v1:     NewDFloat(1.23),
			v2:     NewDDecimal(decimal.NewFromFloat(2.77)),
			field:  &IField{RealResult, NotFixedDec, false, false},
			resTyp: TypeFloat,
			resStr: "4",
		},
		{
			v1:    NewDFloat(math.MaxFloat64),
			v2:    NewDDecimal(decimal.NewFromFloat(math.MaxFloat64)),
			field: &IField{RealResult, NotFixedDec, false, false},
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
			field:  &IField{IntResult, 0, false, false},
			resTyp: TypeNull,
			resStr: "NULL",
		},
		{
			v1:     NewDInt(2, false),
			v2:     NewDInt(1, false),
			field:  &IField{IntResult, 0, false, false},
			resTyp: TypeInt,
			resStr: "1",
		},
		{
			// int64(math.UInt64).
			v1:    NewDInt(1, true),
			v2:    NewDInt(2, true),
			field: &IField{IntResult, 0, true, false},
			err:   "BIGINT.UNSIGNED.value.is.out.of.range.in: '1' - '2'",
		},
		{
			v1: NewDInt(1, false),
			// int64(math.UInt64).
			v2:    NewDInt(-1, true),
			field: &IField{IntResult, 0, true, false},
			err:   "BIGINT.UNSIGNED.value.is.out.of.range.in: '1' - '18446744073709551615'",
		},
		{
			v1:    NewDInt(1, true),
			v2:    NewDInt(2, false),
			field: &IField{IntResult, 0, true, false},
			err:   "BIGINT.UNSIGNED.value.is.out.of.range.in: '1' - '2'",
		},
		{
			// int64(math.UInt64).
			v1:    NewDInt(-1, true),
			v2:    NewDInt(-1, false),
			field: &IField{IntResult, 0, true, false},
			err:   "BIGINT.UNSIGNED.value.is.out.of.range.in: '18446744073709551615' - '-1'",
		},
		{
			v1:    NewDInt(1, false),
			v2:    NewDInt(math.MinInt64, false),
			field: &IField{IntResult, 0, false, false},
			err:   "BIGINT.value.is.out.of.range.in: '1' - '-9223372036854775808'",
		},
		{
			v1:    NewDInt(-2, false),
			v2:    NewDInt(math.MaxInt64, false),
			field: &IField{IntResult, 0, false, false},
			err:   "BIGINT.value.is.out.of.range.in: '-2' - '9223372036854775807'",
		},
		{
			v1:     NewDDecimal(decimal.NewFromFloat(1.23)),
			v2:     NewDDecimal(decimal.NewFromFloat(2.77)),
			field:  &IField{DecimalResult, 2, false, false},
			resTyp: TypeDecimal,
			resStr: "-1.54",
		},
		{
			v1:    NewDDecimal(decimal.NewFromFloat(math.MaxFloat64)),
			v2:    NewDDecimal(decimal.NewFromFloat(-math.MaxFloat64)),
			field: &IField{DecimalResult, 2, false, false},
			err:   "DOUBLE.value.is.out.of.range.in: '179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000' - '-179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000'",
		},
		{
			v1:     NewDFloat(1.23),
			v2:     NewDDecimal(decimal.NewFromFloat(2.77)),
			field:  &IField{RealResult, NotFixedDec, false, false},
			resTyp: TypeFloat,
			resStr: "-1.54",
		},
		{
			v1:    NewDFloat(math.MaxFloat64),
			v2:    NewDDecimal(decimal.NewFromFloat(-math.MaxFloat64)),
			field: &IField{RealResult, NotFixedDec, false, false},
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
			field:  &IField{IntResult, 0, false, false},
			resTyp: TypeNull,
			resStr: "NULL",
		},
		{
			v1:     NewDInt(2, false),
			v2:     NewDInt(1, false),
			field:  &IField{IntResult, 0, false, false},
			resTyp: TypeInt,
			resStr: "2",
		},
		{
			v1:     NewDInt(2, true),
			v2:     NewDInt(1, false),
			field:  &IField{IntResult, 0, true, false},
			resTyp: TypeInt,
			resStr: "2",
		},
		{
			v1:     NewDInt(1, true),
			v2:     NewDInt(2, true),
			field:  &IField{IntResult, 0, true, false},
			resTyp: TypeInt,
			resStr: "2",
		},
		{
			v1:    NewDInt(math.MaxInt64, true),
			v2:    NewDInt(3, false),
			field: &IField{IntResult, 0, true, false},
			err:   "BIGINT.UNSIGNED.value.is.out.of.range.in: '9223372036854775807' * '3'",
		},
		{
			v1:    NewDInt(math.MaxInt64, false),
			v2:    NewDInt(3, false),
			field: &IField{IntResult, 0, false, false},
			err:   "BIGINT.value.is.out.of.range.in: '9223372036854775807' * '3'",
		},

		{
			v1:     NewDDecimal(decimal.NewFromFloat(1.23)),
			v2:     NewDDecimal(decimal.NewFromFloat(2.77)),
			field:  &IField{DecimalResult, 4, false, false},
			resTyp: TypeDecimal,
			resStr: "3.4071",
		},
		{
			// int64(math.UInt64).
			v1:    NewDInt(2, true),
			v2:    NewDDecimal(decimal.NewFromFloat(math.MaxFloat64)),
			field: &IField{DecimalResult, 0, true, false},
			err:   "DOUBLE.value.is.out.of.range.in: '2' * '179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000'",
		},
		{
			v1:     NewDFloat(1.23),
			v2:     NewDDecimal(decimal.NewFromFloat(2.77)),
			field:  &IField{RealResult, NotFixedDec, false, false},
			resTyp: TypeFloat,
			resStr: "3.4071",
		},
		{
			v1:    NewDFloat(math.MaxFloat64),
			v2:    NewDString("2", 10),
			field: &IField{RealResult, NotFixedDec, false, false},
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
			field:  &IField{IntResult, 0, false, false},
			resTyp: TypeNull,
			resStr: "NULL",
		},
		{
			v1:     NewDInt(2, false),
			v2:     NewDInt(1, false),
			field:  &IField{DecimalResult, 4, false, false},
			resTyp: TypeDecimal,
			resStr: "2",
		},
		{
			v1:     NewDInt(2, true),
			v2:     NewDInt(0, false),
			field:  &IField{DecimalResult, 0, true, false},
			resTyp: TypeNull,
			resStr: "NULL",
		},
		{
			v1:     NewDFloat(1),
			v2:     NewDInt(0, false),
			field:  &IField{RealResult, NotFixedDec, false, false},
			resTyp: TypeNull,
			resStr: "NULL",
		},
		{
			v1:     NewDFloat(1),
			v2:     NewDInt(2, false),
			field:  &IField{RealResult, NotFixedDec, false, false},
			resTyp: TypeFloat,
			resStr: "0.5",
		},
		{
			v1:    NewDDecimal(decimal.NewFromFloat(math.MaxFloat64)),
			v2:    NewDDecimal(decimal.NewFromFloat(0.5)),
			field: &IField{DecimalResult, 0, true, false},
			err:   "DOUBLE.value.is.out.of.range.in: '179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000' / '0.5'",
		},
		{
			v1:    NewDFloat(math.MaxFloat64),
			v2:    NewDString("0.5", 10),
			field: &IField{RealResult, 0, true, false},
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
