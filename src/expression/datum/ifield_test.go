/*
 * Radon
 *
 * Copyright 2020 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package datum

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
)

func TestToNumeric(t *testing.T) {
	tcases := []struct {
		field  *IField
		resTyp ResultType
		dec    int
	}{
		{
			field:  &IField{StringResult, 0, 0, false, false, 33},
			resTyp: RealResult,
			dec:    31,
		},
		{
			field:  &IField{StringResult, 5, 0, true, false, 63},
			resTyp: IntResult,
			dec:    0,
		},
		{
			field:  &IField{TimeResult, 0, 2, false, false, 63},
			resTyp: DecimalResult,
			dec:    2,
		},
		{
			field:  &IField{DurationResult, 0, 0, false, false, 63},
			resTyp: IntResult,
			dec:    0,
		},
	}
	for _, tcase := range tcases {
		field := tcase.field
		field.ToNumeric()
		assert.Equal(t, tcase.resTyp, field.ResTyp)
		assert.Equal(t, tcase.dec, field.Scale)
	}
}

func TestNewIField(t *testing.T) {
	tcases := []struct {
		field *querypb.Field
		res   *IField
	}{
		{
			field: &querypb.Field{
				Name:     "id",
				Type:     querypb.Type_INT32,
				Decimals: 0,
				Flags:    32,
			},
			res: &IField{IntResult, 0, 0, true, false, 63},
		},
		{
			field: &querypb.Field{
				Name:     "id",
				Type:     querypb.Type_FLOAT64,
				Decimals: 4,
				Flags:    129,
			},
			res: &IField{RealResult, 0, 4, false, false, 63},
		},
		{
			field: &querypb.Field{
				Name:         "id",
				Type:         querypb.Type_DECIMAL,
				ColumnLength: 12,
				Decimals:     4,
				Flags:        129,
			},
			res: &IField{DecimalResult, 12, 4, false, false, 63},
		},
		{
			field: &querypb.Field{
				Name:     "id",
				Type:     querypb.Type_DATETIME,
				Decimals: 4,
				Flags:    129,
			},
			res: &IField{TimeResult, 0, 4, false, false, 63},
		},
		{
			field: &querypb.Field{
				Name:     "id",
				Type:     querypb.Type_TIME,
				Decimals: 4,
				Flags:    129,
			},
			res: &IField{DurationResult, 0, 4, false, false, 63},
		},
		{
			field: &querypb.Field{
				Name:         "id",
				Type:         querypb.Type_CHAR,
				ColumnLength: 15,
				Decimals:     31,
				Flags:        129,
			},
			res: &IField{StringResult, 5, 31, false, false, 33},
		},
		{
			field: &querypb.Field{
				Name:         "id",
				Type:         querypb.Type_BINARY,
				ColumnLength: 15,
				Decimals:     31,
				Flags:        129,
			},
			res: &IField{StringResult, 15, 31, false, false, 63},
		},
	}
	for _, tcase := range tcases {
		res := NewIField(tcase.field)
		assert.Equal(t, tcase.res, res)
	}
}

func TestField(t *testing.T) {
	tcases := []struct {
		val    Datum
		res    *IField
		isTemp bool
	}{
		{
			val: NewDInt(1, false),
			res: &IField{IntResult, 0, 0, false, true, 63},
		},
		{
			val: NewDDecimal(decimal.NewFromFloatWithExponent(1.2222222222222222222222222222222222, -31)),
			res: &IField{DecimalResult, 0, 30, false, true, 63},
		},
		{
			val: NewDString("1", 10, 63),
			res: &IField{StringResult, 0, 31, false, true, 33},
		},
		{
			val: NewDString("1", 16, 63),
			res: &IField{StringResult, 0, 0, true, true, 63},
		},
		{
			val: NewDNull(true),
			res: &IField{IntResult, 0, 0, true, true, 63},
		},
	}
	for _, tcase := range tcases {
		res := ConstantField(tcase.val)
		assert.Equal(t, tcase.res, res)
		assert.Equal(t, tcase.isTemp, IsTemporal(tcase.res.ResTyp))
	}
}

func TestConvertField(t *testing.T) {
	tcases := []struct {
		cvt   *sqlparser.ConvertType
		field *IField
		err   string
	}{
		{
			cvt: &sqlparser.ConvertType{
				Type: "unsigned",
			},
			field: &IField{
				ResTyp:  IntResult,
				Flag:    true,
				Charset: 63,
			},
		},
		{
			cvt: &sqlparser.ConvertType{
				Type: "signed",
			},
			field: &IField{
				ResTyp:  IntResult,
				Charset: 63,
			},
		},
		{
			cvt: &sqlparser.ConvertType{
				Type:   "decimal",
				Length: sqlparser.NewIntVal([]byte("6")),
				Scale:  sqlparser.NewIntVal([]byte("2")),
			},
			field: &IField{
				ResTyp:  DecimalResult,
				Length:  8,
				Scale:   2,
				Charset: 63,
			},
		},
		{
			cvt: &sqlparser.ConvertType{
				Type:   "binary",
				Length: sqlparser.NewIntVal([]byte("6")),
			},
			field: &IField{
				ResTyp:  StringResult,
				Length:  6,
				Charset: 63,
			},
		},
		{
			cvt: &sqlparser.ConvertType{
				Type: "char",
			},
			field: &IField{
				ResTyp:  StringResult,
				Charset: 33,
			},
		},
		{
			cvt: &sqlparser.ConvertType{
				Type:     "char",
				Length:   sqlparser.NewIntVal([]byte("6")),
				Operator: sqlparser.CharacterSetStr,
				Charset:  "utf8mb4",
			},
			field: &IField{
				ResTyp:  StringResult,
				Length:  6,
				Charset: 45,
			},
		},
		{
			cvt: &sqlparser.ConvertType{
				Type: "date",
			},
			field: &IField{
				ResTyp:  TimeResult,
				Length:  10,
				Charset: 63,
			},
		},
		{
			cvt: &sqlparser.ConvertType{
				Type:  "datetime",
				Scale: sqlparser.NewIntVal([]byte("2")),
			},
			field: &IField{
				ResTyp:  TimeResult,
				Length:  22,
				Scale:   2,
				Charset: 63,
			},
		},
		{
			cvt: &sqlparser.ConvertType{
				Type:  "time",
				Scale: sqlparser.NewIntVal([]byte("2")),
			},
			field: &IField{
				ResTyp:  DurationResult,
				Scale:   2,
				Charset: 63,
			},
		},
		{
			cvt: &sqlparser.ConvertType{
				Type:   "char",
				Length: sqlparser.NewValArg([]byte("::arg")),
			},
			err: "unsupport.val.type[*sqlparser.SQLVal]",
		},
		{
			cvt: &sqlparser.ConvertType{
				Type:   "decimal",
				Length: sqlparser.NewIntVal([]byte("6")),
				Scale:  sqlparser.NewValArg([]byte("::arg")),
			},
			err: "unsupport.val.type[*sqlparser.SQLVal]",
		},
		{
			cvt: &sqlparser.ConvertType{
				Type:    "char",
				Length:  sqlparser.NewIntVal([]byte("6")),
				Charset: "tttt",
			},
			err: "unknown.character.set: 'tttt'",
		},
		{
			cvt: &sqlparser.ConvertType{
				Type: "nchar",
			},
			err: "unsupport.convert.type: 'nchar'",
		},
	}

	for _, tcase := range tcases {
		res, err := ConvertField(tcase.cvt)
		if err != nil {
			assert.Equal(t, tcase.err, err.Error())
		} else {
			assert.Equal(t, tcase.field, res)
		}
	}
}
