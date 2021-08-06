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
			field:  mockField(StringResult, 0, false, false, false),
			resTyp: RealResult,
			dec:    31,
		},
		{
			field:  &IField{StringResult, 63, 5, 0, true, true, false},
			resTyp: IntResult,
			dec:    0,
		},
		{
			field:  mockField(TimeResult, 2, false, true, false),
			resTyp: DecimalResult,
			dec:    2,
		},
		{
			field:  mockField(DurationResult, 0, false, true, false),
			resTyp: IntResult,
			dec:    0,
		},
	}
	for _, tcase := range tcases {
		field := tcase.field
		field.ToNumeric()
		assert.Equal(t, tcase.resTyp, field.Type)
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
			res: mockField(IntResult, 0, true, false, false),
		},
		{
			field: &querypb.Field{
				Name:         "id",
				Type:         querypb.Type_FLOAT64,
				ColumnLength: 22,
				Decimals:     4,
				Flags:        129,
			},
			res: &IField{RealResult, 0, 22, 4, false, true, false},
		},
		{
			field: &querypb.Field{
				Name:         "id",
				Type:         querypb.Type_DECIMAL,
				ColumnLength: 12,
				Decimals:     4,
				Flags:        129,
			},
			res: &IField{DecimalResult, 0, 12, 4, false, true, false},
		},
		{
			field: &querypb.Field{
				Name:     "id",
				Type:     querypb.Type_DATETIME,
				Decimals: 4,
				Flags:    129,
			},
			res: mockField(TimeResult, 4, false, true, false),
		},
		{
			field: &querypb.Field{
				Name:     "id",
				Type:     querypb.Type_TIME,
				Decimals: 4,
				Flags:    129,
			},
			res: mockField(DurationResult, 4, false, true, false),
		},
		{
			field: &querypb.Field{
				Name:         "id",
				Type:         querypb.Type_CHAR,
				Charset:      33,
				ColumnLength: 15,
				Decimals:     31,
				Flags:        1,
			},
			res: &IField{StringResult, 33, 5, 31, false, false, false},
		},
		{
			field: &querypb.Field{
				Name:         "id",
				Type:         querypb.Type_BINARY,
				Charset:      63,
				ColumnLength: 15,
				Decimals:     31,
				Flags:        129,
			},
			res: &IField{StringResult, 63, 15, 31, false, true, false},
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
			res: mockField(IntResult, 0, false, true, true),
		},
		{
			val: NewDDecimal(decimal.NewFromFloatWithExponent(1.2222222222222222222222222222222222, -31)),
			res: mockField(DecimalResult, 30, false, true, true),
		},
		{
			val: NewDString("1", 10, true),
			res: mockField(StringResult, 31, false, false, true),
		},
		{
			val: NewDString("1", 16, true),
			res: mockField(StringResult, 0, true, true, true),
		},
		{
			val: NewDNull(true),
			res: mockField(IntResult, 0, false, true, true),
		},
	}
	for _, tcase := range tcases {
		res := ConstantField(tcase.val)
		assert.Equal(t, tcase.res, res)
		assert.Equal(t, tcase.isTemp, IsTemporal(tcase.res.Type))
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
				Type:       IntResult,
				IsUnsigned: true,
				IsBinary:   true,
			},
		},
		{
			cvt: &sqlparser.ConvertType{
				Type: "signed",
			},
			field: &IField{
				Type:     IntResult,
				IsBinary: true,
			},
		},
		{
			cvt: &sqlparser.ConvertType{
				Type:   "decimal",
				Length: sqlparser.NewIntVal([]byte("6")),
				Scale:  sqlparser.NewIntVal([]byte("2")),
			},
			field: &IField{
				Type:     DecimalResult,
				Length:   8,
				Scale:    2,
				IsBinary: true,
			},
		},
		{
			cvt: &sqlparser.ConvertType{
				Type:   "binary",
				Length: sqlparser.NewIntVal([]byte("6")),
			},
			field: &IField{
				Type:     StringResult,
				Length:   6,
				IsBinary: true,
			},
		},
		{
			cvt: &sqlparser.ConvertType{
				Type: "char",
			},
			field: &IField{
				Type: StringResult,
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
				Type:    StringResult,
				Length:  6,
				Charset: 45,
			},
		},
		{
			cvt: &sqlparser.ConvertType{
				Type: "date",
			},
			field: &IField{
				Type:     TimeResult,
				Length:   10,
				IsBinary: true,
			},
		},
		{
			cvt: &sqlparser.ConvertType{
				Type:  "datetime",
				Scale: sqlparser.NewIntVal([]byte("2")),
			},
			field: &IField{
				Type:     TimeResult,
				Length:   22,
				Scale:    2,
				IsBinary: true,
			},
		},
		{
			cvt: &sqlparser.ConvertType{
				Type:  "time",
				Scale: sqlparser.NewIntVal([]byte("2")),
			},
			field: &IField{
				Type:     DurationResult,
				Scale:    2,
				IsBinary: true,
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
