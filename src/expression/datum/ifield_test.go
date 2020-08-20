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
		val Datum
		res *IField
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
	}
}
