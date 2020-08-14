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
	"time"

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
			field:  &IField{StringResult, 0, false, false},
			resTyp: RealResult,
			dec:    31,
		},
		{
			field:  &IField{TimeResult, 2, false, false},
			resTyp: DecimalResult,
			dec:    2,
		},
		{
			field:  &IField{DurationResult, 0, false, false},
			resTyp: IntResult,
			dec:    0,
		},
	}
	for _, tcase := range tcases {
		field := tcase.field
		field.ToNumeric()
		assert.Equal(t, tcase.resTyp, field.ResTyp)
		assert.Equal(t, tcase.dec, field.Decimal)
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
			res: &IField{IntResult, 0, true, false},
		},
		{
			field: &querypb.Field{
				Name:     "id",
				Type:     querypb.Type_FLOAT64,
				Decimals: 4,
				Flags:    129,
			},
			res: &IField{RealResult, 4, false, false},
		},
		{
			field: &querypb.Field{
				Name:     "id",
				Type:     querypb.Type_DECIMAL,
				Decimals: 4,
				Flags:    129,
			},
			res: &IField{DecimalResult, 4, false, false},
		},
		{
			field: &querypb.Field{
				Name:     "id",
				Type:     querypb.Type_DATETIME,
				Decimals: 4,
				Flags:    129,
			},
			res: &IField{TimeResult, 4, false, false},
		},
		{
			field: &querypb.Field{
				Name:     "id",
				Type:     querypb.Type_TIME,
				Decimals: 4,
				Flags:    129,
			},
			res: &IField{DurationResult, 4, false, false},
		},
		{
			field: &querypb.Field{
				Name:     "id",
				Type:     querypb.Type_CHAR,
				Decimals: 0,
				Flags:    129,
			},
			res: &IField{StringResult, 0, false, false},
		},
	}
	for _, tcase := range tcases {
		res := NewIField(tcase.field)
		assert.Equal(t, tcase.res, res)
	}
}

func TestConstantField(t *testing.T) {
	dec, _ := decimal.NewFromString("1.2222222222222222222222222222222222")
	tcases := []struct {
		val Datum
		res *IField
	}{
		{
			val: NewDInt(1, false),
			res: &IField{IntResult, 0, false, true},
		},
		{
			val: NewDDecimal(dec),
			res: &IField{DecimalResult, 30, false, true},
		},
		{
			val: NewDString("1", 16),
			res: &IField{StringResult, 31, true, true},
		},
		{
			val: NewDFloat(1.2),
			res: &IField{RealResult, 31, false, true},
		},
		{
			val: NewDNull(true),
			res: &IField{StringResult, 0, false, true},
		},
		{
			val: &Duration{
				duration: time.Duration(8*3600) * time.Second,
				fsp:      0,
			},
			res: &IField{StringResult, 31, false, true},
		},
	}
	for _, tcase := range tcases {
		res := ConstantField(tcase.val)
		assert.Equal(t, tcase.res, res)
	}
}
