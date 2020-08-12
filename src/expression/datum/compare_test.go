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
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

func TestNullsafeCompare(t *testing.T) {
	dec, _ := decimal.NewFromString("14530529080000.2333")

	tcases := []struct {
		v1      Datum
		v2      Datum
		cmpFunc CompareFunc
		res     int64
	}{
		{
			v1:      NewDNull(true),
			v2:      NewDNull(true),
			cmpFunc: CompareInt,
			res:     0,
		},
		{
			v1:      NewDNull(true),
			v2:      NewDString("2", 10),
			cmpFunc: CompareInt,
			res:     -1,
		},
		{
			v1:      NewDString("2", 10),
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
			v1:      NewDString("luoyang", 10),
			v2:      NewDString("luohe", 10),
			cmpFunc: CompareString,
			res:     1,
		},
		{
			v1:      NewDString("luoyang", 10),
			v2:      NewDString("luohe", 10),
			cmpFunc: CompareString,
			res:     1,
		},
		{
			v1:      NewDString("ABCD", 10),
			v2:      NewDString("abcd", 10),
			cmpFunc: CompareString,
			res:     0,
		},
		{
			v1:      NewDFloat(2.33),
			v2:      NewDInt(2, false),
			cmpFunc: CompareFloat64,
			res:     1,
		},
		{
			v1:      NewDFloat(2.33),
			v2:      NewDString("2.33", 10),
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
			v1:      NewDInt(3, false),
			v2:      NewDDecimal(decimal.NewFromFloat(3.23)),
			cmpFunc: CompareDecimal,
			res:     -1,
		},
		{
			v1:      NewDDecimal(dec),
			v2:      NewDTime(sqltypes.Datetime, 4, 1453, 5, 29, 9, 0, 0, 233300),
			cmpFunc: CompareDatetime,
			res:     -1,
		},
		{
			v1:      NewDDecimal(dec),
			v2:      NewDTime(sqltypes.Datetime, 4, 1453, 5, 29, 8, 0, 0, 223300),
			cmpFunc: CompareDatetime,
			res:     1,
		},
		{
			v1:      NewDInt(14530529080000, true),
			v2:      NewDTime(sqltypes.Datetime, 4, 1453, 5, 29, 8, 0, 0, 233300),
			cmpFunc: CompareDatetime,
			res:     -1,
		},
		{
			v1:      NewDDecimal(dec),
			v2:      NewDDecimal(dec),
			cmpFunc: CompareDatetime,
			res:     0,
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
			v1: NewDString("70000", 10),
			v2: &Duration{
				duration: time.Duration(8*3600) * time.Second,
				fsp:      0,
			},
			cmpFunc: CompareDuration,
			res:     -1,
		},
		{
			v1:      NewDString("1T08:00:00", 10),
			v2:      NewDString("1 08:00:00", 10),
			cmpFunc: CompareDuration,
			res:     1,
		},
	}
	for _, tcase := range tcases {
		res := NullsafeCompare(tcase.v1, tcase.v2, tcase.cmpFunc)
		assert.Equal(t, tcase.res, res)
	}
}
