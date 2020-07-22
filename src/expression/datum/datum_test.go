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

func TestValToDatum(t *testing.T) {
	tcases := []struct {
		val    sqltypes.Value
		resTyp Type
		resStr string
		err    string
	}{
		{
			val:    sqltypes.NULL,
			resTyp: TypeNull,
			resStr: "0",
		},
		{
			val:    sqltypes.NewInt32(1),
			resTyp: TypeInt,
			resStr: "1",
		},
		{
			val:    sqltypes.NewUint64(1),
			resTyp: TypeInt,
			resStr: "1",
		},
		{
			val:    sqltypes.NewFloat32(1.22),
			resTyp: TypeFloat,
			resStr: "1.2200000286102295",
		},
		{
			val:    sqltypes.MakeTrusted(sqltypes.Decimal, []byte("1.222")),
			resTyp: TypeDecimal,
			resStr: "1.222",
		},
		{
			val:    sqltypes.NewVarChar("byz"),
			resTyp: TypeString,
			resStr: "byz",
		},
		{
			val:    sqltypes.MakeTrusted(sqltypes.Datetime, []byte("1453-05-29 08:00:00.233")),
			resTyp: TypeTime,
			resStr: "1453-05-29 08:00:00.233",
		},
		{
			val:    sqltypes.MakeTrusted(sqltypes.Date, []byte("1453-05-29")),
			resTyp: TypeTime,
			resStr: "1453-05-29",
		},
		{
			val:    sqltypes.MakeTrusted(sqltypes.Year, []byte("1453")),
			resTyp: TypeInt,
			resStr: "1453",
		},
		{
			val:    sqltypes.MakeTrusted(sqltypes.Time, []byte("-12:12:12.2333")),
			resTyp: TypeDuration,
			resStr: "-12:12:12.2333",
		},
		{
			val: sqltypes.MakeTrusted(sqltypes.Time, []byte("-12:12:12:2333")),
			err: "incorrect.time.value.'-12:12:12:2333'",
		},
	}
	for _, tcase := range tcases {
		res, err := ValToDatum(tcase.val)
		if err != nil {
			assert.Equal(t, tcase.err, err.Error())
		} else {
			assert.Equal(t, tcase.resTyp, res.Type())
			assert.Equal(t, tcase.resStr, res.ValStr())
		}
	}
}

func TestDatumFunction(t *testing.T) {
	tcases := []struct {
		val      Datum
		typ      Type
		integral int64
		flag     bool
		real     float64
		dec      decimal.Decimal
		str      string
	}{
		{
			val:      NewDNull(true),
			typ:      TypeNull,
			flag:     false,
			integral: 0,
			real:     0,
			dec:      decimal.NewFromFloat(0),
			str:      "0",
		},
		{
			val:      NewDInt(1, true),
			typ:      TypeInt,
			flag:     true,
			integral: 1,
			real:     1,
			dec:      decimal.NewFromFloat(1),
			str:      "1",
		},
		{
			val:      NewDInt(1, false),
			typ:      TypeInt,
			flag:     false,
			integral: 1,
			real:     1,
			dec:      decimal.NewFromFloat(1),
			str:      "1",
		},
		{
			val:      NewDDecimal(decimal.NewFromFloat(1.22)),
			typ:      TypeDecimal,
			flag:     false,
			integral: 1,
			real:     1.22,
			dec:      decimal.NewFromFloat(1.22),
			str:      "1.22",
		},
		{
			val:      NewDFloat(1.22),
			typ:      TypeFloat,
			flag:     false,
			integral: 1,
			real:     1.22,
			dec:      decimal.NewFromFloat(1.22),
			str:      "1.22",
		},
		{
			val:      NewDString("1.22"),
			typ:      TypeString,
			flag:     false,
			integral: 1,
			real:     1.22,
			dec:      decimal.NewFromFloat(1.22),
			str:      "1.22",
		},
		{
			val:      NewDTime(sqltypes.Datetime, 2, 1453, 5, 29, 8, 0, 0, 230000),
			typ:      TypeTime,
			flag:     false,
			integral: 14530529080000,
			real:     1.453052908000023e+13,
			dec:      decimal.NewFromFloat(14530529080000.23),
			str:      "1453-05-29 08:00:00.23",
		},
		{
			val:      NewDTime(sqltypes.Timestamp, 0, 2020, 5, 29, 8, 0, 0, 0),
			typ:      TypeTime,
			flag:     false,
			integral: 20200529080000,
			real:     2.020052908e+13,
			dec:      decimal.NewFromFloat(20200529080000),
			str:      "2020-05-29 08:00:00",
		},
		{
			val:      NewDTime(sqltypes.Date, 4, 2020, 5, 29, 0, 0, 0, 0),
			typ:      TypeTime,
			flag:     false,
			integral: 20200529,
			real:     2.0200529e+07,
			dec:      decimal.NewFromFloat(20200529),
			str:      "2020-05-29",
		},
		{
			val: &Duration{
				duration: time.Duration(8*3600) * time.Second,
				fsp:      0,
			},
			typ:      TypeDuration,
			flag:     false,
			integral: 80000,
			real:     80000,
			dec:      decimal.NewFromFloat(80000),
			str:      "08:00:00",
		},
		{
			val: &Duration{
				duration: -(time.Duration(8*3600)*time.Second + time.Duration(int64(230000)*1000)),
				fsp:      4,
			},
			typ:      TypeDuration,
			flag:     false,
			integral: -80000,
			real:     -80000.23,
			dec:      decimal.NewFromFloat(-80000.23),
			str:      "-08:00:00.2300",
		},
		{
			val: NewDTuple(NewDInt(1, false), NewDString("1.22")),
			typ: TypeTuple,
			str: "11.22",
		},
	}
	for _, tcase := range tcases {
		d := tcase.val
		assert.Equal(t, tcase.typ, d.Type())
		if tcase.typ != TypeTuple {
			integral, flag := d.ValInt()
			assert.Equal(t, tcase.integral, integral)
			assert.Equal(t, tcase.flag, flag)
			assert.Equal(t, tcase.real, d.ValReal())
			assert.Equal(t, tcase.dec, d.ValDecimal())
		}
		assert.Equal(t, tcase.str, d.ValStr())
	}
}