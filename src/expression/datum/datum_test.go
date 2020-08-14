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
	"github.com/xelabs/go-mysqlstack/sqlparser"
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
			resStr: "NULL",
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
			val:    sqltypes.MakeTrusted(sqltypes.Int64, []byte("1.222")),
			resTyp: TypeInt,
			err:    "strconv.ParseInt: parsing \"1.222\": invalid syntax",
		},
		{
			val:    sqltypes.MakeTrusted(sqltypes.Float64, []byte("1qa")),
			resTyp: TypeFloat,
			err:    "strconv.ParseFloat: parsing \"1qa\": invalid syntax",
		},
		{
			val:    sqltypes.MakeTrusted(sqltypes.Decimal, []byte("1qa")),
			resTyp: TypeDecimal,
			err:    "can't convert 1qa to decimal",
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
			str:      "NULL",
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
			val:      NewDDecimal(decimal.NewFromFloat(-1.5)),
			typ:      TypeDecimal,
			flag:     false,
			integral: -2,
			real:     -1.5,
			dec:      decimal.NewFromFloat(-1.5),
			str:      "-1.5",
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
			val:      NewDString("1.22e3", 10),
			typ:      TypeString,
			flag:     false,
			integral: 1220,
			real:     1220,
			dec:      decimal.NewFromFloat(1220),
			str:      "1.22e3",
		},
		{
			val:      NewDString("12", 16),
			typ:      TypeString,
			flag:     true,
			integral: 12594,
			real:     12594,
			dec:      decimal.NewFromFloat(12594),
			str:      "12",
		},
		// truncate.
		{
			val:      NewDString("1.22e", 10),
			typ:      TypeString,
			flag:     false,
			integral: 1,
			real:     1.22,
			dec:      decimal.NewFromFloat(1.22),
			str:      "1.22e",
		},
		{
			val:      NewDString("15", 16),
			typ:      TypeString,
			flag:     true,
			integral: 12597,
			real:     12597,
			dec:      decimal.NewFromFloat(12597),
			str:      "15",
		},
		{
			val:      NewDString("1.22", 10),
			typ:      TypeString,
			flag:     false,
			integral: 1,
			real:     1.22,
			dec:      decimal.NewFromFloat(1.22),
			str:      "1.22",
		},
		// over range.
		{
			val:      NewDString("123456789", 16),
			typ:      TypeString,
			flag:     true,
			integral: -1,
			real:     18446744073709551615,
			dec:      decimal.NewFromFloat(18446744073709551615),
			str:      "123456789",
		},
		// over range.
		{
			val:      NewDString("2e+308", 10),
			typ:      TypeString,
			flag:     false,
			integral: 9223372036854775807,
			real:     1.7976931348623157e+308,
			dec:      decimal.NewFromFloat(1.7976931348623157e+308),
			str:      "2e+308",
		},
		// over range.
		{
			val:      NewDString("-2e+308", 10),
			typ:      TypeString,
			flag:     false,
			integral: -9223372036854775808,
			real:     -1.7976931348623157e+308,
			dec:      decimal.NewFromFloat(-1.7976931348623157e+308),
			str:      "-2e+308",
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
			val: NewDTuple(NewDInt(1, false), NewDString("1.22", 10)),
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

func TestSQLValToDatum(t *testing.T) {
	tcases := []struct {
		val *sqlparser.SQLVal
		res Datum
		err string
	}{
		{
			val: sqlparser.NewIntVal([]byte("123")),
			res: NewDInt(123, false),
		},

		{
			val: sqlparser.NewFloatVal([]byte("22.1")),
			res: NewDDecimal(decimal.NewFromFloat(22.1)),
		},
		{
			val: sqlparser.NewStrVal([]byte("byz")),
			res: NewDString("byz", 10),
		},
		{
			val: sqlparser.NewHexNum([]byte("0x3132")),
			res: NewDString("12", 16),
		},
		{
			val: sqlparser.NewHexVal([]byte("3132")),
			res: NewDString("12", 16),
		},
		{
			val: sqlparser.NewFloatVal([]byte("22a1")),
			err: "can't convert 22a1 to decimal",
		},
		{
			val: sqlparser.NewIntVal([]byte("1a3")),
			err: "strconv.ParseInt: parsing \"1a3\": invalid syntax",
		},
		{
			val: sqlparser.NewHexNum([]byte("0x313")),
			err: "encoding/hex: odd length hex string",
		},
		{
			val: sqlparser.NewHexVal([]byte("313")),
			err: "encoding/hex: odd length hex string",
		},
		{
			val: sqlparser.NewValArg([]byte("::arg")),
			err: "unsupport.val.type[*sqlparser.SQLVal]",
		},
	}
	for _, tcase := range tcases {
		res, err := SQLValToDatum(tcase.val)
		if err != nil {
			assert.Equal(t, tcase.err, err.Error())
		} else {
			assert.Equal(t, tcase.res, res)
		}
	}
}

func TestSetIgnoreCase(t *testing.T) {
	d := NewDString("12", 10)
	assert.Equal(t, d.ignoreCase, true)
	d.setIgnoreCase(false)
	assert.Equal(t, d.ignoreCase, false)
}

func TestDTupleArgs(t *testing.T) {
	d := NewDTuple(NewDInt(1, false), NewDString("1.22", 10))
	assert.Equal(t, 2, len(d.Args()))
}

func TestTimeToDatumErr(t *testing.T) {
	tcases := []struct {
		v   sqltypes.Value
		err string
	}{
		{
			v:   sqltypes.MakeTrusted(sqltypes.Timestamp, []byte("2i20-08-15 12:12:12.2333")),
			err: "strconv.Atoi: parsing \"2i20\": invalid syntax",
		},
		{
			v:   sqltypes.MakeTrusted(sqltypes.Timestamp, []byte("2020-i8-15 12:12:12.2333")),
			err: "strconv.Atoi: parsing \"i8\": invalid syntax",
		},
		{
			v:   sqltypes.MakeTrusted(sqltypes.Timestamp, []byte("2020-08-i5 12:12:12.2333")),
			err: "strconv.Atoi: parsing \"i5\": invalid syntax",
		},
		{
			v:   sqltypes.MakeTrusted(sqltypes.Datetime, []byte("2020-08-15 i2:12:12.2333")),
			err: "strconv.Atoi: parsing \"i2\": invalid syntax",
		},
		{
			v:   sqltypes.MakeTrusted(sqltypes.Datetime, []byte("2020-08-15 12:i2:12.2333")),
			err: "strconv.Atoi: parsing \"i2\": invalid syntax",
		},
		{
			v:   sqltypes.MakeTrusted(sqltypes.Datetime, []byte("2020-08-15 12:12:i2.2333")),
			err: "strconv.Atoi: parsing \"i2\": invalid syntax",
		},
		{
			v:   sqltypes.MakeTrusted(sqltypes.Datetime, []byte("2020-08-15 12:12:12.i333")),
			err: "strconv.Atoi: parsing \"i333\": invalid syntax",
		},
		{
			v:   sqltypes.MakeTrusted(sqltypes.Date, []byte("2i20-08-15")),
			err: "strconv.Atoi: parsing \"2i20\": invalid syntax",
		},
		{
			v:   sqltypes.MakeTrusted(sqltypes.Date, []byte("2020-i8-15")),
			err: "strconv.Atoi: parsing \"i8\": invalid syntax",
		},
		{
			v:   sqltypes.MakeTrusted(sqltypes.Date, []byte("2020-08-i5")),
			err: "strconv.Atoi: parsing \"i5\": invalid syntax",
		},
		{
			v:   sqltypes.MakeTrusted(sqltypes.Time, []byte("-12:12:12:2333")),
			err: "incorrect.time.value.'-12:12:12:2333'",
		},
		{
			v:   sqltypes.MakeTrusted(sqltypes.Time, []byte("i2:12:12.2333")),
			err: "strconv.Atoi: parsing \"i2\": invalid syntax",
		},
		{
			v:   sqltypes.MakeTrusted(sqltypes.Time, []byte("12:i2:12.2333")),
			err: "strconv.Atoi: parsing \"i2\": invalid syntax",
		},
		{
			v:   sqltypes.MakeTrusted(sqltypes.Time, []byte("12:12:i2.2333")),
			err: "strconv.Atoi: parsing \"i2\": invalid syntax",
		},
		{
			v:   sqltypes.MakeTrusted(sqltypes.Time, []byte("12:12:12.i333")),
			err: "strconv.Atoi: parsing \"i333\": invalid syntax",
		},
		{
			v:   sqltypes.MakeTrusted(sqltypes.Year, []byte("2003")),
			err: "can.not.cast.'YEAR'.to.time.type",
		},
	}
	for _, tcase := range tcases {
		_, err := timeToDatum(tcase.v)
		assert.Equal(t, tcase.err, err.Error())
	}
}
