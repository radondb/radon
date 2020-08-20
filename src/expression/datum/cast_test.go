package datum

import (
	"math"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

func TestCast(t *testing.T) {
	tcases := []struct {
		in         Datum
		field      *IField
		isCastFunc bool
		out        Datum
		err        string
	}{
		{
			in:    NewDNull(true),
			field: &IField{},
			out:   NewDNull(true),
		},
		{
			in: NewDInt(-1, false),
			field: &IField{
				ResTyp: IntResult,
				Flag:   true,
			},
			out: NewDInt(0, true),
		},
		{
			in: NewDInt(-1, true),
			field: &IField{
				ResTyp: IntResult,
				Flag:   true,
			},
			out: NewDInt(-1, true),
		},
		{
			in: NewDDecimal(decimal.NewFromFloat(1.22)),
			field: &IField{
				ResTyp: IntResult,
				Flag:   true,
			},
			out: NewDInt(1, true),
		},
		{
			in: NewDFloat(1.22),
			field: &IField{
				ResTyp: IntResult,
				Flag:   true,
			},
			out: NewDInt(1, true),
		},
		{
			in: NewDFloat(1.22),
			field: &IField{
				ResTyp: IntResult,
				Flag:   false,
			},
			out: NewDInt(1, false),
		},
		{
			in: NewDString("2e20", 10, 33),
			field: &IField{
				ResTyp: IntResult,
				Flag:   true,
			},
			out: NewDInt(-1, true),
		},
		{
			in: &Duration{
				duration: time.Duration(-25*3600+59*60+59) * time.Second,
				fsp:      2,
			},
			field: &IField{
				ResTyp: IntResult,
				Flag:   true,
			},
			out: NewDInt(0, true),
		},
		{
			in: &Duration{
				duration: time.Duration(25*3600+59*60+59) * time.Second,
				fsp:      2,
			},
			field: &IField{
				ResTyp: IntResult,
				Flag:   false,
			},
			out: NewDInt(255959, false),
		},
		{
			in: NewDTime(sqltypes.Datetime, 2, 2008, 8, 8, 20, 0, 1, 0),
			field: &IField{
				ResTyp: IntResult,
				Flag:   false,
			},
			out: NewDInt(20080808200001, false),
		},
		{
			in: NewDInt(255959, true),
			field: &IField{
				ResTyp:  StringResult,
				Charset: 33,
			},
			out: NewDString("255959", 10, 33),
		},
		{
			in: NewDInt(255959, true),
			field: &IField{
				ResTyp: DecimalResult,
				Length: 5,
				Scale:  2,
			},
			out: NewDDecimal(decimal.NewFromFloatWithExponent(9.99, -2)),
		},
		{
			in: NewDInt(255959, true),
			field: &IField{
				ResTyp: RealResult,
			},
			out: NewDFloat(255959),
		},
		{
			in: NewDFloat(math.NaN()),
			field: &IField{
				ResTyp: RealResult,
				Length: 5,
				Scale:  2,
			},
			out: NewDFloat(0),
		},
		{
			in: NewDInt(255959, true),
			field: &IField{
				ResTyp: DurationResult,
				Scale:  2,
			},
			out: &Duration{
				duration: time.Duration(25*3600+59*60+59) * time.Second,
				fsp:      2,
			},
		},
		{
			in: &Duration{
				duration: time.Duration(25*3600+59*60+59)*time.Second + 999999000,
				fsp:      6,
			},
			field: &IField{
				ResTyp: DurationResult,
				Scale:  2,
			},
			out: &Duration{
				duration: time.Duration(26*3600) * time.Second,
				fsp:      2,
			},
		},
		{
			in: NewDString("08-08-08 20:00:00.9999", 10, 63),
			field: &IField{
				ResTyp: TimeResult,
				Scale:  2,
			},
			out: NewDTime(sqltypes.Datetime, 2, 2008, 8, 8, 20, 0, 1, 0),
		},
		{
			in: NewDTime(sqltypes.Datetime, 4, 2008, 8, 8, 20, 0, 1, 999900),
			field: &IField{
				ResTyp: TimeResult,
				Scale:  2,
			},
			out: NewDTime(sqltypes.Datetime, 2, 2008, 8, 8, 20, 0, 2, 0),
		},
		{
			in: NewDString("08-08-08 20:00:00.9999", 10, 63),
			field: &IField{
				ResTyp: RowResult,
			},
			err: "unsupport.type",
		},
	}
	for _, tcase := range tcases {
		out, err := Cast(tcase.in, tcase.field, tcase.isCastFunc)
		if err != nil {
			assert.Equal(t, tcase.err, err.Error())
		} else {
			assert.Equal(t, tcase.out, out)
		}
	}
}

func TestCastStrToInt(t *testing.T) {
	tcases := []struct {
		in         string
		flag       bool
		isCastFunc bool
		out        int64
	}{
		{
			in:         "2e20s",
			flag:       true,
			isCastFunc: true,
			out:        2,
		},
		{
			in:         "2e20s",
			flag:       true,
			isCastFunc: false,
			out:        -1,
		},
		{
			in:   "18446744073709551615",
			flag: true,
			out:  -1,
		},
		{
			in:   "18446744073709551615",
			flag: false,
			out:  -1,
		},
		{
			in:   "-18446744073709551614",
			flag: false,
			out:  -9223372036854775808,
		},
		{
			in:   "-1",
			flag: true,
			out:  0,
		},
	}
	for _, tcase := range tcases {
		res := CastStrToInt(tcase.in, tcase.flag, tcase.isCastFunc)
		assert.Equal(t, tcase.out, res)
	}
}

func TestCastDecWithField(t *testing.T) {
	tcases := []struct {
		in    string
		field *IField
		res   string
	}{
		{
			in: "123.22",
			field: &IField{
				Length: 6,
				Scale:  2,
			},
			res: "99.99",
		},
		{
			in: "12333.22",
			field: &IField{
				Length: 6,
				Scale:  0,
			},
			res: "12333",
		},
		{
			in: "-123.22",
			field: &IField{
				Length: 6,
				Scale:  2,
			},
			res: "-99.99",
		},
		{
			in: "-123.225",
			field: &IField{
				Length: 7,
				Scale:  2,
			},
			res: "-123.23",
		},
	}
	for _, tcase := range tcases {
		dec, err := decimal.NewFromString(tcase.in)
		assert.Nil(t, err)
		res := CastDecWithField(dec, tcase.field)
		assert.Equal(t, tcase.res, res.String())
	}
}

func TestCastStrWithField(t *testing.T) {
	tcases := []struct {
		in    string
		field *IField
		res   string
	}{
		{
			in: "张三",
			field: &IField{
				Charset: 63,
				Length:  3,
			},
			res: "张",
		},
		{
			in: "张三",
			field: &IField{
				Charset: 33,
				Length:  3,
			},
			res: "张三",
		},
		{
			in: "张三",
			field: &IField{
				Charset: 33,
				Length:  1,
			},
			res: "张",
		},
	}
	for _, tcase := range tcases {
		res := CastStrWithField(tcase.in, tcase.field)
		assert.Equal(t, tcase.res, res)
	}
}

func TestCastFloat64WithField(t *testing.T) {
	tcases := []struct {
		in    float64
		field *IField
		res   float64
	}{
		{
			in: 1.2222,
			field: &IField{
				Charset: 63,
				Length:  3,
				Scale:   2,
			},
			res: 1.22,
		},
		{
			in: 122.22,
			field: &IField{
				Charset: 63,
				Length:  3,
				Scale:   2,
			},
			res: 9.99,
		},
		{
			in: -122.22,
			field: &IField{
				Charset: 63,
				Length:  3,
				Scale:   2,
			},
			res: -9.99,
		},
	}
	for _, tcase := range tcases {
		res := CastFloat64WithField(tcase.in, tcase.field)
		assert.Equal(t, tcase.res, res)
	}
}
