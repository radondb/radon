package common

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func TestTernaryOpt(t *testing.T) {
	res := TernaryOpt(true, 1, 0).(int)
	assert.Equal(t, 1, res)

	res = TernaryOpt(false, 1, 0).(int)
	assert.Equal(t, 0, res)
}

func TestStrToHex(t *testing.T) {
	str := "12"
	res := StrToHex(str)
	assert.Equal(t, "3132", res)
}

func TestFloat64ToInt64(t *testing.T) {
	tcases := []struct {
		f float64
		i int64
	}{
		{
			f: 0.45,
			i: 0,
		},
		{
			f: 9223372036854775807.5,
			i: 9223372036854775807,
		},
		{
			f: -1e20,
			i: -9223372036854775808,
		},
		{
			f: 12.45,
			i: 12,
		},
	}
	for _, tcase := range tcases {
		res := Float64ToInt64(tcase.f)
		assert.Equal(t, tcase.i, res)
	}
}

func TestIsDecimalInf(t *testing.T) {
	dec, _ := decimal.NewFromString("1.79769e+309")
	res := IsDecimalInf(dec)
	assert.Equal(t, true, res)

	dec, _ = decimal.NewFromString("233")
	res = IsDecimalInf(dec)
	assert.Equal(t, false, res)
}

func TestGetFloatPrefix(t *testing.T) {
	tcases := []struct {
		in  string
		out string
	}{
		{
			in:  " ",
			out: "0",
		},
		{
			in:  "e31",
			out: "0",
		},
		{
			in:  "0xFF",
			out: "0xFF",
		},
		{
			in:  "-0.2333.2",
			out: "-0.2333",
		},
		{
			in:  "1.233e-23s",
			out: "1.233e-23",
		},
	}
	for _, tcase := range tcases {
		res := GetFloatPrefix(tcase.in)
		assert.Equal(t, tcase.out, res)
	}
}
