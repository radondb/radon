/*
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 */

package sqltypes

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TesttimeToNumeric(t *testing.T) {
	testcases := []struct {
		in  Value
		out interface{}
	}{
		{
			in:  testVal(Timestamp, "2012-02-24 23:19:43"),
			out: int64(20120224231943),
		},
		{
			in:  testVal(Timestamp, "2012-02-24 23:19:43.120"),
			out: float64(20120224231943.120),
		},
		{
			in:  testVal(Time, "-23:19:43.120"),
			out: float64(-231943.120),
		},
		{
			in:  testVal(Time, "-63:19:43"),
			out: int64(-631943),
		},
		{
			in:  testVal(Datetime, "0000-00-00 00:00:00"),
			out: int64(0),
		},
		{
			in:  testVal(Datetime, "2012-02-24 23:19:43.000012"),
			out: float64(20120224231943.000012),
		},
		{
			in:  testVal(Date, "0000-00-00"),
			out: int64(0),
		},
		{
			in:  testVal(Date, "2012-02-24"),
			out: int64(20120224),
		},
		{
			in:  testVal(Year, "2012"),
			out: uint64(2012),
		},
		{
			in:  testVal(Year, "12"),
			out: uint64(12),
		},
	}

	for _, tcase := range testcases {
		got, err := timeToNumeric(tcase.in)
		assert.Nil(t, err)

		var v interface{}
		switch got.typ {
		case Uint64:
			v = got.uval
		case Float64:
			v = got.fval
		case Int64:
			v = got.ival
		}

		if !reflect.DeepEqual(v, tcase.out) {
			t.Errorf("%v.ToNative = %#v, want %#v", makePretty(tcase.in), v, tcase.out)
		}
	}
}
