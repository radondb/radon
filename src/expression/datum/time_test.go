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

	"github.com/stretchr/testify/assert"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

func TestNumToDatetime(t *testing.T) {
	tcases := []struct {
		in  int64
		typ querypb.Type
		out string
		err string
	}{
		{
			in:  0,
			typ: sqltypes.Datetime,
			out: "0000-00-00 00:00:00",
		},
		{
			in:  200721,
			typ: sqltypes.Date,
			out: "2020-07-21",
		},
		{
			in:  880721,
			typ: sqltypes.Date,
			out: "1988-07-21",
		},
		{
			in:  14530529,
			typ: sqltypes.Date,
			out: "1453-05-29",
		},
		{
			in:  880721221212,
			typ: sqltypes.Datetime,
			out: "1988-07-21 22:12:12",
		},
		{
			in:  200721221212,
			typ: sqltypes.Datetime,
			out: "2020-07-21 22:12:12",
		},
		{
			in:  14530529080000,
			typ: sqltypes.Datetime,
			out: "1453-05-29 08:00:00",
		},
		{
			in:  100000000000000,
			typ: sqltypes.Datetime,
			out: "0000-00-00 00:00:00",
			err: "time.value'100000000000000'.is.out.of.range",
		},
		{
			in:  100,
			typ: sqltypes.Date,
			out: "0000-00-00",
			err: "invalid.time.format: '100'",
		},
		{
			in:  700000,
			typ: sqltypes.Date,
			out: "0000-00-00",
			err: "invalid.time.format: '700000'",
		},
		{
			in:  9990101,
			typ: sqltypes.Date,
			out: "0000-00-00",
			err: "invalid.time.format: '9990101'",
		},
		{
			in:  100000000,
			typ: sqltypes.Datetime,
			out: "0000-00-00 00:00:00",
			err: "invalid.time.format: '100000000'",
		},
		{
			in:  700100000000,
			typ: sqltypes.Datetime,
			out: "0000-00-00 00:00:00",
			err: "invalid.time.format: '700100000000'",
		},
	}
	for _, tcase := range tcases {
		res, err := NumToDatetime(tcase.in)
		assert.Equal(t, tcase.typ, res.typ)
		got := res.ValStr()
		assert.Equal(t, tcase.out, got)
		if err != nil {
			got := err.Error()
			assert.Equal(t, tcase.err, got)
		}
	}
}

func TestNumToDuration(t *testing.T) {
	tcases := []struct {
		in  int64
		out string
		err string
	}{
		{
			in:  -80000,
			out: "-08:00:00",
		},
		{
			in:  14530529080000,
			out: "08:00:00",
		},
		{
			in:  8385960,
			out: "838:59:59",
			err: "time.value'8385960'.is.out.of.range",
		},
		{
			in:  -8385960,
			out: "-838:59:59",
			err: "time.value'-8385960'.is.out.of.range",
		},
		{
			in:  85960,
			out: "00:00:00",
			err: "incorrect.time.value'85960'",
		},
	}
	for _, tcase := range tcases {
		res, err := NumToDuration(tcase.in)
		got := res.ValStr()
		assert.Equal(t, tcase.out, got)
		if err != nil {
			got := err.Error()
			assert.Equal(t, tcase.err, got)
		}
	}
}
func TestStrToDatetime(t *testing.T) {
	tcases := []struct {
		in        string
		fsp       int
		typ       querypb.Type
		round     bool
		truncated bool
		out       string
		err       string
	}{
		{
			in:  "12:12:12.023",
			fsp: 2,
			typ: sqltypes.Datetime,
			out: "2012-12-12 23:00:00.00",
		},
		{
			in:  "79-01-01T12:12:12.023",
			fsp: 2,
			typ: sqltypes.Datetime,
			out: "1979-01-01 12:12:12.02",
		},
		{
			in:  "2020/01/01 12:12:12.025",
			fsp: 2,
			typ: sqltypes.Datetime,
			out: "2020-01-01 12:12:12.03",
		},
		{
			in:    "2020-01-01 23:59:59.9999995",
			fsp:   6,
			round: true,
			typ:   sqltypes.Datetime,
			out:   "2020-01-02 00:00:00.000000",
		},
		{
			in:    "190101235959.999999",
			fsp:   5,
			round: true,
			typ:   sqltypes.Datetime,
			out:   "2019-01-02 00:00:00.00000",
		},
		{
			in:  "200101235959.0000009",
			fsp: 6,
			typ: sqltypes.Datetime,
			out: "2020-01-01 23:59:59.000001",
		},
		{
			in:  "200102",
			fsp: 5,
			typ: sqltypes.Date,
			out: "2020-01-02",
		},
		{
			in:        "2020-01-01T 12:12:12.025",
			fsp:       2,
			typ:       sqltypes.Date,
			truncated: true,
			out:       "2020-01-01",
		},
		{
			in:        "2020-01-01T12:12:12.",
			fsp:       2,
			typ:       sqltypes.Datetime,
			truncated: false,
			out:       "2020-01-01 12:12:12.00",
		},
		{
			in:        "200101235959.0000009s",
			fsp:       6,
			typ:       sqltypes.Datetime,
			truncated: true,
			out:       "2020-01-01 23:59:59.000001",
		},
		{
			in:  "2020-02-30 01:01:59",
			fsp: 0,
			typ: sqltypes.Datetime,
			out: "0000-00-00 00:00:00",
			err: "incorrect.datetime.value: '2020-02-30 01:01:59'",
		},
		{
			in:        "2020 01 01",
			fsp:       2,
			typ:       sqltypes.Datetime,
			truncated: true,
			out:       "0000-00-00 00:00:00",
			err:       "truncated.incorrect.datetime.value: '2020 01 01'",
		},
		{
			in:  "0000-00s",
			fsp: 2,
			typ: sqltypes.Datetime,
			out: "0000-00-00 00:00:00",
			err: "incorrect.datetime.value: '0000-00s'",
		},
		{
			in:  "0000-00",
			fsp: 2,
			typ: sqltypes.Datetime,
			out: "0000-00-00 00:00:00",
		},
		{
			in:        "time",
			fsp:       0,
			typ:       sqltypes.Datetime,
			truncated: true,
			out:       "0000-00-00 00:00:00",
			err:       "truncated.incorrect.datetime.value: 'time'",
		},
		{
			in:  "201302",
			fsp: 0,
			typ: sqltypes.Datetime,
			out: "0000-00-00 00:00:00",
			err: "incorrect.datetime.value: '201302'",
		},
		{
			in:        "1000000-12-21 12:23:32",
			fsp:       0,
			typ:       sqltypes.Datetime,
			truncated: true,
			out:       "0000-00-00 00:00:00",
			err:       "truncated.incorrect.datetime.value: '1000000-12-21 12:23:32'",
		},
	}
	for _, tcase := range tcases {
		status := &TimeStatus{}
		res, err := StrToDatetime(tcase.in, tcase.fsp, status)
		assert.Equal(t, tcase.round, status.Round)
		assert.Equal(t, tcase.truncated, status.Truncated)
		assert.Equal(t, tcase.typ, res.typ)
		got := res.ValStr()
		assert.Equal(t, tcase.out, got)
		if err != nil {
			got := err.Error()
			assert.Equal(t, tcase.err, got)
		}
	}
}

func TestStrToDuration(t *testing.T) {
	tcases := []struct {
		in  string
		fsp int
		out string
		err string
	}{
		{
			in:  "-38:59:59.9999",
			fsp: 3,
			out: "-39:00:00.000",
		},
		{
			in:  "23 18:59:59.99999993",
			fsp: 6,
			out: "571:00:00.000000",
		},
		{
			in:  "0000-00-00 00:00:00",
			fsp: 3,
			out: "00:00:00",
		},
		{
			in:  "38:59:59.233",
			fsp: 4,
			out: "38:59:59.2330",
		},
		{
			in:  "12:59:59.",
			fsp: 4,
			out: "12:59:59.0000",
		},
		{
			in:  "2020-01-01 23:59:59.9996",
			fsp: 3,
			out: "24:00:00.000",
		},
		{
			in:  "121212.233",
			fsp: 3,
			out: "12:12:12.233",
		},
		{
			in:  "-  ",
			fsp: 3,
			out: "00:00:00",
		},
		{
			in:  "1000000-12-21 12:23:32",
			fsp: 3,
			out: "00:00:00",
			err: "truncated.incorrect.datetime.value: '1000000-12-21 12:23:32'",
		},
		{
			in:  "200101235959.0000009s",
			fsp: 6,
			out: "00:00:00",
			err: "incorrect.time.value: '200101235959.0000009s'",
		},
		{
			in:  "233961",
			fsp: 4,
			out: "00:00:00",
			err: "time.value'233961'.is.out.of.range",
		},
		{
			in:  "233 18:59:59",
			fsp: 4,
			out: "838:59:59.0000",
			err: "time.value'233 18:59:59'.is.out.of.range",
		},
		{
			in:  "-233 18:59:59",
			fsp: 4,
			out: "-838:59:59.0000",
			err: "time.value'-233 18:59:59'.is.out.of.range",
		},
	}
	for _, tcase := range tcases {
		res, err := StrToDuration(tcase.in, tcase.fsp)
		got := res.ValStr()
		assert.Equal(t, tcase.out, got)
		if err != nil {
			got := err.Error()
			assert.Equal(t, tcase.err, got)
		}
	}
}

func TestStrToYear(t *testing.T) {
	tcases := []struct {
		in  string
		out uint16
		err string
	}{
		{
			in:  "2",
			out: 2002,
		},
		{
			in:  "88",
			out: 1988,
		},
		{
			in:  "2020",
			out: 2020,
		},
		{
			in:  "188",
			out: 0,
			err: "invalid.year.value:'188'",
		},
		{
			in:  "2156",
			out: 0,
			err: "invalid.year.value:'2156'",
		},
		{
			in:  "-23",
			out: 0,
			err: "invalid.year.value:'-23'",
		},
	}
	for _, tcase := range tcases {
		res, err := StrToYear(tcase.in)
		assert.Equal(t, tcase.out, res)
		if err != nil {
			got := err.Error()
			assert.Equal(t, tcase.err, got)
		}
	}
}

func TestCalcDaysInYear(t *testing.T) {
	assert.Equal(t, 365, calcDaysInYear(2019))
	assert.Equal(t, 366, calcDaysInYear(2020))
	assert.Equal(t, 366, calcDaysInYear(2000))
	assert.Equal(t, 365, calcDaysInYear(1900))
}
