/*
 * Radon
 *
 * Copyright 2020 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package datum

import (
	"bytes"
	"fmt"
	"math"
	"time"

	"github.com/shopspring/decimal"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
)

// DTime is the internal struct type for Time.
type DTime struct {
	typ querypb.Type
	// fsp is short for Fractional Seconds Precision.
	// See http://dev.mysql.com/doc/refman/5.7/en/fractional-seconds.html
	fsp int

	// year <= 9999
	year uint16
	// month <= 12
	month uint8
	// day <= 31
	day uint8
	// hour <= 23
	hour int16
	// minute <= 59
	minute uint8
	// second <= 59
	second uint8
	// second <= 999999
	microsecond uint32
}

// NewDTime new a DTime.
func NewDTime(typ querypb.Type, fsp, year, month, day, hour, minute, second, microsec int) *DTime {
	return &DTime{
		typ:         typ,
		fsp:         fsp,
		year:        uint16(year),
		month:       uint8(month),
		day:         uint8(day),
		hour:        int16(hour),
		minute:      uint8(minute),
		second:      uint8(second),
		microsecond: uint32(microsec),
	}
}

// ZeroDTime is the zero value for DTime.
func ZeroDTime(typ querypb.Type, fsp int) *DTime {
	return &DTime{
		typ: typ,
		fsp: fsp,
	}
}

// Type return datum type.
func (*DTime) Type() Type {
	return TypeTime
}

// toNumeric cast the DString to a numeric datum(DInt, DFloat, DDcimal).
func (d *DTime) toNumeric() Datum {
	switch d.typ {
	case querypb.Type_TIMESTAMP, querypb.Type_DATETIME:
		val := datetimeToInt64(d)
		if d.fsp == 0 {
			return NewDInt(int64(val), false)
		}

		s := fmt.Sprintf("%d.%06d", val, d.microsecond)
		dval, err := decimal.NewFromString(s[:len(s)-6+d.fsp])
		if err != nil {
			return NewDInt(0, false)
		}
		return NewDDecimal(dval)
	default:
		return NewDInt(dateToInt64(d), false)
	}
}

// ValInt used to return int64. true: unsigned, false: signed.
func (d *DTime) ValInt() (int64, bool) {
	return d.toNumeric().ValInt()
}

// ValReal used to return float64.
func (d *DTime) ValReal() float64 {
	return d.toNumeric().ValReal()
}

// ValDecimal used to return decimal.
func (d *DTime) ValDecimal() decimal.Decimal {
	return d.toNumeric().ValDecimal()
}

// ValStr used to return string.
func (d *DTime) ValStr() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%04d-%02d-%02d", d.year, d.month, d.day)
	if d.typ == querypb.Type_DATE {
		return buf.String()
	}

	fmt.Fprintf(&buf, " %02d:%02d:%02d", d.hour, d.minute, d.second)
	if d.fsp > 0 {
		buf.WriteString(".")
		fracStr := fmt.Sprintf("%06d", d.microsecond)
		buf.WriteString(fracStr[0:d.fsp])
	}
	return buf.String()
}

func (d *DTime) RoundFsp(fsp int) *DTime {
	round := false
	if fsp < d.fsp {
		microsec := float64(d.microsecond)
		microsec = (microsec/math.Pow10(d.fsp-fsp-1) + 5) / 10
		if microsec >= math.Pow10(fsp) {
			round = true
		}
		d.microsecond = uint32(microsec) * uint32(math.Pow10(6-fsp))
	}
	if round {
		tmp := time.Date(int(d.year), time.Month(d.month), int(d.day), int(d.hour), int(d.minute), int(d.second), 0, time.Local)
		return castToDTime(tmp.Add(time.Second), fsp)
	}
	d.fsp = fsp
	return d
}

// toDuration converts mysql datetime, timestamp and date to mysql time type.
// e.g,
// 2012-12-12T10:10:10 -> 10:10:10
// 2012-12-12 -> 0
func (d *DTime) toDuration() *Duration {
	if CompareDatetime(d, ZeroDTime(querypb.Type_DATE, 0)) == 0 {
		return NewDuration(0, d.fsp)
	}

	dur := time.Duration(int64(d.hour)*3600+int64(d.minute)*60+int64(d.second))*time.Second + time.Duration(int64(d.microsecond)*1000)
	return &Duration{duration: dur, fsp: d.fsp}
}
