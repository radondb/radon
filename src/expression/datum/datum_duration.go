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
)

// Duration is the type for MySQL TIME type.
type Duration struct {
	duration time.Duration
	// fsp is short for Fractional Seconds Precision.
	// See http://dev.mysql.com/doc/refman/5.7/en/fractional-seconds.html
	fsp int
}

// ZeroDuration is the zero value for Duration type.
func ZeroDuration(fsp int) *Duration {
	return &Duration{
		duration: 0,
		fsp:      fsp,
	}
}

// Type return datum type.
func (*Duration) Type() Type {
	return TypeDuration
}

// toNumeric cast the Duration to a numeric datum(DInt, DFloat, DDcimal).
func (d *Duration) toNumeric() Datum {
	sign, hours, minutes, seconds, fraction := splitDuration(d.duration)
	val := sign * (hours*10000 + minutes*100 + seconds)
	if d.fsp == 0 {
		return NewDInt(int64(val), false)
	}
	s := fmt.Sprintf("%d.%06d", val, fraction)
	dval, err := decimal.NewFromString(s[:len(s)-6+d.fsp])
	if err != nil {
		return NewDInt(0, false)
	}
	return NewDDecimal(dval)
}

// ValInt used to return int64. true: unsigned, false: signed.
func (d *Duration) ValInt() (int64, bool) {
	return d.toNumeric().ValInt()
}

// ValReal used to return float64.
func (d *Duration) ValReal() float64 {
	return d.toNumeric().ValReal()
}

// ValDecimal used to return decimal.
func (d *Duration) ValDecimal() decimal.Decimal {
	return d.toNumeric().ValDecimal()
}

// ValStr used to return string.
func (d *Duration) ValStr() string {
	var buf bytes.Buffer
	sign, hours, minutes, seconds, frac := splitDuration(d.duration)
	if sign < 0 {
		buf.WriteByte('-')
	}

	fmt.Fprintf(&buf, "%02d:%02d:%02d", hours, minutes, seconds)
	if d.fsp > 0 {
		buf.WriteString(".")
		fracStr := fmt.Sprintf("%06d", frac)
		buf.WriteString(fracStr[0:d.fsp])
	}
	return buf.String()
}

// RoundFsp round the microsecond with fsp.
// Such as: 23:59:59.999 fsp:2 -> 24:00:00.00
func (d *Duration) RoundFsp(fsp int) *Duration {
	if fsp < d.fsp {
		n := time.Date(0, 0, 0, 0, 0, 0, 0, time.Local)
		d.duration = n.Add(d.duration).Round(time.Duration(math.Pow10(9-fsp)) * time.Nanosecond).Sub(n)
	}
	d.fsp = fsp
	return d
}

// toTime cast Duration to DTime.
func (d *Duration) toTime() *DTime {
	year, month, day := time.Now().Local().Date()
	sign, hour, minute, second, frac := splitDuration(d.duration)
	t := time.Date(year, month, day, sign*hour, sign*minute, sign*second, sign*frac*1000, time.Local)
	return castToDTime(t, d.fsp)
}

// splitDuration split Duration to hour/minute/second/microsecond.
func splitDuration(t time.Duration) (int, int, int, int, int) {
	sign := 1
	if t < 0 {
		t = -t
		sign = -1
	}

	hours := t / time.Hour
	t -= hours * time.Hour
	minutes := t / time.Minute
	t -= minutes * time.Minute
	seconds := t / time.Second
	t -= seconds * time.Second
	fraction := t / time.Microsecond
	return sign, int(hours), int(minutes), int(seconds), int(fraction)
}
