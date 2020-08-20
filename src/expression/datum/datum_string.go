/*
 * Radon
 *
 * Copyright 2020 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package datum

import (
	"math"
	"strconv"

	"github.com/shopspring/decimal"
)

// DString ...
type DString struct {
	value string
	// default: 10.
	base    int
	charset int
}

// NewDString new DString.
func NewDString(v string, base, charset int) *DString {
	return &DString{
		value:   v,
		base:    base,
		charset: charset,
	}
}

// Type return datum type.
func (d *DString) Type() Type {
	return TypeString
}

// toNumeric cast the DString to a numeric datum(DInt, DFloat, DDcimal).
func (d *DString) toNumeric() Datum {
	if d.base == 16 {
		hex := StrToHex(d.value)
		val, _ := strconv.ParseUint(hex, 16, 64)
		return NewDInt(int64(val), true)
	}

	str := GetFloatPrefix(d.value)
	fval, err1 := strconv.ParseFloat(str, 64)
	if err1 != nil {
		if err2, ok := err1.(*strconv.NumError); ok {
			if err2.Err == strconv.ErrRange {
				if math.IsInf(fval, 1) {
					fval = math.MaxFloat64
				} else if math.IsInf(fval, -1) {
					fval = -math.MaxFloat64
				}
			}
		}
	}
	return NewDFloat(fval)
}

// ValInt used to return int64. true: unsigned, false: signed.
func (d *DString) ValInt() (int64, bool) {
	return d.toNumeric().ValInt()
}

// ValReal used to return float64.
func (d *DString) ValReal() float64 {
	return d.toNumeric().ValReal()
}

// ValDecimal used to return decimal.
func (d *DString) ValDecimal() decimal.Decimal {
	return d.toNumeric().ValDecimal()
}

// ValStr used to return string.
func (d *DString) ValStr() string {
	return string(d.value)
}
