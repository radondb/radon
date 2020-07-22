/*
 * Radon
 *
 * Copyright 2020 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package datum

import (
	"strconv"

	"github.com/shopspring/decimal"
)

// DString ...
type DString string

// NewDString new DString.
func NewDString(v string) *DString {
	r := DString(v)
	return &r
}

// Type return datum type.
func (d *DString) Type() Type {
	return TypeString
}

// toNumeric cast the DString to a numeric datum(DInt, DFloat, DDcimal).
func (d *DString) toNumeric() Datum {
	var fval float64
	if val, err := strconv.ParseFloat(string(*d), 64); err == nil {
		fval = val
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
	return string(*d)
}
