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

// DFloat ...
type DFloat float64

// NewDFloat new DFloat.
func NewDFloat(v float64) *DFloat {
	r := DFloat(v)
	return &r
}

// Type return datum type.
func (d *DFloat) Type() Type {
	return TypeFloat
}

// ValInt used to return int64. true: unsigned, false: signed.
func (d *DFloat) ValInt() (int64, bool) {
	fval := float64(*d)
	return Float64ToInt64(fval), false
}

// ValReal used to return float64.
func (d *DFloat) ValReal() float64 {
	return float64(*d)
}

// ValDecimal used to return decimal.
func (d *DFloat) ValDecimal() decimal.Decimal {
	return decimal.NewFromFloat(float64(*d))
}

// ValStr used to return string.
func (d *DFloat) ValStr() string {
	return strconv.FormatFloat(float64(*d), 'g', -1, 64)
}
