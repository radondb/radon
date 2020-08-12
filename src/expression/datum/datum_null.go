/*
 * Radon
 *
 * Copyright 2020 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package datum

import (
	"github.com/shopspring/decimal"
)

// DNull ...
type DNull struct {
	null bool
}

// NewDNull new DNull.
func NewDNull(null bool) *DNull {
	return &DNull{null}
}

// Type return datum type.
func (d *DNull) Type() Type {
	return TypeNull
}

// ValInt used to return int64. true: unsigned, false: signed.
func (d *DNull) ValInt() (int64, bool) {
	return 0, false
}

// ValReal used to return float64.
func (d *DNull) ValReal() float64 {
	return 0
}

// ValDecimal used to return decimal.
func (d *DNull) ValDecimal() decimal.Decimal {
	return decimal.NewFromFloat(0)
}

// ValStr used to return string.
func (d *DNull) ValStr() string {
	return "NULL"
}
