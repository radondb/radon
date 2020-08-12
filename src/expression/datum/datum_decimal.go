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
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
)

// DDecimal ...
type DDecimal struct {
	value decimal.Decimal
}

// NewDDecimal used to new a DDecimal.
func NewDDecimal(value decimal.Decimal) *DDecimal {
	return &DDecimal{
		value: value,
	}
}

// Type return datum type.
func (d *DDecimal) Type() Type {
	return TypeDecimal
}

// ValInt used to return int64. true: unsigned, false: signed.
func (d *DDecimal) ValInt() (int64, bool) {
	fval, _ := d.value.Float64()
	return common.Float64ToInt64(fval), false
}

// ValReal used to return float64.
func (d *DDecimal) ValReal() float64 {
	fval, _ := d.value.Float64()
	return fval
}

// ValDecimal used to return decimal.
func (d *DDecimal) ValDecimal() decimal.Decimal {
	return d.value
}

// ValStr used to return string.
func (d *DDecimal) ValStr() string {
	return d.value.String()
}
