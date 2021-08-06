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

// DInt ...
type DInt struct {
	value int64
	// unsigned: true, signed: false
	flag bool
}

// NewDInt new DInt.
func NewDInt(v int64, flag bool) *DInt {
	return &DInt{
		value: v,
		flag:  flag,
	}
}

// Type return datum type.
func (d *DInt) Type() Type {
	return TypeInt
}

// ValInt used to return int64. true: unsigned, false: signed.
func (d *DInt) ValInt() (int64, bool) {
	return d.value, d.flag
}

// ValReal used to return float64.
func (d *DInt) ValReal() float64 {
	if d.flag {
		return float64(uint64(d.value))
	}
	return float64(d.value)
}

// ValDecimal used to return decimal.
func (d *DInt) ValDecimal() decimal.Decimal {
	dec, _ := decimal.NewFromString(d.ValStr())
	return dec
}

// ValStr used to return string.
func (d *DInt) ValStr() string {
	if d.flag {
		return strconv.FormatUint(uint64(d.value), 10)
	}
	return strconv.FormatInt(int64(d.value), 10)
}
