/*
 * Radon
 *
 * Copyright 2020 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package datum

import (
	"strings"

	"github.com/shopspring/decimal"
)

// DTuple ...
type DTuple struct {
	args []Datum
}

// NewDTuple new DTuple.
func NewDTuple(v ...Datum) *DTuple {
	return &DTuple{args: v}
}

// Args return the args.
func (d *DTuple) Args() []Datum {
	return d.args
}

// Type return datum type.
func (d *DTuple) Type() Type {
	return TypeTuple
}

// ValInt used to return int64. true: unsigned, false: signed.
func (d *DTuple) ValInt() (int64, bool) {
	panic("unreachable")
}

// ValReal used to return float64.
func (d *DTuple) ValReal() float64 {
	panic("unreachable")
}

// ValDecimal used to return decimal.
func (d *DTuple) ValDecimal() decimal.Decimal {
	panic("unreachable")
}

// ValStr used to return string.
func (d *DTuple) ValStr() string {
	result := make([]string, len(d.args))
	for i, arg := range d.args {
		result[i] = arg.ValStr()
	}
	return strings.Join(result, "")
}
