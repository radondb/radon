/*
 * Radon
 *
 * Copyright 2020 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package datum

import (
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// ResultType is type of the expression return.
type ResultType int

const (
	// StringResult string.
	StringResult ResultType = iota
	// IntResult int.
	IntResult
	// DecimalResult decimal.
	DecimalResult
	// RealResult float64.
	RealResult
	// RowResult tuple.
	RowResult
	// TimeResult time.
	TimeResult
	// DurationResult duration.
	DurationResult
)

// IField is the property of expression's result.
type IField struct {
	// ResTyp result type.
	ResTyp ResultType
	// Decimal is the fraction digits.
	Decimal uint32
	// Flag, unsigned: true, signed: false.
	Flag     bool
	Constant bool
}

// NewIField new IField.
func NewIField(field *querypb.Field) *IField {
	var resTyp ResultType
	typ := field.Type
	switch {
	case sqltypes.IsIntegral(typ):
		resTyp = IntResult
	case sqltypes.IsFloat(typ):
		resTyp = RealResult
	case typ == sqltypes.Decimal:
		resTyp = DecimalResult
	case sqltypes.IsTemporal(typ):
		if typ == sqltypes.Time {
			resTyp = DurationResult
		} else {
			resTyp = TimeResult
		}
	default:
		resTyp = StringResult
	}
	return &IField{resTyp, field.Decimals, (field.Flags & 32) > 0, false}
}

// ToNumeric cast the resulttype to a numeric type.
func (f *IField) ToNumeric() {
	switch f.ResTyp {
	case StringResult:
		f.ResTyp = RealResult
		f.Decimal = NotFixedDec
	case TimeResult, DurationResult:
		if f.Decimal == 0 {
			f.ResTyp = IntResult
		} else {
			f.ResTyp = DecimalResult
		}
	}
}

// IsStringType return true for StringResult, TimeResult or DurationResult.
func IsStringType(typ ResultType) bool {
	return typ == StringResult || typ == TimeResult || typ == DurationResult
}
