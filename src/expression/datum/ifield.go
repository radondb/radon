/*
 * Radon
 *
 * Copyright 2020 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package datum

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
	Decimal int32
	// Flag, unsigned: true, signed: false.
	Flag bool
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
