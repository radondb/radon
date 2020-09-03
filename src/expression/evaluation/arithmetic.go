package evaluation

import (
	"expression/datum"
)

// setArithmeticIField for ADD|SUB|MUL.
func setArithmeticIField(left, right *datum.IField, isMul bool) *datum.IField {
	field := &datum.IField{
		Length:   -1,
		IsBinary: true,
	}

	left.ToNumeric()
	right.ToNumeric()
	isReal := false
	if left.Type == datum.RealResult || right.Type == datum.RealResult {
		field.Type = datum.RealResult
		isReal = true
	} else if left.Type == datum.DecimalResult || right.Type == datum.DecimalResult {
		field.Type = datum.DecimalResult
	} else {
		field.Type = datum.IntResult
		field.IsUnsigned = left.IsUnsigned || right.IsUnsigned
	}

	if !isMul {
		field.Scale = datum.TernaryOpt(left.Scale > right.Scale, left.Scale, right.Scale).(int)
	} else {
		if isReal {
			field.Scale = datum.TernaryOpt(left.Scale+right.Scale > datum.NotFixedDec, datum.NotFixedDec, left.Scale+right.Scale).(int)
		} else {
			field.Scale = datum.TernaryOpt(left.Scale+right.Scale > datum.DecimalMaxScale, datum.DecimalMaxScale, left.Scale+right.Scale).(int)
		}
	}
	return field
}

// ADD returns the sum of the two arguments.
func ADD(left, right Evaluation) Evaluation {
	return &BinaryEval{
		name:     "+",
		left:     left,
		right:    right,
		validate: AllArgs(TypeOf(false, datum.RowResult)),
		fixFieldFn: func(left, right *datum.IField) *datum.IField {
			return setArithmeticIField(left, right, false)
		},
		updateFn: func(field *datum.IField, left, right datum.Datum) (datum.Datum, error) {
			return datum.Add(left, right, field)
		},
	}
}

// SUB returns the difference between the two arguments.
func SUB(left, right Evaluation) Evaluation {
	return &BinaryEval{
		name:     "-",
		left:     left,
		right:    right,
		validate: AllArgs(TypeOf(false, datum.RowResult)),
		fixFieldFn: func(left, right *datum.IField) *datum.IField {
			return setArithmeticIField(left, right, false)
		},
		updateFn: func(field *datum.IField, left, right datum.Datum) (datum.Datum, error) {
			return datum.Sub(left, right, field)
		},
	}
}

// MUL returns the dot product of the two arguments.
func MUL(left, right Evaluation) Evaluation {
	return &BinaryEval{
		name:     "*",
		left:     left,
		right:    right,
		validate: AllArgs(TypeOf(false, datum.RowResult)),
		fixFieldFn: func(left, right *datum.IField) *datum.IField {
			return setArithmeticIField(left, right, true)
		},
		updateFn: func(field *datum.IField, left, right datum.Datum) (datum.Datum, error) {
			return datum.Mul(left, right, field)
		},
	}
}

// DIV returns the division of the two arguments.
func DIV(left, right Evaluation) Evaluation {
	return &BinaryEval{
		name:     "/",
		left:     left,
		right:    right,
		validate: AllArgs(TypeOf(false, datum.RowResult)),
		fixFieldFn: func(left, right *datum.IField) *datum.IField {
			left.ToNumeric()
			right.ToNumeric()
			field := &datum.IField{
				Length:   -1,
				IsBinary: true,
			}
			if left.Type == datum.RealResult || right.Type == datum.RealResult {
				field.Type = datum.RealResult
				field.Scale = datum.TernaryOpt(left.Scale+4 > datum.NotFixedDec, datum.NotFixedDec, left.Scale+4).(int)
			} else {
				field.Type = datum.DecimalResult
				field.Scale = datum.TernaryOpt(left.Scale+4 > datum.DecimalMaxScale, datum.DecimalMaxScale, left.Scale+4).(int)
			}
			return field
		},
		updateFn: func(field *datum.IField, left, right datum.Datum) (datum.Datum, error) {
			return datum.Div(left, right, field)
		},
	}
}

// INTDIV returns the int division of the two arguments.
func INTDIV(left, right Evaluation) Evaluation {
	return &BinaryEval{
		name:     "div",
		left:     left,
		right:    right,
		validate: AllArgs(TypeOf(false, datum.RowResult)),
		fixFieldFn: func(left, right *datum.IField) *datum.IField {
			return &datum.IField{
				Type:       datum.IntResult,
				Length:     -1,
				Scale:      0,
				IsUnsigned: left.IsUnsigned || right.IsUnsigned,
				IsBinary:   true,
			}
		},
		updateFn: func(field *datum.IField, left, right datum.Datum) (datum.Datum, error) {
			return datum.IntDiv(left, right, field)
		},
	}
}
