package evaluation

import (
	"expression/datum"
)

// ADD returns the sum of the two arguments.
func ADD(left, right Evaluation) Evaluation {
	return &BinaryEval{
		name:     "+",
		left:     left,
		right:    right,
		validate: AllArgs(TypeOf(false, datum.RowResult)),
		fixFieldFn: func(left, right *datum.IField) *datum.IField {
			left.ToNumeric()
			right.ToNumeric()

			field := &datum.IField{
				Scale:    datum.TernaryOpt(left.Scale > right.Scale, left.Scale, right.Scale).(int),
				Constant: left.Constant && right.Constant,
			}
			if left.ResTyp == datum.RealResult || right.ResTyp == datum.RealResult {
				field.ResTyp = datum.RealResult
			} else if left.ResTyp == datum.DecimalResult || right.ResTyp == datum.DecimalResult {
				field.ResTyp = datum.DecimalResult
			} else {
				field.ResTyp = datum.IntResult
				field.Flag = left.Flag || right.Flag
			}
			return field
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
			left.ToNumeric()
			right.ToNumeric()
			field := &datum.IField{
				Scale:    datum.TernaryOpt(left.Scale > right.Scale, left.Scale, right.Scale).(int),
				Constant: left.Constant && right.Constant,
			}

			if left.ResTyp == datum.RealResult || right.ResTyp == datum.RealResult {
				field.ResTyp = datum.RealResult
			} else if left.ResTyp == datum.DecimalResult || right.ResTyp == datum.DecimalResult {
				field.ResTyp = datum.DecimalResult
			} else {
				field.ResTyp = datum.IntResult
				field.Flag = left.Flag || right.Flag
			}
			return field
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
			left.ToNumeric()
			right.ToNumeric()
			field := &datum.IField{
				Constant: left.Constant && right.Constant,
			}
			if left.ResTyp == datum.RealResult || right.ResTyp == datum.RealResult {
				field.ResTyp = datum.RealResult
				field.Scale = datum.TernaryOpt(left.Scale+right.Scale > datum.NotFixedDec, datum.NotFixedDec, left.Scale+right.Scale).(int)
			} else if left.ResTyp == datum.DecimalResult || right.ResTyp == datum.DecimalResult {
				field.ResTyp = datum.DecimalResult
				field.Scale = datum.TernaryOpt(left.Scale+right.Scale > datum.DecimalMaxScale, datum.DecimalMaxScale, left.Scale+right.Scale).(int)
			} else {
				field.ResTyp = datum.IntResult
				field.Flag = left.Flag || right.Flag
			}
			return field
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
				Constant: left.Constant && right.Constant,
			}
			if left.ResTyp == datum.RealResult || right.ResTyp == datum.RealResult {
				field.ResTyp = datum.RealResult
				field.Scale = datum.TernaryOpt(left.Scale+4 > datum.NotFixedDec, datum.NotFixedDec, left.Scale+4).(int)
			} else {
				field.ResTyp = datum.DecimalResult
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
				ResTyp:   datum.IntResult,
				Scale:    0,
				Flag:     left.Flag || right.Flag,
				Constant: left.Constant && right.Constant,
			}
		},
		updateFn: func(field *datum.IField, left, right datum.Datum) (datum.Datum, error) {
			return datum.IntDiv(left, right, field)
		},
	}
}
