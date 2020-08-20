package evaluation

import (
	"expression/datum"
)

// IF (<cond>, <expr1>, <expr2>). Evaluates <cond>, then evaluates <expr1> if the condition is true, or <expr2> otherwise.
func IF(args ...Evaluation) Evaluation {
	return &FunctionEval{
		name: "if",
		args: args,
		validate: All(
			ExactlyNArgs(3),
			AllArgs(TypeOf(false, datum.RowResult)),
		),
		fixFieldFn: func(args ...*datum.IField) *datum.IField {
			field := &datum.IField{}
			left, right := args[1], args[2]
			if datum.IsStringType(left.ResTyp) || datum.IsStringType(right.ResTyp) {
				field.ResTyp = datum.StringResult
				field.Scale = datum.NotFixedDec
			} else if left.ResTyp == datum.RealResult || right.ResTyp == datum.RealResult {
				field.ResTyp = datum.RealResult
				field.Scale = datum.TernaryOpt(left.Scale > right.Scale, left.Scale, right.Scale).(int)
			} else if left.ResTyp == datum.DecimalResult || right.ResTyp == datum.DecimalResult {
				field.ResTyp = datum.DecimalResult
				field.Scale = datum.TernaryOpt(left.Scale > right.Scale, left.Scale, right.Scale).(int)
				field.Length = field.Scale + 11
			} else {
				field.ResTyp = datum.IntResult
				field.Flag = left.Flag && right.Flag
			}
			return field
		},
		updateFn: func(field *datum.IField, args ...datum.Datum) (datum.Datum, error) {
			cond, _ := args[0].ValInt()
			if cond == 0 {
				return datum.Cast(args[2], field, false)
			}
			return datum.Cast(args[1], field, false)
		},
	}
}
