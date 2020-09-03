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
			left, right := args[1], args[2]
			field := &datum.IField{
				IsBinary: left.IsBinary || right.IsBinary,
				Scale:    datum.TernaryOpt(left.Scale > right.Scale, left.Scale, right.Scale).(int),
			}
			if left.Type == datum.DurationResult && right.Type == datum.DurationResult {
				field.Type = datum.DurationResult
			} else if datum.IsTemporal(left.Type) && datum.IsTemporal(left.Type) {
				field.Type = datum.TimeResult
			} else if left.Type == datum.StringResult || right.Type == datum.StringResult {
				field.Type = datum.StringResult
				field.Scale = datum.NotFixedDec
			} else if left.Type == datum.RealResult || right.Type == datum.RealResult {
				field.Type = datum.RealResult
			} else if left.Type == datum.DecimalResult || right.Type == datum.DecimalResult {
				field.Type = datum.DecimalResult
				field.Length = 10
				if field.Scale > 0 {
					field.Length += field.Scale + 1
				}
			} else {
				field.Type = datum.IntResult
				field.IsUnsigned = left.IsUnsigned && right.IsUnsigned
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
