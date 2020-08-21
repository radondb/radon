package evaluation

import (
	"expression/datum"

	"github.com/xelabs/go-mysqlstack/sqldb"
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
				Charset: datum.TernaryOpt(left.Charset == sqldb.CharacterSetUtf8 && right.Charset == sqldb.CharacterSetUtf8,
					sqldb.CharacterSetUtf8, sqldb.CharacterSetBinary).(int),
				Scale: datum.TernaryOpt(left.Scale > right.Scale, left.Scale, right.Scale).(int),
			}
			if left.ResTyp == datum.DurationResult && right.ResTyp == datum.DurationResult {
				field.ResTyp = datum.DurationResult
			} else if datum.IsTemporal(left.ResTyp) && datum.IsTemporal(left.ResTyp) {
				field.ResTyp = datum.TimeResult
			} else if left.ResTyp == datum.StringResult || right.ResTyp == datum.StringResult {
				field.ResTyp = datum.StringResult
				field.Scale = datum.NotFixedDec
			} else if left.ResTyp == datum.RealResult || right.ResTyp == datum.RealResult {
				field.ResTyp = datum.RealResult
			} else if left.ResTyp == datum.DecimalResult || right.ResTyp == datum.DecimalResult {
				field.ResTyp = datum.DecimalResult
				field.Length = 10
				if field.Scale > 0 {
					field.Length += field.Scale + 1
				}
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
