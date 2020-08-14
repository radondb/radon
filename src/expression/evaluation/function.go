package evaluation

import (
	"expression/datum"

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
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
				field.Decimal = datum.NotFixedDec
			} else if left.ResTyp == datum.RealResult || right.ResTyp == datum.RealResult {
				field.ResTyp = datum.RealResult
				field.Decimal = common.TernaryOpt(left.Decimal > right.Decimal, left.Decimal, right.Decimal).(int)
			} else if left.ResTyp == datum.DecimalResult || right.ResTyp == datum.DecimalResult {
				field.ResTyp = datum.DecimalResult
				field.Decimal = common.TernaryOpt(left.Decimal > right.Decimal, left.Decimal, right.Decimal).(int)
			} else {
				field.ResTyp = datum.IntResult
				field.Flag = left.Flag && right.Flag
			}
			return field
		},
		updateFn: func(field *datum.IField, args ...datum.Datum) (datum.Datum, error) {
			cond, _ := args[0].ValInt()
			if cond == 0 {
				return args[2], nil
			}
			return args[1], nil
		},
	}
}
