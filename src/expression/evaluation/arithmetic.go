package evaluation

import (
	"expression/datum"

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
)

func ADD(left, right Evaluation) Evaluation {
	return &BinaryEval{
		name:  "+",
		left:  left,
		right: right,
		fixFieldFn: func(left, right *datum.IField) *datum.IField {
			field := &datum.IField{}
			if left.ResTyp == datum.RealResult || right.ResTyp == datum.RealResult {
				field.ResTyp = datum.RealResult
			} else if left.ResTyp == datum.DecimalResult || right.ResTyp == datum.DecimalResult {
				field.ResTyp = datum.DecimalResult
			} else {
				field.ResTyp = datum.IntResult
			}
			field.Decimal = common.TernaryOpt(left.Decimal > right.Decimal, left.Decimal, right.Decimal).(uint32)
			field.Flag = left.Flag || right.Flag
			field.Constant = left.Constant && right.Constant
			return field
		},
		updateFn: func(left, right datum.Datum, field *datum.IField) (datum.Datum, error) {
			return datum.Add(left, right, field)
		},
	}
}

func SUB(left, right Evaluation) Evaluation {
	return &BinaryEval{
		name:  "-",
		left:  left,
		right: right,
		fixFieldFn: func(left, right *datum.IField) *datum.IField {
			var field *datum.IField
			if left.ResTyp == datum.RealResult || right.ResTyp == datum.RealResult {
				field.ResTyp = datum.RealResult
			} else if left.ResTyp == datum.DecimalResult || right.ResTyp == datum.DecimalResult {
				field.ResTyp = datum.DecimalResult
			} else {
				field.ResTyp = datum.IntResult
			}
			field.Decimal = common.TernaryOpt(left.Decimal > right.Decimal, left.Decimal, right.Decimal).(uint32)
			field.Flag = left.Flag || right.Flag
			field.Constant = left.Constant && right.Constant
			return field
		},
		updateFn: func(left, right datum.Datum, field *datum.IField) (datum.Datum, error) {
			return datum.Sub(left, right, field)
		},
	}
}

func MUL(left, right Evaluation) Evaluation {
	return &BinaryEval{
		name:  "*",
		left:  left,
		right: right,
		fixFieldFn: func(left, right *datum.IField) *datum.IField {
			var field *datum.IField
			if left.ResTyp == datum.RealResult || right.ResTyp == datum.RealResult {
				field.ResTyp = datum.RealResult
				field.Decimal = common.TernaryOpt(left.Decimal+right.Decimal > datum.NotFixedDec, datum.NotFixedDec, left.Decimal+right.Decimal).(uint32)
			} else if left.ResTyp == datum.DecimalResult || right.ResTyp == datum.DecimalResult {
				field.ResTyp = datum.DecimalResult
				field.Decimal = common.TernaryOpt(left.Decimal+right.Decimal > datum.DecimalMaxScale, datum.DecimalMaxScale, left.Decimal+right.Decimal).(uint32)
			} else {
				field.ResTyp = datum.IntResult
				field.Flag = left.Flag || right.Flag
			}
			field.Constant = left.Constant && right.Constant
			return field
		},
		updateFn: func(left, right datum.Datum, field *datum.IField) (datum.Datum, error) {
			return datum.Mul(left, right, field)
		},
	}
}

func DIV(left, right Evaluation) Evaluation {
	return &BinaryEval{
		name:  "/",
		left:  left,
		right: right,
		fixFieldFn: func(left, right *datum.IField) *datum.IField {
			var field *datum.IField
			if left.ResTyp == datum.RealResult || right.ResTyp == datum.RealResult {
				field.ResTyp = datum.RealResult
				field.Decimal = common.TernaryOpt(left.Decimal+4 > datum.NotFixedDec, datum.NotFixedDec, left.Decimal+4).(uint32)
			} else {
				field.ResTyp = datum.DecimalResult
				field.Decimal = common.TernaryOpt(left.Decimal+4 > datum.DecimalMaxScale, datum.DecimalMaxScale, left.Decimal+4).(uint32)
			}
			field.Constant = left.Constant && right.Constant
			return field
		},
		updateFn: func(left, right datum.Datum, field *datum.IField) (datum.Datum, error) {
			return datum.Div(left, right, field)
		},
	}
}
