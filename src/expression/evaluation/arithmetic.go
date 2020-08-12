package evaluation

import (
	"math"

	"expression/datum"

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
)

func ADD(left, right Evaluation) Evaluation {
	return &BinaryEval{
		name:     "+",
		left:     left,
		right:    right,
		validate: AllArgs(ResTyp(false, datum.RowResult)),
		fixFieldFn: func(left, right *datum.IField) *datum.IField {
			left.ToNumeric()
			right.ToNumeric()
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
		updateFn: func(field *datum.IField, left, right datum.Datum) (datum.Datum, error) {
			return datum.Add(left, right, field)
		},
	}
}

func SUB(left, right Evaluation) Evaluation {
	return &BinaryEval{
		name:     "-",
		left:     left,
		right:    right,
		validate: AllArgs(ResTyp(false, datum.RowResult)),
		fixFieldFn: func(left, right *datum.IField) *datum.IField {
			left.ToNumeric()
			right.ToNumeric()
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
		updateFn: func(field *datum.IField, left, right datum.Datum) (datum.Datum, error) {
			return datum.Sub(left, right, field)
		},
	}
}

func MUL(left, right Evaluation) Evaluation {
	return &BinaryEval{
		name:     "*",
		left:     left,
		right:    right,
		validate: AllArgs(ResTyp(false, datum.RowResult)),
		fixFieldFn: func(left, right *datum.IField) *datum.IField {
			left.ToNumeric()
			right.ToNumeric()
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
		updateFn: func(field *datum.IField, left, right datum.Datum) (datum.Datum, error) {
			return datum.Mul(left, right, field)
		},
	}
}

func DIV(left, right Evaluation) Evaluation {
	return &BinaryEval{
		name:     "/",
		left:     left,
		right:    right,
		validate: AllArgs(ResTyp(false, datum.RowResult)),
		fixFieldFn: func(left, right *datum.IField) *datum.IField {
			left.ToNumeric()
			right.ToNumeric()
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
		updateFn: func(field *datum.IField, left, right datum.Datum) (datum.Datum, error) {
			return datum.Div(left, right, field)
		},
	}
}

func INTDIV(left, right Evaluation) Evaluation {
	return &BinaryEval{
		name:     "div",
		left:     left,
		right:    right,
		validate: AllArgs(ResTyp(false, datum.RowResult)),
		fixFieldFn: func(left, right *datum.IField) *datum.IField {
			left.ToNumeric()
			right.ToNumeric()
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
		updateFn: func(field *datum.IField, left, right datum.Datum) (datum.Datum, error) {
			res, err := datum.Div(left, right, field)
			if err != nil {
				return nil, err
			}
			if datum.CheckNull(res) {
				return res, nil
			}
			return datum.NewDInt(int64(math.Trunc(res.ValReal())), false), nil
		},
	}
}
