package evaluation

import (
	"expression/datum"
)

// AND process logic and.
func AND(left, right Evaluation) Evaluation {
	return &BinaryEval{
		name:     "and",
		left:     left,
		right:    right,
		validate: AllArgs(TypeOf(false, datum.RowResult)),
		fixFieldFn: func(left, right *datum.IField) *datum.IField {
			return &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: left.Constant && right.Constant,
			}
		},
		updateFn: func(field *datum.IField, left, right datum.Datum) (datum.Datum, error) {
			if datum.CheckNull(left, right) {
				return datum.NewDNull(true), nil
			}

			res := int64(0)
			v1, _ := left.ValInt()
			if v1 != 0 {
				v2, _ := right.ValInt()
				if v2 != 0 {
					res = 1
				}
			}
			return datum.NewDInt(res, false), nil
		},
	}
}

// OR process logic or.
func OR(left, right Evaluation) Evaluation {
	return &BinaryEval{
		name:     "or",
		left:     left,
		right:    right,
		validate: AllArgs(TypeOf(false, datum.RowResult)),
		fixFieldFn: func(left, right *datum.IField) *datum.IField {
			return &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: left.Constant && right.Constant,
			}
		},
		updateFn: func(field *datum.IField, left, right datum.Datum) (datum.Datum, error) {
			res := int64(0)
			v1, _ := left.ValInt()
			if v1 != 0 {
				res = 1
			} else {
				v2, _ := right.ValInt()
				if v2 != 0 {
					res = 1
				}
			}

			if res == 0 {
				if datum.CheckNull(left, right) {
					return datum.NewDNull(true), nil
				}
			}
			return datum.NewDInt(res, false), nil
		},
	}
}

// NOT process logic NOT.
func NOT(arg Evaluation) Evaluation {
	return &UnaryEval{
		name:     "not",
		arg:      arg,
		validate: AllArgs(TypeOf(false, datum.RowResult)),
		fixFieldFn: func(arg *datum.IField) *datum.IField {
			return &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: arg.Constant,
			}
		},
		updateFn: func(arg datum.Datum, field *datum.IField) (datum.Datum, error) {
			if datum.CheckNull(arg) {
				return datum.NewDNull(true), nil
			}

			res := int64(0)
			val, _ := arg.ValInt()
			if val == 0 {
				res = 1
			}
			return datum.NewDInt(res, false), nil
		},
	}
}
