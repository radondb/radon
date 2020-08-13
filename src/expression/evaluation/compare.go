package evaluation

import (
	"expression/datum"
)

// GT process greater than.
func GT(left, right Evaluation) Evaluation {
	return &CompareEval{
		name:     ">",
		left:     left,
		right:    right,
		validate: AllArgs(TypeOf(false, datum.RowResult)),
		updateFn: func(left, right datum.Datum, cmpFunc datum.CompareFunc) datum.Datum {
			if datum.CheckNull(left, right) {
				return datum.NewDNull(true)
			}
			res := cmpFunc(left, right)
			if res == 0 {
				res = -1
			}
			return datum.NewDInt(res, false)
		},
	}
}

// GE process greater than or equal to.
func GE(left, right Evaluation) Evaluation {
	return &CompareEval{
		name:     ">=",
		left:     left,
		right:    right,
		validate: AllArgs(TypeOf(false, datum.RowResult)),
		updateFn: func(left, right datum.Datum, cmpFunc datum.CompareFunc) datum.Datum {
			if datum.CheckNull(left, right) {
				return datum.NewDNull(true)
			}
			res := cmpFunc(left, right)
			if res == 0 {
				res = 1
			}
			return datum.NewDInt(res, false)
		},
	}
}

// EQ process equal.
func EQ(left, right Evaluation) Evaluation {
	return &CompareEval{
		name:     "=",
		left:     left,
		right:    right,
		validate: AllArgs(TypeOf(false, datum.RowResult)),
		updateFn: func(left, right datum.Datum, cmpFunc datum.CompareFunc) datum.Datum {
			if datum.CheckNull(left, right) {
				return datum.NewDNull(true)
			}
			res := cmpFunc(left, right)
			if res == 0 {
				res = 1
			} else {
				res = -1
			}
			return datum.NewDInt(res, false)
		},
	}
}

// LT process less than.
func LT(left, right Evaluation) Evaluation {
	return &CompareEval{
		name:     "<",
		left:     left,
		right:    right,
		validate: AllArgs(TypeOf(false, datum.RowResult)),
		updateFn: func(left, right datum.Datum, cmpFunc datum.CompareFunc) datum.Datum {
			if datum.CheckNull(left, right) {
				return datum.NewDNull(true)
			}
			res := cmpFunc(left, right)
			if res < 0 {
				res = 1
			} else {
				res = -1
			}
			return datum.NewDInt(res, false)
		},
	}
}

// LE process less than or equal to.
func LE(left, right Evaluation) Evaluation {
	return &CompareEval{
		name:     "<=",
		left:     left,
		right:    right,
		validate: AllArgs(TypeOf(false, datum.RowResult)),
		updateFn: func(left, right datum.Datum, cmpFunc datum.CompareFunc) datum.Datum {
			if datum.CheckNull(left, right) {
				return datum.NewDNull(true)
			}
			res := cmpFunc(left, right)
			if res > 0 {
				res = -1
			} else {
				res = 1
			}
			return datum.NewDInt(res, false)
		},
	}
}

// NE process not equal.
func NE(left, right Evaluation) Evaluation {
	return &CompareEval{
		name:     "!=",
		left:     left,
		right:    right,
		validate: AllArgs(TypeOf(false, datum.RowResult)),
		updateFn: func(left, right datum.Datum, cmpFunc datum.CompareFunc) datum.Datum {
			if datum.CheckNull(left, right) {
				return datum.NewDNull(true)
			}
			res := cmpFunc(left, right)
			if res == 0 {
				res = -1
			} else {
				res = 1
			}
			return datum.NewDInt(res, false)
		},
	}
}

// SE process null safe equal.
func SE(left, right Evaluation) Evaluation {
	return &CompareEval{
		name:     "<=>",
		left:     left,
		right:    right,
		validate: AllArgs(TypeOf(false, datum.RowResult)),
		updateFn: func(left, right datum.Datum, cmpFunc datum.CompareFunc) datum.Datum {
			res := datum.NullsafeCompare(left, right, cmpFunc)
			if res == 0 {
				res = 1
			} else {
				res = -1
			}
			return datum.NewDInt(res, false)
		},
	}
}

// IN process in operator.
func IN(left, right Evaluation) Evaluation {
	return &InEval{
		left:  left,
		right: right,
		validate: All(
			Arg(1, TypeOf(false, datum.RowResult)),
			Arg(2, TypeOf(true, datum.RowResult)),
		),
	}
}

// NOTIN process not in operator.
func NOTIN(left, right Evaluation) Evaluation {
	return &InEval{
		not:   true,
		left:  left,
		right: right,
		validate: All(
			Arg(1, TypeOf(false, datum.RowResult)),
			Arg(2, TypeOf(true, datum.RowResult)),
		),
	}
}

// LIKE process like operator.
func LIKE(args ...Evaluation) Evaluation {
	return &FunctionEval{
		name: "like",
		args: args,
		validate: All(
			ExactlyNArgs(3),
			AllArgs(TypeOf(false, datum.RowResult)),
		),
		fixFieldFn: func(args ...*datum.IField) *datum.IField {
			return &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: false,
			}
		},
		updateFn: func(field *datum.IField, args ...datum.Datum) (datum.Datum, error) {
			return datum.Like(args[0], args[1], args[2], false)
		},
	}
}

// NOTLIKE process not like operator.
func NOTLIKE(args ...Evaluation) Evaluation {
	return &FunctionEval{
		name: "not like",
		args: args,
		validate: All(
			ExactlyNArgs(3),
			AllArgs(TypeOf(false, datum.RowResult)),
		),
		fixFieldFn: func(args ...*datum.IField) *datum.IField {
			return &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: false,
			}
		},
		updateFn: func(field *datum.IField, args ...datum.Datum) (datum.Datum, error) {
			return datum.Like(args[0], args[1], args[2], true)
		},
	}
}

// REGEXP process regexp operator.
func REGEXP(left, right Evaluation) Evaluation {
	return &BinaryEval{
		name:     "regexp",
		left:     left,
		right:    right,
		validate: AllArgs(TypeOf(false, datum.RowResult)),
		fixFieldFn: func(left, right *datum.IField) *datum.IField {
			return &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: false,
			}
		},
		updateFn: func(field *datum.IField, left, right datum.Datum) (datum.Datum, error) {
			return datum.Regexp(left, right, false), nil
		},
	}
}

// NOTREGEXP process not regexp operator.
func NOTREGEXP(left, right Evaluation) Evaluation {
	return &BinaryEval{
		name:     "not regexp",
		left:     left,
		right:    right,
		validate: AllArgs(TypeOf(false, datum.RowResult)),
		fixFieldFn: func(left, right *datum.IField) *datum.IField {
			return &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: false,
			}
		},
		updateFn: func(field *datum.IField, left, right datum.Datum) (datum.Datum, error) {
			return datum.Regexp(left, right, true), nil
		},
	}
}
