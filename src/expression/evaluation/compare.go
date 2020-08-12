package evaluation

import (
	"expression/datum"
)

func GT(left, right Evaluation) Evaluation {
	return &CompareEval{
		name:     ">",
		left:     left,
		right:    right,
		validate: AllArgs(ResTyp(false, datum.RowResult)),
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

func GE(left, right Evaluation) Evaluation {
	return &CompareEval{
		name:     ">=",
		left:     left,
		right:    right,
		validate: AllArgs(ResTyp(false, datum.RowResult)),
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

func EQ(left, right Evaluation) Evaluation {
	return &CompareEval{
		name:     "=",
		left:     left,
		right:    right,
		validate: AllArgs(ResTyp(false, datum.RowResult)),
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

func LT(left, right Evaluation) Evaluation {
	return &CompareEval{
		name:     "<",
		left:     left,
		right:    right,
		validate: AllArgs(ResTyp(false, datum.RowResult)),
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

func LE(left, right Evaluation) Evaluation {
	return &CompareEval{
		name:     "<=",
		left:     left,
		right:    right,
		validate: AllArgs(ResTyp(false, datum.RowResult)),
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

func NE(left, right Evaluation) Evaluation {
	return &CompareEval{
		name:     "!=",
		left:     left,
		right:    right,
		validate: AllArgs(ResTyp(false, datum.RowResult)),
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

func SE(left, right Evaluation) Evaluation {
	return &CompareEval{
		name:     "<=>",
		left:     left,
		right:    right,
		validate: AllArgs(ResTyp(false, datum.RowResult)),
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

func IN(left, right Evaluation) Evaluation {
	return &InEval{
		left:  left,
		right: right,
		validate: All(
			Arg(1, ResTyp(false, datum.RowResult)),
			Arg(2, ResTyp(true, datum.RowResult)),
		),
	}
}

func NOTIN(left, right Evaluation) Evaluation {
	return &InEval{
		not:   true,
		left:  left,
		right: right,
		validate: All(
			Arg(1, ResTyp(false, datum.RowResult)),
			Arg(2, ResTyp(true, datum.RowResult)),
		),
	}
}

func LIKE(args ...Evaluation) Evaluation {
	return &FunctionEval{
		name: "like",
		args: args,
		validate: All(
			ExactlyNArgs(3),
			AllArgs(ResTyp(false, datum.RowResult)),
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

func NOTLIKE(args ...Evaluation) Evaluation {
	return &FunctionEval{
		name: "not like",
		args: args,
		validate: All(
			ExactlyNArgs(3),
			AllArgs(ResTyp(false, datum.RowResult)),
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

func REGEXP(left, right Evaluation) Evaluation {
	return &BinaryEval{
		name:     "regexp",
		left:     left,
		right:    right,
		validate: AllArgs(ResTyp(false, datum.RowResult)),
		fixFieldFn: func(left, right *datum.IField) *datum.IField {
			return &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: false,
			}
		},
		updateFn: func(field *datum.IField, left, right datum.Datum) (datum.Datum, error) {
			return datum.Regexp(left, right, false)
		},
	}
}

func NOTREGEXP(left, right Evaluation) Evaluation {
	return &BinaryEval{
		name:     "not regexp",
		left:     left,
		right:    right,
		validate: AllArgs(ResTyp(false, datum.RowResult)),
		fixFieldFn: func(left, right *datum.IField) *datum.IField {
			return &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: false,
			}
		},
		updateFn: func(field *datum.IField, left, right datum.Datum) (datum.Datum, error) {
			return datum.Regexp(left, right, true)
		},
	}
}
