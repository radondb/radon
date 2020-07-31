package evaluation

import (
	"expression/datum"
)

func GT(left, right Evaluation) Evaluation {
	return &CompareEval{
		name:  ">",
		left:  left,
		right: right,
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
		name:  ">=",
		left:  left,
		right: right,
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
		name:  "=",
		left:  left,
		right: right,
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
		name:  "<",
		left:  left,
		right: right,
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
		name:  "<=",
		left:  left,
		right: right,
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
		name:  "!=",
		left:  left,
		right: right,
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
		name:  "<=>",
		left:  left,
		right: right,
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

/* in|not in to or
func IN(left, right Evaluation) Evaluation {
	return &CompareEval{
		name:  "in",
		left:  left,
		right: right,
		updateFn: func(left, right datum.Datum, cmpFunc datum.CompareFunc) datum.Datum {
			if datum.CheckNull(left) {
				return datum.NewDNull(true)
			}

			var (
				hasNull  = false
				match    = false
				val      = int64(-1)
				tuple, _ = right.(*datum.DTuple)
			)

			for _, arg := range tuple.Args() {
				if datum.CheckNull(arg) {
					hasNull = true
					continue
				}
				res := datum.NullsafeCompare(left, right, cmpFunc)
				if res == 0 {
					match = true
					break
				}
			}

			if !match && hasNull {
				return datum.NewDNull(true)
			}
			if match {
				val = 1
			}
			return datum.NewDInt(val, false)
		},
	}
}*/
