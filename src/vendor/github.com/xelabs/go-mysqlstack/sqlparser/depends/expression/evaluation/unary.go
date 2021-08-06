package evaluation

import "github.com/xelabs/go-mysqlstack/sqlparser/depends/expression/datum"

func CAST(arg Evaluation) Evaluation {
	return &CastEval{
		name:     "cast",
		arg:      arg,
		validate: AllArgs(TypeOf(false, datum.RowResult)),
	}
}
