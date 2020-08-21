package evaluation

import "expression/datum"

func CAST(arg Evaluation) Evaluation {
	return &CastEval{
		name:     "cast",
		arg:      arg,
		validate: AllArgs(TypeOf(false, datum.RowResult)),
	}
}
