package planner

import (
	"expression/evaluation"
)

// Plan interface.
type Plan interface {
	String() string
	Materialize() (evaluation.Evaluation, error)
}

func getArgsNum(p Plan) int {
	if t, ok := p.(*TuplePlan); ok {
		return len(t.args)
	}
	return 1
}

func popFirstArg(p Plan) Plan {
	if t, ok := p.(*TuplePlan); ok {
		if len(t.args) == 2 {
			return t.args[1]
		}
		return NewTuplePlan(t.args[1:])
	}
	return nil
}
