package planner

import (
	"expression/evaluation"
)

// Plan interface.
type Plan interface {
	Walk(visit Visit) error
	String() string
	Materialize() (evaluation.Evaluation, error)
}

// Visit defines the signature of a function that
// can be used to visit all nodes of a plan tree.
type Visit func(plan Plan) (kontinue bool, err error)

// Walk calls visit on every plan.
// If visit returns true, the underlying nodes
// are also visited. If it returns an error, walking
// is interrupted, and the error is returned.
func Walk(visit Visit, plans ...Plan) error {
	for _, plan := range plans {
		if plan == nil {
			continue
		}
		kontinue, err := visit(plan)
		if err != nil {
			return err
		}
		if kontinue {
			err = plan.Walk(visit)
			if err != nil {
				return err
			}
		}
	}
	return nil
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
