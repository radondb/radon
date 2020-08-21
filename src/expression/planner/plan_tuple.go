package planner

import (
	"fmt"
	"strings"

	"expression/evaluation"
)

// TuplePlan ...
type TuplePlan struct {
	args []Plan
}

// NewTuplePlan new a TuplePlan.
func NewTuplePlan(args []Plan) *TuplePlan {
	return &TuplePlan{
		args: args,
	}
}

// Materialize returns Evaluation by Plan.
func (p *TuplePlan) Materialize() (evaluation.Evaluation, error) {
	evals := make([]evaluation.Evaluation, len(p.args))
	for i, arg := range p.args {
		eval, err := arg.Materialize()
		if err != nil {
			return nil, err
		}
		evals[i] = eval
	}
	return evaluation.TUPLE(evals...), nil
}

// String return the plan info.
func (p *TuplePlan) String() string {
	result := make([]string, len(p.args))
	for i, arg := range p.args {
		result[i] = arg.String()
	}
	return fmt.Sprintf("(%s)", strings.Join(result, ", "))
}
