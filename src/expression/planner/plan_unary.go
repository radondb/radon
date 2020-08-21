package planner

import (
	"fmt"

	"expression/evaluation"
)

// UnaryPlan ...
type UnaryPlan struct {
	name string
	arg  Plan
}

// NewUnaryPlan new a UnaryPlan.
func NewUnaryPlan(name string, arg Plan) *UnaryPlan {
	return &UnaryPlan{
		name: name,
		arg:  arg,
	}
}

// Materialize returns Evaluation by Plan.
func (p *UnaryPlan) Materialize() (evaluation.Evaluation, error) {
	eval, err := p.arg.Materialize()
	if err != nil {
		return nil, err
	}
	return evaluation.EvalFactory(p.name, eval)
}

// String return the plan info.
func (p *UnaryPlan) String() string {
	return fmt.Sprintf("%s(%s)", p.name, p.arg.String())
}
