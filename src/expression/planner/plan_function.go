package planner

import (
	"fmt"
	"strings"

	"expression/evaluation"
)

// FunctionPlan ...
type FunctionPlan struct {
	name string
	args []Plan
}

// NewFunctionPlan new a FunctionPlan.
func NewFunctionPlan(name string, args ...Plan) *FunctionPlan {
	return &FunctionPlan{
		name: name,
		args: args,
	}
}

// Materialize returns Evaluation by Plan.
func (p *FunctionPlan) Materialize() (evaluation.Evaluation, error) {
	evals := make([]evaluation.Evaluation, len(p.args))
	for i, arg := range p.args {
		eval, err := arg.Materialize()
		if err != nil {
			return nil, err
		}
		evals[i] = eval
	}
	return evaluation.EvalFactory(p.name, evals...)
}

// Walk calls visit on the plan.
func (p *FunctionPlan) Walk(visit Visit) error {
	return Walk(visit, p.args...)
}

// String return the plan info.
func (p *FunctionPlan) String() string {
	result := make([]string, len(p.args))
	for i, arg := range p.args {
		result[i] = arg.String()
	}
	str := strings.Join(result, ", ")
	return fmt.Sprintf("%s(%s)", p.name, str)
}
