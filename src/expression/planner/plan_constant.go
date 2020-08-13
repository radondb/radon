package planner

import (
	"fmt"

	"expression/datum"
	"expression/evaluation"
)

// ConstantPlan ...
type ConstantPlan struct {
	value datum.Datum
}

// NewConstantPlan new a ConstantPlan.
func NewConstantPlan(value datum.Datum) *ConstantPlan {
	return &ConstantPlan{
		value: value,
	}
}

// Materialize returns Evaluation by Plan.
func (p *ConstantPlan) Materialize() (evaluation.Evaluation, error) {
	return evaluation.CONST(p.value), nil
}

// Walk calls visit on the plan.
func (p *ConstantPlan) Walk(visit Visit) error {
	return nil
}

// String return the plan info.
func (p *ConstantPlan) String() string {
	return fmt.Sprintf("%v", p.value.ValStr())
}
