package planner

import (
	"fmt"

	"expression/evaluation"
)

// BinaryPlan ...
type BinaryPlan struct {
	name        string
	left, right Plan
}

// NewBinaryPlan new a BinaryPlan.
func NewBinaryPlan(name string, left, right Plan) *BinaryPlan {
	return &BinaryPlan{
		name:  name,
		left:  left,
		right: right,
	}
}

// Materialize returns Evaluation by Plan.
func (p *BinaryPlan) Materialize() (evaluation.Evaluation, error) {
	left, err := p.left.Materialize()
	if err != nil {
		return nil, err
	}
	right, err := p.right.Materialize()
	if err != nil {
		return nil, err
	}
	return evaluation.EvalFactory(p.name, left, right)
}

// Walk calls visit on the plan.
func (p *BinaryPlan) Walk(visit Visit) error {
	return Walk(visit, p.left, p.right)
}

// String return the plan info.
func (p *BinaryPlan) String() string {
	return fmt.Sprintf("%s(%s, %s)", p.name, p.left.String(), p.right.String())
}
