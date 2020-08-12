package planner

import (
	"expression/datum"
	"fmt"
)

type ConstantPlan struct {
	value datum.Datum
}

func NewConstantPlan(value datum.Datum) *ConstantPlan {
	return &ConstantPlan{
		value: value,
	}
}

func (p *ConstantPlan) Walk(visit Visit) error {
	return nil
}

func (p *ConstantPlan) String() string {
	return fmt.Sprintf("%v", p.value.ValStr())
}
