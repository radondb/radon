package planner

import "fmt"

type UnaryPlan struct {
	name string
	arg  Plan
}

func NewUnaryPlan(name string, arg Plan) *UnaryPlan {
	return &UnaryPlan{
		name: name,
		arg:  arg,
	}
}

func (p *UnaryPlan) Walk(visit Visit) error {
	return Walk(visit, p.arg)
}

func (p *UnaryPlan) String() string {
	return fmt.Sprintf("%s(%s)", p.name, p.arg.String())
}
