package planner

import "fmt"

type BinaryPlan struct {
	name        string
	left, right Plan
}

func NewBinaryPlan(name string, left, right Plan) *BinaryPlan {
	return &BinaryPlan{
		name:  name,
		left:  left,
		right: right,
	}
}

func (p *BinaryPlan) Walk(visit Visit) error {
	return Walk(visit, p.left, p.right)
}

func (p *BinaryPlan) String() string {
	return fmt.Sprintf("%s(%s, %s)", p.name, p.left.String(), p.right.String())
}
