package planner

import (
	"fmt"
	"strings"
)

type TuplePlan struct {
	args []Plan
}

func NewTuplePlan(args []Plan) *TuplePlan {
	return &TuplePlan{
		args: args,
	}
}

func (p *TuplePlan) Walk(visit Visit) error {
	return Walk(visit, p.args...)
}

func (p *TuplePlan) String() string {
	result := make([]string, len(p.args))
	for i, arg := range p.args {
		result[i] = arg.String()
	}
	return fmt.Sprintf("(%s)", strings.Join(result, ", "))
}
