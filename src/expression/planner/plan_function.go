package planner

import (
	"fmt"
	"strings"
)

type FunctionPlan struct {
	name string
	args []Plan
}

func NewFunctionPlan(name string, args ...Plan) *FunctionPlan {
	return &FunctionPlan{
		name: name,
		args: args,
	}
}

func (p *FunctionPlan) Walk(visit Visit) error {
	return Walk(visit, p.args...)
}

func (p *FunctionPlan) String() string {
	result := make([]string, len(p.args))
	for i, arg := range p.args {
		result[i] = arg.String()
	}
	str := strings.Join(result, ", ")
	return fmt.Sprintf("%s(%s)", p.name, str)
}
