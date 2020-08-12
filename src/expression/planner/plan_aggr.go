package planner

import (
	"fmt"
	"strings"
)

type AggregatePlan struct {
	name     string
	distinct bool
	args     []Plan
}

func NewAggregatePlan(name string, distinct bool, args ...Plan) *AggregatePlan {
	return &AggregatePlan{
		name:     name,
		distinct: distinct,
		args:     args,
	}
}

func (p *AggregatePlan) Walk(visit Visit) error {
	return Walk(visit, p.args...)
}

func (p *AggregatePlan) String() string {
	dist := ""
	if p.distinct {
		dist = "distinct "
	}
	args := make([]string, len(p.args))
	for i, arg := range p.args {
		args[i] = arg.String()
	}
	return fmt.Sprintf("%s(%s%s)", p.name, dist, strings.Join(args, ", "))
}
