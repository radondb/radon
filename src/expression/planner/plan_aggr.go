package planner

import (
	"fmt"
	"strings"

	"expression/evaluation"

	"github.com/pkg/errors"
)

// AggregatePlan ...
type AggregatePlan struct {
	name     string
	distinct bool
	args     []Plan
}

// NewAggregatePlan new a AggregatePlan.
func NewAggregatePlan(name string, distinct bool, args ...Plan) *AggregatePlan {
	return &AggregatePlan{
		name:     name,
		distinct: distinct,
		args:     args,
	}
}

// Materialize returns Evaluation by Plan.
func (p *AggregatePlan) Materialize() (evaluation.Evaluation, error) {
	return nil, errors.Errorf("temporarily.unsupport")
}

// String return the plan info.
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
