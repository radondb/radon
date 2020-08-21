package planner

import (
	"fmt"

	"expression/evaluation"
)

// VariablePlan ..
type VariablePlan struct {
	column   string
	table    string
	database string
}

// NewVariablePlan new a VariablePlan.
func NewVariablePlan(column, table, database string) *VariablePlan {
	return &VariablePlan{
		column:   column,
		table:    table,
		database: database,
	}
}

// Materialize returns Evaluation by Plan.
func (p *VariablePlan) Materialize() (evaluation.Evaluation, error) {
	return evaluation.VAR(p.String()), nil
}

// String return the plan info.
func (p *VariablePlan) String() string {
	str := fmt.Sprintf("`%s`", p.column)
	if p.table != "" {
		str = fmt.Sprintf("`%s`.%s", p.table, str)
		if p.database != "" {
			str = fmt.Sprintf("`%s`.%s", p.database, str)
		}
	}
	return str
}
