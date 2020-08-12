package planner

import "fmt"

type VariablePlan struct {
	column   string
	table    string
	database string
}

func NewVariablePlan(column, table, database string) *VariablePlan {
	return &VariablePlan{
		column:   column,
		table:    table,
		database: database,
	}
}

func (p *VariablePlan) Walk(visit Visit) error {
	return nil
}

// `db`.`tb`.`col`
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
