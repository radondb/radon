/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package planner

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	_ Plan = &AggregatePlan{}
)

// AggrType type.
type AggrType string

const (
	// AggrTypeNull enum.
	AggrTypeNull AggrType = ""

	// AggrTypeCount enum.
	AggrTypeCount AggrType = "COUNT"

	// AggrTypeSum enum.
	AggrTypeSum AggrType = "SUM"

	// AggrTypeMin enum.
	AggrTypeMin AggrType = "MIN"

	// AggrTypeMax enum.
	AggrTypeMax AggrType = "MAX"

	// AggrTypeAvg enum.
	AggrTypeAvg AggrType = "AVG"

	// AggrTypeGroupBy enum.
	AggrTypeGroupBy AggrType = "GROUP BY"
)

// Aggregator tuple.
type Aggregator struct {
	Field string
	Index int
	Type  AggrType
}

// AggregatePlan represents order-by plan.
type AggregatePlan struct {
	log       *xlog.Log
	node      *sqlparser.Select
	tuples    []selectTuple
	rewritten sqlparser.SelectExprs

	normalAggrs []Aggregator
	groupAggrs  []Aggregator

	// type
	typ PlanType
}

// NewAggregatePlan used to create AggregatePlan.
func NewAggregatePlan(log *xlog.Log, node *sqlparser.Select, tuples []selectTuple) *AggregatePlan {
	return &AggregatePlan{
		log:       log,
		node:      node,
		tuples:    tuples,
		rewritten: node.SelectExprs,
		typ:       PlanTypeAggregate,
	}
}

// analyze used to check the aggregator is at the support level.
// Supports:
// SUM/COUNT/MIN/MAX/AVG/GROUPBY
// Notes:
// group by fields must be in the select list, for example:
// select count(a), a from t group by a --[OK]
// select count(a) from t group by a    --[ER]
func (p *AggregatePlan) analyze() error {
	var nullAggrs []Aggregator
	node := p.node
	tuples := p.tuples

	// Check the having has expr value.
	exprInHaving := false
	exprInHavingStr := ""
	if node.Having != nil {
		_ = sqlparser.Walk(func(n sqlparser.SQLNode) (kontinue bool, err error) {
			switch n.(type) {
			case *sqlparser.FuncExpr:
				exprInHaving = true
				buf := sqlparser.NewTrackedBuffer(nil)
				n.Format(buf)
				exprInHavingStr = buf.String()
				return false, nil
			}
			return true, nil
		}, node.Having)
	}

	if exprInHaving {
		return errors.Errorf("unsupported: expr[%s].in.having.clause", exprInHavingStr)
	}

	// aggregators.
	k := 0
	for _, tuple := range tuples {
		if tuple.distinct {
			return errors.Errorf("unsupported: distinct.in.function:%+v", tuple.fn)
		}

		aggrType := strings.ToLower(tuple.fn)
		switch aggrType {
		case "":
			// non-func
			nullAggrs = append(nullAggrs, Aggregator{Field: tuple.field, Index: k, Type: AggrTypeNull})
		case "sum":
			p.normalAggrs = append(p.normalAggrs, Aggregator{Field: tuple.field, Index: k, Type: AggrTypeSum})
		case "count":
			p.normalAggrs = append(p.normalAggrs, Aggregator{Field: tuple.field, Index: k, Type: AggrTypeCount})
		case "min":
			p.normalAggrs = append(p.normalAggrs, Aggregator{Field: tuple.field, Index: k, Type: AggrTypeMin})
		case "max":
			p.normalAggrs = append(p.normalAggrs, Aggregator{Field: tuple.field, Index: k, Type: AggrTypeMax})
		case "avg":
			p.normalAggrs = append(p.normalAggrs, Aggregator{Field: tuple.field, Index: k, Type: AggrTypeAvg})
			p.normalAggrs = append(p.normalAggrs, Aggregator{Field: fmt.Sprintf("sum(%s)", tuple.column), Index: k + 1, Type: AggrTypeSum})
			p.normalAggrs = append(p.normalAggrs, Aggregator{Field: fmt.Sprintf("count(%s)", tuple.column), Index: k + 2, Type: AggrTypeCount})

			avgs := decomposeAvg(&tuple)
			p.rewritten = append(p.rewritten, &sqlparser.AliasedExpr{}, &sqlparser.AliasedExpr{})
			copy(p.rewritten[(k+1)+2:], p.rewritten[(k+1):])
			p.rewritten[(k + 1)] = avgs[0]
			p.rewritten[(k+1)+1] = avgs[1]
			k += 2
		default:
			return errors.Errorf("unsupported: function:%+v", tuple.fn)
		}
		k++
	}

	// Groupbys.
	groupbys := node.GroupBy
	for _, by := range groupbys {
		by1 := by.(*sqlparser.ColName)
		// check: select ... from t groupby t.a
		if !by1.Qualifier.IsEmpty() {
			return errors.Errorf("unsupported: group.by.field[%s].have.table.name[%s].please.use.AS.keyword", by1.Name, by1.Qualifier.Name)
		}
		field := by1.Name.String()
		// check: groupby field in select list
		idx := -1
		for _, null := range nullAggrs {
			if null.Field == field {
				idx = null.Index
				break
			}
		}
		if idx == -1 {
			return errors.Errorf("unsupported: group.by.field[%s].should.be.in.select.list", field)
		}
		p.groupAggrs = append(p.groupAggrs, Aggregator{Field: field, Index: idx, Type: AggrTypeGroupBy})
	}
	return nil
}

// Build used to build distributed querys.
func (p *AggregatePlan) Build() error {
	return p.analyze()
}

// Type returns the type of the plan.
func (p *AggregatePlan) Type() PlanType {
	return p.typ
}

// JSON returns the plan info.
func (p *AggregatePlan) JSON() string {
	type aggrs struct {
		Aggrs     []Aggregator
		ReWritten string
	}
	a := &aggrs{}
	a.Aggrs = append(a.Aggrs, p.normalAggrs...)
	a.Aggrs = append(a.Aggrs, p.groupAggrs...)

	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("%v", p.rewritten)
	a.ReWritten = buf.String()

	bout, err := json.MarshalIndent(a, "", "\t")
	if err != nil {
		return err.Error()
	}
	return string(bout)
}

// Children returns the children of the plan.
func (p *AggregatePlan) Children() *PlanTree {
	return nil
}

// NormalAggregators returns the aggregators.
func (p *AggregatePlan) NormalAggregators() []Aggregator {
	return p.normalAggrs
}

// GroupAggregators returns the group aggregators.
func (p *AggregatePlan) GroupAggregators() []Aggregator {
	return p.groupAggrs
}

// ReWritten used to re-write the SelectExprs clause.
func (p *AggregatePlan) ReWritten() sqlparser.SelectExprs {
	return p.rewritten
}

// Empty returns the aggregator number more than zero.
func (p *AggregatePlan) Empty() bool {
	return (len(p.normalAggrs) == 0 && len(p.groupAggrs) == 0)
}

// Size returns the memory size.
func (p *AggregatePlan) Size() int {
	return 0
}
