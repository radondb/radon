/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package builder

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	_ ChildPlan = &AggregatePlan{}
)

// Aggregator tuple.
type Aggregator struct {
	Field    string
	Index    int
	Type     sqltypes.AggrType
	Distinct bool
}

// AggregatePlan represents order-by plan.
type AggregatePlan struct {
	log       *xlog.Log
	tuples    []selectTuple
	groups    []selectTuple
	rewritten sqlparser.SelectExprs

	normalAggrs []Aggregator
	groupAggrs  []Aggregator

	// type
	typ ChildType
	// IsPushDown whether aggfunc can be pushed down.
	IsPushDown bool
}

// NewAggregatePlan used to create AggregatePlan.
func NewAggregatePlan(log *xlog.Log, exprs []sqlparser.SelectExpr, tuples, groups []selectTuple, isPushDown bool) *AggregatePlan {
	return &AggregatePlan{
		log:        log,
		tuples:     tuples,
		groups:     groups,
		rewritten:  exprs,
		typ:        ChildTypeAggregate,
		IsPushDown: isPushDown,
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
	tuples := p.tuples

	// aggregators.
	k := 0
	for _, tuple := range tuples {
		aggrFuc := strings.ToLower(tuple.aggrFuc)
		if aggrFuc == "" {
			if tuple.field == "*" {
				return errors.Errorf("unsupported: exists.aggregate.and.'*'.select.exprs")
			}
			nullAggrs = append(nullAggrs, Aggregator{Field: tuple.field, Index: k, Type: sqltypes.AggrTypeNull})
			k++
			continue
		}

		var aggType sqltypes.AggrType
		switch aggrFuc {
		case "sum":
			aggType = sqltypes.AggrTypeSum
		case "count":
			aggType = sqltypes.AggrTypeCount
		case "min":
			aggType = sqltypes.AggrTypeMin
		case "max":
			aggType = sqltypes.AggrTypeMax
		case "avg":
			aggType = sqltypes.AggrTypeAvg
		default:
			return errors.Errorf("unsupported: function:%+v", tuple.aggrFuc)
		}

		p.normalAggrs = append(p.normalAggrs, Aggregator{Field: tuple.field, Index: k, Type: aggType, Distinct: tuple.distinct})
		if p.IsPushDown {
			if aggType == sqltypes.AggrTypeAvg {
				p.normalAggrs = append(p.normalAggrs, Aggregator{Field: fmt.Sprintf("sum(%s)", tuple.aggrField), Index: k, Type: sqltypes.AggrTypeSum})
				p.normalAggrs = append(p.normalAggrs, Aggregator{Field: fmt.Sprintf("count(%s)", tuple.aggrField), Index: k + 1, Type: sqltypes.AggrTypeCount})
				avgs := decomposeAvg(&tuple)
				p.rewritten = append(p.rewritten, &sqlparser.AliasedExpr{})
				copy(p.rewritten[(k+2):], p.rewritten[k+1:])
				p.rewritten[k] = avgs[0]
				p.rewritten[(k + 1)] = avgs[1]
				k++
			}
		} else {
			p.rewritten[k] = decomposeAgg(&tuple)
			p.tuples[k].expr = p.rewritten[k]
		}
		k++
	}

	// Groupbys.
	for _, by := range p.groups {
		// check: groupby field in select list
		idx := -1
		for _, null := range nullAggrs {
			if strings.EqualFold(null.Field, by.field) {
				idx = null.Index
				break
			}
		}
		if idx == -1 {
			return errors.Errorf("unsupported: group.by.field[%s].should.be.in.noaggregate.select.list", by.field)
		}
		p.groupAggrs = append(p.groupAggrs, Aggregator{Field: by.field, Index: idx, Type: sqltypes.AggrTypeGroupBy})
	}
	return nil
}

// Build used to build distributed querys.
func (p *AggregatePlan) Build() error {
	return p.analyze()
}

// Type returns the type of the plan.
func (p *AggregatePlan) Type() ChildType {
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
