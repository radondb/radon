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

	"router"
	"xcontext"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/hack"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	_ Plan = &SelectPlan{}
)

// SelectPlan represents select plan.
type SelectPlan struct {
	log *xlog.Log

	// router
	router *router.Router

	// select ast
	node *sqlparser.Select

	// database
	database string

	// raw query
	RawQuery string

	// type
	typ PlanType

	// mode
	ReqMode xcontext.RequestMode

	// query and backend tuple
	Querys []xcontext.QueryTuple

	// children plans in select(such as: orderby, limit or join).
	children *PlanTree
}

// TableInfo represents one table information.
type TableInfo struct {
	// database.
	database string
	// table's name.
	tableName string
	// table's shard key.
	shardKey string
	// table expression in select ast 'From'.
	tableExpr *sqlparser.AliasedTableExpr
}

// NewSelectPlan used to create SelectPlan.
func NewSelectPlan(log *xlog.Log, database string, query string, node *sqlparser.Select, router *router.Router) *SelectPlan {
	return &SelectPlan{
		log:      log,
		node:     node,
		router:   router,
		database: database,
		RawQuery: query,
		typ:      PlanTypeSelect,
		Querys:   make([]xcontext.QueryTuple, 0, 16),
		children: NewPlanTree(),
	}
}

// analyze used to check the 'select' is at the support level, and get the db, table, etc..
// Unsupports:
// 1. subquery.
func (p *SelectPlan) analyze() ([]TableInfo, error) {
	var err error
	tableInfos := make([]TableInfo, 0, 4)
	node := p.node

	// Check subquery.
	if hasSubquery(node) || len(node.From) > 1 {
		return nil, errors.New("unsupported: subqueries.in.select")
	}

	// Get table info in the node.From.
	// Only support AliasedTableExpr, JoinTableExpr select.
	switch expr := (node.From[0]).(type) {
	case *sqlparser.AliasedTableExpr:
		tableInfo, err := p.getOneTableInfo(expr)
		if err != nil {
			return nil, err
		}
		tableInfos = append(tableInfos, tableInfo)
	case *sqlparser.JoinTableExpr:
		tableInfos, err = p.getJoinTableInfos(expr, tableInfos)
	default:
		err = errors.New("unsupported: ParenTableExpr.in.select")
	}
	return tableInfos, err
}

// Build used to build distributed querys.
// For now, we don't support subquery in select.
func (p *SelectPlan) Build() error {
	log := p.log
	node := p.node

	tableInfos, err := p.analyze()
	if err != nil {
		return err
	}
	t := tableInfos[0]
	// Support only one shard tables.
	var num int
	for _, tableInfo := range tableInfos {
		if tableInfo.shardKey != "" {
			t = tableInfo
			num++
		}
		if num > 1 {
			return errors.New("unsupported: more.than.one.shard.tables")
		}
	}

	segments, err := getDMLRouting(t.database, t.tableName, t.shardKey, node.Where, p.router)
	if err != nil {
		return err
	}

	// Add sub-plans.
	children := p.children
	if len(segments) > 1 {
		tuples, err := parserSelectExprs(node.SelectExprs)
		if err != nil {
			return err
		}
		// Distinct SubPlan.
		distinctPlan := NewDistinctPlan(log, node)
		if err := distinctPlan.Build(); err != nil {
			return err
		}
		children.Add(distinctPlan)

		// Join SubPlan.
		joinPlan := NewJoinPlan(log, node)
		if err := joinPlan.Build(); err != nil {
			return err
		}
		children.Add(joinPlan)

		// Aggregate SubPlan.
		aggrPlan := NewAggregatePlan(log, node, tuples)
		if err := aggrPlan.Build(); err != nil {
			return err
		}
		children.Add(aggrPlan)
		node.SelectExprs = aggrPlan.ReWritten()

		// Orderby SubPlan.
		orderPlan := NewOrderByPlan(log, node, tuples)
		if err := orderPlan.Build(); err != nil {
			return err
		}
		children.Add(orderPlan)

		// Limit SubPlan.
		if node.Limit != nil {
			limitPlan := NewLimitPlan(log, node)
			if err := limitPlan.Build(); err != nil {
				return err
			}
			children.Add(limitPlan)
			// Rewrite the limit clause.
			node.Limit = limitPlan.ReWritten()
		}
	}

	expr, _ := t.tableExpr.Expr.(sqlparser.TableName)
	// Rewritten the query.
	for _, segment := range segments {
		// Rewrite the shard table's name.
		expr.Name = sqlparser.NewTableIdent(segment.Table)
		t.tableExpr.Expr = expr
		buf := sqlparser.NewTrackedBuffer(nil)
		node.Format(buf)
		rewritten := buf.String()

		tuple := xcontext.QueryTuple{
			Query:   rewritten,
			Backend: segment.Backend,
			Range:   segment.Range.String(),
		}
		p.Querys = append(p.Querys, tuple)
	}
	return nil
}

// Type returns the type of the plan.
func (p *SelectPlan) Type() PlanType {
	return p.typ
}

// JSON returns the plan info.
func (p *SelectPlan) JSON() string {
	type limit struct {
		Offset int
		Limit  int
	}

	type explain struct {
		RawQuery    string                `json:",omitempty"`
		Project     string                `json:",omitempty"`
		Partitions  []xcontext.QueryTuple `json:",omitempty"`
		Aggregate   []string              `json:",omitempty"`
		GatherMerge []string              `json:",omitempty"`
		HashGroupBy []string              `json:",omitempty"`
		Limit       *limit                `json:",omitempty"`
	}

	// Project.
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("%v", p.node.SelectExprs)
	project := buf.String()

	// Aggregate.
	var aggregate []string
	var hashGroup []string
	var gatherMerge []string
	var lim *limit
	for _, sub := range p.children.Plans() {
		switch sub.Type() {
		case PlanTypeAggregate:
			plan := sub.(*AggregatePlan)
			for _, aggr := range plan.normalAggrs {
				aggregate = append(aggregate, aggr.Field)
			}
			for _, aggr := range plan.groupAggrs {
				hashGroup = append(hashGroup, aggr.Field)
			}
		case PlanTypeOrderby:
			plan := sub.(*OrderByPlan)
			for _, order := range plan.OrderBys {
				gatherMerge = append(gatherMerge, order.Field)
			}
		case PlanTypeLimit:
			plan := sub.(*LimitPlan)
			lim = &limit{Offset: plan.Offset, Limit: plan.Limit}
		}
	}

	exp := &explain{Project: project,
		RawQuery:    p.RawQuery,
		Partitions:  p.Querys,
		Aggregate:   aggregate,
		GatherMerge: gatherMerge,
		HashGroupBy: hashGroup,
		Limit:       lim,
	}
	bout, err := json.MarshalIndent(exp, "", "\t")
	if err != nil {
		return err.Error()
	}
	return hack.String(bout)
}

// Children returns the children of the plan.
func (p *SelectPlan) Children() *PlanTree {
	return p.children
}

// Size returns the memory size.
func (p *SelectPlan) Size() int {
	size := len(p.RawQuery)
	for _, q := range p.Querys {
		size += len(q.Query)
	}
	return size
}

// getOneTableInfo returns one table info.
func (p *SelectPlan) getOneTableInfo(aliasTableExpr *sqlparser.AliasedTableExpr) (TableInfo, error) {
	var tableInfo TableInfo
	if aliasTableExpr == nil {
		return tableInfo, errors.New("unsupported: aliasTableExpr cannot be nil")
	}

	switch expr := aliasTableExpr.Expr.(type) {
	case sqlparser.TableName:
		if expr.Qualifier.IsEmpty() {
			expr.Qualifier = sqlparser.NewTableIdent(p.database)
		}
		aliasTableExpr.Expr = expr
		tableInfo.database = expr.Qualifier.String()
		tableInfo.tableName = expr.Name.String()
		shardkey, err := p.router.ShardKey(tableInfo.database, tableInfo.tableName)
		if err != nil {
			return tableInfo, err
		}
		tableInfo.shardKey = shardkey
		tableInfo.tableExpr = aliasTableExpr
	default:
		return tableInfo, errors.New("unsupported: subqueries.in.select")
	}

	if tableInfo.shardKey != "" && aliasTableExpr.As.String() == "" {
		aliasTableExpr.As = sqlparser.NewTableIdent(tableInfo.tableName)
	}
	return tableInfo, nil
}

// getJoinTableInfos used to get the tables' info for join type.
func (p *SelectPlan) getJoinTableInfos(joinTableExpr *sqlparser.JoinTableExpr, tableInfos []TableInfo) ([]TableInfo, error) {
	rightExpr, OK := (joinTableExpr.RightExpr).(*sqlparser.AliasedTableExpr)
	if !OK {
		return nil, errors.New("unsupported: JOIN.expression")
	}
	tableInfo, err := p.getOneTableInfo(rightExpr)
	if err != nil {
		return nil, err
	}
	tableInfos = append(tableInfos, tableInfo)
	switch joinTableExpr.LeftExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		tableInfo, err = p.getOneTableInfo(joinTableExpr.LeftExpr.(*sqlparser.AliasedTableExpr))
		if err != nil {
			return nil, err
		}
		tableInfos = append(tableInfos, tableInfo)
	case *sqlparser.JoinTableExpr:
		return p.getJoinTableInfos(joinTableExpr.LeftExpr.(*sqlparser.JoinTableExpr), tableInfos)
	default:
		return nil, errors.New("unsupported: ParenTableExpr.in.select")
	}
	return tableInfos, nil
}
