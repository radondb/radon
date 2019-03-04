/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package planner

import (
	"math/rand"
	"router"
	"time"
	"xcontext"

	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// MergeNode can be pushed down.
type MergeNode struct {
	log *xlog.Log
	// database.
	database string
	// select ast.
	sel *sqlparser.Select
	// router.
	router *router.Router
	// shard tables' count in the MergeNode.
	shardCount int
	// if the query can be pushed down a backend, record.
	backend string
	// the shard index, default is -1.
	index int
	// length of the route.
	routeLen int
	// referred tables' tableInfo map.
	referredTables map[string]*TableInfo
	// whether has parenthese in FROM clause.
	hasParen bool
	// parent node in the plan tree.
	parent PlanNode
	// children plans in select(such as: orderby, limit..).
	children *PlanTree
	// query and backend tuple
	Querys []xcontext.QueryTuple
}

// newMergeNode used to create MergeNode.
func newMergeNode(log *xlog.Log, database string, router *router.Router) *MergeNode {
	return &MergeNode{
		log:            log,
		database:       database,
		router:         router,
		referredTables: make(map[string]*TableInfo),
		index:          -1,
		children:       NewPlanTree(),
	}
}

// getReferredTables get the referredTables.
func (m *MergeNode) getReferredTables() map[string]*TableInfo {
	return m.referredTables
}

// setParenthese set hasParen.
func (m *MergeNode) setParenthese(hasParen bool) {
	m.hasParen = hasParen
}

// pushFilter used to push the filters.
func (m *MergeNode) pushFilter(filters []filterTuple) error {
	var err error
	for _, filter := range filters {
		m.sel.AddWhere(filter.expr)
		if len(filter.referTables) == 1 {
			tbInfo, _ := m.referredTables[filter.referTables[0]]
			if tbInfo.shardType != "GLOBAL" && tbInfo.parent.index == -1 && filter.col != nil {
				if nameMatch(filter.col, filter.referTables[0], tbInfo.shardKey) {
					if sqlval, ok := filter.val.(*sqlparser.SQLVal); ok {
						if tbInfo.parent.index, err = m.router.GetIndex(tbInfo.database, tbInfo.tableName, sqlval); err != nil {
							return err
						}
					}
				}
			}
		}
	}
	return err
}

// setParent set the parent node.
func (m *MergeNode) setParent(p PlanNode) {
	m.parent = p
}

// setWhereFilter used to push the where filters.
func (m *MergeNode) setWhereFilter(filter sqlparser.Expr) {
	m.sel.AddWhere(filter)
}

// setNoTableFilter used to push the no table filters.
func (m *MergeNode) setNoTableFilter(exprs []sqlparser.Expr) {
	for _, expr := range exprs {
		m.sel.AddWhere(expr)
	}
}

// pushJoinInWhere used to push the 'join' type filters.
func (m *MergeNode) pushJoinInWhere(joins []joinTuple) (PlanNode, error) {
	for _, joinFilter := range joins {
		m.sel.AddWhere(joinFilter.expr)
	}
	return m, nil
}

// calcRoute used to calc the route.
func (m *MergeNode) calcRoute() (PlanNode, error) {
	var err error
	for _, tbInfo := range m.referredTables {
		if m.shardCount == 0 {
			segments, err := m.router.Lookup(tbInfo.database, tbInfo.tableName, nil, nil)
			if err != nil {
				return nil, err
			}
			rand := rand.New(rand.NewSource(time.Now().UnixNano()))
			m.index = rand.Intn(len(segments))
			m.backend = segments[m.index].Backend
			m.routeLen = 1
			break
		}
		if tbInfo.shardType == "GLOBAL" {
			continue
		}
		tbInfo.Segments, err = m.router.GetSegments(tbInfo.database, tbInfo.tableName, m.index)
		if err != nil {
			return m, err
		}
		if m.backend == "" && len(tbInfo.Segments) == 1 {
			m.backend = tbInfo.Segments[0].Backend
		}
		if m.routeLen == 0 {
			m.routeLen = len(tbInfo.Segments)
		}
	}
	return m, nil
}

// spliceWhere used to splice where clause.
func (m *MergeNode) spliceWhere() error {
	for _, tbInfo := range m.referredTables {
		for _, filter := range tbInfo.whereFilter {
			m.sel.AddWhere(filter)
		}
	}
	return nil
}

// pushSelectExprs used to push the select fileds.
func (m *MergeNode) pushSelectExprs(fileds, groups []selectTuple, sel *sqlparser.Select, hasAggregates bool) error {
	m.sel.SelectExprs = sel.SelectExprs
	m.sel.GroupBy = sel.GroupBy
	m.sel.Distinct = sel.Distinct
	if hasAggregates || len(groups) > 0 {
		aggrPlan := NewAggregatePlan(m.log, sel, fileds, groups)
		if err := aggrPlan.Build(); err != nil {
			return err
		}
		m.children.Add(aggrPlan)
		m.sel.SelectExprs = aggrPlan.ReWritten()
	}
	return nil
}

// pushHaving used to push having exprs.
func (m *MergeNode) pushHaving(havings []filterTuple) error {
	for _, filter := range havings {
		m.sel.AddHaving(filter.expr)
	}
	return nil
}

// pushOrderBy used to push the order by exprs.
func (m *MergeNode) pushOrderBy(sel *sqlparser.Select, fileds []selectTuple) error {
	if len(sel.OrderBy) > 0 {
		m.sel.OrderBy = sel.OrderBy
	} else {
		// group by implicitly contains order by.
		for _, by := range m.sel.GroupBy {
			m.sel.OrderBy = append(m.sel.OrderBy, &sqlparser.Order{
				Expr:      by,
				Direction: sqlparser.AscScr,
			})
		}
	}

	if len(m.sel.OrderBy) > 0 {
		orderPlan := NewOrderByPlan(m.log, m.sel, fileds)
		if err := orderPlan.Build(); err != nil {
			return err
		}
		m.children.Add(orderPlan)
	}
	return nil
}
