/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package planner

import (
	"router"
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
		for _, tb := range filter.referTables {
			tbInfo, _ := m.referredTables[tb]
			if len(filter.referTables) == 1 {
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
