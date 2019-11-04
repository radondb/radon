/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package builder

import (
	"fmt"
	"math/rand"
	"time"

	"router"
	"xcontext"

	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// MergeNode can be pushed down.
type MergeNode struct {
	log *xlog.Log
	// select ast.
	Sel sqlparser.SelectStatement
	// router.
	router *router.Router
	// non-global tables' count in the MergeNode.
	nonGlobalCnt int
	// if the query can be pushed down a backend, record.
	backend string
	// the shard index slice.
	index []int
	// length of the route.
	routeLen int
	// referred tables' tableInfo map.
	referTables map[string]*tableInfo
	// whether has parenthese in FROM clause.
	hasParen bool
	// parent node in the plan tree.
	parent SelectNode
	// children plans in select(such as: orderby, limit..).
	children []ChildPlan
	// query and backend tuple
	Querys []xcontext.QueryTuple
	// querys with bind locations.
	ParsedQuerys []*sqlparser.ParsedQuery
	// the returned result fields, used in the Multiple Plan Tree.
	fields []selectTuple
	order  int
	// Mode.
	ReqMode xcontext.RequestMode
	// aliasIndex is the tmp col's alias index.
	aliasIndex int
}

// newMergeNode used to create MergeNode.
func newMergeNode(log *xlog.Log, router *router.Router) *MergeNode {
	return &MergeNode{
		log:         log,
		router:      router,
		referTables: make(map[string]*tableInfo),
		ReqMode:     xcontext.ReqNormal,
	}
}

// getReferTables get the referTables.
func (m *MergeNode) getReferTables() map[string]*tableInfo {
	return m.referTables
}

// getFields get the fields.
func (m *MergeNode) getFields() []selectTuple {
	if len(m.fields) == 0 {
		exprs := getSelectExprs(m.Sel)
		if len(exprs) > 0 {
			var err error
			m.fields, _, err = parserSelectExprs(exprs, m)
			if err != nil {
				panic(err)
			}
		}
	}
	return m.fields
}

// setParenthese set hasParen.
func (m *MergeNode) setParenthese(hasParen bool) {
	m.hasParen = hasParen
}

// pushFilter used to push the filters.
func (m *MergeNode) pushFilter(filters []exprInfo) error {
	var err error
	for _, filter := range filters {
		m.addWhere(filter.expr)
		if len(filter.referTables) == 1 {
			tbInfo := m.referTables[filter.referTables[0]]
			if tbInfo.shardKey != "" && len(filter.vals) > 0 {
				if nameMatch(filter.cols[0], filter.referTables[0], tbInfo.shardKey) {
					for _, val := range filter.vals {
						if err = getIndex(m.router, tbInfo, val); err != nil {
							return err
						}
					}
				}
			}
		}
	}
	return err
}

func (m *MergeNode) pushKeyFilter(filter exprInfo, table, field string) error {
	expr := sqlparser.CloneExpr(filter.expr)
	m.addWhere(expr)

	tbInfo := m.referTables[table]
	if field == tbInfo.shardKey && len(filter.vals) > 0 {
		for _, val := range filter.vals {
			if err := getIndex(m.router, tbInfo, val); err != nil {
				return err
			}
		}
	}
	return nil
}

// setParent set the parent node.
func (m *MergeNode) setParent(p SelectNode) {
	m.parent = p
}

func (m *MergeNode) addWhere(expr sqlparser.Expr) {
	m.Sel.(*sqlparser.Select).AddWhere(expr)
}

func (m *MergeNode) addHaving(expr sqlparser.Expr) {
	m.Sel.(*sqlparser.Select).AddHaving(expr)
}

// setWhereFilter used to push the where filters.
func (m *MergeNode) setWhereFilter(filter exprInfo) {
	m.addWhere(filter.expr)
}

// setNoTableFilter used to push the no table filters.
func (m *MergeNode) setNoTableFilter(exprs []sqlparser.Expr) {
	for _, expr := range exprs {
		m.addWhere(expr)
	}
}

// pushEqualCmpr used to push the 'join' type filters.
func (m *MergeNode) pushEqualCmpr(joins []exprInfo) SelectNode {
	for _, joinFilter := range joins {
		m.addWhere(joinFilter.expr)
	}
	return m
}

// calcRoute used to calc the route.
func (m *MergeNode) calcRoute() (SelectNode, error) {
	var err error
	for _, tbInfo := range m.referTables {
		if m.nonGlobalCnt == 0 {
			segments, err := m.router.Lookup(tbInfo.database, tbInfo.tableName, nil, nil)
			if err != nil {
				return nil, err
			}
			rand := rand.New(rand.NewSource(time.Now().UnixNano()))
			idx := rand.Intn(len(segments))
			m.backend = segments[idx].Backend
			m.index = append(m.index, idx)
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

// pushSelectExprs used to push the select fields.
func (m *MergeNode) pushSelectExprs(fields, groups []selectTuple, sel *sqlparser.Select, aggTyp aggrType) error {
	node := m.Sel.(*sqlparser.Select)
	node.SelectExprs = sel.SelectExprs
	node.GroupBy = sel.GroupBy
	node.Distinct = sel.Distinct
	m.fields = fields

	if len(sel.GroupBy) > 0 {
		// group by implicitly contains order by.
		if len(sel.OrderBy) == 0 {
			for _, by := range sel.GroupBy {
				node.OrderBy = append(node.OrderBy, &sqlparser.Order{
					Expr:      by,
					Direction: sqlparser.AscScr,
				})
			}
		}
		if len(groups) == 0 {
			if len(node.OrderBy) > 0 {
				orderPlan := NewOrderByPlan(m.log, node.OrderBy, m.fields, m.referTables)
				if err := orderPlan.Build(); err != nil {
					return err
				}
				m.children = append(m.children, orderPlan)
			}
			return nil
		}
	}

	if aggTyp != nullAgg || len(groups) > 0 {
		aggrPlan := NewAggregatePlan(m.log, node.SelectExprs, fields, groups, aggTyp == canPush)
		if err := aggrPlan.Build(); err != nil {
			return err
		}
		m.children = append(m.children, aggrPlan)
		node.SelectExprs = aggrPlan.ReWritten()
	}
	return nil
}

// pushSelectExpr used to push the select field, called by JoinNode.pushSelectExpr.
func (m *MergeNode) pushSelectExpr(field selectTuple) (int, error) {
	if !field.isCol && field.alias == "tmpc" {
		field.alias = fmt.Sprintf("%s_%d", field.alias, m.aliasIndex)
		field.expr.(*sqlparser.AliasedExpr).As = sqlparser.NewColIdent(field.alias)
		m.aliasIndex++
	}

	node := m.Sel.(*sqlparser.Select)
	node.SelectExprs = append(node.SelectExprs, field.expr)
	m.fields = append(m.fields, field)
	return len(m.fields) - 1, nil
}

// pushHaving used to push having exprs.
func (m *MergeNode) pushHaving(havings []exprInfo) error {
	for _, filter := range havings {
		m.addHaving(filter.expr)
	}
	return nil
}

// pushOrderBy used to push the order by exprs.
func (m *MergeNode) pushOrderBy(sel sqlparser.SelectStatement) error {
	node := m.Sel.(*sqlparser.Select)
	if len(sel.(*sqlparser.Select).OrderBy) > 0 {
		node.OrderBy = sel.(*sqlparser.Select).OrderBy
		orderPlan := NewOrderByPlan(m.log, node.OrderBy, m.fields, m.referTables)
		if err := orderPlan.Build(); err != nil {
			return err
		}
		m.children = append(m.children, orderPlan)
	}
	return nil
}

// pushLimit used to push limit.
func (m *MergeNode) pushLimit(sel sqlparser.SelectStatement) error {
	limitPlan := NewLimitPlan(m.log, sel.(*sqlparser.Select).Limit)
	if err := limitPlan.Build(); err != nil {
		return err
	}
	m.children = append(m.children, limitPlan)
	if len(m.Sel.(*sqlparser.Select).GroupBy) == 0 {
		// Rewrite the limit clause.
		m.Sel.SetLimit(limitPlan.ReWritten())
	}
	return nil
}

// pushMisc used tp push miscelleaneous constructs.
func (m *MergeNode) pushMisc(sel *sqlparser.Select) {
	node := m.Sel.(*sqlparser.Select)
	node.Comments = sel.Comments
	node.Lock = sel.Lock
}

// Children returns the children of the plan.
func (m *MergeNode) Children() []ChildPlan {
	return m.children
}

// reOrder satisfies the plannode interface.
func (m *MergeNode) reOrder(order int) {
	m.order = order + 1
}

// Order satisfies the plannode interface.
func (m *MergeNode) Order() int {
	return m.order
}

// buildQuery used to build the QueryTuple.
func (m *MergeNode) buildQuery(tbInfos map[string]*tableInfo) {
	var Range string
	if sel, ok := m.Sel.(*sqlparser.Select); ok {
		if len(sel.SelectExprs) == 0 {
			sel.SelectExprs = append(sel.SelectExprs, &sqlparser.AliasedExpr{
				Expr: sqlparser.NewIntVal([]byte("1"))})
		}
	}

	varFormatter := func(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			tableName := node.Qualifier.Name.String()
			if tableName != "" {
				if _, ok := m.referTables[tableName]; !ok {
					joinVar := procure(tbInfos, node)
					buf.Myprintf("%a", ":"+joinVar)
					return
				}
			}
		}
		node.Format(buf)
	}

	for i := 0; i < m.routeLen; i++ {
		// Rewrite the shard table's name.
		backend := m.backend
		for _, tbInfo := range m.referTables {
			if tbInfo.shardKey == "" {
				continue
			}
			if backend == "" {
				backend = tbInfo.Segments[i].Backend
			}
			Range = tbInfo.Segments[i].Range.String()
			expr, _ := tbInfo.tableExpr.Expr.(sqlparser.TableName)
			expr.Name = sqlparser.NewTableIdent(tbInfo.Segments[i].Table)
			tbInfo.tableExpr.Expr = expr
		}

		buf := sqlparser.NewTrackedBuffer(varFormatter)
		varFormatter(buf, m.Sel)
		pq := buf.ParsedQuery()
		m.ParsedQuerys = append(m.ParsedQuerys, pq)

		tuple := xcontext.QueryTuple{
			Query:   pq.Query,
			Backend: backend,
			Range:   Range,
		}
		m.Querys = append(m.Querys, tuple)
	}
}

// GetQuery used to get the Querys.
func (m *MergeNode) GetQuery() []xcontext.QueryTuple {
	return m.Querys
}

// GenerateFieldQuery generates a query with an impossible where.
// This will be used on the RHS node to fetch field info if the LHS
// returns no result.
func (m *MergeNode) GenerateFieldQuery() *sqlparser.ParsedQuery {
	formatter := func(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			tableName := node.Qualifier.Name.String()
			if tableName != "" {
				if _, ok := m.referTables[tableName]; !ok {
					buf.Myprintf("%a", ":"+node.Qualifier.Name.CompliantName()+"_"+node.Name.CompliantName())
					return
				}
			}
		}
		sqlparser.FormatImpossibleQuery(buf, node)
	}

	buf := sqlparser.NewTrackedBuffer(formatter)
	formatter(buf, m.Sel)
	return buf.ParsedQuery()
}
