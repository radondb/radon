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
	"strings"
	"time"

	"router"
	"xcontext"

	"github.com/pkg/errors"
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
	referredTables map[string]*TableInfo
	// whether has parenthese in FROM clause.
	hasParen bool
	// parent node in the plan tree.
	parent SelectNode
	// children plans in select(such as: orderby, limit..).
	children *PlanTree
	// query and backend tuple
	Querys []xcontext.QueryTuple
	// querys with bind locations.
	ParsedQuerys []*sqlparser.ParsedQuery
	// the returned result fields, used in the Multiple Plan Tree.
	fields []selectTuple
	// filters record the filter, map struct for remove duplicate.
	// eg: from t1 join t2 on t1.a=t2.a join t3 on t3.a=t2.a and t2.a=1.
	// need avoid the duplicate filter `t2.a=1`.
	filters map[sqlparser.Expr]int
	order   int
	// Mode.
	ReqMode xcontext.RequestMode
}

// newMergeNode used to create MergeNode.
func newMergeNode(log *xlog.Log, router *router.Router) *MergeNode {
	return &MergeNode{
		log:            log,
		router:         router,
		referredTables: make(map[string]*TableInfo),
		filters:        make(map[sqlparser.Expr]int),
		children:       NewPlanTree(),
		ReqMode:        xcontext.ReqNormal,
	}
}

// getReferredTables get the referredTables.
func (m *MergeNode) getReferredTables() map[string]*TableInfo {
	return m.referredTables
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
func (m *MergeNode) pushFilter(filters []filterTuple) error {
	var err error
	for _, filter := range filters {
		for i, tb := range filter.referTables {
			tbInfo := m.referredTables[tb]
			if tbInfo.inSubquery {
				return errors.Errorf("unsupported: unknow.table.name.'%s'.", tbInfo.alias)
			}
			if tbInfo.tableName == "" {
				f, err := getMatchedField(filter.col.Name.String(), m.fields)
				if err != nil {
					return err
				}
				filter.referTables[i] = f.referTables[0]
				filter.col.Name = sqlparser.NewColIdent(f.field)
			}
		}

		m.addWhere(filter.expr)
		if len(filter.referTables) == 1 {
			tbInfo := m.referredTables[filter.referTables[0]]
			if tbInfo.shardKey != "" && len(filter.vals) > 0 {
				if nameMatch(filter.col, filter.referTables[0], tbInfo.shardKey) {
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
func (m *MergeNode) setWhereFilter(filter filterTuple) {
	m.addWhere(filter.expr)
}

// setNoTableFilter used to push the no table filters.
func (m *MergeNode) setNoTableFilter(exprs []sqlparser.Expr) {
	for _, expr := range exprs {
		m.addWhere(expr)
	}
}

// pushEqualCmpr used to push the 'join' type filters.
func (m *MergeNode) pushEqualCmpr(joins []joinTuple) (SelectNode, error) {
	for _, joinFilter := range joins {
		m.addWhere(joinFilter.expr)
	}
	return m, nil
}

// calcRoute used to calc the route.
func (m *MergeNode) calcRoute() (SelectNode, error) {
	var err error
	for _, tbInfo := range m.referredTables {
		if tbInfo.tableName != "" {
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
	}
	return m, nil
}

// pushSelectExprs used to push the select fields.
func (m *MergeNode) pushSelectExprs(fields, groups []selectTuple, sel *sqlparser.Select, aggTyp aggrType) error {
	var secondTime bool
	node := m.Sel.(*sqlparser.Select)
	if node.SelectExprs == nil {
		if len(m.referredTables) > 1 {
			for _, expr := range sel.SelectExprs {
				if aliasedExpr, ok := expr.(*sqlparser.AliasedExpr); ok {
					if funcExpr, ok := aliasedExpr.Expr.(*sqlparser.FuncExpr); ok {
						if e, ok := funcExpr.Exprs[0].(*sqlparser.AliasedExpr); ok {
							aliasedExpr = e
						}
					}
					if colName, ok := aliasedExpr.Expr.(*sqlparser.ColName); ok && colName.Qualifier.Name.String() == "" {
						return errors.Errorf("unsupported: unknown.column.'%s'.in.clause", colName.Name.String())
					}

				}
			}
		}
		node.SelectExprs = sel.SelectExprs
	}
	if m.fields == nil {
		node.GroupBy = sel.GroupBy
	} else {
		secondTime = true
	}
	node.Distinct = sel.Distinct
	if m.fields == nil {
		m.fields = fields
	} else {
		for _, subPlan := range m.children.children {
			if aggPlan, ok := subPlan.(*AggregatePlan); ok {
				if len(aggPlan.normalAggrs) > 0 {
					return errors.Errorf("unsupported: aggregation.function.in.subquery")
				}
				return errors.Errorf("unsupported: group.by.in.subquery")
			}
		}
		if aggTyp == canPush {
			aggTyp = notPush
		}

		//rebuild the fields
		node.SelectExprs = nil
		unusedFields := make([]selectTuple, len(m.fields))
		dupFields := make([]selectTuple, len(m.fields))
		copy(unusedFields, m.fields)
		copy(dupFields, m.fields)
		var newFields []selectTuple

		for _, field := range fields {
			for _, tb := range field.referTables {
				tbInfo := m.referredTables[tb]
				if tbInfo.inSubquery {
					return errors.Errorf("unsupported: unknow.table.name.'%s'.", tbInfo.alias)
				}
			}
			if field.field == "*" {
				for _, f := range m.fields {
					newFields = append(newFields, f)
					node.SelectExprs = append(node.SelectExprs, f.expr)
				}
			} else {
				if field.aggrFuc == "" {
					f, err := getMatchedField(field.field, m.fields)
					if err != nil {
						return err
					}
					newFields = append(newFields, f)
					node.SelectExprs = append(node.SelectExprs, f.expr)
				} else {
					if field.aggrField == "*" {
						newFields = append(newFields, field)
						node.SelectExprs = append(node.SelectExprs, field.expr)
						continue
					}
					s := strings.Split(field.aggrField, ".")
					field.field = s[len(s)-1]
					f, err := getMatchedField(field.field, m.fields)
					if err != nil {
						return err
					}
					f.aggrFuc = field.aggrFuc
					field.expr.(*sqlparser.AliasedExpr).Expr.(*sqlparser.FuncExpr).Exprs[0] = f.expr.(*sqlparser.AliasedExpr)
					f.expr = field.expr
					f.aggrField = field.field
					if len(f.referTables) > 0 {
						f.aggrField = f.referTables[0] + "." + f.field
					}
					f.alias = field.alias
					f.field = f.aggrFuc + "(" + field.aggrField + ")"

					newFields = append(newFields, f)
					node.SelectExprs = append(node.SelectExprs, f.expr)
				}
			}
		}
		m.fields = newFields
	}

	if len(sel.GroupBy) > 0 {
		// group by implicitly contains order by.
		if len(sel.OrderBy) == 0 {
			for i, by := range sel.GroupBy {
				e, ok := by.(*sqlparser.ColName)
				if ok && secondTime {
					field, _ := getMatchedField(e.Name.String(), m.fields)
					e.Name = sqlparser.NewColIdent(field.field)
					groups[i].field = field.field
					if len(field.referTables) > 0 && e.Qualifier.Name.String() != "" {
						e.Qualifier.Name = sqlparser.NewTableIdent(field.referTables[0])
					}
				}
				node.OrderBy = append(node.OrderBy, &sqlparser.Order{
					Expr:      by,
					Direction: sqlparser.AscScr,
				})
			}
		}
		if len(groups) == 0 {
			if len(node.OrderBy) > 0 {
				orderPlan := NewOrderByPlan(m.log, node.OrderBy, m.fields, m.referredTables)
				if err := orderPlan.Build(); err != nil {
					return err
				}
				m.children.Add(orderPlan)
			}
			return nil
		}
	}

	if aggTyp != nullAgg || len(groups) > 0 {
		aggrPlan := NewAggregatePlan(m.log, node.SelectExprs, m.fields, groups, aggTyp == canPush)
		if err := aggrPlan.Build(); err != nil {
			return err
		}
		m.children.Add(aggrPlan)
		node.SelectExprs = aggrPlan.ReWritten()
	}
	return nil
}

// pushSelectExpr used to push the select field, called by JoinNode.pushSelectExpr.
func (m *MergeNode) pushSelectExpr(field selectTuple) (int, error) {
	for i, f := range m.fields {
		if field.field == f.field {
			node := m.Sel.(*sqlparser.Select)
			node.SelectExprs[i].(*sqlparser.AliasedExpr).As = sqlparser.NewColIdent(field.alias)
			return i, nil
		}
	}
	node := m.Sel.(*sqlparser.Select)
	node.SelectExprs = append(node.SelectExprs, field.expr)
	m.fields = append(m.fields, field)
	return len(m.fields) - 1, nil
}

// pushHaving used to push having exprs.
func (m *MergeNode) pushHaving(havings []filterTuple) error {
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
		for _, order := range node.OrderBy {
			e, ok := order.Expr.(*sqlparser.ColName)
			if ok && e.Qualifier.Name.String() != "" {
				tbInfo := m.referredTables[e.Qualifier.Name.String()]
				if tbInfo.inSubquery {
					return errors.Errorf("unsupported: unknow.table.name.'%s'.", tbInfo.alias)
				}
			}
			if ok && (e.Qualifier.Name.String() != "" && m.referredTables[e.Qualifier.Name.String()].tableName == "") {
				field, err := getMatchedField(e.Name.String(), m.fields)
				if err != nil {
					return err
				}
				e.Name = sqlparser.NewColIdent(field.field)
				if len(field.referTables) > 0 {
					e.Qualifier.Name = sqlparser.NewTableIdent(field.referTables[0])
				}
			}
		}
		orderPlan := NewOrderByPlan(m.log, node.OrderBy, m.fields, m.referredTables)
		if err := orderPlan.Build(); err != nil {
			return err
		}
		m.children.Add(orderPlan)
	}
	return nil
}

// pushLimit used to push limit.
func (m *MergeNode) pushLimit(sel sqlparser.SelectStatement) error {
	limitPlan := NewLimitPlan(m.log, sel.(*sqlparser.Select).Limit)
	if err := limitPlan.Build(); err != nil {
		return err
	}
	m.children.Add(limitPlan)
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
func (m *MergeNode) Children() *PlanTree {
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
func (m *MergeNode) buildQuery(tbInfos map[string]*TableInfo) {
	var Range string
	if sel, ok := m.Sel.(*sqlparser.Select); ok {
		for expr := range m.filters {
			m.addWhere(expr)
		}

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
				if tbInfo, ok := tbInfos[tableName]; ok && tbInfo.tableName == "" {
					f, err := getMatchedField(node.Name.String(), m.fields)
					if err != nil {
						tableName = ""
					} else {
						tableName = f.referTables[0]
					}
					node.Qualifier.Name = sqlparser.NewTableIdent(tableName)
					if tableName == "" {
						break
					}
				}
				if _, ok := m.referredTables[tableName]; !ok {
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
		for _, tbInfo := range m.referredTables {
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
