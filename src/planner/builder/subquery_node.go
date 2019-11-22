/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package builder

import (
	"router"
	"xcontext"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// derived record the subquery info.
type derived struct {
	// derived table.
	alias *tableInfo
	// map of derived table's cols.
	colMap map[string]selectTuple
	// referred tables in subquery.
	referTables map[string]*tableInfo
}

// SubNode used to process from subquery.
type SubNode struct {
	log *xlog.Log
	// PlanNode in subquery.
	Sub PlanNode
	// parent node in the plan tree.
	parent *JoinNode
	// the returned result fields.
	fields []selectTuple
	// the filter without tables.
	noTableFilter []sqlparser.Expr
	// children plans in select(such as: orderby, limit..).
	children []ChildPlan
	// the subquery info
	subInfo *derived
	// referred tables.
	referTables map[string]*tableInfo
	// Cols defines which columns from results used to build the return result.
	Cols  []int `json:",omitempty"`
	order int
}

// newSubNode used to create SubNode.
func newSubNode(log *xlog.Log, router *router.Router, Sub PlanNode, subInfo *derived, referTables map[string]*tableInfo) *SubNode {
	return &SubNode{
		log:         log,
		Sub:         Sub,
		subInfo:     subInfo,
		referTables: referTables,
	}
}

// getReferTables get the referTables.
func (s *SubNode) getReferTables() map[string]*tableInfo {
	return s.referTables
}

// getFields get the fields.
func (s *SubNode) getFields() []selectTuple {
	return s.fields
}

// pushFilter used to push the filters.
func (s *SubNode) pushFilter(filter exprInfo) error {
	var err error
	filter, err = replaceCol(filter, s.subInfo.colMap)
	if err != nil {
		return err
	}

	if len(filter.referTables) == 0 {
		s.noTableFilter = append(s.noTableFilter, filter.expr)
		return nil
	}

	if len(filter.vals) > 0 {
		if _, ok := filter.expr.(*sqlparser.ComparisonExpr).Left.(*sqlparser.ColName); !ok {
			filter.vals = nil
		}
	}
	return handleFilter(filter, s.Sub)
}

// setParent set the parent node.
func (s *SubNode) setParent(p *JoinNode) {
	s.parent = p
}

// addNoTableFilter used to push the no table filters.
func (s *SubNode) addNoTableFilter(exprs []sqlparser.Expr) {
	s.noTableFilter = append(s.noTableFilter, exprs...)
}

// calcRoute used to calc the route.
func (s *SubNode) calcRoute() (PlanNode, error) {
	node, err := s.Sub.calcRoute()
	if err != nil {
		return nil, err
	}

	if m, ok := node.(*MergeNode); ok && m.routeLen == 1 {
		m.fields = nil
		s.subInfo.alias.parent = m
		m.subInfos = append(m.subInfos, s.subInfo)
		m.hasParen = false
		m.referTables = s.referTables
		m.Sel = &sqlparser.Select{From: sqlparser.TableExprs([]sqlparser.TableExpr{
			&sqlparser.AliasedTableExpr{
				Expr: &sqlparser.Subquery{Select: m.Sel},
				As:   sqlparser.NewTableIdent(s.subInfo.alias.alias)},
		})}
		m.addNoTableFilter(s.noTableFilter)
		m.parent = s.parent
		return m, nil
	}

	s.Sub = node
	return s, nil
}

func (s *SubNode) pushKeyFilter(filter exprInfo, table, field string) error {
	tuple, err := getMatchedField(field, s.subInfo.colMap)
	if err != nil {
		return err
	}
	if tuple.aggrFuc != "" {
		return errors.New("unsupported: aggregation.field.in.subquery.is.used.in.clause")
	}

	origin := *(filter.cols[0])
	if !tuple.isCol {
		expr := sqlparser.CloneExpr(filter.expr)
		newInfo := exprInfo{
			expr:        sqlparser.ReplaceExpr(expr, fetchCols(expr)[0], tuple.info.expr),
			referTables: tuple.info.referTables,
			cols:        tuple.info.cols,
		}
		if err = handleFilter(newInfo, s.Sub); err != nil {
			return err
		}
	} else {
		filter.cols[0].Name = sqlparser.NewColIdent(tuple.field)
		filter.cols[0].Qualifier = sqlparser.TableName{Name: sqlparser.NewTableIdent(tuple.info.referTables[0])}
		if err = s.Sub.pushKeyFilter(filter, tuple.info.referTables[0], tuple.field); err != nil {
			return err
		}
	}
	// recover the colname.
	*(filter.cols[0]) = origin
	return nil
}

// pushSelectExprs used to push the select fields.
func (s *SubNode) pushSelectExprs(fields, groups []selectTuple, sel *sqlparser.Select, aggTyp aggrType) error {
	if len(groups) > 0 || aggTyp != nullAgg {
		aggrPlan := NewAggregatePlan(s.log, sel.SelectExprs, fields, groups, false)
		if err := aggrPlan.Build(); err != nil {
			return err
		}
		s.children = append(s.children, aggrPlan)
		fields = aggrPlan.tuples
	}

	for _, field := range fields {
		if _, err := s.pushSelectExpr(field); err != nil {
			return err
		}
	}
	return nil
}

// pushSelectExpr used to push the select field, called by JoinNode.pushSelectExpr.
func (s *SubNode) pushSelectExpr(field selectTuple) (int, error) {
	var err error
	var newField selectTuple
	index := -1
	if field.isCol {
		for i, f := range s.Sub.getFields() {
			name := f.alias
			if name == "" {
				name = f.field
			}
			if field.field == name {
				index = i
			}
		}
		if index == -1 {
			return -1, errors.Errorf("unsupported: unknown.column.name.'%s'", field.field)
		}
		if field.alias == "" || field.alias == field.field {
			goto end
		}
	}

	if newField, err = replaceSelect(field, s.subInfo.colMap); err != nil {
		return index, err
	}
	if index, err = handleSelectExpr(newField, s.Sub); err != nil {
		return index, err
	}
end:
	s.Cols = append(s.Cols, index)
	s.fields = append(s.fields, field)
	return len(s.fields) - 1, nil
}

// pushHaving used to push having exprs.
func (s *SubNode) pushHaving(filter exprInfo) error {
	var err error
	filter, err = replaceCol(filter, s.subInfo.colMap)
	if err != nil {
		return err
	}

	if len(filter.referTables) == 0 {
		return s.Sub.pushHaving(filter)
	}
	return handleHaving(filter, s.Sub)
}

// pushOrderBy used to push the order by exprs.
func (s *SubNode) pushOrderBy(orderBy sqlparser.OrderBy) error {
	orderPlan := NewOrderByPlan(s.log, orderBy, s.fields, s.referTables)
	s.children = append(s.children, orderPlan)
	return orderPlan.Build()
}

// pushLimit used to push limit.
func (s *SubNode) pushLimit(limit *sqlparser.Limit) error {
	limitPlan := NewLimitPlan(s.log, limit)
	s.children = append(s.children, limitPlan)
	return limitPlan.Build()
}

// pushMisc used tp push miscelleaneous constructs.
func (s *SubNode) pushMisc(sel *sqlparser.Select) {
	s.Sub.pushMisc(sel)
}

// Children returns the children of the plan.
func (s *SubNode) Children() []ChildPlan {
	return s.children
}

// reOrder satisfies the plannode interface.
func (s *SubNode) reOrder(order int) {
	s.order = order + 1
}

// Order satisfies the plannode interface.
func (s *SubNode) Order() int {
	return s.order
}

// buildQuery used to build the QueryTuple.
func (s *SubNode) buildQuery(root PlanNode) {
	s.Sub.addNoTableFilter(s.noTableFilter)
	s.Sub.buildQuery(s.Sub)
}

// GetQuery used to get the Querys.
func (s *SubNode) GetQuery() []xcontext.QueryTuple {
	return s.Sub.GetQuery()
}
