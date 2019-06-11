/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package planner

import (
	"config"
	"router"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// TableInfo represents one table information.
type TableInfo struct {
	// database.
	database string
	// table's name.
	tableName string
	// table's alias.
	alias string
	// table's shard key.
	shardKey string
	// table's shard type.
	shardType string
	// table's config.
	tableConfig *config.TableConfig
	// table expression in select ast 'From'.
	tableExpr *sqlparser.AliasedTableExpr
	// table's route.
	Segments []router.Segment `json:",omitempty"`
	// table's parent node, the type always a MergeNode.
	parent *MergeNode
}

/* scanTableExprs analyzes the 'FROM' clause, build a plannode tree.
 * eg: select t1.a, t3.b from t1 join t2 on t1.a=t2.a join t3 on t1.a=t3.a;
 *             JoinNode
 *               /  \
 *              /    \
 *        JoinNode  MergeNode
 *          /  \
 *         /    \
 *  MergeNode  MergeNode
 */
func scanTableExprs(log *xlog.Log, router *router.Router, database string, tableExprs sqlparser.TableExprs) (PlanNode, error) {
	if len(tableExprs) == 1 {
		return scanTableExpr(log, router, database, tableExprs[0])
	}

	var lpn, rpn PlanNode
	var err error
	if lpn, err = scanTableExpr(log, router, database, tableExprs[0]); err != nil {
		return nil, err
	}
	if rpn, err = scanTableExprs(log, router, database, tableExprs[1:]); err != nil {
		return nil, err
	}
	return join(log, lpn, rpn, nil, router)
}

// scanTableExpr produces a plannode subtree by the TableExpr.
func scanTableExpr(log *xlog.Log, router *router.Router, database string, tableExpr sqlparser.TableExpr) (PlanNode, error) {
	var err error
	var p PlanNode
	switch tableExpr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		p, err = scanAliasedTableExpr(log, router, database, tableExpr)
	case *sqlparser.JoinTableExpr:
		p, err = scanJoinTableExpr(log, router, database, tableExpr)
	case *sqlparser.ParenTableExpr:
		p, err = scanTableExprs(log, router, database, tableExpr.Exprs)
		// If finally p is a MergeNode, the pushed query need keep the parenthese.
		p.setParenthese(true)
	}
	return p, err
}

// scanAliasedTableExpr produces the table's TableInfo by the AliasedTableExpr, and build a MergeNode subtree.
func scanAliasedTableExpr(log *xlog.Log, r *router.Router, database string, tableExpr *sqlparser.AliasedTableExpr) (PlanNode, error) {
	var err error
	mn := newMergeNode(log, database, r)
	switch expr := tableExpr.Expr.(type) {
	case sqlparser.TableName:
		if expr.Qualifier.IsEmpty() {
			expr.Qualifier = sqlparser.NewTableIdent(database)
		}
		tn := &TableInfo{
			database: expr.Qualifier.String(),
			Segments: make([]router.Segment, 0, 16),
		}
		if expr.Qualifier.IsEmpty() {
			expr.Qualifier = sqlparser.NewTableIdent(database)
		}
		tableExpr.Expr = expr
		tn.tableName = expr.Name.String()
		tn.tableConfig, err = r.TableConfig(tn.database, tn.tableName)
		if err != nil {
			return nil, err
		}
		tn.shardKey = tn.tableConfig.ShardKey
		tn.shardType = tn.tableConfig.ShardType
		tn.tableExpr = tableExpr

		switch tn.shardType {
		case "GLOBAL":
			mn.nonGlobalCnt = 0
		case "SINGLE":
			mn.index = append(mn.index, 0)
			mn.nonGlobalCnt = 1
		case "HASH":
			// if a shard table hasn't alias, create one in order to push.
			if tableExpr.As.String() == "" {
				tableExpr.As = sqlparser.NewTableIdent(tn.tableName)
			}
			mn.nonGlobalCnt = 1
		}

		tn.parent = mn
		tn.alias = tableExpr.As.String()
		if tn.alias != "" {
			mn.referredTables[tn.alias] = tn
		} else {
			mn.referredTables[tn.tableName] = tn
		}
	case *sqlparser.Subquery:
		err = errors.New("unsupported: subquery.in.select")
	}
	mn.Sel = &sqlparser.Select{From: sqlparser.TableExprs([]sqlparser.TableExpr{tableExpr})}
	return mn, err
}

// scanJoinTableExpr produces a PlanNode subtree by the JoinTableExpr.
func scanJoinTableExpr(log *xlog.Log, router *router.Router, database string, joinExpr *sqlparser.JoinTableExpr) (PlanNode, error) {
	switch joinExpr.Join {
	case sqlparser.JoinStr, sqlparser.StraightJoinStr, sqlparser.LeftJoinStr:
	case sqlparser.RightJoinStr:
		convertToLeftJoin(joinExpr)
	default:
		return nil, errors.Errorf("unsupported: join.type:%s", joinExpr.Join)
	}
	lpn, err := scanTableExpr(log, router, database, joinExpr.LeftExpr)
	if err != nil {
		return nil, err
	}

	rpn, err := scanTableExpr(log, router, database, joinExpr.RightExpr)
	if err != nil {
		return nil, err
	}
	return join(log, lpn, rpn, joinExpr, router)
}

// join build a PlanNode subtree by judging whether left and right can be merged.
// If can be merged, left and right merge into one MergeNode.
// else build a JoinNode, the two nodes become new joinnode's Left and Right.
func join(log *xlog.Log, lpn, rpn PlanNode, joinExpr *sqlparser.JoinTableExpr, router *router.Router) (PlanNode, error) {
	var joinOn []joinTuple
	var otherJoinOn []filterTuple
	var err error

	referredTables := make(map[string]*TableInfo)
	for k, v := range lpn.getReferredTables() {
		referredTables[k] = v
	}
	for k, v := range rpn.getReferredTables() {
		if _, ok := referredTables[k]; ok {
			return nil, errors.Errorf("unsupported: not.unique.table.or.alias:'%s'", k)
		}
		referredTables[k] = v
	}
	if joinExpr != nil {
		if joinExpr.On == nil {
			joinExpr = nil
		} else {
			if joinOn, otherJoinOn, err = parserWhereOrJoinExprs(joinExpr.On, referredTables); err != nil {
				return nil, err
			}
			for i, jt := range joinOn {
				if jt, err = checkJoinOn(lpn, rpn, jt); err != nil {
					return nil, err
				}
				joinOn[i] = jt
			}

			// inner join's other join on would add to where.
			if joinExpr.Join != sqlparser.LeftJoinStr && len(otherJoinOn) > 0 {
				if len(joinOn) == 0 {
					joinExpr = nil
				}
				for idx, join := range joinOn {
					if idx == 0 {
						joinExpr.On = join.expr
						continue
					}
					joinExpr.On = &sqlparser.AndExpr{
						Left:  joinExpr.On,
						Right: join.expr,
					}
				}
			}
		}
	}

	// analyse if can be merged.
	if lmn, ok := lpn.(*MergeNode); ok {
		if rmn, ok := rpn.(*MergeNode); ok {
			// if all of left's or right's tables are global tables.
			if lmn.nonGlobalCnt == 0 || rmn.nonGlobalCnt == 0 {
				return mergeRoutes(lmn, rmn, joinExpr, otherJoinOn)
			}
			// if join on condition's cols are both shardkey, and the tables have same shards.
			for _, jt := range joinOn {
				if isSameShard(lmn.referredTables, rmn.referredTables, jt.left, jt.right) {
					return mergeRoutes(lmn, rmn, joinExpr, otherJoinOn)
				}
			}
		}
	}
	jn := newJoinNode(log, lpn, rpn, router, joinExpr, joinOn, referredTables)
	lpn.setParent(jn)
	rpn.setParent(jn)
	if jn.IsLeftJoin {
		jn.setOtherJoin(otherJoinOn)
	} else {
		err = jn.pushFilter(otherJoinOn)
	}
	return jn, err
}

// mergeRoutes merges two MergeNode.
func mergeRoutes(lmn, rmn *MergeNode, joinExpr *sqlparser.JoinTableExpr, otherJoinOn []filterTuple) (*MergeNode, error) {
	var err error
	if lmn.hasParen {
		lmn.Sel.From = sqlparser.TableExprs{&sqlparser.ParenTableExpr{Exprs: lmn.Sel.From}}
	}
	if rmn.hasParen {
		rmn.Sel.From = sqlparser.TableExprs{&sqlparser.ParenTableExpr{Exprs: rmn.Sel.From}}
	}
	if joinExpr == nil {
		lmn.Sel.From = append(lmn.Sel.From, rmn.Sel.From...)
	} else {
		lmn.Sel.From = sqlparser.TableExprs{joinExpr}
	}

	for k, v := range rmn.getReferredTables() {
		v.parent = lmn
		lmn.referredTables[k] = v
	}
	if rmn.Sel.Where != nil {
		lmn.Sel.AddWhere(rmn.Sel.Where.Expr)
	}

	lmn.nonGlobalCnt += rmn.nonGlobalCnt
	if joinExpr == nil || joinExpr.Join != sqlparser.LeftJoinStr {
		err = lmn.pushFilter(otherJoinOn)
	}
	return lmn, err
}

// isSameShard used to judge lcn|rcn contain shardkey and have same shards.
func isSameShard(ltb, rtb map[string]*TableInfo, lcn, rcn *sqlparser.ColName) bool {
	lt := ltb[lcn.Qualifier.Name.String()]
	if lt.shardKey == "" || lt.shardKey != lcn.Name.String() {
		return false
	}
	ltp := lt.tableConfig.Partitions
	rt := rtb[rcn.Qualifier.Name.String()]
	if rt.shardKey == "" || lt.shardKey != rcn.Name.String() {
		return false
	}
	rtp := rt.tableConfig.Partitions

	if len(ltp) != len(rtp) {
		return false
	}
	for i, lpart := range ltp {
		if lpart.Segment != rtp[i].Segment || lpart.Backend != rtp[i].Backend {
			return false
		}
	}
	return true
}
