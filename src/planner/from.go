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
	// referred table's where clause.
	whereFilter []sqlparser.Expr
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
		tn := &TableInfo{
			database: database,
			Segments: make([]router.Segment, 0, 16),
		}
		if expr.Qualifier.IsEmpty() {
			expr.Qualifier = sqlparser.NewTableIdent(database)
		}
		tableExpr.Expr = expr
		tn.database = expr.Qualifier.String()
		tn.tableName = expr.Name.String()
		tn.tableConfig, err = r.TableConfig(tn.database, tn.tableName)
		if err != nil {
			return nil, err
		}
		tn.shardKey = tn.tableConfig.ShardKey
		tn.shardType = tn.tableConfig.ShardType
		tn.tableExpr = tableExpr

		// if a shard table hasn't alias, create one in order to push.
		if tn.tableConfig.ShardKey != "" {
			if tableExpr.As.String() == "" {
				tableExpr.As = sqlparser.NewTableIdent(tn.tableName)
			}
			mn.shardCount = 1
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
	mn.sel = &sqlparser.Select{From: sqlparser.TableExprs([]sqlparser.TableExpr{tableExpr})}
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
		referredTables[k] = v
	}
	if joinExpr != nil && joinExpr.On != nil {
		if joinOn, otherJoinOn, err = parserWhereOrJoinExprs(joinExpr.On, referredTables); err != nil {
			return nil, err
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

	// analyse if can be merged.
	if lmn, ok := lpn.(*MergeNode); ok {
		if rmn, ok := rpn.(*MergeNode); ok {
			// if all of left's or right's tables are global tables.
			if lmn.shardCount == 0 || rmn.shardCount == 0 {
				return mergeRoutes(lmn, rmn, joinExpr, otherJoinOn)
			}
			// if join on condition's cols are both shardkey, and the tables have same shards.
			for _, jt := range joinOn {
				left := jt.expr.Left.(*sqlparser.ColName)
				right := jt.expr.Right.(*sqlparser.ColName)
				if isSameShard(lmn.referredTables, rmn.referredTables, left, right) {
					return mergeRoutes(lmn, rmn, joinExpr, otherJoinOn)
				}
			}
		}
	}
	jn := newJoinNode(log, lpn, rpn, router, joinExpr, joinOn, referredTables)
	lpn.setParent(jn)
	rpn.setParent(jn)
	if jn.isLeftJoin {
		jn.otherJoinOn = otherJoinOn
	} else {
		err = jn.pushFilter(otherJoinOn)
	}
	return jn, err
}

// mergeRoutes merges two MergeNode.
func mergeRoutes(lmn, rmn *MergeNode, joinExpr *sqlparser.JoinTableExpr, otherJoinOn []filterTuple) (*MergeNode, error) {
	var err error
	if lmn.hasParen {
		lmn.sel.From = sqlparser.TableExprs{&sqlparser.ParenTableExpr{Exprs: lmn.sel.From}}
	}
	if rmn.hasParen {
		rmn.sel.From = sqlparser.TableExprs{&sqlparser.ParenTableExpr{Exprs: rmn.sel.From}}
	}
	if joinExpr == nil {
		lmn.sel.From = append(lmn.sel.From, rmn.sel.From...)
	} else {
		lmn.sel.From = sqlparser.TableExprs{joinExpr}
	}

	for k, v := range rmn.getReferredTables() {
		v.parent = lmn
		lmn.referredTables[k] = v
	}
	if rmn.sel.Where != nil {
		lmn.setWhereFilter(rmn.sel.Where.Expr)
	}

	lmn.shardCount += rmn.shardCount
	if joinExpr == nil || joinExpr.Join != sqlparser.LeftJoinStr {
		err = lmn.pushFilter(otherJoinOn)
	}
	return lmn, err
}

// isShardKey used to judge whether the col contains shardkey.
func isShardKey(col *sqlparser.ColName, tbInfos map[string]*TableInfo) (bool, []*config.PartitionConfig) {
	tbInfo, ok := tbInfos[col.Qualifier.Name.String()]
	if ok {
		if tbInfo.shardKey == col.Name.String() {
			return true, tbInfo.tableConfig.Partitions
		}
	}
	return false, nil
}

// isSameShard used to judge lcn|rcn contain shardkey and have same shards.
func isSameShard(ltb, rtb map[string]*TableInfo, lcn, rcn *sqlparser.ColName) bool {
	var ltp, rtp []*config.PartitionConfig
	lt, ok := ltb[lcn.Qualifier.Name.String()]
	if !ok {
		ok, ltp = isShardKey(rcn, ltb)
		if !ok {
			return false
		}
		ok, rtp = isShardKey(lcn, rtb)
		if !ok {
			return false
		}
	} else {
		if lt.shardKey != lcn.Name.String() {
			return false
		}
		ltp = lt.tableConfig.Partitions
		ok, rtp = isShardKey(rcn, rtb)
		if !ok {
			return false
		}
	}

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
