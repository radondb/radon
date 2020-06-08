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
	"reflect"
	"strconv"
	"testing"

	"router"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestSqlJoinParser(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	querys := []string{
		"select u1.id, u2.id from user u1 join user u2 on u2.id = u1.col where u1.id = 1",
	}

	for _, query := range querys {
		log.Debug("query:%s", query)
		sel, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		tree := sel.(*sqlparser.Select)
		processTableExprs(log, tree.From)
		filters := splitAndExpression(nil, tree.Where.Expr)
		log.Debug("where:%+v", filters[0])
		log.Debug("\n")
	}

}

func processTableExprs(log *xlog.Log, tableExprs sqlparser.TableExprs) {
	log.Debug("tables.count:%d", len(tableExprs))
	for _, tableExpr := range tableExprs {
		processTableExpr(log, tableExpr)
	}
}

func processTableExpr(log *xlog.Log, tableExpr sqlparser.TableExpr) {
	switch tableExpr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		log.Debug("AliasedTableExpr %+v, %+v", tableExpr.Expr, tableExpr.As)
	case *sqlparser.ParenTableExpr:
		log.Debug("ParenTableExpr %+v", tableExpr)
	case *sqlparser.JoinTableExpr:
		processJoin(log, tableExpr)
	}
}

func debugNode(log *xlog.Log, tableExpr sqlparser.TableExpr) {
	sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		default:
			//log.Debug("type:%v", node)
			_ = node
			return true, nil
		}
	}, tableExpr)
}

func processOnExpr(log *xlog.Log, on sqlparser.Expr) {
	switch on.(type) {
	case *sqlparser.ComparisonExpr:
		on := on.(*sqlparser.ComparisonExpr)
		log.Debug("on.compareison... %+v,%+v", on.Left, on.Right)
		left := on.Left
		switch left.(type) {
		case *sqlparser.ColName:
			cname := left.(*sqlparser.ColName)
			log.Debug("left:....%+v", cname)
		}
	}
}

func processJoin(log *xlog.Log, ajoin *sqlparser.JoinTableExpr) {
	debugNode(log, ajoin)
	log.Debug("jointype:%v", ajoin.Join)
	switch ajoin.Join {
	case sqlparser.JoinStr, sqlparser.StraightJoinStr, sqlparser.LeftJoinStr:
		log.Debug("leftjoin")
	case sqlparser.RightJoinStr:
		log.Debug("rightjoin")
	}
	processTableExpr(log, ajoin.LeftExpr)
	processTableExpr(log, ajoin.RightExpr)
	processOnExpr(log, ajoin.On)
}

func TestSQLInsert(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	querys := []string{
		"insert into t(a,b,c,d) values(1,2,3)",
		"insert into a.t(a,b,c,d)values(3.1415,4,'5',6),(4,4,'4',4)",
	}

	for _, query := range querys {
		log.Debug("query:%s", query)
		ast, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		tree := ast.(*sqlparser.Insert)

		// table
		qua := sqlparser.NewTableIdent("xx")
		tree.Table.Qualifier = qua
		log.Debug("table:%+v", tree.Table)
		log.Debug("ondup:%+v, %d", tree.OnDup, len(tree.OnDup))

		// columns
		log.Debug("columns:%+v", tree.Columns)

		// rows
		for _, rows := range tree.Rows.(sqlparser.Values) {
			log.Debug("row:%+v", rows)
			for _, row := range rows {
				log.Debug("\titem:%+v, type:%+v", row, reflect.TypeOf(row))
			}
		}

		// end
		log.Debug("\n")
	}
}

func valConvert(node sqlparser.Expr) (interface{}, error) {
	switch node := node.(type) {
	case *sqlparser.SQLVal:
		switch node.Type {
		case sqlparser.ValArg:
			return string(node.Val), nil
		case sqlparser.StrVal:
			return []byte(node.Val), nil
		case sqlparser.HexVal:
			return node.HexDecode()
		case sqlparser.IntVal:
			val := string(node.Val)
			signed, err := strconv.ParseInt(val, 0, 64)
			if err == nil {
				return signed, nil
			}
			unsigned, err := strconv.ParseUint(val, 0, 64)
			if err == nil {
				return unsigned, nil
			}
			return nil, err
		}
	case *sqlparser.NullVal:
		return nil, nil
	}
	return nil, fmt.Errorf("%v is not a value", sqlparser.String(node))
}

func TestSQLDelete(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	querys := []string{
		"delete from sbtest.t where t.id >=3 and id=4 and id='x'",
	}

	for _, query := range querys {
		log.Debug("query:%s", query)
		ast, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		tree := ast.(*sqlparser.Delete)

		// table
		log.Debug("table:%+v", tree.Table)

		// where
		filters := splitAndExpression(nil, tree.Where.Expr)
		log.Debug("where:%+v", filters)
		for _, filter := range filters {
			comparison, ok := filter.(*sqlparser.ComparisonExpr)
			if !ok {
				continue
			}
			val, err := valConvert(comparison.Right)
			if err != nil {
				continue
			}
			log.Debug("%+v%+v%+v.type:%+v", comparison.Left, comparison.Operator, val, comparison.Right.(*sqlparser.SQLVal).Type)
		}

		// end
		log.Debug("\n")
	}
}

func TestSqlParserSelect(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	querys := []string{
		"select tb1.id from test.tb1",
	}

	for _, query := range querys {
		log.Debug("query:%s", query)
		sel, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		selectTree := sel.(*sqlparser.Select)
		for _, tableExpr := range selectTree.From {
			switch tableExpr := tableExpr.(type) {
			case *sqlparser.AliasedTableExpr:
				log.Debug("AliasedTableExpr")
				processAliasedTable(log, tableExpr)
			case *sqlparser.ParenTableExpr:
				log.Debug("ParenTableExpr")
			case *sqlparser.JoinTableExpr:
				log.Debug("JoinTableExpr")
				processJoin(log, tableExpr)
			}
		}
		log.Debug("\n")
	}
}

func processAliasedTable(log *xlog.Log, tableExpr *sqlparser.AliasedTableExpr) {
	switch expr := tableExpr.Expr.(type) {
	case *sqlparser.TableName:
		log.Debug("table:%+v", expr)
	case *sqlparser.Subquery:
	}
}

func TestSQLXA(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	querys := []string{
		"xa start 'xatest'",
		" XA END 'xatest'",
		"XA PREPARE 'xatest'",
		"XA COMMIT 'xatest'",
	}

	for _, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		log.Debug("%+v", node)
	}
}

func TestSQLShardKey(t *testing.T) {
	querys := []string{
		"CREATE TABLE t1 (col1 INT, col2 CHAR(5)) PARTITION BY HASH(col1)",
	}

	for _, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		ddl := node.(*sqlparser.DDL)
		want := "col1"
		got := ddl.PartitionOption.(*sqlparser.PartOptHash).Name
		assert.Equal(t, want, got)
	}
}

func TestSQLUseDB(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	querys := []string{
		"use xx",
	}

	for _, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		log.Debug("%+v", node)

		sef := node.(*sqlparser.Use)
		assert.Equal(t, "xx", sef.DBName.String())
	}
}

func TestSQLDDL(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	querys := []string{
		"create database xx",
		"create table `你/ 好`(a int, b int, c varchar)",
		"create database if not exists xx",
		"create table db.foo(a int, b int, c varchar)",
		"create table foo(a int, b int, c varchar) partition by hash(a)",
		"create table foo(a int, b int, c varchar) partition by hash(a)  PARTITIONS 6",
		"create index a on b(x,c)",
		"create index a on db.b(x,c)",
	}

	for _, query := range querys {
		log.Debug("%+v", query)
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		sef := node.(*sqlparser.DDL)
		log.Debug("%+v", sef.Table)
	}
}

func TestSQLDDLWithDatabase(t *testing.T) {
	querys := []string{
		"create table db.foo(a int, b int, c varchar)",
		"create table db.foo(a int, b int, c varchar) partition by hash(a)",
		"create table db.foo(a int, b int, c varchar) partition by hash(a)  PARTITIONS 6",
		"create index a on db.b(x,c)",
		"create index a on db.b(x,c)",
	}

	for _, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		sef := node.(*sqlparser.DDL)
		assert.Equal(t, "db", sef.Table.Qualifier.String())
	}
}

func TestSQLDDLWithUnique(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	querys := []string{
		"CREATE TABLE t1(a int primary key,a1 char(12), b int unique) PARTITION BY HASH(a);",
	}

	for _, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		sef := node.(*sqlparser.DDL)
		columns := sef.TableSpec.Columns
		for _, col := range columns {
			log.Debug("--ddl.columns:%+v", col)
		}
	}
}

func TestSQLSelect(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	querys := []string{
		"select a.id from t.a,b where a.id=b.id and a.id >(select count(*) from d)",
	}

	for _, query := range querys {
		log.Debug("query:%s", query)
		sel, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		node := sel.(*sqlparser.Select)

		var table string
		switch v := (node.From[0]).(type) {
		case *sqlparser.AliasedTableExpr:
			table = sqlparser.String(v.Expr)
		case *sqlparser.JoinTableExpr:
			if ate, ok := (v.LeftExpr).(*sqlparser.AliasedTableExpr); ok {
				table = sqlparser.String(ate.Expr)
			} else {
				table = sqlparser.String(v)
			}
		default:
			table = sqlparser.String(v)
		}
		log.Debug("+++++table:%s", table)

		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch node := node.(type) {
			case *sqlparser.TableName:
				log.Debug("find.table.name:%s", node)
				if node.Qualifier.IsEmpty() {
					node.Qualifier = sqlparser.NewTableIdent("sbtest")
				}
			}
			return true, nil
		}, node.From)

		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			log.Debug("--node:[type:%v, values:%+v]", reflect.TypeOf(node), node)
			return true, nil
		}, node.Where)

		buf := sqlparser.NewTrackedBuffer(nil)
		node.Format(buf)
		log.Debug("--newquery:%s", buf.String())
	}
}

func TestSQLSelectShardKey(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	querys := []string{
		"select a.id from t.a,b where a.id=b.id and a.id >(select count(*) from d)",
		"select 1 from sbtest.t1 right outer join t2 on a = b",
		"select u1.id, u2.id from user u1 join user u2 on u2.id = u1.col where u1.id = 1",
		"select u1.id, u2.id from user u1 left join user u2 on u2.id = u1.col where u1.id = 1",
		"select 1 from t1 right outer join t2 on a = b",
		"select a.id from a,b where a.id=b.id",
		"select a.id from a",
		"select id from db.a",
	}

	for _, query := range querys {
		log.Debug("query:%s", query)
		sel, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		node := sel.(*sqlparser.Select)

		var table, database string
		var tableExpr *sqlparser.AliasedTableExpr
		switch expr := (node.From[0]).(type) {
		case *sqlparser.AliasedTableExpr:
			tableExpr = expr
		case *sqlparser.JoinTableExpr:
			if ate, ok := (expr.LeftExpr).(*sqlparser.AliasedTableExpr); ok {
				tableExpr = ate
			}
		case *sqlparser.ParenTableExpr:
			log.Panic("don't support ParenTableExpr, %+v", expr)
		}

		switch expr := tableExpr.Expr.(type) {
		case *sqlparser.TableName:
			if !expr.Qualifier.IsEmpty() {
				database = expr.Qualifier.String()
			}
			table = expr.Name.String()
		}

		log.Debug("db:%s, table:%s\n", database, table)
	}
}

func TestSQLSelectOrderBy(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	querys := []string{
		"select a,b from t order by a desc",
		"select a,b from t order by rand()",
		"select a,b from t order by abs(a)",
		"select a as a1 from t order by a1",
	}

	for _, query := range querys {
		log.Debug("query:%s", query)
		sel, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		node := sel.(*sqlparser.Select)
		orderby := node.OrderBy
		for _, o := range orderby {
			log.Debug("orderby:type:%T, expr:%+v, %+v", o.Expr, o.Expr, o)
		}
		log.Debug("\n")
	}
}

func TestSQLSelectExprs(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	querys := []string{
		"select a as a1, b as b1 from t order by a1",
	}

	for _, query := range querys {
		log.Debug("query:%s", query)
		sel, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		node := sel.(*sqlparser.Select)
		exprs := node.SelectExprs
		for _, e := range exprs {
			switch e.(type) {
			case *sqlparser.AliasedExpr:
				e1 := e.(*sqlparser.AliasedExpr)
				log.Debug("expr:type:%T, expr:%+v, %+v", e, e, e1.Expr)
			}
		}
		log.Debug("\n")
	}
}

func TestSQLSelectLimit(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	querys := []string{
		"select a from t",
		"select a from t limit 5,9",
		"select a from t limit 9 offset 5",
		"select a from t limit b,-9",
		"select a from t limit 1",
	}

	for _, query := range querys {
		log.Debug("query:%s", query)
		sel, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		node := sel.(*sqlparser.Select)
		limit := node.Limit
		if limit != nil {
			log.Debug("limit:%+v(%T), %+v(%T)", limit.Offset, limit.Offset, limit.Rowcount, limit.Rowcount)
		}

		sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch node := node.(type) {
			default:
				log.Debug("type:%T, value:%+v", node, node)
				return true, nil
			}
		}, limit)

		log.Debug("\n")
	}
}

func TestSQLSelectAggregator(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	querys := []string{
		"select 1, count(*), count(*) as cstar, avg(a), sum(a), count(a), max(a), max(b) as mb, a as a1, x.b from A,B group by a1,b, d.name",
	}
	route, cleanup := router.MockNewRouter(log)
	defer cleanup()
	err := route.CreateDatabase("sbtest")
	assert.Nil(t, err)
	err = route.AddForTest("sbtest", router.MockTableMConfig(), router.MockTableBConfig())
	assert.Nil(t, err)
	for _, query := range querys {
		log.Debug("query:%s", query)
		sel, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		node := sel.(*sqlparser.Select)
		selexprs := node.SelectExprs
		var tuples []*selectTuple
		p, err := scanTableExprs(log, route, "sbtest", node.From)
		assert.Nil(t, err)
		if selexprs != nil {
			for _, exp := range selexprs {
				switch exp.(type) {
				case *sqlparser.AliasedExpr:
					expr := exp.(*sqlparser.AliasedExpr)
					tuple, _, _ := parseSelectExpr(expr, p.getReferTables())
					tuples = append(tuples, tuple)
				}
			}
			for _, tuple := range tuples {
				log.Debug("--%+v", tuple)
			}
		}

		groupbys := node.GroupBy
		if groupbys != nil {
			for _, by := range groupbys {
				log.Debug("group:%+v(%T)", by, by)
			}
		}
		log.Debug("\n")
	}
}

func TestSQLSelectRewritten(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	querys := []string{
		"select avg(a), sum(a), count(a) from A",
	}
	route, cleanup := router.MockNewRouter(log)
	defer cleanup()
	err := route.CreateDatabase("sbtest")
	assert.Nil(t, err)
	err = route.AddForTest("sbtest", router.MockTableMConfig())
	assert.Nil(t, err)
	for _, query := range querys {
		log.Debug("query:%s", query)
		sel, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		node := sel.(*sqlparser.Select)
		selexprs := node.SelectExprs
		rewritten := node.SelectExprs
		var tuples []*selectTuple
		p, err := scanTableExprs(log, route, "sbtest", node.From)
		assert.Nil(t, err)
		for _, exp := range selexprs {
			switch exp.(type) {
			case *sqlparser.AliasedExpr:
				expr := exp.(*sqlparser.AliasedExpr)
				tuple, _, _ := parseSelectExpr(expr, p.getReferTables())
				tuples = append(tuples, tuple)
			}
		}

		k := 0
		for _, tuple := range tuples {
			switch tuple.aggrFuc {
			case "avg":
				avgs := decomposeAvg(tuple)
				rewritten = append(rewritten, &sqlparser.AliasedExpr{}, &sqlparser.AliasedExpr{})
				copy(rewritten[(k+1)+2:], rewritten[(k+1):])
				rewritten[(k + 1)] = avgs[0]
				rewritten[(k+1)+1] = avgs[1]
			}
			log.Debug("--%+v", tuple)
			k++
		}

		buf := sqlparser.NewTrackedBuffer(nil)
		rewritten.Format(buf)
		log.Debug("--newquery:%s", buf.String())
	}
}

// TestSqlParserSelectOr used to check the or clause type.
func TestSqlParserSelectOr(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	querys := []string{
		"select * from tb1 where id=1 or id=3",
	}

	for _, query := range querys {
		log.Debug("query:%s", query)
		sel, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		selectTree := sel.(*sqlparser.Select)
		filters := splitAndExpression(nil, selectTree.Where.Expr)
		for _, filter := range filters {
			switch filter.(type) {
			case *sqlparser.ComparisonExpr:
				log.Debug("comparison.expr....")
			case *sqlparser.OrExpr:
				log.Debug("or.expr....")
			}

			buf := sqlparser.NewTrackedBuffer(nil)
			filter.Format(buf)
			log.Debug(buf.String())
			log.Debug("\n")
		}
	}
}

func TestSqlParserHaving(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	querys := []string{
		"select age,count(*) from t1 group by age having count(*) >=2",
		"select age,count(*) from t1 having a>2",
	}

	for _, query := range querys {
		log.Debug("query:%s", query)
		sel, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		node := sel.(*sqlparser.Select)
		if node.Having != nil {
			_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
				switch node := node.(type) {
				case *sqlparser.FuncExpr:
					buf := sqlparser.NewTrackedBuffer(nil)
					node.Format(buf)
					log.Debug(buf.String())
					log.Debug("found.expr.in.having:%#v....", node)
					return false, nil
				}
				return true, nil
			}, node.Having)
		}
	}
}

func TestSqlParserTableAs(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	querys := []string{
		"select t1.age  from test.t1 as t1",
		"select 1",
	}

	for _, query := range querys {
		log.Debug("query:%s", query)
		sel, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		node := sel.(*sqlparser.Select)
		log.Debug("from:%T, %+v", node.From[0], node.From[0])

		buf := sqlparser.NewTrackedBuffer(nil)
		node.Format(buf)
		log.Debug(buf.String())
	}
}

func TestSqlSet(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	querys := []string{
		"set @@SESSION.radon_streaming_fetch='ON', @@GLOBAL.xx='OFF'",
	}

	for _, query := range querys {
		sel, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		log.Debug("query:%v", query)

		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			log.Debug("node:%T, %+v", node, node)
			switch setexpr := node.(type) {
			case *sqlparser.SetExpr:
				switch expr := setexpr.Val.(*sqlparser.OptVal).Value.(type) {
				case *sqlparser.SQLVal:
					switch expr.Type {
					case sqlparser.StrVal:
						log.Debug("%s,%s", setexpr.Type, expr.Val)
					}
				}
			}
			return true, nil
		}, sel)
	}
}

func TestSqlBindVariables(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	tests := []struct {
		query string
		want  string
	}{
		{
			query: "select * from t1 where id = 1",
			want:  "select * from t1 where id = :v1",
		},

		{
			query: "insert into t1(id, name) values (1, 2)",
			want:  "insert into t1(id, name) values (:v1, :v2)",
		},
	}

	for _, test := range tests {
		stmt, err := sqlparser.Parse(test.query)
		assert.Nil(t, err)

		bv := make(map[string]*querypb.BindVariable)
		bv["v1"] = sqltypes.Int64BindVariable(1)
		bv["v2"] = sqltypes.Int64BindVariable(2)

		sqlparser.Normalize(stmt, bv, "v")
		bindvarQuery := sqlparser.String(stmt)
		log.Debug("bindvar.query: %s", bindvarQuery)

		parsedQuery := sqlparser.NewParsedQuery(stmt)
		bytes, err := parsedQuery.GenerateQuery(bv, nil)
		assert.Nil(t, err)
		finalQuery := string(bytes)
		log.Debug("final.query: %s", finalQuery)

		assert.Equal(t, test.want, bindvarQuery)
		assert.Equal(t, test.query, finalQuery)
	}
}
