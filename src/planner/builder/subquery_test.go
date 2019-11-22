/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package builder

import (
	"testing"

	"router"
	"xcontext"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestFromSubQuery(t *testing.T) {
	tcases := []struct {
		query string
		out   []xcontext.QueryTuple
	}{
		{
			query: "select a from (select a, id from A) as t where t.id=1",
			out: []xcontext.QueryTuple{
				{
					Query:   "select a from (select a, id from sbtest.A6 as A where id = 1) as t",
					Backend: "backend6",
					Range:   "[512-4096)",
				}},
		},
		{
			query: "select a from (select a, id from A where A.id=1) as t where t.a > 1",
			out: []xcontext.QueryTuple{
				{
					Query:   "select a from (select a, id from sbtest.A6 as A where A.id = 1) as t where t.a > 1",
					Backend: "backend6",
					Range:   "[512-4096)",
				}},
		},
		{
			query: "select a from (select a, id from S) as t where t.a > 1",
			out: []xcontext.QueryTuple{
				{
					Query:   "select a from (select a, id from sbtest.S) as t where t.a > 1",
					Backend: "backend1",
					Range:   "",
				}},
		},
		{
			query: "select t.id from (select a, id from G) as t,A where A.id=1 and t.a>1",
			out: []xcontext.QueryTuple{
				{
					Query:   "select t.id from (select a, id from sbtest.G) as t, sbtest.A6 as A where A.id = 1 and t.a > 1",
					Backend: "backend6",
					Range:   "[512-4096)",
				}},
		},
		{
			query: "select t.b from (select A.a, A.id, B.b from A join B on A.a = B.a where A.a>1) as t where t.a<5 and id=1 group by b order by b limit 2",
			out: []xcontext.QueryTuple{
				{
					Query:   "select A.a, A.id from sbtest.A6 as A where A.a > 1 and A.a < 5 and A.id = 1 order by A.a asc",
					Backend: "backend6",
					Range:   "[512-4096)",
				},
				{
					Query:   "select B.b, B.a from sbtest.B0 as B where B.a > 1 order by B.a asc",
					Backend: "backend1",
					Range:   "[0-512)",
				},
				{
					Query:   "select B.b, B.a from sbtest.B1 as B where B.a > 1 order by B.a asc",
					Backend: "backend2",
					Range:   "[512-4096)",
				},
			},
		},
		{
			query: "select t.a from (select id as tmp,a from B where B.a>1) as t join C on t.tmp = C.id where C.id=1",
			out: []xcontext.QueryTuple{
				{
					Query:   "select t.a, t.tmp from (select id as tmp, a from sbtest.B1 as B where B.a > 1 and B.id = 1) as t order by t.tmp asc",
					Backend: "backend2",
					Range:   "[512-4096)",
				},
				{
					Query:   "select C.id from sbtest.C0 as C where C.id = 1 order by C.id asc",
					Backend: "backend1",
					Range:   "[0-512)",
				},
				{
					Query:   "select C.id from sbtest.C1 as C where C.id = 1 order by C.id asc",
					Backend: "backend2",
					Range:   "[512-4096)",
				},
			},
		},
		{
			query: "select t.a from (select id as tmp,a from B where B.a>1) as t join C on t.tmp = C.a where C.a=1",
			out: []xcontext.QueryTuple{
				{
					Query:   "select t.a from (select id as tmp, a from sbtest.B1 as B where B.a > 1) as t join sbtest.C1 as C on t.tmp = C.a where C.a = 1",
					Backend: "backend2",
					Range:   "[512-4096)",
				},
			},
		},
		{
			query: "select a from (select A.a, B.b from A join B on concat(A.str,B.str) is not null where A.id=1) t where a+b>1",
			out: []xcontext.QueryTuple{
				{
					Query:   "select A.a, A.str from sbtest.A6 as A where A.id = 1",
					Backend: "backend6",
					Range:   "[512-4096)",
				},
				{
					Query:   "select B.b from sbtest.B0 as B where concat(:A_str, B.str) is not null and :A_a + B.b > 1",
					Backend: "backend1",
					Range:   "[0-512)",
				},
				{
					Query:   "select B.b from sbtest.B1 as B where concat(:A_str, B.str) is not null and :A_a + B.b > 1",
					Backend: "backend2",
					Range:   "[512-4096)",
				},
			},
		},
		{
			query: "select t.a, t.id + B.id as id from (select S.a,A.id from A,S) t join B on t.id=B.id where t.id=1",
			out: []xcontext.QueryTuple{
				{
					Query:   "select A.id from sbtest.A6 as A where A.id = 1",
					Backend: "backend6",
					Range:   "[512-4096)",
				},
				{
					Query:   "select S.a from sbtest.S",
					Backend: "backend1",
					Range:   "",
				},
				{
					Query:   "select :t_id + B.id as id from sbtest.B1 as B where B.id = 1 and :t_id = B.id",
					Backend: "backend2",
					Range:   "[512-4096)",
				},
			},
		},
		{
			query: "select * from (select A.id from A) t where id=1",
			out: []xcontext.QueryTuple{
				{
					Query:   "select * from (select A.id from sbtest.A6 as A where A.id = 1) as t",
					Backend: "backend6",
					Range:   "[512-4096)",
				},
			},
		},
	}
	{
		log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
		database := "sbtest"

		route, cleanup := router.MockNewRouter(log)
		defer cleanup()

		err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableSConfig(), router.MockTableGConfig(), router.MockTableBConfig(), router.MockTableCConfig())
		assert.Nil(t, err)
		for _, tcase := range tcases {
			node, err := sqlparser.Parse(tcase.query)
			assert.Nil(t, err)

			// plan build
			{
				log.Info("--select.query:%+v", tcase.query)
				plan, err := BuildNode(log, route, database, node.(sqlparser.SelectStatement))
				assert.Nil(t, err)
				q := plan.GetQuery()
				assert.Equal(t, tcase.out, q)
				plan.Children()
			}
		}
	}
}

func TestSubqueryUnsupported(t *testing.T) {
	testcases := []struct {
		query string
		out   string
	}{
		{
			"select a from (select a,b from A union select a,b from B)t",
			"unsupported: unknown.select.statement",
		},
		{
			"select a from (select a,a,b from A)t",
			"unsupported: duplicate.column.name.'a'",
		},
		{
			"select t.a from (select a,sum(b) as cnt from A)t join B on t.a = B.a where t.cnt>1",
			"unsupported: aggregation.field.in.subquery.is.used.in.clause",
		},
		{
			"select t.a from (select a,sum(b) as cnt from A)t join B on t.cnt = B.a where B.a=1",
			"unsupported: aggregation.field.in.subquery.is.used.in.clause",
		},
		{
			"select t.a from B join (select a,sum(b) as cnt from A)t on t.cnt = B.a where B.a=1",
			"unsupported: aggregation.field.in.subquery.is.used.in.clause",
		},
		{
			"select t.a from B left join (select a,sum(b) as cnt from A)t on t.a = B.a and t.cnt=1 where B.a=1",
			"unsupported: aggregation.field.in.subquery.is.used.in.clause",
		},
		{
			"select t.a from B join (select a,sum(b),b as cnt from A)t on t.a = B.a where B.b+t.b>1",
			"unsupported: clause.'B.b + t.b > 1'.in.cross-shard.join",
		},
		{
			"select t.a+B.a as a from B join (select a,sum(b),b as cnt from A)t on t.a = B.a",
			"unsupported: expr.'t.a + B.a'.in.cross-shard.join",
		},
		{
			"select  t.a from B join (select a,sum(b),b as cnt from A)t on t.a = B.a join G on G.a+B.a>1",
			"unsupported: clause.'t.a = B.a'.in.cross-shard.join",
		},
		{
			"select  t.a from G join (B join (select a,sum(b),b as cnt from A)t on t.a = B.a) on G.a+B.a>1",
			"unsupported: clause.'t.a = B.a'.in.cross-shard.join",
		},
		{
			"select  t.a from (select a from A where a >1 having b>1)t",
			"unsupported: unknown.column.'b'.in.having.clause",
		},
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableGConfig(), router.MockTableCConfig())
	assert.Nil(t, err)
	for _, testcase := range testcases {
		node, err := sqlparser.Parse(testcase.query)
		assert.Nil(t, err)

		// plan build
		{
			log.Info("--select.query:%+v", testcase.query)
			_, err := BuildNode(log, route, database, node.(sqlparser.SelectStatement))
			assert.Equal(t, testcase.out, err.Error())
		}
	}
}

func MockSubNode(p PlanNode, sel sqlparser.SelectStatement, log *xlog.Log, r *router.Router) *SubNode {
	tn := &tableInfo{
		database: "sbtest",
		alias:    "t",
		tableExpr: &sqlparser.AliasedTableExpr{
			Expr: &sqlparser.Subquery{Select: sel},
			As:   sqlparser.NewTableIdent("t")},
	}
	referTables := map[string]*tableInfo{
		tn.alias: tn,
	}

	// store the cols, and check the col whether exists duplicate.
	colMap := make(map[string]selectTuple)
	for _, field := range p.getFields() {
		name := field.alias
		if name == "" {
			name = field.field
		}
		colMap[name] = field
	}

	subInfo := &derived{tn, colMap, p.getReferTables()}
	return newSubNode(log, r, p, subInfo, referTables)
}

func TestSubNodePushFilter(t *testing.T) {
	testcases := []struct {
		query string
		want  string
	}{
		{
			"select a from (select a, b+1 as tmp from A) t where t.tmp > 1",
			"b + 1 > 1",
		},
		{
			"select a from (select a, b+1 as tmp from A) t where t.tmp = 1",
			"b + 1 = 1",
		},
		{
			"select a from (select a, b+1 as tmp from A) t where t.a = 1",
			"a = 1",
		},
		{
			"select a from (select a, now() as time from A) t where time > '2019-11-11 00:00:00'",
			"now() > '2019-11-11 00:00:00'",
		},
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig())
	assert.Nil(t, err)

	for _, testcase := range testcases {
		log.Info("--select.query:%+v", testcase.query)
		node, err := sqlparser.Parse(testcase.query)
		sub := node.(*sqlparser.Select).From[0].(*sqlparser.AliasedTableExpr).Expr.(*sqlparser.Subquery).Select
		assert.Nil(t, err)
		p, err := processPart(log, route, "sbtest", sub)
		assert.Nil(t, err)

		s := MockSubNode(p, sub, log, route)
		joins, filters, err := parseWhereOrJoinExprs(node.(*sqlparser.Select).Where.Expr, s.getReferTables())
		assert.Nil(t, err)
		assert.Equal(t, 0, len(joins))
		assert.Equal(t, 1, len(filters))

		err = s.pushFilter(filters[0])
		assert.Nil(t, err)
		buf := sqlparser.NewTrackedBuffer(nil)
		filters[0].expr.Format(buf)
		assert.Equal(t, testcase.want, buf.String())
	}
}

func TestSubNodePushFilterErr(t *testing.T) {
	testcases := []struct {
		query string
		want  string
	}{
		{
			"select a from (select a, sum(b+1) as tmp from A) t where t.tmp > 1",
			"unsupported: aggregation.field.in.subquery.is.used.in.clause",
		},
		{
			"select a from (select A.a, B.b from A join B on A.a=B.a) t where a+b>1",
			"unsupported: where.clause.'A.a + B.b > 1'.in.cross-shard.join",
		},
		{
			"select a from (select a, b+1 as tmp from A) t where t.id > 1",
			"unsupported: unknown.column.name.'id'",
		},
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig())
	assert.Nil(t, err)

	for _, testcase := range testcases {
		log.Info("--select.query:%+v", testcase.query)
		node, err := sqlparser.Parse(testcase.query)
		sub := node.(*sqlparser.Select).From[0].(*sqlparser.AliasedTableExpr).Expr.(*sqlparser.Subquery).Select
		assert.Nil(t, err)
		p, err := processPart(log, route, "sbtest", sub)
		assert.Nil(t, err)

		s := MockSubNode(p, sub, log, route)
		joins, filters, err := parseWhereOrJoinExprs(node.(*sqlparser.Select).Where.Expr, s.getReferTables())
		assert.Nil(t, err)
		assert.Equal(t, 0, len(joins))
		assert.Equal(t, 1, len(filters))

		err = s.pushFilter(filters[0])
		assert.NotNil(t, err)
		got := err.Error()
		assert.Equal(t, testcase.want, got)
	}
}

func getExprInfo(expr sqlparser.Expr) exprInfo {
	var filter exprInfo
	filter.expr = expr
	sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			filter.cols = append(filter.cols, node)
			tableName := node.Qualifier.Name.String()
			if isContainKey(filter.referTables, tableName) {
				return true, nil
			}
			filter.referTables = append(filter.referTables, tableName)
		}
		return true, nil
	}, expr)

	condition, ok := expr.(*sqlparser.ComparisonExpr)
	if ok {
		if _, lok := condition.Left.(*sqlparser.ColName); lok {
			if condition.Operator == sqlparser.EqualStr {
				if sqlVal, ok := condition.Right.(*sqlparser.SQLVal); ok {
					filter.vals = append(filter.vals, sqlVal)
				}
			}
		}
	}
	return filter
}

func TestSubNodePushKeyFilter(t *testing.T) {
	querys := []string{
		"select t.a from (select a, b+1 as tmp from A where A.id=1) t join B on t.tmp = B.b where B.b > 1",
		"select t.a from (select a, b+1 as tmp from A where A.id=1) t join B on t.tmp = B.b where B.b = 1",
		"select t.a from (select a, b as tmp from A where A.id=1) t join B on t.tmp = B.b where B.b = 1",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig())
	assert.Nil(t, err)

	for _, query := range querys {
		log.Info("--select.query:%+v", query)
		node, err := sqlparser.Parse(query)
		sub := node.(*sqlparser.Select).From[0].(*sqlparser.JoinTableExpr).LeftExpr.(*sqlparser.AliasedTableExpr).Expr.(*sqlparser.Subquery).Select
		assert.Nil(t, err)
		p, err := processPart(log, route, "sbtest", sub)
		assert.Nil(t, err)

		s := MockSubNode(p, sub, log, route)
		filter := getExprInfo(node.(*sqlparser.Select).Where.Expr)

		err = s.pushKeyFilter(filter, "t", "tmp")
		assert.Nil(t, err)
	}
}

func TestSubNodePushKeyFilterErr(t *testing.T) {
	testcases := []struct {
		query string
		want  string
	}{
		{
			"select t.a from (select a, sum(b) as tmp from A where A.id=1) t join B on t.tmp = B.b where B.b > 1",
			"unsupported: aggregation.field.in.subquery.is.used.in.clause",
		},
		{
			"select t.a from (select a, b from A where A.id=1) t join B on t.tmp = B.b where B.b > 1",
			"unsupported: unknown.column.name.'tmp'",
		},
		{
			"select t.a from (select a, id as tmp from A where A.a=1) t join B on t.tmp = B.b where B.b = 0x12",
			"hash.unsupported.key.type:[3]",
		},
		{
			"select B.a from (select A.a+S.a as tmp from A,S) t join B on t.tmp = B.b where B.b = 1",
			"unsupported: where.clause.'A.a + S.a = 1'.in.cross-shard.join",
		},
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableSConfig())
	assert.Nil(t, err)

	for _, testcase := range testcases {
		log.Info("--select.query:%+v", testcase.query)
		node, err := sqlparser.Parse(testcase.query)
		sub := node.(*sqlparser.Select).From[0].(*sqlparser.JoinTableExpr).LeftExpr.(*sqlparser.AliasedTableExpr).Expr.(*sqlparser.Subquery).Select
		assert.Nil(t, err)
		p, err := processPart(log, route, "sbtest", sub)
		assert.Nil(t, err)
		if j, ok := p.(*JoinNode); ok {
			j.Strategy = SortMerge
		}

		s := MockSubNode(p, sub, log, route)
		filter := getExprInfo(node.(*sqlparser.Select).Where.Expr)

		err = s.pushKeyFilter(filter, "t", "tmp")
		assert.NotNil(t, err)
		got := err.Error()
		assert.Equal(t, testcase.want, got)
	}
}

func TestSubNodePushSelectExprs(t *testing.T) {
	testcases := []struct {
		query       string
		projects    string
		subProjects string
	}{
		{
			"select a from (select a, b+1 as tmp from A) t group by a",
			"a",
			"a, tmp",
		},
		{
			"select sum(a) as cnt from (select a, b+1 as tmp from A) t",
			"cnt",
			"a, tmp, cnt",
		},
		{
			"select a as num from (select a, b+1 as tmp from A) t",
			"num",
			"a, tmp, num",
		},
		{
			"select * from (select a, b+1 as tmp from A) t group by tmp",
			"a, tmp",
			"a, tmp",
		},
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig())
	assert.Nil(t, err)

	for _, testcase := range testcases {
		log.Info("--select.query:%+v", testcase.query)
		node, err := sqlparser.Parse(testcase.query)
		sub := node.(*sqlparser.Select).From[0].(*sqlparser.AliasedTableExpr).Expr.(*sqlparser.Subquery).Select
		assert.Nil(t, err)
		p, err := processPart(log, route, "sbtest", sub)
		assert.Nil(t, err)

		s := MockSubNode(p, sub, log, route)
		fields, aggTyp, err := parseSelectExprs(node.(*sqlparser.Select).SelectExprs, s)
		assert.Nil(t, err)
		groups, err := checkGroupBy(node.(*sqlparser.Select).GroupBy, fields, route, s.getReferTables(), false)
		assert.Nil(t, err)
		err = s.pushSelectExprs(fields, groups, node.(*sqlparser.Select), aggTyp)
		assert.Nil(t, err)
		assert.Equal(t, testcase.projects, GetProject(s))
		assert.Equal(t, testcase.subProjects, GetProject(p))
	}
}

func TestSubNodePushSelectExprsErr(t *testing.T) {
	testcases := []struct {
		query string
		want  string
	}{
		{
			"select a, num + 1 from (select a,sum(a) as num, b+1 as tmp from A) t group by a",
			"unsupported: aggregation.field.in.subquery.is.used.in.clause",
		},
		{
			"select b from (select a, sum(a) as num, b+1 as tmp from A) t",
			"unsupported: unknown.column.name.'b'",
		},
		{
			"select a+b from (select A.a,B.b from A,B) t",
			"unsupported: 'a + b'.expression.in.cross-shard.query",
		},
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig())
	assert.Nil(t, err)

	for _, testcase := range testcases {
		log.Info("--select.query:%+v", testcase.query)
		node, err := sqlparser.Parse(testcase.query)
		sub := node.(*sqlparser.Select).From[0].(*sqlparser.AliasedTableExpr).Expr.(*sqlparser.Subquery).Select
		assert.Nil(t, err)
		p, err := processPart(log, route, "sbtest", sub)
		assert.Nil(t, err)

		s := MockSubNode(p, sub, log, route)
		fields, aggTyp, err := parseSelectExprs(node.(*sqlparser.Select).SelectExprs, s)
		assert.Nil(t, err)
		groups, err := checkGroupBy(node.(*sqlparser.Select).GroupBy, fields, route, s.getReferTables(), false)
		assert.Nil(t, err)
		err = s.pushSelectExprs(fields, groups, node.(*sqlparser.Select), aggTyp)
		assert.NotNil(t, err)
		assert.Equal(t, testcase.want, err.Error())
	}
}

func TestSubNodePushHaving(t *testing.T) {
	testcases := []struct {
		query string
		want  string
	}{
		{
			"select a from (select a, b+1 as tmp from A) t having t.a > 1",
			"a > 1",
		},
		{
			"select a,time from (select a, now() as time from A) t having time > '2019-11-11 00:00:00'",
			"now() > '2019-11-11 00:00:00'",
		},
		{
			"select t.a,t.b from (select a,b from A) t, B where t.a=B.a having t.a > 1",
			"a > 1",
		},
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig())
	assert.Nil(t, err)

	for _, testcase := range testcases {
		log.Info("--select.query:%+v", testcase.query)
		node, err := sqlparser.Parse(testcase.query)
		sub := node.(*sqlparser.Select).From[0].(*sqlparser.AliasedTableExpr).Expr.(*sqlparser.Subquery).Select
		assert.Nil(t, err)
		p, err := processPart(log, route, "sbtest", sub)
		assert.Nil(t, err)

		s := MockSubNode(p, sub, log, route)
		s.fields, _, err = parseSelectExprs(node.(*sqlparser.Select).SelectExprs, s)
		assert.Nil(t, err)
		err = pushHavings(s, node.(*sqlparser.Select).Having.Expr, s.getReferTables())
		assert.Nil(t, err)

		buf := sqlparser.NewTrackedBuffer(nil)
		node.(*sqlparser.Select).Having.Expr.Format(buf)
		assert.Equal(t, testcase.want, buf.String())
	}
}

func TestSubNodePushHavingErr(t *testing.T) {
	testcases := []struct {
		query string
		want  string
	}{
		{
			"select a from (select a, b+1 as tmp from A) t having t.tmp > 1",
			"unsupported: unknown.column.'t.tmp'.in.having.clause",
		},
		{
			"select a,tmp from (select a, sum(b+1) as tmp from A) t having t.tmp > 1",
			"unsupported: aggregation.field.in.subquery.is.used.in.clause",
		},
		{
			"select a,b from (select A.a,B.b from A,B) t having a+b > 1",
			"unsupported: having.clause.'A.a + B.b > 1'.in.cross-shard.join",
		},
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig())
	assert.Nil(t, err)

	for _, testcase := range testcases {
		log.Info("--select.query:%+v", testcase.query)
		node, err := sqlparser.Parse(testcase.query)
		sub := node.(*sqlparser.Select).From[0].(*sqlparser.AliasedTableExpr).Expr.(*sqlparser.Subquery).Select
		assert.Nil(t, err)
		p, err := processPart(log, route, "sbtest", sub)
		assert.Nil(t, err)

		s := MockSubNode(p, sub, log, route)
		s.fields, _, err = parseSelectExprs(node.(*sqlparser.Select).SelectExprs, s)
		assert.Nil(t, err)
		err = pushHavings(s, node.(*sqlparser.Select).Having.Expr, s.getReferTables())
		assert.NotNil(t, err)
		assert.Equal(t, testcase.want, err.Error())
	}
}
