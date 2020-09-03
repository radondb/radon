/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package planner

import (
	"testing"

	"router"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestDDLPlan1(t *testing.T) {
	results := []string{
		"{\n\t\"RawQuery\": \"create table A(a int)\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A0 (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A2 (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A4 (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A8 (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"create table G(a int)\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.G (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend1\",\n\t\t\t\"Range\": \"\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.G (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"create table S(a int)\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.S (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend1\",\n\t\t\t\"Range\": \"\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"alter table A engine = tokudb\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"alter table sbtest.A0 engine = tokudb\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table sbtest.A2 engine = tokudb\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table sbtest.A4 engine = tokudb\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table sbtest.A8 engine = tokudb\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"create index idx_a on A(a)\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"create index idx_a on sbtest.A0(`a`)\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create index idx_a on sbtest.A2(`a`)\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create index idx_a on sbtest.A4(`a`)\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create index idx_a on sbtest.A8(`a`)\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"drop index idx_a on sbtest.A\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"drop index idx_a on sbtest.A0\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"drop index idx_a on sbtest.A2\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"drop index idx_a on sbtest.A4\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"drop index idx_a on sbtest.A8\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"alter table A add column(b int)\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"alter table sbtest.A0 add column (\\n\\t`b` int\\n)\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table sbtest.A2 add column (\\n\\t`b` int\\n)\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table sbtest.A4 add column (\\n\\t`b` int\\n)\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table sbtest.A8 add column (\\n\\t`b` int\\n)\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"alter table sbtest.A add column(b int)\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"alter table sbtest.A0 add column (\\n\\t`b` int\\n)\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table sbtest.A2 add column (\\n\\t`b` int\\n)\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table sbtest.A4 add column (\\n\\t`b` int\\n)\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table sbtest.A8 add column (\\n\\t`b` int\\n)\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"alter table sbtest.A add column(b int, c varchar(100))\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"alter table sbtest.A0 add column (\\n\\t`b` int,\\n\\t`c` varchar(100)\\n)\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table sbtest.A2 add column (\\n\\t`b` int,\\n\\t`c` varchar(100)\\n)\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table sbtest.A4 add column (\\n\\t`b` int,\\n\\t`c` varchar(100)\\n)\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table sbtest.A8 add column (\\n\\t`b` int,\\n\\t`c` varchar(100)\\n)\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"alter table A modify column b int\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"alter table sbtest.A0 modify column `b` int\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table sbtest.A2 modify column `b` int\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table sbtest.A4 modify column `b` int\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table sbtest.A8 modify column `b` int\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"alter table A drop column b\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"alter table sbtest.A0 drop column `b`\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table sbtest.A2 drop column `b`\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table sbtest.A4 drop column `b`\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table sbtest.A8 drop column `b`\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"truncate table A\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"truncate table sbtest.A0\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"truncate table sbtest.A2\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"truncate table sbtest.A4\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"truncate table sbtest.A8\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
	}

	querys := []string{
		"create table A(a int)",
		"create table G(a int)",
		"create table S(a int)",
		"alter table A engine = tokudb",
		"create index idx_a on A(a)",
		"drop index idx_a on sbtest.A",
		"alter table A add column(b int)",
		"alter table sbtest.A add column(b int)",
		"alter table sbtest.A add column(b int, c varchar(100))",
		"alter table A modify column b int",
		"alter table A drop column b",
		"truncate table A",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableAConfig(), router.MockTableGConfig(), router.MockTableSConfig())
	assert.Nil(t, err)
	planTree := NewPlanTree()
	for i, query := range querys {
		log.Debug("%v", query)
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		plan := NewDDLPlan(log, database, query, node.(*sqlparser.DDL), route)

		// plan build
		{
			err := plan.Build()
			assert.Nil(t, err)
			{
				err := planTree.Add(plan)
				assert.Nil(t, err)
			}
			want := results[i]
			got := plan.JSON()
			log.Info(got)
			assert.Equal(t, want, got)
		}

		// type
		{
			want := PlanTypeDDL
			got := plan.Type()
			assert.Equal(t, want, got)
		}
	}
}

func TestDROPPlan(t *testing.T) {
	results := []string{
		"{\n\t\"RawQuery\": \"drop table sbtest.A\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"drop table sbtest.A0\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"drop table sbtest.A2\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"drop table sbtest.A4\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"drop table sbtest.A8\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
	}

	querys := []string{
		"drop table sbtest.A",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableAConfig(), router.MockTableGConfig(), router.MockTableSConfig())
	assert.Nil(t, err)
	planTree := NewPlanTree()
	for i, query := range querys {
		log.Debug("%v", query)
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		dropNode := node.(*sqlparser.DDL)

		for _, tableIdent := range dropNode.Tables {
			if !tableIdent.Qualifier.IsEmpty() {
				database = tableIdent.Qualifier.String()
			}
			dropNode.Table = tableIdent

			plan := NewDDLPlan(log, database, query, dropNode, route)
			// plan build
			{
				err := plan.Build()
				assert.Nil(t, err)
				{
					err := planTree.Add(plan)
					assert.Nil(t, err)
				}
				want := results[i]
				got := plan.JSON()
				log.Info(got)
				assert.Equal(t, want, got)
			}

			// type
			{
				want := PlanTypeDDL
				got := plan.Type()
				assert.Equal(t, want, got)
			}
		}
	}
}

func TestDDLAlterError(t *testing.T) {
	results := []string{
		"unsupported: cannot.modify.the.column.on.shard.key",
		"unsupported: cannot.modify.the.column.on.shard.key",
		"unsupported: cannot.drop.the.column.on.shard.key",
		"unsupported: cannot.drop.the.column.on.shard.key",
		"The unique/primary constraint should be only defined on the sharding key column[id]",
		"The unique/primary constraint should be only defined on the sharding key column[id]",
		"The unique/primary constraint should be only defined on the sharding key column[id]",
		"The unique/primary constraint should be only defined on the sharding key column[id]",
		"The unique/primary constraint should be only defined on the sharding key column[id]",
		"The unique/primary constraint should be only defined on the sharding key column[id]",
		"The unique/primary constraint should be only defined on the sharding key column[id]",
		"The unique/primary constraint should be only defined on the sharding key column[id]",
		"The unique/primary constraint should be only defined on the sharding key column[id]",
	}

	// For now we doesn`t support unique in add column, e.g.:
	// "alter table A add column(c12 int, c13 varchar(100), unique key(c10, c11))",
	// "alter table A add column(c14 int, c15 varchar(100), unique(c12))",
	querys := []string{
		"alter table A modify column id int",
		"alter table A modify column ID int",
		"alter table A drop column id",
		"alter table A drop column ID",
		"alter table A modify column b varchar(1) key",
		"alter table A modify column b varchar(1) primary key",
		"alter table A modify column b varchar(1) unique",
		"alter table A modify column b varchar(1) unique key",
		"alter table A add column(c3 int key, c4 varchar(100))",
		"alter table A add column(c4 int primary key, c5 varchar(100))",
		"alter table A add column(c6 int unique key, c7 varchar(100))",
		"alter table A add column(c8 int, c9 varchar(100), primary key(c8, c9))",
		"alter table A add column(c10 int, c11 varchar(100), primary key(c14))",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableAConfig())
	assert.Nil(t, err)
	for i, query := range querys {
		log.Debug("%v", query)
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		plan := NewDDLPlan(log, database, query, node.(*sqlparser.DDL), route)

		// plan build
		{
			err := plan.Build()
			if err != nil {
				want := results[i]
				got := err.Error()
				assert.Equal(t, want, got)
			} else {
				log.Panic("planner.test.ddl.alter.test.case.did.not.return.err")
			}
		}
	}
}

func TestDDLPlanCreateIndex(t *testing.T) {
	results := []string{
		`{
	"RawQuery": "create index idx_A_id on A(a)",
	"Partitions": [
		{
			"Query": "create index idx_A_id on sbtest.A0(` + "`a`" + `)",
			"Backend": "backend0",
			"Range": "[0-2)"
		},
		{
			"Query": "create index idx_A_id on sbtest.A2(` + "`a`" + `)",
			"Backend": "backend2",
			"Range": "[2-4)"
		},
		{
			"Query": "create index idx_A_id on sbtest.A4(` + "`a`" + `)",
			"Backend": "backend4",
			"Range": "[4-8)"
		},
		{
			"Query": "create index idx_A_id on sbtest.A8(` + "`a`" + `)",
			"Backend": "backend8",
			"Range": "[8-4096)"
		}
	]
}`,
		`{
	"RawQuery": "create fulltext index idx on A(a) with parser ngram lock=none algorithm=copy",
	"Partitions": [
		{
			"Query": "create fulltext index idx on sbtest.A0(` + "`a`" + `) WITH PARSER ngram algorithm = copy lock = none",
			"Backend": "backend0",
			"Range": "[0-2)"
		},
		{
			"Query": "create fulltext index idx on sbtest.A2(` + "`a`" + `) WITH PARSER ngram algorithm = copy lock = none",
			"Backend": "backend2",
			"Range": "[2-4)"
		},
		{
			"Query": "create fulltext index idx on sbtest.A4(` + "`a`" + `) WITH PARSER ngram algorithm = copy lock = none",
			"Backend": "backend4",
			"Range": "[4-8)"
		},
		{
			"Query": "create fulltext index idx on sbtest.A8(` + "`a`" + `) WITH PARSER ngram algorithm = copy lock = none",
			"Backend": "backend8",
			"Range": "[8-4096)"
		}
	]
}`,
		`{
	"RawQuery": "create index idx on A(a) using hash comment 'c' lock=shared",
	"Partitions": [
		{
			"Query": "create index idx on sbtest.A0(` + "`a`" + `) using hash comment 'c' lock = shared",
			"Backend": "backend0",
			"Range": "[0-2)"
		},
		{
			"Query": "create index idx on sbtest.A2(` + "`a`" + `) using hash comment 'c' lock = shared",
			"Backend": "backend2",
			"Range": "[2-4)"
		},
		{
			"Query": "create index idx on sbtest.A4(` + "`a`" + `) using hash comment 'c' lock = shared",
			"Backend": "backend4",
			"Range": "[4-8)"
		},
		{
			"Query": "create index idx on sbtest.A8(` + "`a`" + `) using hash comment 'c' lock = shared",
			"Backend": "backend8",
			"Range": "[8-4096)"
		}
	]
}`,
		`{
	"RawQuery": "create spatial index idx on A(gis) key_block_size=10 algorithm=default",
	"Partitions": [
		{
			"Query": "create spatial index idx on sbtest.A0(` + "`gis`" + `) key_block_size = 10 algorithm = default",
			"Backend": "backend0",
			"Range": "[0-2)"
		},
		{
			"Query": "create spatial index idx on sbtest.A2(` + "`gis`" + `) key_block_size = 10 algorithm = default",
			"Backend": "backend2",
			"Range": "[2-4)"
		},
		{
			"Query": "create spatial index idx on sbtest.A4(` + "`gis`" + `) key_block_size = 10 algorithm = default",
			"Backend": "backend4",
			"Range": "[4-8)"
		},
		{
			"Query": "create spatial index idx on sbtest.A8(` + "`gis`" + `) key_block_size = 10 algorithm = default",
			"Backend": "backend8",
			"Range": "[8-4096)"
		}
	]
}`,
	}

	querys := []string{
		// issue 10.
		"create index idx_A_id on A(a)",
		// issue 592.
		"create fulltext index idx on A(a) with parser ngram lock=none algorithm=copy",
		"create index idx on A(a) using hash comment 'c' lock=shared",
		"create spatial index idx on A(gis) key_block_size=10 algorithm=default",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"
	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableAConfig())
	assert.Nil(t, err)
	for i, query := range querys {
		log.Debug("%v", query)
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		plan := NewDDLPlan(log, database, query, node.(*sqlparser.DDL), route)

		// plan build
		{
			err := plan.Build()
			assert.Nil(t, err)
			want := results[i]
			got := plan.JSON()
			log.Info("--got:%+v", got)
			assert.Equal(t, want, got)
		}

		// type
		{
			want := PlanTypeDDL
			got := plan.Type()
			assert.Equal(t, want, got)
		}
	}
}

func TestDDLPlanWithQuote(t *testing.T) {
	results := []string{
		"{\n\t\"RawQuery\": \"create table A (\\n\\t`a` int\\n)\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A0 (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A2 (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A4 (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A8 (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"create table A (\\n\\t`a` int\\n)\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A0 (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A2 (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A4 (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A8 (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"create table A (\\n\\t`a` int\\n)\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A0 (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A2 (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A4 (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A8 (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"create table sbtest.A (\\n\\t`a` int\\n)\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A0 (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A2 (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A4 (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A8 (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"create table sbtest.A (\\n\\t`a` int\\n)\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A0 (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A2 (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A4 (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A8 (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"create table sbtest.A (\\n\\t`a` int\\n)\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A0 (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A2 (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A4 (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A8 (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"create table sbtest.A (\\n\\t`a` int\\n)\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A0 (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A2 (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A4 (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A8 (\\n\\t`a` int\\n)\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"create table B (\\n\\t`a` int,\\n\\t`b` varchar(10),\\n\\tunique key `a_idx` (`a`) using btree comment 'key'\\n)\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.B0 (\\n\\t`a` int,\\n\\t`b` varchar(10),\\n\\tunique key `a_idx` (`a`) using btree comment 'key'\\n)\",\n\t\t\t\"Backend\": \"backend1\",\n\t\t\t\"Range\": \"[0-512)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.B1 (\\n\\t`a` int,\\n\\t`b` varchar(10),\\n\\tunique key `a_idx` (`a`) using btree comment 'key'\\n)\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[512-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"create table B (\\n\\t`b` varchar(10),\\n\\tfulltext index `b_idx` (`b`) key_block_size = 10 WITH PARSER ngram\\n)\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.B0 (\\n\\t`b` varchar(10),\\n\\tfulltext index `b_idx` (`b`) key_block_size = 10 WITH PARSER ngram\\n)\",\n\t\t\t\"Backend\": \"backend1\",\n\t\t\t\"Range\": \"[0-512)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.B1 (\\n\\t`b` varchar(10),\\n\\tfulltext index `b_idx` (`b`) key_block_size = 10 WITH PARSER ngram\\n)\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[512-4096)\"\n\t\t}\n\t]\n}",
	}

	querys := []string{
		"create table `A`(a int)",
		"create table A(`a` int)",
		"create table A(a int)",
		"create table sbtest.A(a int)",
		"create table sbtest.`A`(a int)",
		"create table `sbtest`.A(a int)",
		"create table `sbtest`.`A`(a int)",
		"create table B(a int, b varchar(10), unique key a_idx(a) using btree comment 'key')",
		"create table B(b varchar(10), fulltext index b_idx(b) with parser ngram key_block_size=10)",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableAConfig(), router.MockTableBConfig())
	assert.Nil(t, err)
	for i, query := range querys {
		log.Debug("%v", query)
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		ddl := node.(*sqlparser.DDL)
		// After sqlparser.String(ddl), we`ll get sql like: "create table A (\n\t`a` int\n)"
		plan := NewDDLPlan(log, database, sqlparser.String(ddl), node.(*sqlparser.DDL), route)

		// plan build
		{
			err := plan.Build()
			assert.Nil(t, err)
			want := results[i]
			got := plan.JSON()
			assert.Equal(t, want, got)
		}
	}
}

func TestDDLPlanWithSameColumn(t *testing.T) {
	results := []string{
		"{\n\t\"RawQuery\": \"create table A (\\n\\t`A` int\\n)\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A0 (\\n\\t`A` int\\n)\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A2 (\\n\\t`A` int\\n)\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A4 (\\n\\t`A` int\\n)\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A8 (\\n\\t`A` int\\n)\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"create table A (\\n\\t`A` int\\n)\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A0 (\\n\\t`A` int\\n)\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A2 (\\n\\t`A` int\\n)\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A4 (\\n\\t`A` int\\n)\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A8 (\\n\\t`A` int\\n)\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"create table sbtest.A (\\n\\t`A` int\\n)\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A0 (\\n\\t`A` int\\n)\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A2 (\\n\\t`A` int\\n)\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A4 (\\n\\t`A` int\\n)\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table sbtest.A8 (\\n\\t`A` int\\n)\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"alter table A add column (\\n\\t`A` int\\n)\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"alter table sbtest.A0 add column (\\n\\t`A` int\\n)\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table sbtest.A2 add column (\\n\\t`A` int\\n)\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table sbtest.A4 add column (\\n\\t`A` int\\n)\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table sbtest.A8 add column (\\n\\t`A` int\\n)\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"alter table sbtest.A add column (\\n\\t`A` int\\n)\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"alter table sbtest.A0 add column (\\n\\t`A` int\\n)\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table sbtest.A2 add column (\\n\\t`A` int\\n)\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table sbtest.A4 add column (\\n\\t`A` int\\n)\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table sbtest.A8 add column (\\n\\t`A` int\\n)\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
	}

	querys := []string{
		"CREATE table A(A int)",
		"CREATE table `A`(A int)",
		"create table sbtest.A(A int)",
		"alter table A add column(A int)",
		"alter table sbtest.A add column(A int)",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableAConfig())
	assert.Nil(t, err)
	for i, query := range querys {
		log.Debug("%v", query)
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		ddl := node.(*sqlparser.DDL)
		// After sqlparser.String(ddl), we`ll get sql like: "create table A (\n\t`a` int\n)"
		plan := NewDDLPlan(log, database, sqlparser.String(ddl), node.(*sqlparser.DDL), route)

		// plan build
		{
			err := plan.Build()
			assert.Nil(t, err)
			want := results[i]
			got := plan.JSON()
			assert.Equal(t, want, got)
		}
	}
}

func TestDDLPlanWithRename(t *testing.T) {
	results := []string{
		"{\n\t\"RawQuery\": \"alter table R rename RR\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"rename table sbtest.R_0000 to sbtest.RR_0000\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"rename table sbtest.R_0008 to sbtest.RR_0008\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[2-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"alter table sbtest.R rename sbtest.RR\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"rename table sbtest.R_0000 to sbtest.RR_0000\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"rename table sbtest.R_0008 to sbtest.RR_0008\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[2-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"alter table sbtest.R rename RR\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"rename table sbtest.R_0000 to sbtest.RR_0000\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"rename table sbtest.R_0008 to sbtest.RR_0008\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[2-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"alter table R rename sbtest.RR\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"rename table sbtest.R_0000 to sbtest.RR_0000\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"rename table sbtest.R_0008 to sbtest.RR_0008\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[2-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"alter table `R` rename `sbtest`.`RR`\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"rename table sbtest.R_0000 to sbtest.RR_0000\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"rename table sbtest.R_0008 to sbtest.RR_0008\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[2-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"alter table `R` rename `sbtest`.RR\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"rename table sbtest.R_0000 to sbtest.RR_0000\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"rename table sbtest.R_0008 to sbtest.RR_0008\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[2-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"alter table `R` rename sbtest.`RR`\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"rename table sbtest.R_0000 to sbtest.RR_0000\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"rename table sbtest.R_0008 to sbtest.RR_0008\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[2-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"alter table `sbtest`.`R` rename RR\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"rename table sbtest.R_0000 to sbtest.RR_0000\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"rename table sbtest.R_0008 to sbtest.RR_0008\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[2-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"alter table `sbtest`.R rename RR\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"rename table sbtest.R_0000 to sbtest.RR_0000\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"rename table sbtest.R_0008 to sbtest.RR_0008\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[2-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"alter table sbtest.`R` rename RR\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"rename table sbtest.R_0000 to sbtest.RR_0000\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"rename table sbtest.R_0008 to sbtest.RR_0008\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[2-4096)\"\n\t\t}\n\t]\n}",
	}

	querys := []string{
		"alter table R rename RR",
		"alter table sbtest.R rename sbtest.RR",
		"alter table sbtest.R rename RR",
		"alter table R rename sbtest.RR",
		"alter table `R` rename `sbtest`.`RR`",
		"alter table `R` rename `sbtest`.RR",
		"alter table `R` rename sbtest.`RR`",
		"alter table `sbtest`.`R` rename RR",
		"alter table `sbtest`.R rename RR",
		"alter table sbtest.`R` rename RR",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableRConfig())
	assert.Nil(t, err)
	for i, query := range querys {
		log.Debug("%v", query)
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		plan := NewDDLPlan(log, database, query, node.(*sqlparser.DDL), route)

		// plan build
		{
			err := plan.Build()
			assert.Nil(t, err)
			want := results[i]
			got := plan.JSON()
			assert.Equal(t, want, got)
		}
	}
}

func TestDDLPlanWithRenameNoshard(t *testing.T) {
	results := []string{
		"{\n\t\"RawQuery\": \"alter table S rename S_S\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"rename table sbtest.S to sbtest.S_S\",\n\t\t\t\"Backend\": \"backend1\",\n\t\t\t\"Range\": \"\"\n\t\t}\n\t]\n}",
	}

	querys := []string{
		"alter table S rename S_S",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableSConfig())
	assert.Nil(t, err)
	for i, query := range querys {
		log.Debug("%v", query)
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		plan := NewDDLPlan(log, database, query, node.(*sqlparser.DDL), route)

		// plan build
		{
			err := plan.Build()
			assert.Nil(t, err)
			want := results[i]
			got := plan.JSON()
			assert.Equal(t, want, got)
		}
	}
}
