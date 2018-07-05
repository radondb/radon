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
		"{\n\t\"RawQuery\": \"create table A(a int)\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"create table `sbtest`.`A0`(a int)\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table `sbtest`.`A2`(a int)\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table `sbtest`.`A4`(a int)\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table `sbtest`.`A8`(a int)\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"drop table sbtest.A\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"drop table `sbtest`.`A0`\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"drop table `sbtest`.`A2`\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"drop table `sbtest`.`A4`\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"drop table `sbtest`.`A8`\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"alter table A engine = tokudb\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"alter table `sbtest`.`A0` engine = tokudb\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table `sbtest`.`A2` engine = tokudb\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table `sbtest`.`A4` engine = tokudb\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table `sbtest`.`A8` engine = tokudb\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"create index idx_a on A(a)\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"create index idx_a on `sbtest`.`A0`(a)\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create index idx_a on `sbtest`.`A2`(a)\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create index idx_a on `sbtest`.`A4`(a)\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create index idx_a on `sbtest`.`A8`(a)\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"drop index idx_a on sbtest.A\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"drop index idx_a on `sbtest`.`A0`\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"drop index idx_a on `sbtest`.`A2`\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"drop index idx_a on `sbtest`.`A4`\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"drop index idx_a on `sbtest`.`A8`\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"alter table A add column(b int)\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"alter table `sbtest`.`A0` add column(b int)\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table `sbtest`.`A2` add column(b int)\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table `sbtest`.`A4` add column(b int)\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table `sbtest`.`A8` add column(b int)\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"alter table sbtest.A add column(b int)\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"alter table `sbtest`.`A0` add column(b int)\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table `sbtest`.`A2` add column(b int)\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table `sbtest`.`A4` add column(b int)\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table `sbtest`.`A8` add column(b int)\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"alter table sbtest.A add column(b int, c varchar(100))\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"alter table `sbtest`.`A0` add column(b int, c varchar(100))\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table `sbtest`.`A2` add column(b int, c varchar(100))\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table `sbtest`.`A4` add column(b int, c varchar(100))\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table `sbtest`.`A8` add column(b int, c varchar(100))\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"alter table A modify column b int\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"alter table `sbtest`.`A0` modify column b int\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table `sbtest`.`A2` modify column b int\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table `sbtest`.`A4` modify column b int\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table `sbtest`.`A8` modify column b int\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"alter table A drop column b\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"alter table `sbtest`.`A0` drop column b\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table `sbtest`.`A2` drop column b\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table `sbtest`.`A4` drop column b\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"alter table `sbtest`.`A8` drop column b\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"truncate table A\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"truncate table `sbtest`.`A0`\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"truncate table `sbtest`.`A2`\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"truncate table `sbtest`.`A4`\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"truncate table `sbtest`.`A8`\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
	}

	querys := []string{
		"create table A(a int)",
		"drop table sbtest.A",
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

	err := route.AddForTest(database, router.MockTableAConfig())
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
			assert.True(t, nil == plan.Children())
		}

		// type
		{
			want := PlanTypeDDL
			got := plan.Type()
			assert.Equal(t, want, got)
		}
	}
}

func TestDDLAlterOnShardKey(t *testing.T) {
	results := []string{
		"unsupported: cannot.modify.the.column.on.shard.key",
		"unsupported: cannot.drop.the.column.on.shard.key",
	}

	querys := []string{
		"alter table A modify column id int",
		"alter table A drop column id",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableAConfig())
	assert.Nil(t, err)
	for i, query := range querys {
		log.Debug("%v", query)
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		plan := NewDDLPlan(log, database, query, node.(*sqlparser.DDL), route)

		// plan build
		{
			err := plan.Build()
			want := results[i]
			got := err.Error()
			assert.Equal(t, want, got)
		}
	}
}

func TestDDLPlanScatter(t *testing.T) {
	results := []string{
		`{
	"RawQuery": "create database A"
}`,
	}
	querys := []string{
		"create database A",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableAConfig())
	assert.Nil(t, err)
	for i, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		plan := NewDDLPlan(log, database, query, node.(*sqlparser.DDL), route)

		// plan build
		{
			err := plan.Build()
			assert.Nil(t, err)

			want := results[i]
			got := plan.JSON()
			log.Debug(got)
			assert.Equal(t, want, got)
		}
	}
}

func TestDDLPlanCreateIndexWithTableNameIssue10(t *testing.T) {
	results := []string{
		"{\n\t\"RawQuery\": \"create index idx_A_id on A(a)\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"create index idx_A_id on `sbtest`.`A0`(a)\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create index idx_A_id on `sbtest`.`A2`(a)\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create index idx_A_id on `sbtest`.`A4`(a)\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create index idx_A_id on `sbtest`.`A8`(a)\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
	}

	querys := []string{
		"create index idx_A_id on A(a)",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"
	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableAConfig())
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
		"{\n\t\"RawQuery\": \"create table `A`(a int)\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"create table `sbtest`.`A0`(a int)\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table `sbtest`.`A2`(a int)\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table `sbtest`.`A4`(a int)\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table `sbtest`.`A8`(a int)\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"create table A(`a` int)\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"create table `sbtest`.`A0`(a int)\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table `sbtest`.`A2`(a int)\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table `sbtest`.`A4`(a int)\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table `sbtest`.`A8`(a int)\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"create table A(a int)\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"create table `sbtest`.`A0`(a int)\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table `sbtest`.`A2`(a int)\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table `sbtest`.`A4`(a int)\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table `sbtest`.`A8`(a int)\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"create table sbtest.A(a int)\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"create table `sbtest`.`A0`(a int)\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table `sbtest`.`A2`(a int)\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table `sbtest`.`A4`(a int)\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table `sbtest`.`A8`(a int)\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"create table sbtest.`A`(a int)\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"create table `sbtest`.`A0`(a int)\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table `sbtest`.`A2`(a int)\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table `sbtest`.`A4`(a int)\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table `sbtest`.`A8`(a int)\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"create table `sbtest`.A(a int)\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"create table `sbtest`.`A0`(a int)\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table `sbtest`.`A2`(a int)\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table `sbtest`.`A4`(a int)\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table `sbtest`.`A8`(a int)\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
		"{\n\t\"RawQuery\": \"create table `sbtest`.`A`(a int)\",\n\t\"Partitions\": [\n\t\t{\n\t\t\t\"Query\": \"create table `sbtest`.`A0`(a int)\",\n\t\t\t\"Backend\": \"backend0\",\n\t\t\t\"Range\": \"[0-2)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table `sbtest`.`A2`(a int)\",\n\t\t\t\"Backend\": \"backend2\",\n\t\t\t\"Range\": \"[2-4)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table `sbtest`.`A4`(a int)\",\n\t\t\t\"Backend\": \"backend4\",\n\t\t\t\"Range\": \"[4-8)\"\n\t\t},\n\t\t{\n\t\t\t\"Query\": \"create table `sbtest`.`A8`(a int)\",\n\t\t\t\"Backend\": \"backend8\",\n\t\t\t\"Range\": \"[8-4096)\"\n\t\t}\n\t]\n}",
	}

	querys := []string{
		"create table `A`(a int)",
		"create table A(`a` int)",
		"create table A(a int)",
		"create table sbtest.A(a int)",
		"create table sbtest.`A`(a int)",
		"create table `sbtest`.A(a int)",
		"create table `sbtest`.`A`(a int)",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableAConfig())
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
			assert.True(t, nil == plan.Children())
		}
	}
}
