/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqlparser

import "strings"
import "testing"

func TestDDL1(t *testing.T) {
	validSQL := []struct {
		input  string
		output string
	}{
		// Table.
		{
			input: "create table t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") partition by hash(id)",
			output: "create table t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				")",
		},

		{
			input: "create table t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") partition by hash(id)",
			output: "create table t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				")",
		},

		{
			input: "create table t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") partition by hash(id) partitions 6",
			output: "create table t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				")",
		},

		{
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				")",
			output: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				")",
		},

		{
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") default charset=utf8",
			output: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") default charset=utf8",
		},

		{
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb",
			output: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb",
		},

		{
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb default charset=utf8",
			output: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb default charset=utf8",
		},

		{
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") ENGINE=InnoDB AUTO_INCREMENT=34 DEFAULT CHARSET=utf8mb4 PARTITION BY HASH(id)",
			output: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=InnoDB default charset=utf8mb4",
		},

		{
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb default charset=utf8 partition by hash(id)",
			output: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb default charset=utf8",
		},
		// Index.
		{
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10),\n" +
				"	KEY `IDX_USER` (`user_id`)\n" +
				") engine=tokudb default charset=utf8 partition by hash(id)",
			output: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10),\n" +
				"	key `IDX_USER` (`user_id`)\n" +
				") engine=tokudb default charset=utf8",
		},

		{
			input:  "create table if not exists t1 (a int)",
			output: "create table if not exists t1 (\n\t`a` int\n)",
		},

		{
			input:  "truncate table t1",
			output: "truncate table t1",
		},

		{
			input:  "drop table t1",
			output: "drop table t1",
		},

		{
			input:  "drop table if exists t1",
			output: "drop table if exists t1",
		},

		// Database.
		{
			input:  "drop database test",
			output: "drop database test",
		},

		{
			input:  "create database test",
			output: "create database test",
		},

		{
			input:  "drop database if exists test",
			output: "drop database if exists test",
		},
		{
			input:  "create database if not exists test",
			output: "create database if not exists test",
		},

		// Alter engine.
		{
			input:  "alter table test engine=tokudb",
			output: "alter table test engine = tokudb",
		},
		{
			input:  "alter table test.t1 engine=tokudb",
			output: "alter table test.t1 engine = tokudb",
		},

		// Alter charset.
		{
			input:  "alter table test.t1 convert to character set utf8",
			output: "alter table test.t1 convert to character set utf8",
		},

		// Index.
		{
			input:  "create index idx on test(a,b)",
			output: "create index idx on test",
		},
		{
			input:  "drop index idx on test",
			output: "drop index idx on test",
		},

		// Add column.
		{
			input: "alter table test add column(id int primary key)",
			output: "alter table test add column (\n" +
				"	`id` int primary key\n" +
				")",
		},
		{
			input: "alter table test add column(id int primary key, name varchar(100))",
			output: "alter table test add column (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(100)\n" +
				")",
		},

		// Modify column.
		{
			input:  "alter table test modify column name varchar(200)",
			output: "alter table test modify column `name` varchar(200)",
		},
		{
			input:  "alter table test modify column name varchar(200) not null",
			output: "alter table test modify column `name` varchar(200) not null",
		},

		// Drop column.
		{
			input:  "alter table test drop column name",
			output: "alter table test drop column `name`",
		},
	}

	for _, ddl := range validSQL {
		sql := strings.TrimSpace(ddl.input)
		tree, err := Parse(sql)
		if err != nil {
			t.Errorf("input: %s, err: %v", sql, err)
			continue
		}

		// Walk.
		Walk(func(node SQLNode) (bool, error) {
			return true, nil
		}, tree)

		// Walk.
		node := tree.(*DDL)
		Walk(func(node SQLNode) (bool, error) {
			return true, nil
		}, node.TableSpec)

		got := String(tree.(*DDL))
		if ddl.output != got {
			t.Errorf("want:\n%s\ngot:\n%s", ddl.output, got)
		}
	}
}
