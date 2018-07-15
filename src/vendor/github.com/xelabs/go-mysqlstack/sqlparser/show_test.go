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

func TestShow1(t *testing.T) {
	validSQL := []struct {
		input  string
		output string
	}{
		{
			input:  "show create table t1",
			output: "show create table t1",
		},

		{
			input:  "show tables",
			output: "show tables",
		},

		{
			input:  "show tables from t1",
			output: "show tables",
		},

		{
			input:  "show databases",
			output: "show databases",
		},

		{
			input:  "show create database sbtest",
			output: "show create database sbtest",
		},

		{
			input:  "show engines",
			output: "show engines",
		},

		{
			input:  "show status",
			output: "show status",
		},

		{
			input:  "show versions",
			output: "show versions",
		},

		{
			input:  "show processlist",
			output: "show processlist",
		},

		{
			input:  "show queryz",
			output: "show queryz",
		},

		{
			input:  "show txnz",
			output: "show txnz",
		},

		{
			input:  "show warnings",
			output: "show warnings",
		},

		{
			input:  "show variables",
			output: "show variables",
		},
		{
			input:  "show binlog events",
			output: "show binlog events",
		},
		{
			input:  "show binlog events limit 10",
			output: "show binlog events limit 10",
		},
		{
			input:  "show binlog events from gtid '20171225083823'",
			output: "show binlog events from gtid '20171225083823'",
		},
		{
			input:  "show binlog events from gtid '20171225083823' limit 1",
			output: "show binlog events from gtid '20171225083823' limit 1",
		},
		{
			input:  "show columns from t1",
			output: "show columns from t1",
		},
	}

	for _, show := range validSQL {
		sql := strings.TrimSpace(show.input)
		tree, err := Parse(sql)
		if err != nil {
			t.Errorf("input: %s, err: %v", sql, err)
			continue
		}

		// Walk.
		Walk(func(node SQLNode) (bool, error) {
			return true, nil
		}, tree)

		got := String(tree.(*Show))
		if show.output != got {
			t.Errorf("want:\n%s\ngot:\n%s", show.output, got)
		}
	}
}
