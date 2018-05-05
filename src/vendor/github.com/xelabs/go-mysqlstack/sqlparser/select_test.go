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

func TestSelect1(t *testing.T) {
	validSQL := []struct {
		input  string
		output string
	}{
		{
			input:  "select * from xx",
			output: "select * from xx",
		},
		{
			input:  "select /*backup*/ * from xx",
			output: "select /*backup*/ * from xx",
		},
		{
			input:  "select /*backup*/ * from xx where id=1",
			output: "select /*backup*/ * from xx where id = 1",
		},
	}

	for _, sel := range validSQL {
		sql := strings.TrimSpace(sel.input)
		tree, err := Parse(sql)
		if err != nil {
			t.Errorf("input: %s, err: %v", sql, err)
			continue
		}
		got := String(tree.(*Select))
		if sel.output != got {
			t.Errorf("want:\n%s\ngot:\n%s", sel.output, got)
		}
	}
}
