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

func TestChecksumTable(t *testing.T) {
	validSQL := []struct {
		input  string
		output string
	}{
		{
			input:  "checksum table test.t1 quick",
			output: "checksum table test.t1 quick",
		},
		{
			input:  "checksum table t1, t2 EXTENDED",
			output: "checksum table t1, t2 extended",
		},
		{
			input:  "checksum tables test.t1, t2,test.x",
			output: "checksum table test.t1, t2, test.x",
		},
	}

	for _, s := range validSQL {
		sql := strings.TrimSpace(s.input)
		tree, err := Parse(sql)
		if err != nil {
			t.Errorf("input: %s, err: %v", sql, err)
			continue
		}

		// Walk.
		Walk(func(node SQLNode) (bool, error) {
			return true, nil
		}, tree)

		got := String(tree.(*Checksum))
		if s.output != got {
			t.Errorf("want:\n%s\ngot:\n%s", s.output, got)
		}
	}
}
