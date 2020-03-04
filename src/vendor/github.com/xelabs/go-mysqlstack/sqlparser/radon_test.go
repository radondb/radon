package sqlparser

import (
	"strings"
	"testing"
)

func TestRadon(t *testing.T) {
	validSQL := []struct {
		input  string
		output string
	}{
		// name, address, user, password.
		{
			input:  "radon attach ('attach1', '127.0.0.1:6000', 'root', '123456')",
			output: "radon attach ('attach1', '127.0.0.1:6000', 'root', '123456')",
		},
		{
			input:  "radon attachlist",
			output: "radon attachlist",
		},
		{
			input:  "radon detach('attach1')",
			output: "radon detach ('attach1')",
		},
		{
			input:  "radon reshard db.t db.tt",
			output: "radon reshard db.t to db.tt",
		},
		{
			input:  "radon reshard db.t to a.tt",
			output: "radon reshard db.t to a.tt",
		},
		{
			input:  "radon reshard db.t as b.tt",
			output: "radon reshard db.t to b.tt",
		},
		{
			input:  "radon cleanup",
			output: "radon cleanup",
		},
	}

	for _, exp := range validSQL {
		sql := strings.TrimSpace(exp.input)
		tree, err := Parse(sql)
		if err != nil {
			t.Errorf("input: %s, err: %v", sql, err)
			continue
		}

		// Walk.
		Walk(func(node SQLNode) (bool, error) {
			return true, nil
		}, tree)

		got := String(tree.(*Radon))
		if exp.output != got {
			t.Errorf("want:\n%s\ngot:\n%s", exp.output, got)
		}
	}
}
