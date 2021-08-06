package evaluation

import (
	"testing"

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/expression/datum"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser"
)

func TestUnary(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		arg      Evaluation
		field    *datum.IField
		saved    datum.Datum
	}{
		// NOT.
		{
			name:     "not f",
			funcName: "not",
			arg:      VAR("f"),
			field: &datum.IField{
				Type:     datum.IntResult,
				IsBinary: true,
			},
			saved: datum.NewDNull(true),
		},
		{
			name:     "not a>b",
			funcName: "not",
			arg:      GT(VAR("a"), VAR("b")),
			field: &datum.IField{
				Type:     datum.IntResult,
				IsBinary: true,
			},
			saved: datum.NewDInt(1, false),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			eval, err := EvalFactory(test.funcName, test.arg)
			assert.Nil(t, err)

			field, err := eval.FixField(fields)
			assert.Nil(t, err)

			assert.Equal(t, test.field, field)

			_, err = eval.Update(values)
			assert.Nil(t, err)

			saved := eval.Result()
			assert.Equal(t, test.saved, saved)
		})
	}
}

func TestUnaryErr(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		arg      Evaluation
		err      string
	}{
		// NOT.
		{
			name:     "not g",
			funcName: "not",
			arg:      VAR("g"),
			err:      "can.not.get.the.field.value:g",
		},
		{
			name:     "not tuple",
			funcName: "not",
			arg:      tuple,
			err:      "bad.argument.at.index 0: unexpected.result.type[4].in.the.argument",
		},
		{
			name:     "not a",
			funcName: "not",
			arg:      VAR("a"),
			err:      "can.not.get.the.datum.value:a",
		},
	}

	for _, test := range tests {
		eval, err := EvalFactory(test.funcName, test.arg)
		assert.Nil(t, err)

		_, err = eval.FixField(fields)
		if err != nil {
			assert.Equal(t, test.err, err.Error())
			continue
		}

		_, err = eval.Update(nil)
		if err != nil {
			assert.Equal(t, test.err, err.Error())
		}
	}
}

func TestCast(t *testing.T) {
	tests := []struct {
		name  string
		arg   Evaluation
		typ   *sqlparser.ConvertType
		field *datum.IField
		saved datum.Datum
	}{
		{
			name: "cast(a as unsigned)",
			arg:  VAR("a"),
			typ: &sqlparser.ConvertType{
				Type: "unsigned",
			},
			field: &datum.IField{
				Type:       datum.IntResult,
				IsUnsigned: true,
				IsBinary:   true,
			},
			saved: datum.NewDInt(1, true),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			eval := CAST(test.arg)
			eval.(*CastEval).SetType(test.typ)

			field, err := eval.FixField(fields)
			assert.Nil(t, err)

			assert.Equal(t, test.field, field)

			_, err = eval.Update(values)
			assert.Nil(t, err)

			saved := eval.Result()
			assert.Equal(t, test.saved, saved)
		})
	}
}

func TestCastErr(t *testing.T) {
	tests := []struct {
		name string
		arg  Evaluation
		err  string
	}{
		{
			name: "cast (g as unsigned)",
			arg:  VAR("g"),
			err:  "can.not.get.the.field.value:g",
		},
		{
			name: "cast (tuple as unsigned)",
			arg:  tuple,
			err:  "bad.argument.at.index 0: unexpected.result.type[4].in.the.argument",
		},
		{
			name: "cast (a as unsigned)",
			arg:  VAR("a"),
			err:  "can.not.get.the.datum.value:a",
		},
	}

	for _, test := range tests {
		eval := CAST(test.arg)
		eval.(*CastEval).typ = &sqlparser.ConvertType{Type: "unsigned"}

		_, err := eval.FixField(fields)
		if err != nil {
			assert.Equal(t, test.err, err.Error())
			continue
		}

		_, err = eval.Update(nil)
		if err != nil {
			assert.Equal(t, test.err, err.Error())
		}
	}
}
