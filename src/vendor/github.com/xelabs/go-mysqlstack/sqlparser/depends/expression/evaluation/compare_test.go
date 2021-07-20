package evaluation

import (
	"testing"

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/expression/datum"

	"github.com/stretchr/testify/assert"
)

func TestCompare(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		left     Evaluation
		right    Evaluation
		saved    datum.Datum
	}{
		// GT.
		{
			name:     "a>f",
			funcName: ">",
			left:     VAR("a"),
			right:    VAR("f"),
			saved:    datum.NewDNull(true),
		},
		{
			name:     "a>b",
			funcName: ">",
			left:     VAR("a"),
			right:    VAR("b"),
			saved:    datum.NewDInt(0, false),
		},
		{
			name:     "e>d",
			funcName: ">",
			left:     VAR("e"),
			right:    VAR("d"),
			saved:    datum.NewDInt(1, false),
		},
		// GE.
		{
			name:     "a>=f",
			funcName: ">=",
			left:     VAR("a"),
			right:    VAR("f"),
			saved:    datum.NewDNull(true),
		},
		{
			name:     "a>=b",
			funcName: ">=",
			left:     VAR("a"),
			right:    VAR("b"),
			saved:    datum.NewDInt(0, false),
		},
		{
			name:     "e>=d",
			funcName: ">=",
			left:     VAR("e"),
			right:    VAR("d"),
			saved:    datum.NewDInt(1, false),
		},
		// EQ.
		{
			name:     "a=f",
			funcName: "=",
			left:     VAR("a"),
			right:    VAR("f"),
			saved:    datum.NewDNull(true),
		},
		{
			name:     "a=b",
			funcName: "=",
			left:     VAR("a"),
			right:    VAR("b"),
			saved:    datum.NewDInt(0, false),
		},
		{
			name:     "e=e",
			funcName: "=",
			left:     VAR("e"),
			right:    VAR("e"),
			saved:    datum.NewDInt(1, false),
		},
		// LT.
		{
			name:     "a<f",
			funcName: "<",
			left:     VAR("a"),
			right:    VAR("f"),
			saved:    datum.NewDNull(true),
		},
		{
			name:     "a<b",
			funcName: "<",
			left:     VAR("a"),
			right:    VAR("b"),
			saved:    datum.NewDInt(1, false),
		},
		{
			name:     "e<d",
			funcName: "<",
			left:     VAR("e"),
			right:    VAR("d"),
			saved:    datum.NewDInt(0, false),
		},
		// LE.
		{
			name:     "a<=f",
			funcName: "<=",
			left:     VAR("a"),
			right:    VAR("f"),
			saved:    datum.NewDNull(true),
		},
		{
			name:     "a<=b",
			funcName: "<=",
			left:     VAR("a"),
			right:    VAR("b"),
			saved:    datum.NewDInt(1, false),
		},
		{
			name:     "e<=d",
			funcName: "<=",
			left:     VAR("e"),
			right:    VAR("d"),
			saved:    datum.NewDInt(0, false),
		},
		// NE.
		{
			name:     "a!=f",
			funcName: "!=",
			left:     VAR("a"),
			right:    VAR("f"),
			saved:    datum.NewDNull(true),
		},
		{
			name:     "a!=b",
			funcName: "!=",
			left:     VAR("a"),
			right:    VAR("b"),
			saved:    datum.NewDInt(1, false),
		},
		{
			name:     "e!=e",
			funcName: "!=",
			left:     VAR("e"),
			right:    VAR("e"),
			saved:    datum.NewDInt(0, false),
		},
		// SE.
		{
			name:     "a<=>f",
			funcName: "<=>",
			left:     VAR("a"),
			right:    VAR("f"),
			saved:    datum.NewDInt(0, false),
		},
		{
			name:     "e<=>e",
			funcName: "<=>",
			left:     VAR("e"),
			right:    VAR("e"),
			saved:    datum.NewDInt(1, false),
		},
		// REGEXP.
		{
			name:     "con4 regexp con2",
			funcName: "regexp",
			left:     con4,
			right:    con2,
			saved:    datum.NewDInt(1, false),
		},
		// NOT REGEXP.
		{
			name:     "c not regexp con2",
			funcName: "not regexp",
			left:     VAR("c"),
			right:    con2,
			saved:    datum.NewDInt(1, false),
		},
		// IN.
		{
			name:     "a in tuple",
			funcName: "in",
			left:     VAR("a"),
			right:    tuple,
			saved:    datum.NewDInt(1, false),
		},
		{
			name:     "b in tuple",
			funcName: "in",
			left:     VAR("b"),
			right:    tuple,
			saved:    datum.NewDNull(true),
		},
		{
			name:     "f in tuple",
			funcName: "in",
			left:     VAR("f"),
			right:    tuple,
			saved:    datum.NewDNull(true),
		},
		// NOT IN.
		{
			name:     "a not in tuple",
			funcName: "not in",
			left:     VAR("b"),
			right:    tuple,
			saved:    datum.NewDInt(1, false),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			eval, err := EvalFactory(test.funcName, test.left, test.right)
			assert.Nil(t, err)

			field, err := eval.FixField(fields)
			assert.Nil(t, err)

			assert.Equal(t, &datum.IField{
				Type:       datum.IntResult,
				Scale:      0,
				IsUnsigned: false,
				IsBinary:   true,
				IsConstant: false,
			}, field)

			_, err = eval.Update(values)
			assert.Nil(t, err)

			saved := eval.Result()
			assert.Equal(t, test.saved, saved)
		})
	}
}

func TestCompareErr(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		left     Evaluation
		right    Evaluation
		err      string
	}{
		{
			name:     "a>h",
			funcName: ">",
			left:     VAR("a"),
			right:    VAR("h"),
			err:      "can.not.get.the.field.value:h",
		},
		{
			name:     "h<a",
			funcName: "<",
			left:     VAR("h"),
			right:    VAR("a"),
			err:      "can.not.get.the.field.value:h",
		},
		{
			name:     "a>=tuple",
			funcName: ">=",
			left:     VAR("a"),
			right:    tuple,
			err:      "bad.argument.at.index 1: unexpected.result.type[4].in.the.argument",
		},
		{
			name:     "a<=3",
			funcName: "<=",
			left:     VAR("a"),
			right:    con1,
			err:      "can.not.get.the.datum.value:a",
		},
		{
			name:     "3!=a",
			funcName: "!=",
			left:     con1,
			right:    VAR("a"),
			err:      "can.not.get.the.datum.value:a",
		},
		{
			name:     "h in tuple",
			funcName: "in",
			left:     VAR("h"),
			right:    tuple,
			err:      "can.not.get.the.field.value:h",
		},
		{
			name:     "a in h",
			funcName: "in",
			left:     VAR("a"),
			right:    VAR("h"),
			err:      "can.not.get.the.field.value:h",
		},
		{
			name:     "a in b",
			funcName: "in",
			left:     VAR("a"),
			right:    VAR("b"),
			err:      "bad.argument.at.index 1: expected.result.type[4].but.got.type[1]",
		},
		{
			name:     "a in tuple",
			funcName: "in",
			left:     VAR("a"),
			right:    tuple,
			err:      "can.not.get.the.datum.value:a",
		},
		{
			name:     "3 in tuple",
			funcName: "in",
			left:     con1,
			right:    tuple,
			err:      "can.not.get.the.datum.value:f",
		},
	}

	for _, test := range tests {
		eval, err := EvalFactory(test.funcName, test.left, test.right)
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

func TestLike(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		left     Evaluation
		right    Evaluation
		escape   Evaluation
		saved    datum.Datum
	}{
		{
			name:     "c like con5 escape '\\'",
			funcName: "like",
			left:     VAR("c"),
			right:    con5,
			escape:   CONST(datum.NewDString("\\", 10, false)),
			saved:    datum.NewDInt(1, false),
		},
		{
			name:     "f like con5 escape '\\'",
			funcName: "like",
			left:     VAR("f"),
			right:    con5,
			escape:   CONST(datum.NewDString("\\", 10, false)),
			saved:    datum.NewDNull(true),
		},
		{
			name:     "c not like con5 escape '\\'",
			funcName: "not like",
			left:     VAR("c"),
			right:    con5,
			escape:   CONST(datum.NewDString("\\", 10, false)),
			saved:    datum.NewDInt(0, false),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			eval, err := EvalFactory(test.funcName, test.left, test.right, test.escape)
			assert.Nil(t, err)

			field, err := eval.FixField(fields)
			assert.Nil(t, err)

			assert.Equal(t, &datum.IField{
				Type:       datum.IntResult,
				Scale:      0,
				IsUnsigned: false,
				IsBinary:   true,
				IsConstant: false,
			}, field)

			_, err = eval.Update(values)
			assert.Nil(t, err)

			saved := eval.Result()
			assert.Equal(t, test.saved, saved)
		})
	}
}
