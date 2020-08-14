package evaluation

import (
	"expression/datum"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompare(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		left     Evaluation
		right    Evaluation
		field    *datum.IField
		saved    datum.Datum
	}{
		// GT.
		{
			name:     "a>f",
			funcName: ">",
			left:     VAR("a"),
			right:    VAR("f"),
			field: &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDNull(true),
		},
		{
			name:     "a>b",
			funcName: ">",
			left:     VAR("a"),
			right:    VAR("b"),
			field: &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDInt(0, false),
		},
		{
			name:     "e>d",
			funcName: ">",
			left:     VAR("e"),
			right:    VAR("d"),
			field: &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDInt(1, false),
		},
		// GE.
		{
			name:     "a>=f",
			funcName: ">=",
			left:     VAR("a"),
			right:    VAR("f"),
			field: &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDNull(true),
		},
		{
			name:     "a>=b",
			funcName: ">=",
			left:     VAR("a"),
			right:    VAR("b"),
			field: &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDInt(0, false),
		},
		{
			name:     "e>=d",
			funcName: ">=",
			left:     VAR("e"),
			right:    VAR("d"),
			field: &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDInt(1, false),
		},
		// EQ.
		{
			name:     "a=f",
			funcName: "=",
			left:     VAR("a"),
			right:    VAR("f"),
			field: &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDNull(true),
		},
		{
			name:     "a=b",
			funcName: "=",
			left:     VAR("a"),
			right:    VAR("b"),
			field: &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDInt(0, false),
		},
		{
			name:     "e=e",
			funcName: "=",
			left:     VAR("e"),
			right:    VAR("e"),
			field: &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDInt(1, false),
		},
		// LT.
		{
			name:     "a<f",
			funcName: "<",
			left:     VAR("a"),
			right:    VAR("f"),
			field: &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDNull(true),
		},
		{
			name:     "a<b",
			funcName: "<",
			left:     VAR("a"),
			right:    VAR("b"),
			field: &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDInt(1, false),
		},
		{
			name:     "e<d",
			funcName: "<",
			left:     VAR("e"),
			right:    VAR("d"),
			field: &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDInt(0, false),
		},
		// LE.
		{
			name:     "a<=f",
			funcName: "<=",
			left:     VAR("a"),
			right:    VAR("f"),
			field: &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDNull(true),
		},
		{
			name:     "a<=b",
			funcName: "<=",
			left:     VAR("a"),
			right:    VAR("b"),
			field: &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDInt(1, false),
		},
		{
			name:     "e<=d",
			funcName: "<=",
			left:     VAR("e"),
			right:    VAR("d"),
			field: &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDInt(0, false),
		},
		// NE.
		{
			name:     "a!=f",
			funcName: "!=",
			left:     VAR("a"),
			right:    VAR("f"),
			field: &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDNull(true),
		},
		{
			name:     "a!=b",
			funcName: "!=",
			left:     VAR("a"),
			right:    VAR("b"),
			field: &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDInt(1, false),
		},
		{
			name:     "e!=e",
			funcName: "!=",
			left:     VAR("e"),
			right:    VAR("e"),
			field: &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDInt(0, false),
		},
		// SE.
		{
			name:     "a<=>f",
			funcName: "<=>",
			left:     VAR("a"),
			right:    VAR("f"),
			field: &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDInt(0, false),
		},
		{
			name:     "e<=>e",
			funcName: "<=>",
			left:     VAR("e"),
			right:    VAR("e"),
			field: &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDInt(1, false),
		},
		// REGEXP.
		{
			name:     "con4 regexp con5",
			funcName: "regexp",
			left:     con4,
			right:    con5,
			field: &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDInt(1, false),
		},
		// NOT REGEXP.
		{
			name:     "c not regexp con5",
			funcName: "not regexp",
			left:     VAR("c"),
			right:    con5,
			field: &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDInt(1, false),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			eval, err := EvalFactory(test.funcName, test.left, test.right)
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

func TestIN(t *testing.T) {

}

func TestLike(t *testing.T) {

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
			name:     "a>=con2",
			funcName: ">=",
			left:     VAR("a"),
			right:    con2,
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
