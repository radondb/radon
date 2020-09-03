package evaluation

import (
	"testing"
	"time"

	"expression/datum"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
)

func TestFunc(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		args     []Evaluation
		field    *datum.IField
		saved    datum.Datum
	}{
		{
			name:     "if (a > con1, a, b)",
			funcName: "if",
			args: []Evaluation{
				GT(VAR("a"), con1),
				VAR("a"),
				VAR("b"),
			},
			field: &datum.IField{
				Type:     datum.IntResult,
				IsBinary: true,
			},
			saved: datum.NewDInt(2, false),
		},
		{
			name:     "if (a < con1, a, c)",
			funcName: "if",
			args: []Evaluation{
				LT(VAR("a"), con1),
				VAR("a"),
				VAR("c"),
			},
			field: &datum.IField{
				Type:     datum.StringResult,
				IsBinary: true,
				Scale:    31,
			},
			saved: datum.NewDString("1", 10, true),
		},
		{
			name:     "if (a > con1, a, d)",
			funcName: "if",
			args: []Evaluation{
				GT(VAR("a"), con1),
				VAR("a"),
				VAR("d"),
			},
			field: &datum.IField{
				Type:     datum.RealResult,
				IsBinary: true,
				Scale:    31,
			},
			saved: datum.NewDFloat(3.20),
		},
		{
			name:     "if (a > con1, a, e)",
			funcName: "if",
			args: []Evaluation{
				GT(VAR("a"), con1),
				VAR("a"),
				VAR("e"),
			},
			field: &datum.IField{
				Type:     datum.DecimalResult,
				IsBinary: true,
				Length:   13,
				Scale:    2,
			},
			saved: datum.NewDDecimal(decimal.NewFromFloatWithExponent(4.30, -2)),
		},
		{
			name:     "if (a<con1,t,s)",
			funcName: "if",
			args: []Evaluation{
				LT(VAR("a"), con1),
				VAR("t"),
				VAR("s"),
			},
			field: &datum.IField{
				Type:     datum.TimeResult,
				IsBinary: true,
				Scale:    3,
			},
			saved: datum.NewDTime(querypb.Type_DATETIME, 3, 2020, 8, 21, 14, 58, 36, 666000),
		},
		{
			name:     "if (s>z,s,z)",
			funcName: "if",
			args: []Evaluation{
				GT(VAR("s"), VAR("z")),
				VAR("s"),
				VAR("z"),
			},
			field: &datum.IField{
				Type:     datum.DurationResult,
				IsBinary: true,
				Scale:    4,
			},
			saved: datum.NewDuration(time.Duration(9*3600)*time.Second+time.Duration(int64(586600)*1000), 4),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			eval, err := EvalFactory(test.funcName, test.args...)
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

func TestFuncErr(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		args     []Evaluation
		err      string
	}{
		{
			name:     "if (a < con1, a, g)",
			funcName: "if",
			args: []Evaluation{
				GT(VAR("a"), con1),
				VAR("a"),
				VAR("g"),
			},
			err: "can.not.get.the.field.value:g",
		},
		{
			name:     "if (a < con1, a, e)",
			funcName: "if",
			args: []Evaluation{
				GT(VAR("a"), con1),
				VAR("a"),
				VAR("e"),
				VAR("b"),
			},
			err: "expected.exactly.3.argument(s),but.got.4",
		},
		{
			name:     "if (a < con1, a, e)",
			funcName: "if",
			args: []Evaluation{
				GT(VAR("a"), con1),
				VAR("a"),
				VAR("e"),
			},
			err: "can.not.get.the.datum.value:a",
		},
	}

	for _, test := range tests {
		eval, err := EvalFactory(test.funcName, test.args...)
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
