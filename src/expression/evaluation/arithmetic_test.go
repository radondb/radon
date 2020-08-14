package evaluation

import (
	"testing"

	"expression/datum"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
)

var (
	con1   = CONST(datum.NewDInt(3, false))
	con2   = TUPLE(CONST(datum.NewDInt(3, false)), CONST(datum.NewDInt(1, true)))
	con3   = CONST(datum.NewDInt(-1, true))
	con4   = CONST(datum.NewDString("abc@de.fg", 10))
	con5   = CONST(datum.NewDString("^[A-Z0-9._%-]+@[A-Z0-9.-]+.[A-Z]{2,4}$", 10))
	fields = map[string]*querypb.Field{
		"a": {
			Name:     "a",
			Type:     querypb.Type_INT32,
			Decimals: 0,
			Flags:    128,
		},
		"b": {
			Name:     "b",
			Type:     querypb.Type_UINT64,
			Decimals: 0,
			Flags:    32,
		},
		"c": {
			Name:     "c",
			Type:     querypb.Type_CHAR,
			Decimals: 31,
			Flags:    128,
		},
		"d": {
			Name:     "d",
			Type:     querypb.Type_FLOAT64,
			Decimals: 31,
			Flags:    128,
		},
		"e": {
			Name:     "e",
			Type:     querypb.Type_DECIMAL,
			Decimals: 2,
			Flags:    128,
		},
		"f": {
			Name:     "e",
			Type:     querypb.Type_NULL_TYPE,
			Decimals: 2,
			Flags:    128,
		},
	}
	values = map[string]datum.Datum{
		"a": datum.NewDInt(1, false),
		"b": datum.NewDInt(2, true),
		"c": datum.NewDString("c", 10),
		"d": datum.NewDFloat(3.20),
		"e": datum.NewDDecimal(decimal.NewFromFloat(4.30)),
		"f": datum.NewDNull(true),
	}
)

func TestArithmetic(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		left     Evaluation
		right    Evaluation
		field    *datum.IField
		saved    datum.Datum
	}{
		// ADD.
		{
			name:     "a+f",
			funcName: "+",
			left:     VAR("a"),
			right:    VAR("f"),
			field: &datum.IField{
				ResTyp:   datum.RealResult,
				Decimal:  31,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDNull(true),
		},
		{
			name:     "a+b",
			funcName: "+",
			left:     VAR("a"),
			right:    VAR("b"),
			field: &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     true,
				Constant: false,
			},
			saved: datum.NewDInt(3, true),
		},
		{
			name:     "a+3",
			funcName: "+",
			left:     VAR("a"),
			right:    con1,
			field: &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDInt(4, false),
		},
		{
			name:     "b+e",
			funcName: "+",
			left:     VAR("b"),
			right:    VAR("e"),
			field: &datum.IField{
				ResTyp:   datum.DecimalResult,
				Decimal:  2,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDDecimal(decimal.NewFromFloat(6.30)),
		},
		{
			name:     "c+d",
			funcName: "+",
			left:     VAR("c"),
			right:    VAR("d"),
			field: &datum.IField{
				ResTyp:   datum.RealResult,
				Decimal:  31,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDFloat(3.2),
		},
		// SUB.
		{
			name:     "a-f",
			funcName: "-",
			left:     VAR("a"),
			right:    VAR("f"),
			field: &datum.IField{
				ResTyp:   datum.RealResult,
				Decimal:  31,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDNull(true),
		},
		{
			name:     "b-a",
			funcName: "-",
			left:     VAR("b"),
			right:    VAR("a"),
			field: &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     true,
				Constant: false,
			},
			saved: datum.NewDInt(1, true),
		},
		{
			name:     "a-3",
			funcName: "-",
			left:     VAR("a"),
			right:    con1,
			field: &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDInt(-2, false),
		},
		{
			name:     "b-e",
			funcName: "-",
			left:     VAR("b"),
			right:    VAR("e"),
			field: &datum.IField{
				ResTyp:   datum.DecimalResult,
				Decimal:  2,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDDecimal(decimal.NewFromFloat(-2.30)),
		},
		{
			name:     "c-d",
			funcName: "-",
			left:     VAR("c"),
			right:    VAR("d"),
			field: &datum.IField{
				ResTyp:   datum.RealResult,
				Decimal:  31,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDFloat(-3.2),
		},
		// MUL.
		{
			name:     "a*f",
			funcName: "*",
			left:     VAR("a"),
			right:    VAR("f"),
			field: &datum.IField{
				ResTyp:   datum.RealResult,
				Decimal:  31,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDNull(true),
		},
		{
			name:     "a*b",
			funcName: "*",
			left:     VAR("a"),
			right:    VAR("b"),
			field: &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     true,
				Constant: false,
			},
			saved: datum.NewDInt(2, true),
		},
		{
			name:     "a*3",
			funcName: "*",
			left:     VAR("a"),
			right:    con1,
			field: &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDInt(3, false),
		},
		{
			name:     "b*e",
			funcName: "*",
			left:     VAR("b"),
			right:    VAR("e"),
			field: &datum.IField{
				ResTyp:   datum.DecimalResult,
				Decimal:  2,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDDecimal(decimal.NewFromFloat(8.60)),
		},
		{
			name:     "c*d",
			funcName: "*",
			left:     VAR("c"),
			right:    VAR("d"),
			field: &datum.IField{
				ResTyp:   datum.RealResult,
				Decimal:  31,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDFloat(0),
		},
		// DIV.
		{
			name:     "a/f",
			funcName: "/",
			left:     VAR("a"),
			right:    VAR("f"),
			field: &datum.IField{
				ResTyp:   datum.RealResult,
				Decimal:  4,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDNull(true),
		},
		{
			name:     "a/3",
			funcName: "/",
			left:     VAR("a"),
			right:    con1,
			field: &datum.IField{
				ResTyp:   datum.DecimalResult,
				Decimal:  4,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDDecimal(decimal.NewFromFloat(0.3333)),
		},
		{
			name:     "b/e",
			funcName: "/",
			left:     VAR("b"),
			right:    VAR("e"),
			field: &datum.IField{
				ResTyp:   datum.DecimalResult,
				Decimal:  4,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDDecimal(decimal.NewFromFloat(0.4651)),
		},
		{
			name:     "c/d",
			funcName: "/",
			left:     VAR("c"),
			right:    VAR("d"),
			field: &datum.IField{
				ResTyp:   datum.RealResult,
				Decimal:  31,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDFloat(0),
		},
		// INTDIV.
		{
			name:     "a div f",
			funcName: "div",
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
			name:     "a div b",
			funcName: "div",
			left:     VAR("a"),
			right:    VAR("b"),
			field: &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     true,
				Constant: false,
			},
			saved: datum.NewDInt(0, true),
		},
		{
			name:     "b div e",
			funcName: "div",
			left:     VAR("b"),
			right:    VAR("e"),
			field: &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     true,
				Constant: false,
			},
			saved: datum.NewDInt(0, true),
		},
		{
			name:     "d div c",
			funcName: "div",
			left:     VAR("d"),
			right:    VAR("c"),
			field: &datum.IField{
				ResTyp:   datum.IntResult,
				Decimal:  0,
				Flag:     false,
				Constant: false,
			},
			saved: datum.NewDNull(true),
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

func TestBinaryErr(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		left     Evaluation
		right    Evaluation
		err      string
	}{
		{
			name:     "a+h",
			funcName: "+",
			left:     VAR("a"),
			right:    VAR("h"),
			err:      "can.not.get.the.field.value:h",
		},
		{
			name:     "h+a",
			funcName: "+",
			left:     VAR("h"),
			right:    VAR("a"),
			err:      "can.not.get.the.field.value:h",
		},
		{
			name:     "a+con2",
			funcName: "+",
			left:     VAR("a"),
			right:    con2,
			err:      "bad.argument.at.index 1: unexpected.result.type[4].in.the.argument",
		},
		{
			name:     "a+3",
			funcName: "+",
			left:     VAR("a"),
			right:    con1,
			err:      "can.not.get.the.datum.value:a",
		},
		{
			name:     "3+a",
			funcName: "+",
			left:     con1,
			right:    VAR("a"),
			err:      "can.not.get.the.datum.value:a",
		},
		{
			name:     "con1+con3",
			funcName: "+",
			left:     con1,
			right:    con3,
			err:      "BIGINT.UNSIGNED.value.is.out.of.range.in: '3' + '18446744073709551615'",
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
