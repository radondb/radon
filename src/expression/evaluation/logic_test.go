package evaluation

import (
	"expression/datum"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLogic(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		left     Evaluation
		right    Evaluation
		saved    datum.Datum
	}{
		// AND.
		{
			name:     "f and a>b",
			funcName: "and",
			left:     VAR("f"),
			right:    GT(VAR("a"), VAR("b")),
			saved:    datum.NewDNull(true),
		},
		{
			name:     "a+b=con1 and d<e",
			funcName: "and",
			left:     EQ(ADD(VAR("a"), VAR("b")), con1),
			right:    LT(VAR("d"), VAR("e")),
			saved:    datum.NewDInt(1, false),
		},
		// OR.
		{
			name:     "f or a>b",
			funcName: "or",
			left:     VAR("f"),
			right:    GT(VAR("a"), VAR("b")),
			saved:    datum.NewDNull(true),
		},
		{
			name:     "f or a<b",
			funcName: "or",
			left:     VAR("f"),
			right:    LT(VAR("a"), VAR("b")),
			saved:    datum.NewDInt(1, false),
		},
		{
			name:     "a<b or d<e",
			funcName: "or",
			left:     LT(VAR("a"), VAR("b")),
			right:    LT(VAR("d"), VAR("e")),
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
