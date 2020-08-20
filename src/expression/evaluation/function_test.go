package evaluation

import (
	"testing"

	"expression/datum"

	"github.com/stretchr/testify/assert"
)

func TestIF(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		args     []Evaluation
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
			saved: datum.NewDInt(2, false),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			eval, err := EvalFactory(test.funcName, test.args...)
			assert.Nil(t, err)

			field, err := eval.FixField(fields)
			assert.Nil(t, err)

			assert.Equal(t, &datum.IField{
				ResTyp:   datum.IntResult,
				Scale:    0,
				Flag:     false,
				Constant: false,
			}, field)

			_, err = eval.Update(values)
			assert.Nil(t, err)

			saved := eval.Result()
			assert.Equal(t, test.saved, saved)
		})
	}
}
