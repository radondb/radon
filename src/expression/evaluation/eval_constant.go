package evaluation

import (
	"expression/datum"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
)

type ConstantEval struct {
	value datum.Datum
	field *datum.IField
}

func CONST(val datum.Datum) Evaluation {
	return &ConstantEval{
		value: val,
	}
}

func (e *ConstantEval) FixField(fields map[string]*querypb.Field) (*datum.IField, error) {
	e.field = datum.ConstantField(e.value)
	return e.field, nil
}

func (e *ConstantEval) Update(values map[string]datum.Datum) (datum.Datum, error) {
	return e.value, nil
}

func (e *ConstantEval) Result() datum.Datum {
	return e.value
}
