package evaluation

import (
	"expression/datum"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
)

// ConstantEval represents a constant evaluation.
type ConstantEval struct {
	value datum.Datum
	field *datum.IField
}

// CONST new a ConstantEval.
func CONST(val datum.Datum) Evaluation {
	return &ConstantEval{
		value: val,
	}
}

// FixField use to fix the IField by the fieldmap.
func (e *ConstantEval) FixField(fields map[string]*querypb.Field) (*datum.IField, error) {
	e.field = datum.ConstantField(e.value)
	return e.field, nil
}

// Update used to update the result by the valuemap.
func (e *ConstantEval) Update(values map[string]datum.Datum) (datum.Datum, error) {
	return e.value, nil
}

// Result used to get the result.
func (e *ConstantEval) Result() datum.Datum {
	return e.value
}
