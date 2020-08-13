package evaluation

import (
	"expression/datum"

	"github.com/pkg/errors"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
)

// VariableEval represents a variable evaluation.
type VariableEval struct {
	value string
	saved datum.Datum
}

// VAR new a VariableEval.
func VAR(v string) Evaluation {
	return &VariableEval{
		value: v,
	}
}

// FixField use to fix the IField by the fieldmap.
func (e *VariableEval) FixField(fields map[string]*querypb.Field) (*datum.IField, error) {
	f, ok := fields[e.value]
	if !ok {
		return nil, errors.Errorf("can.not.get.the.field.value:%v", e.value)
	}
	return datum.NewIField(f), nil
}

// Update used to update the result by the valuemap.
func (e *VariableEval) Update(values map[string]datum.Datum) (datum.Datum, error) {
	if values != nil {
		v, ok := values[e.value]
		if !ok {
			return nil, errors.Errorf("can.not.get.the.datum.value:%v", e.value)
		}
		e.saved = v
		return v, nil
	}
	return nil, nil
}

// Result used to get the result.
func (e *VariableEval) Result() datum.Datum {
	return e.saved
}
