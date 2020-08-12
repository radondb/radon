package evaluation

import (
	"expression/datum"

	"github.com/pkg/errors"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
)

type VariableEval struct {
	value string
	saved datum.Datum
}

func VAR(v string) Evaluation {
	return &VariableEval{
		value: v,
	}
}

func (e *VariableEval) FixField(fields map[string]*querypb.Field) (*datum.IField, error) {
	f, ok := fields[e.value]
	if !ok {
		return nil, errors.Errorf("can.not.get.the.field.value:%v", e.value)
	}
	return datum.NewIField(f), nil
}

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

func (e *VariableEval) Result() datum.Datum {
	return e.saved
}
