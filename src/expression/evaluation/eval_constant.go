package evaluation

import (
	"expression/datum"

	"github.com/xelabs/go-mysqlstack/sqlparser"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
)

type ConstantEval struct {
	value datum.Datum
	field *datum.IField
}

func NewConstantEval(val *sqlparser.SQLVal) (*ConstantEval, error) {
	value, field, err := datum.SQLValToDatum(val)
	if err != nil {
		return nil, nil
	}
	return &ConstantEval{
		value: value,
		field: field,
	}, nil
}

func (e *ConstantEval) FixField(fields map[string]*querypb.Field) (*datum.IField, error) {
	return e.field, nil
}

func (e *ConstantEval) Update(values map[string]datum.Datum) (datum.Datum, error) {
	return e.value, nil
}

func (e *ConstantEval) Result() datum.Datum {
	return e.value
}
