package evaluation

import (
	"expression/datum"

	"github.com/xelabs/go-mysqlstack/sqlparser"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
)

// CastEval represents a cast evaluation.
type CastEval struct {
	name     string
	typ      *sqlparser.ConvertType
	arg      Evaluation
	saved    datum.Datum
	field    *datum.IField
	validate Validator
}

func (e *CastEval) SetType(typ *sqlparser.ConvertType) {
	e.typ = typ
}

// FixField use to fix the IField by the fieldmap.
func (e *CastEval) FixField(fields map[string]*querypb.Field) (*datum.IField, error) {
	arg, err := e.arg.FixField(fields)
	if err != nil {
		return nil, err
	}

	if e.validate != nil {
		if err := e.validate.Validate(arg); err != nil {
			return nil, err
		}
	}
	e.field, err = datum.ConvertField(e.typ)
	return e.field, err
}

// Update used to update the result by the valuemap.
func (e *CastEval) Update(values map[string]datum.Datum) (datum.Datum, error) {
	arg, err := e.arg.Update(values)
	if err != nil {
		return nil, err
	}
	e.saved, err = datum.Cast(arg, e.field, true)
	return e.saved, err
}

// Result used to get the result.
func (e *CastEval) Result() datum.Datum {
	return e.saved
}
