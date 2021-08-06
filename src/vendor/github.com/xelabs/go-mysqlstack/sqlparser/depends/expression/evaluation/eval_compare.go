package evaluation

import (
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/expression/datum"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
)

type compareUpdateFunc func(left, right datum.Datum, cmpFunc datum.CompareFunc) datum.Datum

// CompareEval represents a comparison evaluation.
type CompareEval struct {
	name     string
	left     Evaluation
	right    Evaluation
	saved    datum.Datum
	cmpFunc  datum.CompareFunc
	updateFn compareUpdateFunc
	validate Validator
}

// FixField use to fix the IField by the fieldmap.
func (e *CompareEval) FixField(fields map[string]*querypb.Field) (*datum.IField, error) {
	left, err := e.left.FixField(fields)
	if err != nil {
		return nil, err
	}

	right, err := e.right.FixField(fields)
	if err != nil {
		return nil, err
	}

	if e.validate != nil {
		if err := e.validate.Validate(left, right); err != nil {
			return nil, err
		}
	}

	e.cmpFunc = datum.GetCmpFunc(left, right)
	return &datum.IField{
		Type:     datum.IntResult,
		IsBinary: true,
	}, nil
}

// Update used to update the result by the valuemap.
func (e *CompareEval) Update(values map[string]datum.Datum) (datum.Datum, error) {
	var err error
	var left, right datum.Datum

	if left, err = e.left.Update(values); err != nil {
		return nil, err
	}
	if right, err = e.right.Update(values); err != nil {
		return nil, err
	}
	e.saved = e.updateFn(left, right, e.cmpFunc)
	return e.saved, nil
}

// Result used to get the result.
func (e *CompareEval) Result() datum.Datum {
	return e.saved
}
