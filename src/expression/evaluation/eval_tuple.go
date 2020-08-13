package evaluation

import (
	"expression/datum"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
)

// TupleEval represents a tuple evaluation.
type TupleEval struct {
	args     []Evaluation
	saved    *datum.DTuple
	fields   []*datum.IField
	validate Validator
}

// TUPLE new a TupleEval.
func TUPLE(args []Evaluation) Evaluation {
	return &TupleEval{
		args:     args,
		validate: AllArgs(TypeOf(false, datum.RowResult)),
	}
}

// FixField use to fix the IField by the fieldmap.
func (e *TupleEval) FixField(fields map[string]*querypb.Field) (*datum.IField, error) {
	for _, arg := range e.args {
		field, err := arg.FixField(fields)
		if err != nil {
			return nil, err
		}
		e.fields = append(e.fields, field)
	}

	if e.validate != nil {
		if err := e.validate.Validate(e.fields...); err != nil {
			return nil, err
		}
	}
	return &datum.IField{ResTyp: datum.RowResult}, nil
}

// Update used to update the result by the valuemap.
func (e *TupleEval) Update(values map[string]datum.Datum) (datum.Datum, error) {
	var vals []datum.Datum
	for _, arg := range e.args {
		d, err := arg.Update(values)
		if err != nil {
			return nil, err
		}
		vals = append(vals, d)
	}
	e.saved = datum.NewDTuple(vals...)
	return e.saved, nil
}

// Result used to get the result.
func (e *TupleEval) Result() datum.Datum {
	return e.saved
}
