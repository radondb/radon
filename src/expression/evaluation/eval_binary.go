package evaluation

import (
	"expression/datum"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
)

type binaryFixFieldFunc func(left, right *datum.IField) *datum.IField
type binaryUpdateFunc func(field *datum.IField, left, right datum.Datum) (datum.Datum, error)

// BinaryEval represents a binary evaluation.
type BinaryEval struct {
	name       string
	left       Evaluation
	right      Evaluation
	saved      datum.Datum
	field      *datum.IField
	fixFieldFn binaryFixFieldFunc
	updateFn   binaryUpdateFunc
	validate   Validator
}

// FixField use to fix the IField by the fieldmap.
func (e *BinaryEval) FixField(fields map[string]*querypb.Field) (*datum.IField, error) {
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
	e.field = e.fixFieldFn(left, right)
	return e.field, nil
}

// Update used to update the result by the valuemap.
func (e *BinaryEval) Update(values map[string]datum.Datum) (datum.Datum, error) {
	var err error
	var left, right datum.Datum

	if left, err = e.left.Update(values); err != nil {
		return nil, err
	}
	if right, err = e.right.Update(values); err != nil {
		return nil, err
	}
	if e.saved, err = e.updateFn(e.field, left, right); err != nil {
		return nil, err
	}
	return e.saved, nil
}

// Result used to get the result.
func (e *BinaryEval) Result() datum.Datum {
	return e.saved
}
