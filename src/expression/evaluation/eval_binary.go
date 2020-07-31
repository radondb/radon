package evaluation

import (
	"expression/datum"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
)

type binaryUpdateFunc func(left, right datum.Datum, field *datum.IField) (datum.Datum, error)
type binaryFixFieldFunc func(left, right *datum.IField) *datum.IField

type BinaryEval struct {
	name       string
	left       Evaluation
	right      Evaluation
	saved      datum.Datum
	field      *datum.IField
	fixFieldFn binaryFixFieldFunc
	updateFn   binaryUpdateFunc
	//validate IValidator
}

func (e *BinaryEval) FixField(fields map[string]*querypb.Field) (*datum.IField, error) {
	left, err := e.left.FixField(fields)
	if err != nil {
		return nil, err
	}
	left.ToNumeric()
	right, err := e.right.FixField(fields)
	if err != nil {
		return nil, err
	}
	right.ToNumeric()
	e.field = e.fixFieldFn(left, right)
	return e.field, nil
}

func (e *BinaryEval) Update(values map[string]datum.Datum) (datum.Datum, error) {
	var err error
	var left, right datum.Datum

	if left, err = e.left.Update(values); err != nil {
		return nil, err
	}
	if right, err = e.right.Update(values); err != nil {
		return nil, err
	}
	/*	if e.validate != nil {
		if err := e.validate.Validate(left, right); err != nil {
			return nil, err
		}
	}*/
	if e.saved, err = e.updateFn(left, right, e.field); err != nil {
		return nil, err
	}
	return e.saved, nil
}

func (e *BinaryEval) Result() datum.Datum {
	return e.saved
}
