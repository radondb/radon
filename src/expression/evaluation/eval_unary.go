package evaluation

import (
	"expression/datum"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
)

type unaryUpdateFunc func(arg datum.Datum, field *datum.IField) (datum.Datum, error)
type unaryFixFieldFunc func(arg *datum.IField) *datum.IField

type UnaryEval struct {
	name       string
	arg        Evaluation
	saved      datum.Datum
	field      *datum.IField
	fixFieldFn unaryFixFieldFunc
	updateFn   unaryUpdateFunc
	validate   Validator
}

func (e *UnaryEval) FixField(fields map[string]*querypb.Field) (*datum.IField, error) {
	arg, err := e.arg.FixField(fields)
	if err != nil {
		return nil, err
	}

	if e.validate != nil {
		if err := e.validate.Validate(arg); err != nil {
			return nil, err
		}
	}
	e.field = e.fixFieldFn(arg)
	return e.field, nil
}

func (e *UnaryEval) Update(values map[string]datum.Datum) (datum.Datum, error) {
	arg, err := e.arg.Update(values)
	if err != nil {
		return nil, err
	}
	e.saved, err = e.updateFn(arg, e.field)
	if err != nil {
		return nil, err
	}
	return e.saved, nil
}

func (e *UnaryEval) Result() datum.Datum {
	return e.saved
}
