package evaluation

import (
	"expression/datum"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
)

type functionFixFieldFunc func(args ...*datum.IField) *datum.IField

type functionUpdateFunc func(field *datum.IField, args ...datum.Datum) (datum.Datum, error)

type FunctionEval struct {
	name       string
	args       []Evaluation
	saved      datum.Datum
	field      *datum.IField
	fixFieldFn functionFixFieldFunc
	updateFn   functionUpdateFunc
	validate   Validator
}

func (e *FunctionEval) FixField(fields map[string]*querypb.Field) (*datum.IField, error) {
	argFields := make([]*datum.IField, len(e.args))
	for _, arg := range e.args {
		argField, err := arg.FixField(fields)
		if err != nil {
			return nil, err
		}
		argFields = append(argFields, argField)
	}

	if e.validate != nil {
		if err := e.validate.Validate(argFields...); err != nil {
			return nil, err
		}
	}
	e.field = e.fixFieldFn(argFields...)
	return e.field, nil
}

func (e *FunctionEval) Update(values map[string]datum.Datum) (datum.Datum, error) {
	var err error
	argValues := make([]datum.Datum, len(e.args))
	for _, arg := range e.args {
		argValue, err := arg.Update(values)
		if err != nil {
			return nil, err
		}
		argValues = append(argValues, argValue)
	}

	if e.saved, err = e.updateFn(e.field, argValues...); err != nil {
		return nil, err
	}
	return e.saved, nil
}

func (e *FunctionEval) Result() datum.Datum {
	return e.saved
}
