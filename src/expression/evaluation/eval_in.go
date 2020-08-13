package evaluation

import (
	"expression/datum"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
)

// InEval represents a in evaluation.
type InEval struct {
	// true: not in.
	// false: in.
	not      bool
	left     Evaluation
	right    Evaluation
	saved    datum.Datum
	cmpFuncs []datum.CompareFunc
	validate Validator
}

// FixField use to fix the IField by the fieldmap.
func (e *InEval) FixField(fields map[string]*querypb.Field) (*datum.IField, error) {
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

	rights := e.right.(*TupleEval).fields
	for _, right := range rights {
		e.cmpFuncs = append(e.cmpFuncs, datum.GetCmpFunc(left, right))
	}

	return &datum.IField{
		ResTyp:   datum.IntResult,
		Decimal:  0,
		Flag:     false,
		Constant: false,
	}, nil
}

// Update used to update the result by the valuemap.
func (e *InEval) Update(values map[string]datum.Datum) (datum.Datum, error) {
	left, err := e.left.Update(values)
	if err != nil {
		return nil, err
	}
	if datum.CheckNull(left) {
		return datum.NewDNull(true), nil
	}

	_, err = e.right.Update(values)
	if err != nil {
		return nil, err
	}

	var (
		hasNull = false
		match   = false
		val     = int64(-1)
	)

	for i, right := range e.right.(*TupleEval).saved.Args() {
		if datum.CheckNull(right) {
			hasNull = true
			continue
		}
		res := e.cmpFuncs[i](left, right)
		if e.not {
			if res != 0 {
				match = true
				break
			}
		} else {
			if res == 0 {
				match = true
				break
			}
		}
	}

	if !match && hasNull {
		return datum.NewDNull(true), nil
	}
	if match {
		val = 1
	}
	e.saved = datum.NewDInt(val, false)
	return e.saved, nil
}

// Result used to get the result.
func (e *InEval) Result() datum.Datum {
	return e.saved
}
