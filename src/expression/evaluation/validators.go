package evaluation

import (
	"expression/datum"

	"github.com/pkg/errors"
)

type Validator interface {
	Validate(args ...*datum.IField) error
}

type SingleArgValidator interface {
	Validate(arg *datum.IField) error
}

type all struct {
	validators []Validator
}

func All(validators ...Validator) *all {
	return &all{validators: validators}
}

func (v *all) Validate(args ...*datum.IField) error {
	for _, validator := range v.validators {
		err := validator.Validate(args...)
		if err != nil {
			return err
		}
	}
	return nil
}

type oneOf struct {
	validators []Validator
}

func OneOf(validators ...Validator) *oneOf {
	return &oneOf{validators: validators}
}

func (v *oneOf) Validate(args ...*datum.IField) error {
	errs := make([]error, len(v.validators))
	for i, validator := range v.validators {
		errs[i] = validator.Validate(args...)
		if errs[i] == nil {
			return nil
		}
	}
	return errors.Errorf("none.of.the.conditions.have.been.met: %+v", errs)
}

type singleOneOf struct {
	validators []SingleArgValidator
}

func SingleOneOf(validators ...SingleArgValidator) *singleOneOf {
	return &singleOneOf{validators: validators}
}

func (v *singleOneOf) Validate(arg *datum.IField) error {
	errs := make([]error, len(v.validators))
	for i, validator := range v.validators {
		errs[i] = validator.Validate(arg)
		if errs[i] == nil {
			return nil
		}
	}

	return errors.Errorf("none.of.the.conditions.have.been.met: %+v", errs)
}

type ifArgPresent struct {
	i         int
	validator Validator
}

func IfArgPresent(i int, validator Validator) *ifArgPresent {
	return &ifArgPresent{i: i, validator: validator}
}

func (v *ifArgPresent) Validate(args ...*datum.IField) error {
	if len(args) < v.i+1 {
		return nil
	}
	return v.validator.Validate(args...)
}

type atLeastNArgs struct {
	n int
}

func AtLeastNArgs(n int) *atLeastNArgs {
	return &atLeastNArgs{n: n}
}

func (v *atLeastNArgs) Validate(args ...*datum.IField) error {
	if len(args) < v.n {
		return errors.Errorf("expected.at.least.%d.argument(s),but.got.%v", v.n, len(args))
	}
	return nil
}

type atMostNArgs struct {
	n int
}

func AtMostNArgs(n int) *atMostNArgs {
	return &atMostNArgs{n: n}
}

func (v *atMostNArgs) Validate(args ...*datum.IField) error {
	if len(args) > v.n {
		return errors.Errorf("expected.at.most.%d.argument(s),but.got.%v", v.n, len(args))
	}
	return nil
}

type exactlyNArgs struct {
	n int
}

func ExactlyNArgs(n int) *exactlyNArgs {
	return &exactlyNArgs{n: n}
}

func (v *exactlyNArgs) Validate(args ...*datum.IField) error {
	if len(args) != v.n {
		return errors.Errorf("expected.exactly.%d.argument(s),but.got.%v", v.n, len(args))
	}
	return nil
}

type resTyp struct {
	wanted bool
	typ    datum.ResultType
}

func ResTyp(wanted bool, typ datum.ResultType) *resTyp {
	return &resTyp{wanted: wanted, typ: typ}
}

func (v *resTyp) Validate(arg *datum.IField) error {
	if v.typ == arg.ResTyp {
		if !v.wanted {
			return errors.Errorf("unexpected.result.type[%v].in.the.argument", v.typ)
		}
	} else {
		if v.wanted {
			return errors.Errorf("expected.result.type[%v].but.got.type[%v]", v.typ, arg.ResTyp)
		}
	}
	return nil
}

type arg struct {
	i         int
	validator SingleArgValidator
}

func Arg(i int, validator SingleArgValidator) *arg {
	return &arg{i: i, validator: validator}
}

func (v *arg) Validate(args ...*datum.IField) error {
	if err := v.validator.Validate(args[v.i]); err != nil {
		return errors.Errorf("bad.argument.at.index %v: %v", v.i, err)
	}
	return nil
}

type allArgs struct {
	validator SingleArgValidator
}

func AllArgs(validator SingleArgValidator) *allArgs {
	return &allArgs{validator: validator}
}

func (v *allArgs) Validate(args ...*datum.IField) error {
	for i := range args {
		if err := v.validator.Validate(args[i]); err != nil {
			return errors.Errorf("bad.argument.at.index %v: %v", i, err)
		}
	}
	return nil
}
