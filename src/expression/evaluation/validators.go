package evaluation

import (
	"expression/datum"

	"github.com/pkg/errors"
)

// Validator interface.
type Validator interface {
	Validate(args ...*datum.IField) error
}

// SingleArgValidator interface.
type SingleArgValidator interface {
	Validate(arg *datum.IField) error
}

// AllVAL requires all validators to be met.
type AllVAL struct {
	validators []Validator
}

// All new a AllVal.
func All(validators ...Validator) *AllVAL {
	return &AllVAL{validators: validators}
}

// Validate is used to verify that the condition is met.
func (v *AllVAL) Validate(args ...*datum.IField) error {
	for _, validator := range v.validators {
		err := validator.Validate(args...)
		if err != nil {
			return err
		}
	}
	return nil
}

// OneOfVAL requires one of the validators to be met.
type OneOfVAL struct {
	validators []Validator
}

// OneOf new a OneOfVAL.
func OneOf(validators ...Validator) *OneOfVAL {
	return &OneOfVAL{validators: validators}
}

// Validate is used to verify that the condition is met.
func (v *OneOfVAL) Validate(args ...*datum.IField) error {
	errs := make([]error, len(v.validators))
	for i, validator := range v.validators {
		errs[i] = validator.Validate(args...)
		if errs[i] == nil {
			return nil
		}
	}
	return errors.Errorf("none.of.the.conditions.have.been.met: %+v", errs)
}

// AtLeastNArgsVAL requires at least n args.
type AtLeastNArgsVAL struct {
	n int
}

// AtLeastNArgs new a AtLeastNArgsVAL.
func AtLeastNArgs(n int) *AtLeastNArgsVAL {
	return &AtLeastNArgsVAL{n: n}
}

// Validate is used to verify that the condition is met.
func (v *AtLeastNArgsVAL) Validate(args ...*datum.IField) error {
	if len(args) < v.n {
		return errors.Errorf("expected.at.least.%d.argument(s),but.got.%v", v.n, len(args))
	}
	return nil
}

// AtMostNArgsVAL requires at most n args.
type AtMostNArgsVAL struct {
	n int
}

// AtMostNArgs new a AtMostNArgsVAL.
func AtMostNArgs(n int) *AtMostNArgsVAL {
	return &AtMostNArgsVAL{n: n}
}

// Validate is used to verify that the condition is met.
func (v *AtMostNArgsVAL) Validate(args ...*datum.IField) error {
	if len(args) > v.n {
		return errors.Errorf("expected.at.most.%d.argument(s),but.got.%v", v.n, len(args))
	}
	return nil
}

// ExactlyNArgsVAL requires n args.
type ExactlyNArgsVAL struct {
	n int
}

// ExactlyNArgs new a ExactlyNArgsVAL.
func ExactlyNArgs(n int) *ExactlyNArgsVAL {
	return &ExactlyNArgsVAL{n: n}
}

// Validate is used to verify that the condition is met.
func (v *ExactlyNArgsVAL) Validate(args ...*datum.IField) error {
	if len(args) != v.n {
		return errors.Errorf("expected.exactly.%d.argument(s),but.got.%v", v.n, len(args))
	}
	return nil
}

// TypeOfVAL requires the arg type must be the given type.
type TypeOfVAL struct {
	wanted bool
	typ    datum.ResultType
}

// TypeOf new a TypeOfVAL.
func TypeOf(wanted bool, typ datum.ResultType) *TypeOfVAL {
	return &TypeOfVAL{wanted: wanted, typ: typ}
}

// Validate is used to verify that the condition is met.
func (v *TypeOfVAL) Validate(arg *datum.IField) error {
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

// ArgVAL requires the target arg  must meet the condition.
type ArgVAL struct {
	i         int
	validator SingleArgValidator
}

// Arg new a ArgVAL.
func Arg(i int, validator SingleArgValidator) *ArgVAL {
	return &ArgVAL{i: i, validator: validator}
}

// Validate is used to verify that the condition is met.
func (v *ArgVAL) Validate(args ...*datum.IField) error {
	if err := v.validator.Validate(args[v.i]); err != nil {
		return errors.Errorf("bad.argument.at.index %v: %v", v.i, err)
	}
	return nil
}

// AllArgsVAL requires all args must meet the condition.
type AllArgsVAL struct {
	validator SingleArgValidator
}

// AllArgs new a AllArgsVAL.
func AllArgs(validator SingleArgValidator) *AllArgsVAL {
	return &AllArgsVAL{validator: validator}
}

// Validate is used to verify that the condition is met.
func (v *AllArgsVAL) Validate(args ...*datum.IField) error {
	for i := range args {
		if err := v.validator.Validate(args[i]); err != nil {
			return errors.Errorf("bad.argument.at.index %v: %v", i, err)
		}
	}
	return nil
}
