/*
 * Radon
 *
 * Copyright 2020 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package datum

import (
	"strings"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// ResultType is type of the expression return.
type ResultType int

const (
	// StringResult string.
	StringResult ResultType = iota
	// IntResult int.
	IntResult
	// DecimalResult decimal.
	DecimalResult
	// RealResult float64.
	RealResult
	// RowResult tuple.
	RowResult
	// TimeResult time.
	TimeResult
	// DurationResult duration.
	DurationResult
)

// IField is the property of expression's result.
type IField struct {
	// Type result type.
	Type ResultType
	// TODO: Charset of the field.
	Charset int
	// Length of the field.
	Length int
	// Scale is the fraction digits.
	Scale int
	// IsUnsigned flag, unsigned: true, signed: false.
	IsUnsigned bool
	// IsBinary flag.
	IsBinary bool
	// Constant for constanteval.
	IsConstant bool
}

// NewIField new IField.
func NewIField(field *querypb.Field) *IField {
	res := &IField{
		Charset:    int(field.Charset),
		Length:     -1,
		Scale:      int(field.Decimals),
		IsUnsigned: (field.Flags & 32) > 0,
		IsBinary:   (field.Flags & 128) > 0,
		IsConstant: false,
	}
	typ := field.Type
	switch {
	case sqltypes.IsIntegral(field.Type):
		res.Type = IntResult
	case sqltypes.IsFloat(field.Type):
		res.Type = RealResult
		if field.Decimals < NotFixedDec {
			res.Length = int(field.ColumnLength)
		}
	case field.Type == sqltypes.Decimal:
		res.Type = DecimalResult
		res.Length = int(field.ColumnLength)
	case sqltypes.IsTemporal(field.Type):
		if typ == sqltypes.Time {
			res.Type = DurationResult
		} else {
			res.Type = TimeResult
		}
	case sqltypes.IsBinary(field.Type):
		res.Type = StringResult
		res.Length = int(field.ColumnLength)
	default:
		res.Type = StringResult
		res.Length = int(field.ColumnLength / 3)
	}
	return res
}

// ToNumeric cast the resulttype to a numeric type.
func (f *IField) ToNumeric() {
	switch f.Type {
	case StringResult:
		if f.IsUnsigned {
			f.Type = IntResult
		} else {
			f.Type = RealResult
			f.Scale = NotFixedDec
		}
	case TimeResult, DurationResult:
		if f.Scale == 0 {
			f.Type = IntResult
		} else {
			f.Type = DecimalResult
		}
	}
}

// IsStringType return true for StringResult, TimeResult or DurationResult.
func IsStringType(typ ResultType) bool {
	return typ == StringResult || typ == TimeResult || typ == DurationResult
}

// IsTemporal return true for  TimeResult or DurationResult.
func IsTemporal(typ ResultType) bool {
	return typ == TimeResult || typ == DurationResult
}

// ConstantField get IField by the given constant datum.
func ConstantField(d Datum) *IField {
	res := &IField{
		Type:       StringResult,
		Length:     -1,
		Scale:      NotFixedDec,
		IsBinary:   true,
		IsConstant: true,
	}
	switch d := d.(type) {
	case *DInt:
		res.Type = IntResult
		res.Scale = 0
	case *DDecimal:
		res.Type = DecimalResult
		dec := len(strings.Split(d.ValStr(), ".")[1])
		if dec > DecimalMaxScale {
			dec = DecimalMaxScale
		}
		res.Scale = dec
	case *DString:
		if d.base == 16 {
			res.IsUnsigned = true
			res.Scale = 0
		} else {
			res.IsBinary = false
		}
	case *DNull:
		res.Type = IntResult
		res.Scale = 0
	}
	return res
}

func ConvertField(cvt *sqlparser.ConvertType) (*IField, error) {
	field := &IField{
		IsBinary: true,
	}
	typ := strings.ToLower(cvt.Type)

	if cvt.Length != nil {
		len, err := SQLValToDatum(cvt.Length)
		if err != nil {
			return nil, err
		}
		field.Length = int(len.(*DInt).value)
	}
	if cvt.Scale != nil {
		scale, err := SQLValToDatum(cvt.Scale)
		if err != nil {
			return nil, err
		}
		field.Scale = int(scale.(*DInt).value)
	}

	if cvt.Charset != "" {
		charset, ok := sqldb.CharacterSetMap[cvt.Charset]
		if !ok {
			return nil, errors.Errorf("unknown.character.set: '%s'", cvt.Charset)
		}
		field.Charset = int(charset)
	}

	switch typ {
	case "binary":
		field.Type = StringResult
	case "char":
		field.Type = StringResult
		field.IsBinary = false
	case "date":
		field.Type = TimeResult
		field.Length = 10
	case "datetime":
		field.Type = TimeResult
		field.Length = 19
		if field.Scale > 0 {
			field.Length += field.Scale + 1
		}
	case "decimal":
		field.Type = DecimalResult
		if field.Scale > 0 {
			field.Length += 2
		}
	case "signed":
		field.Type = IntResult
	case "unsigned":
		field.Type = IntResult
		field.IsUnsigned = true
	case "time":
		field.Type = DurationResult
	default:
		return nil, errors.Errorf("unsupport.convert.type: '%s'", typ)
	}
	return field, nil
}
