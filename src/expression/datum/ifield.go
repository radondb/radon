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
	// ResTyp result type.
	ResTyp ResultType
	// Length of the field.
	Length int
	// Scale is the fraction digits.
	Scale int
	// Flag, unsigned: true, signed: false.
	Flag bool
	// Constant for constanteval.
	Constant bool
	// Charset of the field.
	Charset int
}

// NewIField new IField.
func NewIField(field *querypb.Field) *IField {
	res := &IField{
		Charset: sqldb.CharacterSetBinary,
		Scale:   int(field.Decimals),
		Flag:    (field.Flags & 32) > 0,
	}
	typ := field.Type
	switch {
	case sqltypes.IsIntegral(field.Type):
		res.ResTyp = IntResult
	case sqltypes.IsFloat(field.Type):
		res.ResTyp = RealResult
		if field.Decimals != NotFixedDec {
			res.Length = int(field.ColumnLength)
		}
	case field.Type == sqltypes.Decimal:
		res.ResTyp = DecimalResult
		res.Length = int(field.ColumnLength)
	case sqltypes.IsTemporal(field.Type):
		if typ == sqltypes.Time {
			res.ResTyp = DurationResult
		} else {
			res.ResTyp = TimeResult
		}
	case sqltypes.IsBinary(field.Type):
		res.ResTyp = StringResult
		res.Length = int(field.ColumnLength)
	default:
		res.ResTyp = StringResult
		res.Charset = sqldb.CharacterSetUtf8
		res.Length = int(field.ColumnLength / 3)
	}
	return res
}

// ToNumeric cast the resulttype to a numeric type.
func (f *IField) ToNumeric() {
	switch f.ResTyp {
	case StringResult:
		if f.Flag {
			f.ResTyp = IntResult
		} else {
			f.ResTyp = RealResult
			f.Scale = NotFixedDec
		}
	case TimeResult, DurationResult:
		if f.Scale == 0 {
			f.ResTyp = IntResult
		} else {
			f.ResTyp = DecimalResult
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
		ResTyp:   StringResult,
		Charset:  sqldb.CharacterSetBinary,
		Scale:    NotFixedDec,
		Flag:     false,
		Constant: true,
	}
	switch d := d.(type) {
	case *DInt:
		res.ResTyp = IntResult
		res.Scale = 0
	case *DDecimal:
		res.ResTyp = DecimalResult
		dec := len(strings.Split(d.ValStr(), ".")[1])
		if dec > DecimalMaxScale {
			dec = DecimalMaxScale
		}
		res.Scale = dec
	case *DString:
		if d.base == 16 {
			res.Flag = true
			res.Scale = 0
		} else {
			res.Charset = sqldb.CharacterSetUtf8
		}
	case *DNull:
		res.ResTyp = IntResult
		res.Scale = 0
		res.Flag = true
	}
	return res
}

func ConvertField(cvt *sqlparser.ConvertType) (*IField, error) {
	field := &IField{
		Charset: sqldb.CharacterSetBinary,
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
		field.ResTyp = StringResult
	case "char":
		field.ResTyp = StringResult
		if cvt.Charset == "" {
			field.Charset = sqldb.CharacterSetUtf8
		}
	case "date":
		field.ResTyp = TimeResult
		field.Length = 10
	case "datetime":
		field.ResTyp = TimeResult
		field.Length = 19
		if field.Scale > 0 {
			field.Length += field.Scale + 1
		}
	case "decimal":
		field.ResTyp = DecimalResult
		if field.Scale > 0 {
			field.Length += 2
		}
	case "signed":
		field.ResTyp = IntResult
	case "unsigned":
		field.ResTyp = IntResult
		field.Flag = true
	case "time":
		field.ResTyp = DurationResult
	default:
		return nil, errors.Errorf("unsupport.convert.type: '%s'", typ)
	}
	return field, nil
}
