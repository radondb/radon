package datum

import (
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
)

// Cast used to cast the Datum by IField.
// If the function is cast(... as ...), isCastFunc will be true.
func Cast(d Datum, field *IField, isCastFunc bool) (Datum, error) {
	if CheckNull(d) {
		return d, nil
	}
	switch field.Type {
	case IntResult:
		return CastToDInt(d, field.IsUnsigned, isCastFunc), nil
	case StringResult:
		val := d.ValStr()
		return NewDString(CastStrWithField(val, field), 10, field.IsBinary), nil
	case DurationResult:
		return CastToDuration(d, field.Scale)
	case TimeResult:
		return CastToDatetime(d, field.Scale)
	case DecimalResult:
		val := d.ValDecimal()
		return NewDDecimal(CastDecWithField(val, field)), nil
	case RealResult:
		val := d.ValReal()
		return NewDFloat(CastFloat64WithField(val, field)), nil
	}
	return nil, errors.New("unsupport.type")
}

// CastToDInt cast the Datum to DInt.
func CastToDInt(d Datum, flag, isCastFunc bool) *DInt {
	var val int64
	switch d := d.(type) {
	case *DInt:
		if flag && (!d.flag && d.value < 0) {
			val = 0
		} else {
			val = d.value
		}
	case *DDecimal:
		val = CastDecimalToInt(d.value, flag)
	case *DFloat:
		if flag {
			val = int64(Float64ToUint64(float64(*d)))
		} else {
			val = Float64ToInt64(float64(*d))
		}
	case *DString:
		val = CastStrToInt(d.ValStr(), flag, isCastFunc)
	case *Duration:
		if flag && d.duration < 0 {
			val = 0
		} else {
			val, _ = d.toNumeric().ValInt()
		}
	case *DTime:
		val, _ = d.toNumeric().ValInt()
	}
	return NewDInt(val, flag)
}

// CastDecimalToInt cast the decimal to int64.
func CastDecimalToInt(d decimal.Decimal, flag bool) int64 {
	var val int64
	str := d.Round(0).String()
	if flag {
		uval, _ := strconv.ParseUint(str, 10, 64)
		val = int64(uval)
	} else {
		val, _ = strconv.ParseInt(str, 10, 64)
	}
	return val
}

// CastStrToInt cast the string to int64.
func CastStrToInt(s string, flag, isCastFunc bool) int64 {
	s = strings.TrimSpace(s)
	isNeg := false
	if len(s) > 1 && s[0] == '-' {
		isNeg = true
	}

	var res int64
	if !isNeg {
		val, _ := StrToUint(s, isCastFunc)
		res = int64(val)
	} else if !flag {
		res, _ = StrToInt(s, isCastFunc)
	}
	return res
}

// CastDecWithField cast the decimal by the IField.
// Such as: cast(3.222 as decimal(3,2))->3.22.
func CastDecWithField(dec decimal.Decimal, field *IField) decimal.Decimal {
	dec = dec.Round(int32(field.Scale))
	isNeg := dec.IsNegative()
	prec := len(dec.String())
	if !isNeg {
		prec++
	}
	if field.Length > 0 && prec > field.Length {
		return NewMaxOrMinDec(isNeg, field.Length, field.Scale)
	}
	return dec
}

// CastFloat64WithField cast the float64 by the IField.
// Such: cast the 2.5666 to float(5,3) -> 2.5667.
func CastFloat64WithField(f float64, field *IField) float64 {
	len, dec := field.Length, field.Scale
	if field.Length <= 0 {
		return f
	}

	if !math.IsInf(f, 0) {
		shift := math.Pow10(dec)
		tmp := f * shift
		if !math.IsInf(tmp, 0) {
			f = roundFloat64(tmp) / shift
		}
	}

	if math.IsNaN(f) {
		return 0
	}

	max := math.Pow10(len - dec)
	max -= math.Pow10(-dec)

	if f > max {
		f = max
	} else if f < -max {
		f = -max
	}
	return f
}

// CastStrWithField use to cast the string by the IField.
// Such as: cast("abcd" as char(3)) -> "abc".
func CastStrWithField(s string, field *IField) string {
	if field.Length <= 0 {
		return s
	}

	isTrunc := false
	truncLen := field.Length
	if !field.IsBinary {
		if utf8.RuneCountInString(s) > field.Length {
			isTrunc = true
			runeCnt := 0
			for i := range s {
				if runeCnt == field.Length {
					truncLen = i
					break
				}
				runeCnt++
			}
		}
	} else {
		if len(s) > field.Length {
			isTrunc = true
		}
	}
	if isTrunc {
		s = s[:truncLen]
	}
	return s
}
