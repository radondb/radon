/*
 * Radon
 *
 * Copyright 2020 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package datum

import (
	"math"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
)

// Add operator.
func Add(v1, v2 Datum, field *IField) (Datum, error) {
	if CheckNull(v1, v2) {
		return NewDNull(true), nil
	}
	switch field.ResTyp {
	case IntResult:
		val1, flag1 := v1.ValInt()
		val2, flag2 := v2.ValInt()
		switch {
		case flag1 && flag2:
			if uint64(val1) > math.MaxUint64-uint64(val2) {
				return nil, errors.Errorf("BIGINT.UNSIGNED.value.is.out.of.range.in: '%v' + '%v'", uint64(val1), uint64(val2))
			}
		case flag1 && !flag2:
			if val2 < 0 && uint64(val1) < uint64(-val2) {
				return nil, errors.Errorf("BIGINT.UNSIGNED.value.is.out.of.range.in: '%v' + '%v'", uint64(val1), val2)
			}
			if val2 > 0 && uint64(val1) > math.MaxUint64-uint64(val2) {
				return nil, errors.Errorf("BIGINT.UNSIGNED.value.is.out.of.range.in: '%v' + '%v'", uint64(val1), val2)
			}
		case !flag1 && flag2:
			if val1 < 0 && uint64(-val1) > uint64(val2) {
				return nil, errors.Errorf("BIGINT.UNSIGNED.value.is.out.of.range.in: '%v' + '%v'", val1, uint64(val2))
			}
			if val1 > 0 && uint64(val2) > math.MaxUint64-uint64(val1) {
				return nil, errors.Errorf("BIGINT.UNSIGNED.value.is.out.of.range.in: '%v' + '%v'", val1, uint64(val2))
			}
		case !flag1 && !flag2:
			if (val1 > 0 && val2 > math.MaxInt64-val1) || (val1 < 0 && val2 < math.MinInt64-val1) {
				return nil, errors.Errorf("BIGINT.value.is.out.of.range.in: '%v' + '%v'", val1, val2)
			}
		}
		return NewDInt(val1+val2, field.Flag), nil
	case DecimalResult:
		val1, val2 := v1.ValDecimal(), v2.ValDecimal()
		res := val1.Add(val2)
		if common.IsDecimalInf(res) {
			return nil, errors.Errorf("DOUBLE.value.is.out.of.range.in: '%v' + '%v'", val1, val2)
		}
		return NewDDecimal(res), nil
	default:
		val1, val2 := v1.ValReal(), v2.ValReal()
		res := val1 + val2
		if math.IsInf(res, 0) {
			return nil, errors.Errorf("DOUBLE.value.is.out.of.range.in: '%v' + '%v'", val1, val2)
		}
		return NewDFloat(res), nil
	}
}

// Sub operator.
func Sub(v1, v2 Datum, field *IField) (Datum, error) {
	if CheckNull(v1, v2) {
		return NewDNull(true), nil
	}
	switch field.ResTyp {
	case IntResult:
		val1, flag1 := v1.ValInt()
		val2, flag2 := v2.ValInt()
		switch {
		case flag1 && flag2:
			if uint64(val1) < uint64(val2) {
				return nil, errors.Errorf("BIGINT.UNSIGNED.value.is.out.of.range.in: '%v' - '%v'", uint64(val1), uint64(val2))
			}
		case flag1 && !flag2:
			if val2 >= 0 && uint64(val1) < uint64(val2) {
				return nil, errors.Errorf("BIGINT.UNSIGNED.value.is.out.of.range.in: '%v' - '%v'", uint64(val1), val2)
			}
			if val2 < 0 && uint64(val1) > math.MaxUint64-uint64(-val2) {
				return nil, errors.Errorf("BIGINT.UNSIGNED.value.is.out.of.range.in: '%v' - '%v'", uint64(val1), val2)
			}
		case !flag1 && flag2:
			if uint64(val1-math.MinInt64) < uint64(val2) {
				return nil, errors.Errorf("BIGINT.UNSIGNED.value.is.out.of.range.in: '%v' - '%v'", val1, uint64(val2))
			}
		case !flag1 && !flag2:
			if val1 > 0 && (-val2 > math.MaxInt64-val1 || val2 == math.MinInt64) || val1 < 0 && -val2 < math.MinInt64-val1 {
				return nil, errors.Errorf("BIGINT.value.is.out.of.range.in: '%v' - '%v'", val1, val2)
			}
		}
		return NewDInt(val1-val2, field.Flag), nil
	case DecimalResult:
		val1, val2 := v1.ValDecimal(), v2.ValDecimal()
		res := val1.Sub(val2)
		if common.IsDecimalInf(res) {
			return nil, errors.Errorf("DOUBLE.value.is.out.of.range.in: '%v' - '%v'", val1, val2)
		}
		return NewDDecimal(res), nil
	default:
		val1, val2 := v1.ValReal(), v2.ValReal()
		res := val1 - val2
		if math.IsInf(res, 0) {
			return nil, errors.Errorf("DOUBLE.value.is.out.of.range.in: '%v' - '%v'", val1, val2)
		}
		return NewDFloat(res), nil
	}
}

// Mul (multiply) operator.
func Mul(v1, v2 Datum, field *IField) (Datum, error) {
	if CheckNull(v1, v2) {
		return NewDNull(true), nil
	}
	switch field.ResTyp {
	case IntResult:
		val1, _ := v1.ValInt()
		val2, _ := v2.ValInt()
		if field.Flag {
			val1 := uint64(val1)
			val2 := uint64(val2)
			res := val1 * val2
			if val1 != 0 && res/val1 != val2 {
				return nil, errors.Errorf("BIGINT.UNSIGNED.value.is.out.of.range.in: '%v' * '%v'", val1, val2)
			}
			return NewDInt(int64(res), field.Flag), nil
		}
		res := val1 * val2
		if val1 != 0 && res/val1 != val2 {
			return nil, errors.Errorf("BIGINT.value.is.out.of.range.in: '%v' * '%v'", val1, val2)
		}
		return NewDInt(res, field.Flag), nil
	case DecimalResult:
		val1, val2 := v1.ValDecimal(), v2.ValDecimal()
		res := val1.Mul(val2)
		if common.IsDecimalInf(res) {
			return nil, errors.Errorf("DOUBLE.value.is.out.of.range.in: '%v' * '%v'", val1, val2)
		}
		return NewDDecimal(res), nil
	default:
		val1, val2 := v1.ValReal(), v2.ValReal()
		res := val1 * val2
		if math.IsInf(res, 0) {
			return nil, errors.Errorf("DOUBLE.value.is.out.of.range.in: '%v' * '%v'", val1, val2)
		}
		return NewDFloat(res), nil
	}
}

// Div (division) operator.
func Div(v1, v2 Datum, field *IField) (Datum, error) {
	if CheckNull(v1, v2) {
		return NewDNull(true), nil
	}
	switch field.ResTyp {
	case DecimalResult:
		val1, val2 := v1.ValDecimal(), v2.ValDecimal()
		if val2.IsZero() {
			return NewDNull(true), nil
		}
		res := val1.DivRound(val2, int32(field.Decimal))
		if common.IsDecimalInf(res) {
			return nil, errors.Errorf("DOUBLE.value.is.out.of.range.in: '%v' / '%v'", val1, val2)
		}
		return NewDDecimal(res), nil
	default:
		val1, val2 := v1.ValReal(), v2.ValReal()
		if val2 == 0 {
			return NewDNull(true), nil
		}
		res := val1 / val2
		if math.IsInf(res, 0) {
			return nil, errors.Errorf("DOUBLE.value.is.out.of.range.in: '%v' / '%v'", val1, val2)
		}
		return NewDFloat(res), nil
	}
}

// IntDiv (int div) operator.
func IntDiv(v1, v2 Datum, field *IField) (Datum, error) {
	if CheckNull(v1, v2) {
		return NewDNull(true), nil
	}

	val1, val2 := v1.ValReal(), v2.ValReal()
	if val2 == 0 {
		return NewDNull(true), nil
	}

	res := val1 / val2
	if field.Flag {
		if res < 0 || res > math.MaxUint64 {
			return nil, errors.Errorf("BIGINT.UNSIGNED.value.is.out.of.range.in: '%v' div '%v'", val1, val2)
		}
	} else {
		if res > math.MaxInt64 || res < math.MinInt64 {
			return nil, errors.Errorf("BIGINT.value.is.out.of.range.in: '%v' div '%v'", val1, val2)
		}
	}
	return NewDInt(int64(math.Trunc(res)), field.Flag), nil
}
