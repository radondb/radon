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
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

const (
	// NotFixedDec means that the precision is not a fixed number.
	NotFixedDec = 31
	// DecimalMaxScale represents the maximum value of the scale property.
	DecimalMaxScale = 30
)

// Type .
type Type int

const (
	// TypeNull null.
	TypeNull Type = iota
	// TypeInt DInt.
	TypeInt
	// TypeFloat DFloat.
	TypeFloat
	// TypeDecimal DDecimal.
	TypeDecimal
	// TypeString DString.
	TypeString
	// TypeTuple DTuple.
	TypeTuple
	// TypeTime DTime.
	TypeTime
	// TypeDuration Duration.
	TypeDuration
)

// Datum interface.
type Datum interface {
	Type() Type
	ValInt() (int64, bool)
	ValReal() float64
	ValDecimal() decimal.Decimal
	ValStr() string
}

// CheckNull check for null in args.
func CheckNull(args ...Datum) bool {
	for _, arg := range args {
		if arg.Type() == TypeNull {
			return true
		}
	}
	return false
}

// ValToDatum cast Value to Datum.
func ValToDatum(v sqltypes.Value) (Datum, error) {
	str := v.String()
	switch {
	case v.IsIntegral():
		flag := false
		ival, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return nil, err
		}
		if v.IsUnsigned() {
			flag = true
		}
		return NewDInt(ival, flag), nil
	case v.IsFloat():
		fval, err := strconv.ParseFloat(str, 64)
		if err != nil {
			return nil, err
		}
		return NewDFloat(fval), nil
	case v.Type() == sqltypes.Decimal:
		dval, err := decimal.NewFromString(str)
		if err != nil {
			return nil, err
		}
		return NewDDecimal(dval), nil
	case v.IsTemporal():
		return timeToDatum(v.Raw(), v.Type())
	case v.IsNull():
		return NewDNull(true), nil
	}
	return NewDString(str), nil
}

// timeToNumeric used to cast time type to numeric.
func timeToDatum(val []byte, typ querypb.Type) (Datum, error) {
	switch typ {
	case sqltypes.Timestamp, sqltypes.Datetime:
		year, err := strconv.Atoi(string(val[0:4]))
		if err != nil {
			return nil, err
		}
		month, err := strconv.Atoi(string(val[5:7]))
		if err != nil {
			return nil, err
		}
		day, err := strconv.Atoi(string(val[8:10]))
		if err != nil {
			return nil, err
		}
		hour, err := strconv.Atoi(string(val[11:13]))
		if err != nil {
			return nil, err
		}
		minute, err := strconv.Atoi(string(val[14:16]))
		if err != nil {
			return nil, err
		}
		second, err := strconv.Atoi(string(val[17:19]))
		if err != nil {
			return nil, err
		}

		fsp := 0
		microsecond := 0
		if len(val) > 19 {
			fsp = len(val[20:])
			res, err := strconv.Atoi(string(val[20:]))
			if err != nil {
				return nil, err
			}
			microsecond = int(float64(res) * math.Pow10(6-fsp))
		}
		return NewDTime(typ, fsp, year, month, day, hour, minute, second, microsecond), nil
	case sqltypes.Date:
		year, err := strconv.Atoi(string(val[0:4]))
		if err != nil {
			return nil, err
		}
		month, err := strconv.Atoi(string(val[5:7]))
		if err != nil {
			return nil, err
		}
		day, err := strconv.Atoi(string(val[8:]))
		if err != nil {
			return nil, err
		}
		return NewDTime(typ, 0, year, month, day, 0, 0, 0, 0), nil
	case sqltypes.Time:
		sub := strings.Split(string(val), ":")
		if len(sub) != 3 {
			return nil, errors.Errorf("incorrect.time.value.'%s'", string(val))
		}

		neg := false
		if strings.HasPrefix(sub[0], "-") {
			neg = true
			sub[0] = sub[0][1:]
		}

		hour, err := strconv.Atoi(string(sub[0]))
		if err != nil {
			return nil, err
		}
		minute, err := strconv.Atoi(string(sub[1]))
		if err != nil {
			return nil, err
		}

		secs := strings.Split(sub[2], ".")
		second, err := strconv.Atoi(secs[0])
		if err != nil {
			return nil, err
		}

		fsp := 0
		fracPart := 0
		if len(secs) > 1 {
			fsp = len(secs[1])
			res, err := strconv.Atoi(string(secs[1]))
			if err != nil {
				return nil, err
			}
			fracPart = int(float64(res) * math.Pow10(6-fsp))
		}
		d := time.Duration(hour*3600+minute*60+second)*time.Second + time.Duration(fracPart)*time.Microsecond
		if neg {
			d = -d
		}
		return &Duration{d, fsp}, nil
	}
	return nil, errors.Errorf("can.not.cast.'%+v'.to.time.type", typ)
}
