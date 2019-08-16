/*
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 */

package sqltypes

import (
	"errors"
	"strconv"
	"strings"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
)

// timeToNumeric used to cast time type to numeric.
func timeToNumeric(v Value) (numeric, error) {
	switch v.Type() {
	case querypb.Type_TIMESTAMP, querypb.Type_DATETIME:
		var i int64
		year, err := strconv.ParseInt(string(v.val[0:4]), 10, 16)
		if err != nil {
			return numeric{}, err
		}
		month, err := strconv.ParseInt(string(v.val[5:7]), 10, 8)
		if err != nil {
			return numeric{}, err
		}
		day, err := strconv.ParseInt(string(v.val[8:10]), 10, 8)
		if err != nil {
			return numeric{}, err
		}
		hour, err := strconv.ParseInt(string(v.val[11:13]), 10, 8)
		if err != nil {
			return numeric{}, err
		}
		minute, err := strconv.ParseInt(string(v.val[14:16]), 10, 8)
		if err != nil {
			return numeric{}, err
		}
		second, err := strconv.ParseInt(string(v.val[17:19]), 10, 8)
		if err != nil {
			return numeric{}, err
		}

		i = (year*10000+month*100+day)*1000000 + (hour*10000 + minute*100 + second)
		if len(v.val) > 19 {
			var f float64
			microSecond, err := strconv.ParseUint(string(v.val[20:]), 10, 32)
			if err != nil {
				return numeric{}, err
			}

			microSec := float64(microSecond)
			n := len(v.val[20:])
			for n != 0 {
				microSec *= 0.1
				n--
			}
			f = float64(i) + microSec
			return numeric{fval: f, typ: Float64}, nil
		}
		return numeric{ival: i, typ: Int64}, nil
	case querypb.Type_DATE:
		var i int64
		year, err := strconv.ParseInt(string(v.val[0:4]), 10, 16)
		if err != nil {
			return numeric{}, err
		}
		month, err := strconv.ParseInt(string(v.val[5:7]), 10, 8)
		if err != nil {
			return numeric{}, err
		}
		day, err := strconv.ParseInt(string(v.val[8:]), 10, 8)
		if err != nil {
			return numeric{}, err
		}
		i = year*10000 + month*100 + day
		return numeric{ival: i, typ: Int64}, nil
	case querypb.Type_TIME:
		var i int64
		sub := strings.Split(string(v.val), ":")
		if len(sub) != 3 {
			return numeric{}, errors.New("incorrect.time.value,':'.is.not.found")
		}

		pre := int64(1)
		if strings.HasPrefix(sub[0], "-") {
			pre = -1
			sub[0] = sub[0][1:]
		}

		hour, err := strconv.ParseInt(string(sub[0]), 10, 32)
		if err != nil {
			return numeric{}, err
		}
		minute, err := strconv.ParseInt(string(sub[1]), 10, 8)
		if err != nil {
			return numeric{}, err
		}

		if strings.Contains(sub[2], ".") {
			second, err := strconv.ParseFloat(sub[2], 64)
			if err != nil {
				return numeric{}, err
			}
			f := float64(pre) * (float64(hour)*10000 + float64(minute)*100 + second)
			return numeric{fval: f, typ: Float64}, nil
		}

		second, err := strconv.ParseInt(sub[2], 10, 8)
		if err != nil {
			return numeric{}, err
		}
		i = pre * (hour*10000 + minute*100 + second)
		return numeric{ival: i, typ: Int64}, nil
	case querypb.Type_YEAR:
		val, err := strconv.ParseUint(v.ToString(), 10, 16)
		if err != nil {
			return numeric{}, err
		}
		return numeric{uval: val, typ: Uint64}, nil
	}
	return numeric{}, errors.New("unsupport: unknown.type")
}
