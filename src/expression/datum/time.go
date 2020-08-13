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
	"unicode"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// TimeStatus to return status from StrToDatetime.
type TimeStatus struct {
	// Truncated means the result is truncated.
	Truncated bool
	// Round record the round up info for StrToDuration.
	// e.g.
	// str: '20200721235959.9999996' fsp:6
	// datetime: '2020072200000000.000000'
	// duration: "24:00:00.000000"
	Round bool
}

const (
	// TimeSeparator used to separator the time value.
	TimeSeparator = ':'
	// PartYear is the line of year. 0-69: 2000-2069, 70-99: 1970-1999.
	PartYear = 70
	// MinYear is the minimum for mysql year type.
	MinYear = 1901
	// MaxYear is the maximum for mysql year type.
	MaxYear = 2155
	// TimeMaxHour is the max hour for mysql time type.
	TimeMaxHour = 838
	// TimeMaxMinute is the max minute for mysql time type.
	TimeMaxMinute = 59
	// TimeMaxSecond is the max second for mysql time type.
	TimeMaxSecond = 59
	// TimeMaxValue is the maximum value for mysql time type.
	TimeMaxValue = TimeMaxHour*10000 + TimeMaxMinute*100 + TimeMaxSecond
	// TimeMaxValueSeconds is the maximum second value for mysql time type.
	TimeMaxValueSeconds = TimeMaxHour*3600 + TimeMaxMinute*60 + TimeMaxSecond
	// MaxTime is the maximum for mysql time type.
	MaxTime = time.Duration(TimeMaxValueSeconds) * time.Second
	// MinTime is the minimum for mysql time type.
	MinTime = -MaxTime
)

var (
	// DaysInMonth represents the number of days in a month.
	DaysInMonth = []int{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0}
)

// CastToDatetime cast the datum to DTime.
func CastToDatetime(d Datum, fsp int) (*DTime, error) {
	switch d := d.(type) {
	case *DTime:
		return d, nil
	case *DInt:
		v, _ := d.ValInt()
		return NumToDatetime(v)
	case *Duration:
		return d.toTime(), nil
	default:
		v := d.ValStr()
		return StrToDatetime(v, fsp, &TimeStatus{})
	}
}

// CastToDuration cast the datum to Duration.
func CastToDuration(d Datum, fsp int) (*Duration, error) {
	switch d := d.(type) {
	case *DTime:
		return d.toDuration(), nil
	case *DInt:
		v, _ := d.ValInt()
		return NumToDuration(v)
	case *Duration:
		return d, nil
	default:
		v := d.ValStr()
		return StrToDuration(v, fsp)
	}
}

// NumToDuration used to convert a number to a datum.Duration.
func NumToDuration(num int64) (*Duration, error) {
	if num > TimeMaxValue {
		// For huge numbers try full DATETIME, like strToTime does.
		if num >= 10000000000 /* '0001-00-00 00:00:00' */ {
			if t, err := NumToDatetime(num); err == nil {
				return t.toDuration(), nil
			}
		}
		return &Duration{duration: time.Duration(TimeMaxValueSeconds * time.Second)}, errors.Errorf("time.value'%d'.is.out.of.range", num)
	}
	if num < -TimeMaxValue {
		return &Duration{duration: time.Duration(-TimeMaxValueSeconds * time.Second)}, errors.Errorf("time.value'%d'.is.out.of.range", num)
	}

	neg := num < 0
	if neg {
		num = -num
	}

	hour := num / 10000
	minute := (num / 100) % 100
	second := num % 100
	// Check minute and second.
	if second > TimeMaxSecond || minute > TimeMaxMinute {
		return ZeroDuration, errors.Errorf("incorrect.time.value'%d'", num)
	}

	dur := time.Duration(hour*3600+minute*60+second) * time.Second
	if neg {
		dur = -dur
	}
	return &Duration{duration: dur}, nil
}

// NumToDatetime used to convert a number to a datum.DTime.
func NumToDatetime(num int64) (*DTime, error) {
	d := &DTime{
		typ: sqltypes.Date,
		fsp: 0,
	}
	// Check zero.
	if num == 0 {
		return ZeroDateTime, nil
	}

	// Check MMDD.
	if num < 101 {
		return d, errors.Errorf("invalid.time.format: '%v'", num)
	}

	// Out of range.
	if num > 99999999999999 {
		return ZeroDateTime, errors.Errorf("time.value'%v'.is.out.of.range", num)
	}

	// Check datetime type.
	if num >= 10000101000000 {
		d.typ = query.Type_DATETIME
		goto ok
	}

	// Adjust YYMMDD, year: 2000-2069.
	if num <= (PartYear-1)*10000+1231 {
		num = (num + 20000000) * 1000000
		goto ok
	}

	// check YYMMDD,700101.
	if num < PartYear*10000+101 {
		return d, errors.Errorf("invalid.time.format: '%v'", num)
	}

	// Adjust YYMMDD, year: 1970-1999.
	if num <= 991231 {
		num = (num + 19000000) * 1000000
		goto ok
	}

	// Check YYYYMMDD.
	if num < 10000101 {
		return d, errors.Errorf("invalid.time.format: '%v'", num)
	}

	//  DATE type.
	if num <= 99991231 {
		num = num * 1000000
		goto ok
	}

	// Check MMDDHHMMSS.
	if num < 101000000 {
		return ZeroDateTime, errors.Errorf("invalid.time.format: '%v'", num)
	}

	// Set DATETIME type.
	d.typ = query.Type_DATETIME

	// Adjust YYMMDDHHMMSS, year: 2000-2069.
	if num <= (PartYear-1)*10000000000+1231235959 {
		num = num + 20000000000000
		goto ok
	}

	// Check YYYYMMDDHHMMSS.
	if num < PartYear*10000000000+101000000 {
		return d, errors.Errorf("invalid.time.format: '%v'", num)
	}

	// Adjust YYMMDDHHMMSS, year: 1970-1999.
	if num <= 991231235959 {
		num = num + 19000000000000
	}
ok:
	part1 := num / 1000000
	part2 := num - part1*1000000

	d.year = uint16(part1 / 10000)
	part1 %= 10000
	d.month = uint8(part1 / 100)
	d.day = uint8(part1 % 100)

	d.hour = int16(part2 / 10000)
	part2 %= 10000
	d.minute = uint8(part2 / 100)
	d.second = uint8(part2 % 100)
	return d, nil
}

// StrToDatetime used to convert a timestamp string to a datum.DTime.
// see: https://github.com/mysql/mysql-server/blob/5.7/sql-common/my_time.c#L282
func StrToDatetime(str string, fsp int, status *TimeStatus) (*DTime, error) {
	str = strings.TrimSpace(str)
	var (
		date             = make([]int, 7)
		fieldLen         = 0
		lastFieldPos     = 0
		isInternalFormat = false
		pos              = 0
		end              = len(str)
	)

	if end == 0 || !unicode.IsNumber(rune(str[0])) {
		status.Truncated = true
		return ZeroDateTime, errors.Errorf("truncated.incorrect.datetime.value: '%-.128s'", str)
	}

	/*
		Calculate number of digits in first part. If length= 8 or >= 14 then
		year is of format YYYY. (YYYY-MM-DD,  YYYYMMDD, YYYYYMMDDHHMMSS)
	*/
	for pos != end && (unicode.IsNumber(rune(str[pos])) || str[pos] == 'T') {
		pos++
	}

	digits := pos
	dateLen := make([]int, 7)
	// Found date in internal format (only numbers like YYYYMMDD).
	if pos == end || str[pos] == '.' {
		// Length of year field.
		fieldLen = common.TernaryOpt(digits == 4 || digits == 8 || digits >= 14, 4, 2).(int)
		isInternalFormat = true
	} else {
		fieldLen = 4
	}

	var (
		notZeroDate = 0
		idx         = 0
		i           = 0
	)

	for ; i < 7 && idx != end && unicode.IsNumber(rune(str[idx])); i++ {
		start := idx
		/*
		 * Internal format means no delimiters; every field has a fixed
		 * width. Otherwise, we scan until we find a delimiter and discard
		 * leading zeroes -- except for the microsecond part, where leading
		 * zeroes are significant, and where we never process more than six
		 * digits.
		 */
		scanUntilDelim := !isInternalFormat && (i != 6)

		idx++
		fieldLen--
		for idx != end && unicode.IsNumber(rune(str[idx])) && (scanUntilDelim || (fieldLen != 0)) {
			idx++
			fieldLen--
		}

		tmpVal, _ := strconv.Atoi(str[start:idx])
		dateLen[i] = idx - start
		//Impossible date part.
		if tmpVal > 999999 {
			status.Truncated = true
			return ZeroDateTime, errors.Errorf("truncated.incorrect.datetime.value: '%-.128s'", str)
		}
		date[i] = tmpVal
		notZeroDate |= tmpVal

		// Length of next field.
		fieldLen = 2
		lastFieldPos = idx
		if lastFieldPos == end {
			// Register last found part.
			i++
			break
		}
		// Allow a 'T' after day to allow CCYYMMDDT type of fields
		if i == 2 && str[idx] == 'T' {
			idx++
			continue
		}
		//seconds.
		if i == 5 {
			// Followed by part seconds.
			if str[idx] == '.' {
				idx++
				lastFieldPos = idx
				fieldLen = 6
			}
			continue
		}

		for idx != end && (unicode.IsPunct(rune(str[idx])) || unicode.IsSpace(rune(str[idx]))) {
			if unicode.IsSpace(rune(str[idx])) {
				if i != 2 {
					status.Truncated = true
					return ZeroDateTime, errors.Errorf("truncated.incorrect.datetime.value: '%-.128s'", str)
				}
			}
			idx++
		}
		lastFieldPos = idx
	}

	idx = lastFieldPos
	numOfFields := i
	for i < 7 {
		dateLen[i] = 0
		date[i] = 0
		i++
	}

	if numOfFields < 3 || checkDateTimeRange(date[0], date[1], date[2], date[3], date[4], date[5], date[6]) {
		if notZeroDate == 0 && idx == end {
			return ZeroDateTime, nil
		}
		return ZeroDateTime, errors.Errorf("incorrect.datetime.value: '%-.128s'", str)
	}

	if dateLen[0] == 2 && notZeroDate != 0 {
		date[0] += common.TernaryOpt(date[0] < PartYear, 2000, 1900).(int)
	}

	if idx != end && unicode.IsNumber(rune(str[idx])) {
		if fsp == 6 && str[idx] > '4' {
			if date[6] == 999999 {
				status.Round = true
			} else {
				date[6]++
			}
		}
		/* Scan all digits left after microseconds */
		for idx != end && unicode.IsNumber(rune(str[idx])) {
			idx++
		}
	}

	microsec := float64(date[6])
	if fsp < dateLen[6] {
		microsec = (microsec/math.Pow10(dateLen[6]-fsp-1) + 5) / 10
		if microsec >= math.Pow10(fsp) {
			status.Round = true
		}
		dateLen[6] = fsp
	}

	if status.Round {
		tmp := time.Date(date[0], time.Month(date[1]), date[2], date[3], date[4], date[5], 0, time.Local)
		return castToDTime(tmp.Add(time.Second), fsp), nil
	}
	date[6] = int(microsec * math.Pow10(6-dateLen[6]))

	typ := common.TernaryOpt(numOfFields <= 3, sqltypes.Date, sqltypes.Datetime).(querypb.Type)
	if idx != end {
		status.Truncated = true
	}

	return NewDTime(typ, fsp, date[0], date[1], date[2], date[3], date[4], date[5], date[6]), nil
}

// checkDateTimeRange used to check heck whether the  time values are legal.
func checkDateTimeRange(year, month, day, hour, minute, second, microsecond int) bool {
	if year > 9999 || month > 12 || day > 31 ||
		hour > 23 || minute > 59 || second > 59 || microsecond > 999999 {
		return true
	}
	if month != 0 && day > DaysInMonth[month-1] && (month != 2 || calcDaysInYear(year) != 366 || day != 29) {
		return true
	}
	return false
}

// StrToDuration used to convert a string to a datum.Duration.
func StrToDuration(str string, fsp int) (*Duration, error) {
	str = strings.TrimSpace(str)
	var (
		date = make([]int, 5)
		neg  = false
		pos  = 0
		end  = len(str)
	)

	if str[0] == '-' {
		neg = true
		pos++
	}

	if pos == end {
		return ZeroDuration, nil
	}

	idx := 0
	if n := strings.IndexByte(str[pos:], ' '); n > 0 {
		day, err := strconv.Atoi(str[pos : pos+n])
		if err != nil {
			/* Check if this is a full TIMESTAMP */
			status := &TimeStatus{}
			t, err := StrToDatetime(str[pos:], fsp, status)
			if err == nil && !status.Truncated {
				d := t.toDuration()
				if d.duration == 0 && status.Round {
					d.duration = time.Duration(24 * 3600 * time.Second)
				}
				return d, nil
			}
			return ZeroDuration, err
		}
		// Try to get this as a DAYS_TO_SECOND string.
		date[0] = day
		idx = 1
		pos += n
		// Skip all space after 'day'.
		for pos != end && unicode.IsSpace(rune(str[pos])) {
			pos++
		}
	}

	start := pos
	for pos != end && unicode.IsNumber(rune(str[pos])) {
		pos++
	}
	val, _ := strconv.Atoi(str[start:pos])

	skip := false
	if end-pos > 1 && str[pos] == TimeSeparator && unicode.IsNumber(rune(str[pos+1])) {
		date[1] = val
		idx = 2
		// skip ':'.
		pos++
	} else {
		// String given as one number; assume HHMMSS format.
		date[0] = 0
		date[1] = (val / 10000)
		date[2] = (val / 100 % 100)
		date[3] = (val % 100)
		idx = 4
		skip = true
	}

	if !skip {
		/* Read hours, minutes and seconds */
		for {
			start := pos
			for pos != end && unicode.IsNumber(rune(str[pos])) {
				pos++
			}
			val, _ := strconv.Atoi(str[start:pos])
			date[idx] = val
			if idx == 3 || end-pos < 2 || str[pos] != TimeSeparator || !unicode.IsNumber(rune(str[pos+1])) {
				break
			}
			idx++
			// skip ':'.
			pos++
		}
	}

	if end-pos >= 2 && str[pos] == '.' && unicode.IsNumber(rune(str[pos+1])) {
		pos++
		start := pos
		fieldLen := 5
		for pos = pos + 1; pos != end && unicode.IsNumber(rune(str[pos])) && fieldLen > 0; pos++ {
			fieldLen--
		}
		val, _ := strconv.Atoi(str[start:pos])

		fracLen := 6 - fieldLen
		if pos != end && unicode.IsNumber(rune(str[pos])) {
			if fsp == 6 && str[pos] > '4' {
				val++
			}
			for pos = pos + 1; pos != end && unicode.IsNumber(rune(str[pos])); pos++ {
				//block.
			}
		}
		microsec := float64(val)
		if fsp < fracLen {
			microsec = (microsec/math.Pow10(fracLen-fsp-1) + 5) / 10
			fracLen = fsp
		}
		date[4] = int(microsec * math.Pow10(6-fracLen))
	} else if (end-pos) == 1 && str[pos] == '.' {
		pos++
		date[4] = 0
	} else {
		date[4] = 0
	}

	if pos != end {
		return ZeroDuration, errors.Errorf("incorrect.time.value: '%-.128s'", str)
	}

	// Check minute and second.
	if date[2] > TimeMaxMinute || date[3] > TimeMaxSecond {
		return ZeroDuration, errors.Errorf("time.value'%-.128s'.is.out.of.range", str)
	}

	d := time.Duration(date[0]*24*3600+date[1]*3600+date[2]*60+date[3])*time.Second + time.Duration(date[4])*time.Microsecond
	if neg {
		d = -d
	}

	if d > MaxTime {
		return &Duration{duration: MaxTime, fsp: fsp}, errors.Errorf("time.value'%-.128s'.is.out.of.range", str)
	} else if d < MinTime {
		return &Duration{duration: MinTime, fsp: fsp}, errors.Errorf("time.value'%-.128s'.is.out.of.range", str)
	}
	return &Duration{duration: d, fsp: fsp}, nil
}

// StrToYear used to convert a string to year.
func StrToYear(str string) (uint16, error) {
	v, err := strconv.ParseUint(str, 10, 16)
	if err != nil {
		return 0, errors.Errorf("invalid.year.value:'%s'", str)
	}

	if len(str) == 4 {
		//block.
	} else if len(str) == 2 || len(str) == 1 {
		v += uint64(common.TernaryOpt(v < PartYear, 2000, 1900).(int))
	} else {
		return 0, errors.Errorf("invalid.year.value:'%s'", str)
	}

	if v < MinYear || v > MaxYear {
		return 0, errors.Errorf("invalid.year.value:'%s'", str)
	}

	return uint16(v), nil
}

// calcDaysInYear calculate the number of days in a year.
func calcDaysInYear(year int) int {
	if year%4 == 0 && (year%100 != 0 || (year%400 == 0 && year != 0)) {
		return 366
	}
	return 365
}

// datetimeToUint64 converts time value to integer in YYYYMMDDHHMMSS format.
func datetimeToInt64(t *DTime) int64 {
	return dateToInt64(t)*1e6 + timeToInt64(t)
}

// dateToInt64 converts time value to integer in YYYYMMDD format.
func dateToInt64(t *DTime) int64 {
	return int64(t.year)*10000 +
		int64(t.month)*100 +
		int64(t.day)
}

// timeToInt64 converts time value to integer in HHMMSS format.
func timeToInt64(t *DTime) int64 {
	return int64(t.hour)*10000 +
		int64(t.minute)*100 +
		int64(t.second)
}

// castToDTime cast time.Time to *DTime.
func castToDTime(t time.Time, fsp int) *DTime {
	// Plus 500 nanosecond for rounding of the millisecond part.
	t = t.Add(500 * time.Nanosecond)
	year, month, day := t.Date()
	hour, minute, second := t.Clock()
	microsecond := t.Nanosecond() / 1000
	return NewDTime(sqltypes.Datetime, fsp, year, int(month), day, hour, minute, second, microsecond)
}

func getFsp(v Datum) int {
	switch v := v.(type) {
	case *DTime:
		return v.fsp
	case *Duration:
		return v.fsp
	}
	return -1
}
