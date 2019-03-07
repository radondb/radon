// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqldb

import (
	"bytes"
	"fmt"
	"regexp"
	"strconv"
)

const (
	// SQLStateGeneral is the SQLSTATE value for "general error".
	SQLStateGeneral = "HY000"
)

// SQLError is the error structure returned from calling a db library function
type SQLError struct {
	Num     uint16
	State   string
	Message string
	Query   string
}

// NewSQLError creates new sql error.
func NewSQLError(number uint16, args ...interface{}) *SQLError {
	sqlErr := &SQLError{}
	err, ok := SQLErrors[number]
	if !ok {
		unknow := SQLErrors[ER_UNKNOWN_ERROR]
		sqlErr.Num = unknow.Num
		sqlErr.State = unknow.State
		err = unknow
	} else {
		sqlErr.Num = err.Num
		sqlErr.State = err.State
	}
	sqlErr.Message = fmt.Sprintf(err.Message, args...)
	return sqlErr
}

func NewSQLErrorf(number uint16, format string, args ...interface{}) *SQLError {
	sqlErr := &SQLError{}
	err, ok := SQLErrors[number]
	if !ok {
		unknow := SQLErrors[ER_UNKNOWN_ERROR]
		sqlErr.Num = unknow.Num
		sqlErr.State = unknow.State
	} else {
		sqlErr.Num = err.Num
		sqlErr.State = err.State
	}
	sqlErr.Message = fmt.Sprintf(format, args...)
	return sqlErr
}

// NewSQLError1 creates new sql error with state.
func NewSQLError1(number uint16, state string, format string, args ...interface{}) *SQLError {
	return &SQLError{
		Num:     number,
		State:   state,
		Message: fmt.Sprintf(format, args...),
	}
}

// Error implements the error interface
func (se *SQLError) Error() string {
	buf := &bytes.Buffer{}
	buf.WriteString(se.Message)

	// Add MySQL errno and SQLSTATE in a format that we can later parse.
	// There's no avoiding string parsing because all errors
	// are converted to strings anyway at RPC boundaries.
	// See NewSQLErrorFromError.
	fmt.Fprintf(buf, " (errno %v) (sqlstate %v)", se.Num, se.State)

	if se.Query != "" {
		fmt.Fprintf(buf, " during query: %s", se.Query)
	}
	return buf.String()
}

var errExtract = regexp.MustCompile(`.*\(errno ([0-9]*)\) \(sqlstate ([0-9a-zA-Z]{5})\).*`)

// NewSQLErrorFromError returns a *SQLError from the provided error.
// If it's not the right type, it still tries to get it from a regexp.
func NewSQLErrorFromError(err error) error {
	if err == nil {
		return nil
	}

	if serr, ok := err.(*SQLError); ok {
		return serr
	}

	msg := err.Error()
	match := errExtract.FindStringSubmatch(msg)
	if len(match) < 2 {
		// Not found, build a generic SQLError.
		// TODO(alainjobart) maybe we can also check the canonical
		// error code, and translate that into the right error.

		// FIXME(alainjobart): 1105 is unknown error. Will
		// merge with sqlconn later.
		unknow := SQLErrors[ER_UNKNOWN_ERROR]
		return &SQLError{
			Num:     unknow.Num,
			State:   unknow.State,
			Message: msg,
		}
	}

	num, err := strconv.Atoi(match[1])
	if err != nil {
		unknow := SQLErrors[ER_UNKNOWN_ERROR]
		return &SQLError{
			Num:     unknow.Num,
			State:   unknow.State,
			Message: msg,
		}
	}

	serr := &SQLError{
		Num:     uint16(num),
		State:   match[2],
		Message: msg,
	}
	return serr
}
