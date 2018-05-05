/*
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package sqldb

import (
	"testing"

	"errors"
	"github.com/stretchr/testify/assert"
)

func TestSqlError(t *testing.T) {
	{
		sqlerr := NewSQLError(1, "i.am.error.man")
		assert.Equal(t, "i.am.error.man (errno 1105) (sqlstate HY000)", sqlerr.Error())
	}

	{
		sqlerr := NewSQLError(1, "i.am.error.man%s", "xx")
		assert.Equal(t, "i.am.error.manxx (errno 1105) (sqlstate HY000)", sqlerr.Error())
	}

	{
		sqlerr := NewSQLError(ER_NO_DB_ERROR, "")
		assert.Equal(t, "No database selected (errno 1046) (sqlstate 3D000)", sqlerr.Error())
	}
}

func TestSqlErrorFromErr(t *testing.T) {
	{
		err := errors.New("errorman")
		sqlerr := NewSQLErrorFromError(err)
		assert.NotNil(t, sqlerr)
	}

	{
		err := errors.New("i.am.error.man (errno 1) (sqlstate HY000)")
		sqlerr := NewSQLErrorFromError(err)
		assert.NotNil(t, sqlerr)
	}

	{
		err := errors.New("No database selected (errno 1046) (sqlstate 3D000)")
		want := &SQLError{Num: 1046, State: "3D000", Message: "No database selected (errno 1046) (sqlstate 3D000)"}
		got := NewSQLErrorFromError(err)
		assert.Equal(t, want, got)
	}

	{
		err := NewSQLError1(10086, "xx", "i.am.the.error.man.%s", "xx")
		want := &SQLError{Num: 10086, State: "xx", Message: "i.am.the.error.man.xx"}
		got := NewSQLErrorFromError(err)
		assert.Equal(t, want, got)
	}
}
