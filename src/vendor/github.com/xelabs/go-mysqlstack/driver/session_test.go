/*
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package driver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestSession(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.DEBUG))
	th := NewTestHandler(log)
	svr, err := MockMysqlServer(log, th)
	assert.Nil(t, err)
	address := svr.Addr()

	// create session 1
	client, err := NewConn("mock", "mock", address, "test", "")
	assert.Nil(t, err)
	defer client.Close()

	var sessions []*Session
	for _, s := range th.ss {
		sessions = append(sessions, s.session)
	}

	{
		session1 := sessions[0]

		// Session ID.
		{
			log.Debug("--id:%v", session1.ID())
			log.Debug("--addr:%v", session1.Addr())
			log.Debug("--salt:%v", session1.Salt())
			log.Debug("--scramble:%v", session1.Scramble())
		}

		// schema.
		{
			want := "xx"
			session1.SetSchema(want)
			got := session1.Schema()
			assert.Equal(t, want, got)
		}

		// charset.
		{
			want := uint8(0x21)
			got := session1.Charset()
			assert.Equal(t, want, got)
		}
	}
}
