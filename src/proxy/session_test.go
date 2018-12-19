/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSession(t *testing.T) {
	sess := &session{}

	{
		sf := sess.getStreamingFetchVar()
		assert.False(t, sf)
	}

	{
		sess.setStreamingFetchVar(false)
		sf := sess.getStreamingFetchVar()
		assert.False(t, sf)

		sess.setStreamingFetchVar(true)
		sf = sess.getStreamingFetchVar()
		assert.True(t, sf)

		sess.setStreamingFetchVar(true)
		sf = sess.getStreamingFetchVar()
		assert.True(t, sf)

		sess.setStreamingFetchVar(false)
		sf = sess.getStreamingFetchVar()
		assert.False(t, sf)
	}
}
