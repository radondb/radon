/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package xbase

import (
	"testing"

	"github.com/radondb/shift/xlog"
	"github.com/stretchr/testify/assert"
)

func TestHttpPost(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.DEBUG))

	url := "http://baidu.com"
	type request struct {
	}
	req := &request{}
	resp, cleanup, err := HTTPPost(url, req)
	assert.Nil(t, err)
	defer cleanup()
	log.Debug("%#v", resp)
}

func TestHttpPut(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.DEBUG))

	url := "http://baidu.com"
	type request struct {
	}
	req := &request{}
	resp, cleanup, err := HTTPPut(url, req)
	assert.NotNil(t, err)
	defer cleanup()
	log.Debug("%#v", resp)
}
