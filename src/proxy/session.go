/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"sync"

	"backend"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser"
)

type bitmask uint32

// session variables capabilities.
const (
	cap_streaming_fetch bitmask = 1 << iota // streaming fetch for this session
)

type session struct {
	mu           sync.Mutex
	node         sqlparser.Statement
	query        string
	session      *driver.Session
	timestamp    int64
	capabilities bitmask
	transaction  backend.Transaction
}

func (s *session) setStreamingFetchVar(r bool) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if r {
		s.capabilities |= cap_streaming_fetch
	} else {
		s.capabilities &= ^cap_streaming_fetch
	}
}

func (s *session) getStreamingFetchVar() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.capabilities&cap_streaming_fetch != 0
}
