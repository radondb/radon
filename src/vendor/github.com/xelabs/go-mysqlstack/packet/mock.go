/*
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * Copyright 2016 The Go-MySQL-Driver Authors. All rights reserved.
 * GPL License
 *
 */

package packet

import (
	"io"
	"net"
	"time"
)

var _ net.Conn = &MockConn{}

// MockConn used to mock a net.Conn for testing purposes.
type MockConn struct {
	laddr  net.Addr
	raddr  net.Addr
	data   []byte
	closed bool
	read   int
}

// NewMockConn creates new mock connection.
func NewMockConn() *MockConn {
	return &MockConn{}
}

// Read implements the net.Conn interface.
func (m *MockConn) Read(b []byte) (n int, err error) {
	// handle the EOF
	if len(m.data) == 0 {
		err = io.EOF
		return
	}

	n = copy(b, m.data)
	m.read += n
	m.data = m.data[n:]
	return
}

// Write implements the net.Conn interface.
func (m *MockConn) Write(b []byte) (n int, err error) {
	m.data = append(m.data, b...)
	return len(b), nil
}

// Datas implements the net.Conn interface.
func (m *MockConn) Datas() []byte {
	return m.data
}

// Close implements the net.Conn interface.
func (m *MockConn) Close() error {
	m.closed = true
	return nil
}

// LocalAddr implements the net.Conn interface.
func (m *MockConn) LocalAddr() net.Addr {
	return m.laddr
}

// RemoteAddr implements the net.Conn interface.
func (m *MockConn) RemoteAddr() net.Addr {
	return m.raddr
}

// SetDeadline implements the net.Conn interface.
func (m *MockConn) SetDeadline(t time.Time) error {
	return nil
}

// SetReadDeadline implements the net.Conn interface.
func (m *MockConn) SetReadDeadline(t time.Time) error {
	return nil
}

// SetWriteDeadline implements the net.Conn interface.
func (m *MockConn) SetWriteDeadline(t time.Time) error {
	return nil
}
