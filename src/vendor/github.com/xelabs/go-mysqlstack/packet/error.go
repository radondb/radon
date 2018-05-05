/*
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package packet

import (
	"errors"
)

var (
	// ErrBadConn used for the error of bad connection.
	ErrBadConn = errors.New("connection.was.bad")
	// ErrMalformPacket used for the bad packet.
	ErrMalformPacket = errors.New("Malform.packet.error")
)
