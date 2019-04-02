/*
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package proto

import (
	"math/rand"
	"time"

	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
)

// Greeting used for greeting packet.
type Greeting struct {
	protocolVersion uint8
	Charset         uint8

	// StatusFlags are the status flags we will base our returned flags on.
	// It is only used by the server.
	// SERVER_STATUS_AUTOCOMMIT is default.
	status uint16

	// Capabilities is the current set of features this connection
	// is using.  It is the features that are both supported by
	// the client and the server, and currently in use.
	// It is set after the initial handshake.
	Capability     uint32
	ConnectionID   uint32
	serverVersion  string
	authPluginName string
	Salt           []byte
}

// NewGreeting creates a new Greeting.
func NewGreeting(connectionID uint32, serverVersion string) *Greeting {
	greeting := &Greeting{
		protocolVersion: 10,
		serverVersion:   serverVersion,
		ConnectionID:    connectionID,
		Capability:      DefaultServerCapability,
		Charset:         sqldb.CharacterSetUtf8,
		status:          sqldb.SERVER_STATUS_AUTOCOMMIT,
		Salt:            make([]byte, 20),
	}

	// Generate the rand salts, range [1, 123].
	for i := 0; i < len(greeting.Salt); i++ {
		greeting.Salt[i] = byteRand(1, 123)
	}
	return greeting
}

func byteRand(min int, max int) byte {
	rand.Seed(time.Now().UTC().UnixNano())
	return byte(min + rand.Intn(max-min))
}

// Status returns status of greeting.
func (g *Greeting) Status() uint16 {
	return g.status
}

// Pack used to pack the greeting packet.
// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeV10
func (g *Greeting) Pack() []byte {
	// greeting buffer
	buf := common.NewBuffer(256)
	capLower := uint16(g.Capability)
	capUpper := uint16(uint32(g.Capability) >> 16)

	// 1: [0a] protocol version
	buf.WriteU8(g.protocolVersion)

	// string[NUL]: server version
	buf.WriteString(g.serverVersion)
	buf.WriteZero(1)

	// 4: connection id
	buf.WriteU32(g.ConnectionID)

	// string[8]: auth-plugin-data-part-1
	buf.WriteBytes(g.Salt[:8])

	// 1: [00] filler
	buf.WriteZero(1)

	// 2: capability flags (lower 2 bytes)
	buf.WriteU16(capLower)

	// 1: character set
	buf.WriteU8(sqldb.CharacterSetUtf8)

	// 2: status flags
	buf.WriteU16(g.status)

	// 2: capability flags (upper 2 bytes)
	buf.WriteU16(capUpper)

	// Length of auth plugin data.
	// Always 21 (8 + 13).
	buf.WriteU8(21)

	// string[10]: reserved (all [00])
	buf.WriteZero(10)

	// string[$len]: auth-plugin-data-part-2 ($len=MAX(13, length of auth-plugin-data - 8))
	buf.WriteBytes(g.Salt[8:])
	buf.WriteZero(1)

	// string[NUL]    auth-plugin name
	pluginName := "mysql_native_password"
	buf.WriteString(pluginName)
	buf.WriteZero(1)
	return buf.Datas()
}

// UnPack used to unpack the greeting packet.
func (g *Greeting) UnPack(payload []byte) error {
	var err error
	buf := common.ReadBuffer(payload)

	// 1: [0a] protocol version
	if g.protocolVersion, err = buf.ReadU8(); err != nil {
		return sqldb.NewSQLError(sqldb.ER_MALFORMED_PACKET, "extracting greeting protocol-version failed")
	}

	// string[NUL]: server version
	if g.serverVersion, err = buf.ReadStringNUL(); err != nil {
		return sqldb.NewSQLError(sqldb.ER_MALFORMED_PACKET, "extracting greeting server-version failed")
	}

	// 4: connection id
	if g.ConnectionID, err = buf.ReadU32(); err != nil {
		return sqldb.NewSQLError(sqldb.ER_MALFORMED_PACKET, "extracting greeting onnection-id failed")
	}

	// string[8]: auth-plugin-data-part-1
	var salt8 []byte
	if salt8, err = buf.ReadBytes(8); err != nil {
		return sqldb.NewSQLError(sqldb.ER_MALFORMED_PACKET, "extracting greeting auth-plugin-data-part-1 failed")
	}
	copy(g.Salt, salt8)

	// 1: [00] filler
	if err = buf.ReadZero(1); err != nil {
		return sqldb.NewSQLError(sqldb.ER_MALFORMED_PACKET, "extracting greeting filler failed")
	}

	// 2: capability flags (lower 2 bytes)
	var capLower uint16
	if capLower, err = buf.ReadU16(); err != nil {
		return sqldb.NewSQLError(sqldb.ER_MALFORMED_PACKET, "extracting greeting capability-flags failed")
	}

	// 1: character set
	if g.Charset, err = buf.ReadU8(); err != nil {
		return sqldb.NewSQLError(sqldb.ER_MALFORMED_PACKET, "extracting greeting charset failed")
	}

	// 2: status flags
	if g.status, err = buf.ReadU16(); err != nil {
		return sqldb.NewSQLError(sqldb.ER_MALFORMED_PACKET, "extracting greeting status-flags failed")
	}

	// 2: capability flags (upper 2 bytes)
	var capUpper uint16
	if capUpper, err = buf.ReadU16(); err != nil {
		return sqldb.NewSQLError(sqldb.ER_MALFORMED_PACKET, "extracting greeting capability-flags-upper failed")
	}
	g.Capability = (uint32(capUpper) << 16) | (uint32(capLower))

	// 1: length of auth-plugin-data-part-1
	var SLEN byte
	if (g.Capability & sqldb.CLIENT_PLUGIN_AUTH) > 0 {
		if SLEN, err = buf.ReadU8(); err != nil {
			return sqldb.NewSQLError(sqldb.ER_MALFORMED_PACKET, "extracting greeting auth-plugin-data length failed")
		}
	} else {
		if err = buf.ReadZero(1); err != nil {
			return sqldb.NewSQLError(sqldb.ER_MALFORMED_PACKET, "extracting greeting zero failed")
		}
	}

	// string[10]: reserved (all [00])
	if err = buf.ReadZero(10); err != nil {
		return sqldb.NewSQLError(sqldb.ER_MALFORMED_PACKET, "extracting greeting reserved failed")
	}

	// string[$len]: auth-plugin-data-part-2 ($len=MAX(13, length of auth-plugin-data - 8))
	if (g.Capability & sqldb.CLIENT_SECURE_CONNECTION) > 0 {
		read := int(SLEN) - 8
		if read < 0 || read > 13 {
			read = 13
		}
		var salt2 []byte
		if salt2, err = buf.ReadBytes(read); err != nil {
			return sqldb.NewSQLError(sqldb.ER_MALFORMED_PACKET, "extracting greeting salt2 failed")
		}

		// The last byte has to be 0, and is not part of the data.
		if salt2[read-1] != 0 {
			return sqldb.NewSQLError(sqldb.ER_MALFORMED_PACKET, "extracting greeting auth-plugin-data-part-2 is not 0 terminated")
		}
		copy(g.Salt[8:], salt2[:read-1])
	}

	// string[NUL]    auth-plugin name
	if (g.Capability & sqldb.CLIENT_PLUGIN_AUTH) > 0 {
		if g.authPluginName, err = buf.ReadStringNUL(); err != nil {
			return sqldb.NewSQLError(sqldb.ER_MALFORMED_PACKET, "extracting greeting auth-plugin-name failed")
		}
	}
	return nil
}
