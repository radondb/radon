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
	"crypto/sha1"
	"fmt"

	"github.com/xelabs/go-mysqlstack/common"
	"github.com/xelabs/go-mysqlstack/sqldb"
)

// Auth packet.
type Auth struct {
	charset         uint8
	maxPacketSize   uint32
	authResponseLen uint8
	clientFlags     uint32
	authResponse    []byte
	pluginName      string
	database        string
	user            string
}

// NewAuth creates new Auth.
func NewAuth() *Auth {
	return &Auth{}
}

// Database returns the database.
func (a *Auth) Database() string {
	return a.database
}

// ClientFlags returns the client flags.
func (a *Auth) ClientFlags() uint32 {
	return a.clientFlags
}

// Charset returns the charset.
func (a *Auth) Charset() uint8 {
	return a.charset
}

// User returns the user.
func (a *Auth) User() string {
	return a.user
}

// AuthResponse returns the auth response.
func (a *Auth) AuthResponse() []byte {
	return a.authResponse
}

// CleanAuthResponse used to set the authResponse to nil.
// To improve the heap gc cost.
func (a *Auth) CleanAuthResponse() {
	a.authResponse = nil
}

// UnPack parses the handshake sent by the client.
// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse41
func (a *Auth) UnPack(payload []byte) error {
	var err error
	buf := common.ReadBuffer(payload)

	if a.clientFlags, err = buf.ReadU32(); err != nil {
		return fmt.Errorf("auth.unpack: can't read client flags")
	}
	if a.clientFlags&sqldb.CLIENT_PROTOCOL_41 == 0 {
		return fmt.Errorf("auth.unpack: only support protocol 4.1")
	}
	if a.maxPacketSize, err = buf.ReadU32(); err != nil {
		return fmt.Errorf("auth.unpack: can't read maxPacketSize")
	}
	if a.charset, err = buf.ReadU8(); err != nil {
		return fmt.Errorf("auth.unpack: can't read charset")
	}
	if err = buf.ReadZero(23); err != nil {
		return fmt.Errorf("auth.unpack: can't read 23zeros")
	}
	if a.user, err = buf.ReadStringNUL(); err != nil {
		return fmt.Errorf("auth.unpack: can't read user")
	}
	if (a.clientFlags & sqldb.CLIENT_SECURE_CONNECTION) > 0 {
		if a.authResponseLen, err = buf.ReadU8(); err != nil {
			return fmt.Errorf("auth.unpack: can't read authResponse length")
		}
		if a.authResponse, err = buf.ReadBytes(int(a.authResponseLen)); err != nil {
			return fmt.Errorf("auth.unpack: can't read authResponse")
		}
	} else {
		if a.authResponse, err = buf.ReadBytesNUL(); err != nil {
			return fmt.Errorf("auth.unpack: can't read authResponse")
		}
	}
	if (a.clientFlags & sqldb.CLIENT_CONNECT_WITH_DB) > 0 {
		if a.database, err = buf.ReadStringNUL(); err != nil {
			return fmt.Errorf("auth.unpack: can't read dbname")
		}
	}
	if (a.clientFlags & sqldb.CLIENT_PLUGIN_AUTH) > 0 {
		if a.pluginName, err = buf.ReadStringNUL(); err != nil {
			return fmt.Errorf("auth.unpack: can't read pluginName")
		}
	}
	if a.pluginName != DefaultAuthPluginName {
		return fmt.Errorf("invalid authPluginName, got %v but only support %v", a.pluginName, DefaultAuthPluginName)
	}
	return nil
}

// Pack used to pack a HandshakeResponse41 packet.
func (a *Auth) Pack(capabilityFlags uint32, charset uint8, username string, password string, salt []byte, database string) []byte {
	buf := common.NewBuffer(256)
	authResponse := nativePassword(password, salt)
	if len(database) > 0 {
		capabilityFlags |= sqldb.CLIENT_CONNECT_WITH_DB
	} else {
		capabilityFlags &= ^sqldb.CLIENT_CONNECT_WITH_DB
	}

	// 4 capability flags, CLIENT_PROTOCOL_41 always set
	buf.WriteU32(capabilityFlags)

	// 4 max-packet size (none)
	buf.WriteU32(0)

	// 1 character set
	buf.WriteU8(charset)

	// string[23] reserved (all [0])
	buf.WriteZero(23)

	// string[NUL] username
	buf.WriteString(username)
	buf.WriteZero(1)

	if (capabilityFlags & sqldb.CLIENT_SECURE_CONNECTION) > 0 {
		// 1 length of auth-response
		// string[n]  auth-response
		buf.WriteU8(uint8(len(authResponse)))
		buf.WriteBytes(authResponse)
	} else {
		buf.WriteBytes(authResponse)
		buf.WriteZero(1)
	}
	capabilityFlags &= ^sqldb.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA

	// string[NUL] database
	if capabilityFlags&sqldb.CLIENT_CONNECT_WITH_DB > 0 {
		buf.WriteString(database)
		buf.WriteZero(1)
	}

	// string[NUL] auth plugin name
	buf.WriteString(DefaultAuthPluginName)
	buf.WriteZero(1)

	// CLIENT_CONNECT_ATTRS none
	//
	return buf.Datas()
}

// https://dev.mysql.com/doc/internals/en/secure-password-authentication.html#packet-Authentication::Native41
// SHA1( password ) XOR SHA1( "20-bytes random data from server" <concat> SHA1( SHA1( password ) ) )
// Encrypt password using 4.1+ method
func nativePassword(password string, salt []byte) []byte {
	if len(password) == 0 {
		return nil
	}

	// stage1Hash = SHA1(password)
	crypt := sha1.New()
	crypt.Write([]byte(password))
	stage1 := crypt.Sum(nil)

	// scrambleHash = SHA1(scramble + SHA1(stage1Hash))
	// inner Hash
	crypt.Reset()
	crypt.Write(stage1)
	stage1SHA1 := crypt.Sum(nil)

	// stage2Hash = SHA1(salt <concat> SHA1(SHA1(password)))
	crypt.Reset()
	crypt.Write(salt)
	crypt.Write(stage1SHA1)
	stage2 := crypt.Sum(nil)

	// srambleHash = stage1Hash ^ stage2Hash
	scramble := make([]byte, len(stage2))
	for i := range stage2 {
		scramble[i] = stage1[i] ^ stage2[i]
	}
	return scramble
}
