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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/common"
	"github.com/xelabs/go-mysqlstack/sqldb"
)

func TestAuth(t *testing.T) {
	auth := NewAuth()
	{
		data := []byte{
			0x8d, 0xa6, 0xff, 0x01, 0x00, 0x00, 0x00, 0x01,
			0x21, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x72, 0x6f, 0x6f, 0x74, 0x00, 0x14, 0x0e, 0xb4,
			0xdd, 0xb5, 0x5b, 0x64, 0xf8, 0x54, 0x40, 0xfd,
			0xf3, 0x45, 0xfa, 0x37, 0x12, 0x20, 0x20, 0xda,
			0x38, 0xaa, 0x61, 0x62, 0x63, 0x00, 0x6d, 0x79,
			0x73, 0x71, 0x6c, 0x5f, 0x6e, 0x61, 0x74, 0x69,
			0x76, 0x65, 0x5f, 0x70, 0x61, 0x73, 0x73, 0x77,
			0x6f, 0x72, 0x64, 0x00}

		auth.UnPack(data)
		want := &Auth{
			charset:         33,
			maxPacketSize:   16777216,
			authResponseLen: 20,
			authResponse: []byte{
				0x0e, 0xb4, 0xdd, 0xb5, 0x5b, 0x64, 0xf8, 0x54,
				0x40, 0xfd, 0xf3, 0x45, 0xfa, 0x37, 0x12, 0x20,
				0x20, 0xda, 0x38, 0xaa},
			pluginName:  "mysql_native_password",
			database:    "abc",
			user:        "root",
			clientFlags: 33531533,
		}
		got := auth
		assert.Equal(t, want, got)
	}

	{
		want := "abc"
		got := auth.Database()
		assert.Equal(t, want, got)
	}

	{
		want := uint32(33531533)
		got := auth.ClientFlags()
		assert.Equal(t, want, got)
	}

	{
		want := uint8(33)
		got := auth.Charset()
		assert.Equal(t, want, got)
	}

	// User.
	{
		want := "root"
		got := auth.User()
		assert.Equal(t, want, got)
	}

	// Resp.
	{
		want := []byte{
			0x0e, 0xb4, 0xdd, 0xb5, 0x5b, 0x64, 0xf8, 0x54,
			0x40, 0xfd, 0xf3, 0x45, 0xfa, 0x37, 0x12, 0x20,
			0x20, 0xda, 0x38, 0xaa}
		got := auth.AuthResponse()
		assert.Equal(t, want, got)

		auth.CleanAuthResponse()
		assert.Nil(t, auth.AuthResponse())
	}
}

func TestAuthUnpackError(t *testing.T) {
	auth := NewAuth()
	{
		data := []byte{
			0x8d, 0xa6, 0xff,
		}
		err := auth.UnPack(data)
		want := "auth.unpack: can't read client flags"
		got := err.Error()
		assert.Equal(t, want, got)
	}
}

func TestAuthUnPack(t *testing.T) {
	want := NewAuth()
	want.charset = 0x02
	want.authResponseLen = 20
	want.clientFlags = DefaultClientCapability
	want.clientFlags |= sqldb.CLIENT_CONNECT_WITH_DB
	want.authResponse = nativePassword("sbtest", DefaultSalt)
	want.database = "sbtest"
	want.user = "sbtest"
	want.pluginName = DefaultAuthPluginName

	got := NewAuth()
	err := got.UnPack(want.Pack(
		DefaultClientCapability,
		0x02,
		"sbtest",
		"sbtest",
		DefaultSalt,
		"sbtest",
	))
	assert.Nil(t, err)
	assert.Equal(t, want, got)
}

func TestAuthWithoutPWD(t *testing.T) {
	want := NewAuth()
	want.charset = 0x02
	want.authResponseLen = 0
	want.clientFlags = DefaultClientCapability
	want.clientFlags |= sqldb.CLIENT_CONNECT_WITH_DB
	want.authResponse = nativePassword("", DefaultSalt)
	want.database = "sbtest"
	want.user = "sbtest"
	want.pluginName = DefaultAuthPluginName

	got := NewAuth()
	err := got.UnPack(want.Pack(
		DefaultClientCapability,
		0x02,
		"sbtest",
		"",
		DefaultSalt,
		"sbtest",
	))
	assert.Nil(t, err)
	assert.Equal(t, want, got)
}

func TestAuthWithoutDB(t *testing.T) {
	want := NewAuth()
	want.charset = 0x02
	want.authResponseLen = 20
	want.clientFlags = DefaultClientCapability
	want.authResponse = nativePassword("sbtest", DefaultSalt)
	want.user = "sbtest"
	want.pluginName = DefaultAuthPluginName

	got := NewAuth()
	err := got.UnPack(want.Pack(
		DefaultClientCapability,
		0x02,
		"sbtest",
		"sbtest",
		DefaultSalt,
		"",
	))
	assert.Nil(t, err)
	assert.Equal(t, want, got)
}

func TestAuthWithoutSecure(t *testing.T) {
	want := NewAuth()
	want.charset = 0x02
	want.authResponseLen = 20
	want.clientFlags = DefaultClientCapability &^ sqldb.CLIENT_SECURE_CONNECTION &^ sqldb.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
	want.clientFlags |= sqldb.CLIENT_CONNECT_WITH_DB
	want.authResponse = nativePassword("sbtest", DefaultSalt)
	want.user = "sbtest"
	want.database = "sbtest"
	want.pluginName = DefaultAuthPluginName

	got := NewAuth()
	err := got.UnPack(want.Pack(
		DefaultClientCapability&^sqldb.CLIENT_SECURE_CONNECTION,
		0x02,
		"sbtest",
		"sbtest",
		DefaultSalt,
		"sbtest",
	))
	got.authResponseLen = 20
	assert.Nil(t, err)
	assert.Equal(t, want, got)
}

func TestAuthUnPackError(t *testing.T) {
	capabilityFlags := DefaultClientCapability
	capabilityFlags |= sqldb.CLIENT_PROTOCOL_41
	capabilityFlags |= sqldb.CLIENT_CONNECT_WITH_DB

	// NULL
	f0 := func(buff *common.Buffer) {
	}

	// Write clientFlags.
	f1 := func(buff *common.Buffer) {
		buff.WriteU32(capabilityFlags)
	}

	// Write maxPacketSize.
	f2 := func(buff *common.Buffer) {
		buff.WriteU32(uint32(16777216))
	}

	// Write charset.
	f3 := func(buff *common.Buffer) {
		buff.WriteU8(0x01)
	}

	// Write 23 NULLs.
	f4 := func(buff *common.Buffer) {
		buff.WriteZero(23)
	}

	// Write username.
	f5 := func(buff *common.Buffer) {
		buff.WriteString("mock")
		buff.WriteZero(1)
	}

	// Write auth-response.
	f6 := func(buff *common.Buffer) {
		authRsp := make([]byte, 8)
		buff.WriteU8(8)
		buff.WriteBytes(authRsp)
	}

	// Write database.
	f7 := func(buff *common.Buffer) {
		buff.WriteString("db1")
		buff.WriteZero(1)
	}

	buff := common.NewBuffer(32)
	fs := []func(buff *common.Buffer){f0, f1, f2, f3, f4, f5, f6, f7}
	for i := 0; i < len(fs); i++ {
		auth := NewAuth()
		err := auth.UnPack(buff.Datas())
		assert.NotNil(t, err)
		fs[i](buff)
	}

	{
		auth := NewAuth()
		err := auth.UnPack(buff.Datas())
		assert.NotNil(t, err)
	}
}
