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

func TestGreetingUnPack(t *testing.T) {
	want := NewGreeting(4)
	got := NewGreeting(4)

	// normal
	{
		want.authPluginName = "mysql_native_password"
		err := got.UnPack(want.Pack())
		assert.Nil(t, err)
		assert.Equal(t, want, got)
		assert.Equal(t, sqldb.SERVER_STATUS_AUTOCOMMIT, int(got.Status()))
	}

	// 1. off sqldb.CLIENT_PLUGIN_AUTH
	{
		want.Capability = want.Capability &^ sqldb.CLIENT_PLUGIN_AUTH
		want.authPluginName = "mysql_native_password"
		err := got.UnPack(want.Pack())
		assert.Nil(t, err)
		assert.Equal(t, want, got)
	}

	// 2. off sqldb.CLIENT_SECURE_CONNECTION
	{
		want.Capability &= ^sqldb.CLIENT_SECURE_CONNECTION
		want.authPluginName = "mysql_native_password"
		err := got.UnPack(want.Pack())
		assert.Nil(t, err)
		assert.Equal(t, want, got)
	}

	// 3. off sqldb.CLIENT_PLUGIN_AUTH && sqldb.CLIENT_SECURE_CONNECTION
	{
		want.Capability &= (^sqldb.CLIENT_PLUGIN_AUTH ^ sqldb.CLIENT_SECURE_CONNECTION)
		want.authPluginName = "mysql_native_password"
		err := got.UnPack(want.Pack())
		assert.Nil(t, err)
		assert.Equal(t, want, got)
	}
}

func TestGreetingUnPackError(t *testing.T) {
	// NULL
	f0 := func(buff *common.Buffer) {
	}

	// Write protocol version.
	f1 := func(buff *common.Buffer) {
		buff.WriteU8(0x01)
	}

	// Write server version.
	f2 := func(buff *common.Buffer) {
		buff.WriteString("5.7.17-11")
		buff.WriteZero(1)
	}

	// Write connection ID.
	f3 := func(buff *common.Buffer) {
		buff.WriteU32(uint32(1))
	}

	// Write salt[8].
	f4 := func(buff *common.Buffer) {
		salt8 := make([]byte, 8)
		buff.WriteBytes(salt8)
	}

	// Write filler.
	f5 := func(buff *common.Buffer) {
		buff.WriteZero(1)
	}

	capability := DefaultServerCapability
	capLower := uint16(capability)
	capUpper := uint16(uint32(capability) >> 16)

	// Write capability lower 2 bytes
	f6 := func(buff *common.Buffer) {
		buff.WriteU16(capLower)
	}

	// Write charset.
	f7 := func(buff *common.Buffer) {
		buff.WriteU8(0x01)
	}

	// Write statu flags
	f8 := func(buff *common.Buffer) {
		buff.WriteU16(uint16(1))
	}

	// Write capability upper 2 bytes
	f9 := func(buff *common.Buffer) {
		buff.WriteU16(capUpper)
	}

	// Write length of auth-plugin
	f10 := func(buff *common.Buffer) {
		buff.WriteU8(0x01)
	}

	// Write reserved.
	f11 := func(buff *common.Buffer) {
		buff.WriteZero(10)
	}

	// Write auth plugin data part 2
	f12 := func(buff *common.Buffer) {
		data2 := make([]byte, 13)
		data2[12] = 0x01
		buff.WriteBytes(data2)
	}

	buff := common.NewBuffer(32)
	fs := []func(buff *common.Buffer){f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12}
	for i := 0; i < len(fs); i++ {
		greeting := NewGreeting(0)
		err := greeting.UnPack(buff.Datas())
		assert.NotNil(t, err)
		fs[i](buff)
	}

	{
		greeting := NewGreeting(0)
		err := greeting.UnPack(buff.Datas())
		assert.NotNil(t, err)
	}
}
