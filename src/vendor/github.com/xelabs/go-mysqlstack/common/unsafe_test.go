/*
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package common

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBytesToString(t *testing.T) {
	{
		bs := []byte{0x61, 0x62}
		want := "ab"
		got := BytesToString(bs)
		assert.Equal(t, want, got)
	}

	{
		bs := []byte{}
		want := ""
		got := BytesToString(bs)
		assert.Equal(t, want, got)
	}
}

func TestSting(t *testing.T) {
	{
		want := []byte{0x61, 0x62}
		got := StringToBytes("ab")
		assert.Equal(t, want, got)
	}

	{
		want := []byte{}
		got := StringToBytes("")
		assert.Equal(t, want, got)
	}
}

func TestStingToBytes(t *testing.T) {
	{
		want := []byte{0x53, 0x45, 0x4c, 0x45, 0x43, 0x54, 0x20, 0x2a, 0x20, 0x46, 0x52, 0x4f, 0x4d, 0x20, 0x74, 0x32}
		got := StringToBytes("SELECT * FROM t2")
		assert.Equal(t, want, got)
	}
}
