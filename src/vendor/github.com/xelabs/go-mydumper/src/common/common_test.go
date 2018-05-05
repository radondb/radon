/*
 * go-mydumper
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package common

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriteReadFile(t *testing.T) {
	file := "/tmp/xx.txt"
	defer os.Remove(file)

	{
		err := WriteFile(file, "fake")
		assert.Nil(t, err)
	}

	{
		got, err := ReadFile(file)
		assert.Nil(t, err)
		want := []byte("fake")
		assert.Equal(t, want, got)
	}

	{
		err := WriteFile("/xxu01/xx.txt", "fake")
		assert.NotNil(t, err)
	}
}

func TestEscapeBytes(t *testing.T) {
	tests := []struct {
		v   []byte
		exp []byte
	}{
		{[]byte("simple"), []byte("simple")},
		{[]byte(`simplers's "world"`), []byte(`simplers\'s \"world\"`)},
		{[]byte("\x00'\"\b\n\r"), []byte(`\0\'\"\b\n\r`)},
		{[]byte("\t\x1A\\"), []byte(`\t\Z\\`)},
	}
	for _, tt := range tests {
		got := EscapeBytes(tt.v)
		want := tt.exp
		assert.Equal(t, want, got)
	}
}
