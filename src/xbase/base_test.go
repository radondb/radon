/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package xbase

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestXbaseWriteFile(t *testing.T) {
	file := "/tmp/xbase.test"
	defer os.RemoveAll(file)

	// Write OK.
	{
		err := WriteFile(file, []byte{0xfd})
		assert.Nil(t, err)
	}

	// Write Error.
	{
		badFile := "/xx/xbase.test"
		err := WriteFile(badFile, []byte{0xfd})
		assert.NotNil(t, err)
	}
}

func TestXbaseTruncateQuery(t *testing.T) {
	var testCases = []struct {
		in, out string
	}{{
		in:  "",
		out: "",
	}, {
		in:  "12345",
		out: "12345",
	}, {
		in:  "123456",
		out: "12345 [TRUNCATED]",
	}}
	for _, testCase := range testCases {
		got := TruncateQuery(testCase.in, 5)
		assert.Equal(t, testCase.out, got)
	}
}
