/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package xbase

import (
	"bytes"
)

// EscapeBytes used to escape the literal byte.
func EscapeBytes(bytesArray []byte) []byte {
	var buffer bytes.Buffer

	for _, b := range bytesArray {
		// See https://dev.mysql.com/doc/refman/5.7/en/string-literals.html
		// for more information on how to escape string literals in MySQL.
		switch b {
		case 0:
			buffer.WriteString(`\0`)
		case '\'':
			buffer.WriteString(`\'`)
		case '"':
			buffer.WriteString(`\"`)
		case '\b':
			buffer.WriteString(`\b`)
		case '\n':
			buffer.WriteString(`\n`)
		case '\r':
			buffer.WriteString(`\r`)
		case '\t':
			buffer.WriteString(`\t`)
		case 0x1A:
			buffer.WriteString(`\Z`)
		case '\\':
			buffer.WriteString(`\\`)
		default:
			buffer.WriteByte(b)
		}
	}
	return buffer.Bytes()
}
