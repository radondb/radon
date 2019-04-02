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
	"io"
	"io/ioutil"
	"os"

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
)

// Args tuple.
type Args struct {
	User            string
	Password        string
	Address         string
	ToUser          string
	ToPassword      string
	ToAddress       string
	ToDatabase      string
	ToEngine        string
	Database        string
	Table           string
	Outdir          string
	Threads         int
	ChunksizeInMB   int
	StmtSize        int
	Allbytes        uint64
	Allrows         uint64
	OverwriteTables bool

	// Interval in millisecond.
	IntervalMs int
}

// WriteFile used to write datas to file.
func WriteFile(file string, data string) error {
	flag := os.O_RDWR | os.O_TRUNC
	if _, err := os.Stat(file); os.IsNotExist(err) {
		flag |= os.O_CREATE
	}
	f, err := os.OpenFile(file, flag, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	n, err := f.Write(common.StringToBytes(data))
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return nil
}

// ReadFile used to read datas from file.
func ReadFile(file string) ([]byte, error) {
	return ioutil.ReadFile(file)
}

// AssertNil used to assert the error.
func AssertNil(err error) {
	if err != nil {
		panic(err)
	}
}

// EscapeBytes used to escape the literal byte.
func EscapeBytes(bytes []byte) []byte {
	buffer := common.NewBuffer(128)
	for _, b := range bytes {
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
			buffer.WriteU8(b)
		}
	}
	return buffer.Datas()
}
