/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package xbase

import (
	"io"
	"os"

	"github.com/pkg/errors"
)

// WriteFile used to write data to file.
func WriteFile(file string, data []byte) error {
	flag := os.O_RDWR | os.O_TRUNC
	if _, err := os.Stat(file); os.IsNotExist(err) {
		flag |= os.O_CREATE
	}
	f, err := os.OpenFile(file, flag, 0644)
	if err != nil {
		return errors.WithStack(err)
	}
	defer f.Close()

	n, err := f.Write(data)
	if err != nil {
		return errors.WithStack(err)
	}
	if n != len(data) {
		return errors.WithStack(io.ErrShortWrite)
	}
	return f.Sync()
}

// TruncateQuery used to truncate the query with max length.
func TruncateQuery(query string, max int) string {
	if max == 0 || len(query) <= max {
		return query
	}
	return query[:max] + " [TRUNCATED]"
}
