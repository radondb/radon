package config

import (
	"bytes"
	"fmt"
	"io"
	"os"
)

func (c *Config) Write(w io.Writer) error {
	var buf bytes.Buffer

	for k, v := range c.Values {
		buf.WriteString(fmt.Sprintf("%s = %s\n", k, v))
	}

	_, err := w.Write(buf.Bytes())
	return err
}

func (c *Config) WriteFile(filePath string) error {
	filePathBak := fmt.Sprintf("%s.bak.tmp", filePath)

	fd, err := os.OpenFile(filePathBak, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return err
	}

	err = c.Write(fd)
	fd.Close()

	if err != nil {
		return err
	}

	return os.Rename(filePathBak, filePath)
}
