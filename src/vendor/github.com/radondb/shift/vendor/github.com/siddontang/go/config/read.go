package config

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
)

func ReadConfigFile(name string) (*Config, error) {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, err
	}

	return ReadConfig(data)

}

func ReadConfig(data []byte) (*Config, error) {
	cfg := NewConfig()

	if err := cfg.Read(bytes.NewBuffer(data)); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) Read(r io.Reader) error {
	rb := bufio.NewReaderSize(r, 4096)

	for {
		l, err := rb.ReadString('\n')
		if err != nil && err != io.EOF {
			return err
		} else if err == io.EOF {
			break
		}

		l = strings.TrimSpace(l)
		if len(l) == 0 {
			continue
		}

		//comment
		if l[0] == '#' {
			continue
		}

		ps := strings.Split(l, "=")
		if len(ps) > 2 {
			return fmt.Errorf("invalid line format %s", l)
		} else if len(ps) == 1 {
			c.SetString(ps[0], "")
		} else {
			c.SetString(strings.TrimSpace(ps[0]), strings.TrimSpace(ps[1]))
		}
	}
	return nil
}
