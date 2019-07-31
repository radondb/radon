/*
   Package config is a simple tool to handle key-value config, like Redis's configuration:

       # commet
       key = value

*/
package config

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var (
	ErrNil = errors.New("nil value")
)

type Config struct {
	Values map[string]string
}

func NewConfig() *Config {
	c := &Config{}
	c.Values = make(map[string]string)
	return c
}

/*
   bool: true, false, 0, 1, or ""
*/
func (c *Config) GetBool(key string) (bool, error) {
	v, err := c.GetString(key)
	if err != nil {
		return false, err
	}

	v = strings.ToLower(v)
	switch v {
	case "true", "1":
		return true, nil
	case "false", "0", "":
		return false, nil
	default:
		return false, fmt.Errorf("invalid bool format %s", v)
	}
}

/*
   int may be pure number or below format:

       # 1k => 1000 bytes
       # 1kb => 1024 bytes
       # 1m => 1000000 bytes
       # 1mb => 1024*1024 bytes
       # 1g => 1000000000 bytes
       # 1gb => 1024*1024*1024 bytes

*/
func (c *Config) GetInt64(key string) (int64, error) {
	v, err := c.GetString(key)
	if err != nil {
		return 0, err
	}

	if len(v) == 0 {
		return 0, ErrNil
	}

	var scale int64 = 1
	v = strings.ToLower(v)

	var b bool = false
	if v[len(v)-1] == 'b' {
		v = v[0 : len(v)-1]
		b = true
	}

	if len(v) == 0 {
		return 0, fmt.Errorf("invalid number format %s", v)
	}

	switch v[len(v)-1] {
	case 'k':
		v = v[0 : len(v)-1]
		if b {
			scale = 1024
		} else {
			scale = 1000
		}
		break
	case 'm':
		v = v[0 : len(v)-1]
		if b {
			scale = 1024 * 1024
		} else {
			scale = 1000 * 1000
		}
	case 'g':
		v = v[0 : len(v)-1]
		if b {
			scale = 1024 * 1024 * 1024
		} else {
			scale = 1000 * 1000 * 1000
		}
	}

	var n int64
	n, err = strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, err
	}

	return n * scale, nil
}

func (c *Config) GetUint64(key string) (uint64, error) {
	v, err := c.GetInt64(key)
	if v < 0 {
		return 0, fmt.Errorf("negative number %d", v)
	}
	return uint64(v), err
}

func (c *Config) GetInt(key string) (int, error) {
	v, err := c.GetInt64(key)
	return int(v), err
}

func (c *Config) GetString(key string) (string, error) {
	v, ok := c.Values[key]
	if !ok {
		return "", ErrNil
	} else {
		return v, nil
	}
}

func (c *Config) SetString(key string, value string) {
	c.Values[key] = value
}

func (c *Config) SetInt64(key string, n int64) {
	c.Values[key] = strconv.FormatInt(n, 10)
}

func (c *Config) SetUint64(key string, n uint64) {
	c.Values[key] = strconv.FormatUint(n, 10)
}

func (c *Config) SetInt(key string, n int) {
	c.SetInt64(key, int64(n))
}

func (c *Config) SetBool(key string, v bool) {
	if v {
		c.Values[key] = "true"
	} else {
		c.Values[key] = "false"
	}
}
