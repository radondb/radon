package config

import (
	"bytes"
	"fmt"
	"testing"
)

func testConfig(cfg *Config, t *testing.T) {
	if v, err := cfg.GetBool("a"); err != nil {
		t.Fatal(err)
	} else if v != true {
		t.Fatal(v)
	}

	checkInt := func(t *testing.T, cfg *Config, key string, check int) {
		if v, err := cfg.GetInt(key); err != nil {
			t.Fatal(err)
		} else if v != check {
			t.Fatal(fmt.Sprintf("%s %d != %d", key, v, check))
		}
	}

	checkInt(t, cfg, "b", 100)
	checkInt(t, cfg, "kb", 1024)
	checkInt(t, cfg, "k", 1000)
	checkInt(t, cfg, "mb", 1024*1024)
	checkInt(t, cfg, "m", 1000*1000)
	checkInt(t, cfg, "gb", 1024*1024*1024)
	checkInt(t, cfg, "g", 1000*1000*1000)
}

func TestGetConfig(t *testing.T) {
	cfg := NewConfig()
	cfg.Values["a"] = "true"
	cfg.Values["b"] = "100"
	cfg.Values["kb"] = "1kb"
	cfg.Values["k"] = "1k"
	cfg.Values["mb"] = "1mb"
	cfg.Values["m"] = "1m"
	cfg.Values["gb"] = "1gb"
	cfg.Values["g"] = "1g"

	testConfig(cfg, t)
}

func TestReadWriteConfig(t *testing.T) {
	var b = []byte(`
            # comment
            a = true
            b = 100
            kb = 1kb
            k = 1k
            mb = 1mb
            m = 1m
            gb = 1gb
            g = 1g
        `)

	cfg, err := ReadConfig(b)
	if err != nil {
		t.Fatal(err)
	}

	testConfig(cfg, t)

	var buf bytes.Buffer

	if err := cfg.Write(&buf); err != nil {
		t.Fatal(err)
	}

	cfg.Values = make(map[string]string)
	if err := cfg.Read(&buf); err != nil {
		t.Fatal(err)
	}

	testConfig(cfg, t)
}
