/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package config

import (
	"encoding/json"
	"io/ioutil"
	"xbase"

	"github.com/pkg/errors"
)

// ProxyConfig tuple.
type ProxyConfig struct {
	IPS         []string `json:"allowip,omitempty"`
	MetaDir     string   `json:"meta-dir"`
	Endpoint    string   `json:"endpoint"`
	TwopcEnable bool     `json:"twopc-enable"`

	MaxConnections      int    `json:"max-connections"`
	MaxResultSize       int    `json:"max-result-size"`
	DDLTimeout          int    `json:"ddl-timeout"`
	QueryTimeout        int    `json:"query-timeout"`
	PeerAddress         string `json:"peer-address,omitempty"`
	BackupDefaultEngine string `json:"backup-default-engine"`
}

// DefaultProxyConfig returns default proxy config.
func DefaultProxyConfig() *ProxyConfig {
	return &ProxyConfig{
		MetaDir:             "./radon-meta",
		Endpoint:            "127.0.0.1:3308",
		MaxConnections:      1024,
		MaxResultSize:       1024 * 1024 * 1024, // 1GB
		DDLTimeout:          10 * 3600 * 1000,   // 10hours
		QueryTimeout:        5 * 60 * 1000,      // 5minutes
		PeerAddress:         "127.0.0.1:8080",
		BackupDefaultEngine: "TokuDB", // Default MySQL storage engine for backup.
	}
}

// UnmarshalJSON interface on ProxyConfig.
func (c *ProxyConfig) UnmarshalJSON(b []byte) error {
	type confAlias *ProxyConfig
	conf := confAlias(DefaultProxyConfig())
	if err := json.Unmarshal(b, conf); err != nil {
		return err
	}
	*c = ProxyConfig(*conf)
	return nil
}

// AuditConfig tuple.
type AuditConfig struct {
	Mode        string `json:"mode"`
	LogDir      string `json:"audit-dir"`
	MaxSize     int    `json:"max-size"`
	ExpireHours int    `json:"expire-hours"`
}

// DefaultAuditConfig returns default audit config.
func DefaultAuditConfig() *AuditConfig {
	return &AuditConfig{
		Mode:        "N",
		LogDir:      "/tmp/auditlog",
		MaxSize:     1024 * 1024 * 256, // 256MB
		ExpireHours: 1,                 // 1hours
	}
}

// UnmarshalJSON interface on AuditConfig.
func (c *AuditConfig) UnmarshalJSON(b []byte) error {
	type confAlias *AuditConfig
	conf := confAlias(DefaultAuditConfig())
	if err := json.Unmarshal(b, conf); err != nil {
		return err
	}
	*c = AuditConfig(*conf)
	return nil
}

// BinlogConfig tuple.
type BinlogConfig struct {
	LogDir       string `json:"binlog-dir"`
	MaxSize      int    `json:"max-size"`
	RelayWorkers int    `json:"relay-workers"`
	RelayWaitMs  int    `json:"relay-wait-ms"`
	EnableBinlog bool   `json:"enable-binlog"`
	EnableRelay  bool   `json:"enable-relay"`
	// type=0, turn off the parallel.
	// type=1, same events type can parallel(default).
	// type=2, all events type can parallel.
	ParallelType int `json:"parallel-type"`
}

// DefaultBinlogConfig returns default binlog config.
func DefaultBinlogConfig() *BinlogConfig {
	return &BinlogConfig{
		LogDir:       "/tmp/binlog",
		MaxSize:      1024 * 1024 * 128, // 128MB
		RelayWorkers: 32,
		RelayWaitMs:  5000,
		ParallelType: 1,
	}
}

// UnmarshalJSON interface on BinlogConfig.
func (c *BinlogConfig) UnmarshalJSON(b []byte) error {
	type confAlias *BinlogConfig
	conf := confAlias(DefaultBinlogConfig())
	if err := json.Unmarshal(b, conf); err != nil {
		return err
	}
	*c = BinlogConfig(*conf)
	return nil
}

// LogConfig tuple.
type LogConfig struct {
	Level string `json:"level"`
}

// DefaultLogConfig returns default log config.
func DefaultLogConfig() *LogConfig {
	return &LogConfig{
		Level: "ERROR",
	}
}

// UnmarshalJSON interface on LogConfig.
func (c *LogConfig) UnmarshalJSON(b []byte) error {
	type confAlias *LogConfig
	conf := confAlias(DefaultLogConfig())
	if err := json.Unmarshal(b, conf); err != nil {
		return err
	}
	*c = LogConfig(*conf)
	return nil
}

// BackendConfig tuple.
type BackendConfig struct {
	Name           string `json:"name"`
	Address        string `json:"address"`
	User           string `json:"user"`
	Password       string `json:"password"`
	DBName         string `json:"database"`
	Charset        string `json:"charset"`
	MaxConnections int    `json:"max-connections"`
}

// BackendsConfig tuple.
type BackendsConfig struct {
	Backup   *BackendConfig   `json:"backup"`
	Backends []*BackendConfig `json:"backends"`
}

// PartitionConfig tuple.
type PartitionConfig struct {
	Table   string `json:"table"`
	Segment string `json:"segment"`
	Backend string `json:"backend"`
}

// TableConfig tuple.
type TableConfig struct {
	Name       string             `json:"name"`
	ShardType  string             `json:"shardtype"`
	ShardKey   string             `json:"shardkey"`
	Partitions []*PartitionConfig `json:"partitions"`
}

// SchemaConfig tuple.
type SchemaConfig struct {
	DB     string         `json:"database"`
	Tables []*TableConfig `json:"tables"`
}

// RouterConfig tuple.
type RouterConfig struct {
	Slots  int `json:"slots-readonly"`
	Blocks int `json:"blocks-readonly"`
}

// DefaultRouterConfig returns the default router config.
func DefaultRouterConfig() *RouterConfig {
	return &RouterConfig{
		Slots:  4096,
		Blocks: 128,
	}
}

// UnmarshalJSON interface on RouterConfig.
func (c *RouterConfig) UnmarshalJSON(b []byte) error {
	type confAlias *RouterConfig
	conf := confAlias(DefaultRouterConfig())
	if err := json.Unmarshal(b, conf); err != nil {
		return err
	}
	*c = RouterConfig(*conf)
	return nil
}

// Config tuple.
type Config struct {
	Proxy  *ProxyConfig  `json:"proxy"`
	Audit  *AuditConfig  `json:"audit"`
	Router *RouterConfig `json:"router"`
	Binlog *BinlogConfig `json:"binlog"`
	Log    *LogConfig    `json:"log"`
}

func checkConfig(conf *Config) {
	if conf.Proxy == nil {
		conf.Proxy = DefaultProxyConfig()
	}

	if conf.Binlog == nil {
		conf.Binlog = DefaultBinlogConfig()
	}

	if conf.Audit == nil {
		conf.Audit = DefaultAuditConfig()
	}

	if conf.Router == nil {
		conf.Router = DefaultRouterConfig()
	}

	if conf.Log == nil {
		conf.Log = DefaultLogConfig()
	}
}

// LoadConfig used to load the config from file.
func LoadConfig(path string) (*Config, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	conf := &Config{}
	if err := json.Unmarshal([]byte(data), conf); err != nil {
		return nil, errors.WithStack(err)
	}
	checkConfig(conf)
	return conf, nil
}

// ReadTableConfig used to read the table config from the data.
func ReadTableConfig(data string) (*TableConfig, error) {
	conf := &TableConfig{}
	if err := json.Unmarshal([]byte(data), conf); err != nil {
		return nil, errors.WithStack(err)
	}
	return conf, nil
}

// ReadBackendsConfig used to read the backend config from the data.
func ReadBackendsConfig(data string) (*BackendsConfig, error) {
	conf := &BackendsConfig{}
	if err := json.Unmarshal([]byte(data), conf); err != nil {
		return nil, errors.WithStack(err)
	}
	return conf, nil
}

// WriteConfig used to write the conf to file.
func WriteConfig(path string, conf interface{}) error {
	b, err := json.MarshalIndent(conf, "", "\t")
	if err != nil {
		return errors.WithStack(err)
	}
	return xbase.WriteFile(path, b)
}
