/*
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package xlog

var (
	DefaultName  = " "
	DefaultLevel = DEBUG
)

type Options struct {
	Name  string
	Level LogLevel
}

type Option func(*Options)

func newOptions(opts ...Option) *Options {
	opt := &Options{}
	for _, o := range opts {
		o(opt)
	}

	if len(opt.Name) == 0 {
		opt.Name = DefaultName
	}

	if opt.Level == 0 {
		opt.Level = DefaultLevel
	}

	return opt
}

// Log Name
func Name(v string) Option {
	return func(o *Options) {
		o.Name = v
	}
}

// Log Level
func Level(v LogLevel) Option {
	return func(o *Options) {
		o.Level = v
	}
}
