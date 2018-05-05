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
	defaultName  = " "
	defaultLevel = DEBUG
)

// Options used for the options of the xlog.
type Options struct {
	Name  string
	Level LogLevel
}

// Option func.
type Option func(*Options)

func newOptions(opts ...Option) *Options {
	opt := &Options{}
	for _, o := range opts {
		o(opt)
	}

	if len(opt.Name) == 0 {
		opt.Name = defaultName
	}

	if opt.Level == 0 {
		opt.Level = defaultLevel
	}
	return opt
}

// Name used to set the name.
func Name(v string) Option {
	return func(o *Options) {
		o.Name = v
	}
}

// Level used to set the log level.
func Level(v LogLevel) Option {
	return func(o *Options) {
		o.Level = v
	}
}
