/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package router

import (
	"config"

	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// GlobalRange tuple.
type GlobalRange struct {
	str string
}

// String returns ''.
func (r *GlobalRange) String() string {
	return r.str
}

// Less impl.
func (r *GlobalRange) Less(b KeyRange) bool {
	return false
}

// Global tuple.
type Global struct {
	log *xlog.Log

	// global method
	typ MethodType

	// table config
	conf *config.TableConfig

	Segments []Segment `json:",omitempty"`
}

// NewGlobal creates new global.
func NewGlobal(log *xlog.Log, conf *config.TableConfig) *Global {
	return &Global{
		log:      log,
		conf:     conf,
		typ:      methodTypeGlobal,
		Segments: make([]Segment, 0, 16),
	}
}

// Build used to build Segments from schema config
func (g *Global) Build() error {
	for _, part := range g.conf.Partitions {
		partition := Segment{
			Table:   part.Table,
			Backend: part.Backend,
			Range: &GlobalRange{
				str: "",
			},
		}
		// Segments
		g.Segments = append(g.Segments, partition)
	}

	return nil
}

// Lookup used to lookup partition(s)
func (g *Global) Lookup(start *sqlparser.SQLVal, end *sqlparser.SQLVal) ([]Segment, error) {
	// global returns all partitions
	return g.Segments, nil
}

// Type returns the global type.
func (g *Global) Type() MethodType {
	return g.typ
}
