/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package xcontext

import (
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// RequestMode type.
type RequestMode int

const (
	// ReqNormal mode will send the query to the backend which computed by the planner.
	// This is the default mode.
	ReqNormal RequestMode = iota

	// ReqScatter mode will send the RawQuery to all backends.
	ReqScatter

	// ReqSingle mode will send the RawQuery to the first backend which computed by the scatter.
	ReqSingle
)

// TxnMode type.
type TxnMode int

const (
	// TxnNone enum.
	TxnNone TxnMode = iota
	// TxnRead enum.
	TxnRead
	// TxnWrite enum.
	TxnWrite
)

// ResultContext tuple.
type ResultContext struct {
	Results *sqltypes.Result
}

// NewResultContext returns the result context.
func NewResultContext() *ResultContext {
	return &ResultContext{}
}

// RequestContext tuple.
type RequestContext struct {
	RawQuery string
	Mode     RequestMode
	TxnMode  TxnMode
	Querys   []QueryTuple
}

// NewRequestContext creates RequestContext
// The default Mode is ReqNormal
func NewRequestContext() *RequestContext {
	return &RequestContext{}
}

// QueryTuple tuple.
type QueryTuple struct {
	// Query string.
	Query string

	// Backend name.
	Backend string

	// Range info.
	Range string
}

// QueryTuples represents the query tuple slice.
type QueryTuples []QueryTuple

// Len impl.
func (q QueryTuples) Len() int { return len(q) }

// Swap impl.
func (q QueryTuples) Swap(i, j int) { q[i], q[j] = q[j], q[i] }

// Less impl.
func (q QueryTuples) Less(i, j int) bool { return q[i].Backend < q[j].Backend }
