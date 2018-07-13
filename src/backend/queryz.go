/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 * This code was derived from https://github.com/youtube/vitess.
 */

package backend

import (
	"sort"
	"sync"
	"time"

	"xbase"
)

// QueryDetail is a simple wrapper for Query
type QueryDetail struct {
	ID     uint64
	connID uint32
	query  string
	conn   Connection
	start  time.Time
}

// NewQueryDetail creates a new QueryDetail
func NewQueryDetail(conn Connection, query string) *QueryDetail {
	q := xbase.TruncateQuery(query, 256)
	return &QueryDetail{conn: conn, connID: conn.ID(), query: q, start: time.Now()}
}

// Queryz holds a thread safe list of QueryDetails
type Queryz struct {
	ID           uint64
	mu           sync.RWMutex
	queryDetails map[uint64]*QueryDetail
}

// NewQueryz creates a new Queryz
func NewQueryz() *Queryz {
	return &Queryz{queryDetails: make(map[uint64]*QueryDetail)}
}

// Add adds a QueryDetail to Queryz
func (qz *Queryz) Add(qd *QueryDetail) {
	qz.mu.Lock()
	defer qz.mu.Unlock()
	qz.ID++
	qd.ID = qz.ID
	qz.queryDetails[qd.ID] = qd
}

// Remove removes a QueryDetail from Queryz
func (qz *Queryz) Remove(qd *QueryDetail) {
	qz.mu.Lock()
	defer qz.mu.Unlock()
	delete(qz.queryDetails, qd.ID)
}

// QueryDetailzRow is used for rendering QueryDetail in a template
type QueryDetailzRow struct {
	Start    time.Time
	Duration time.Duration
	ConnID   uint32
	Query    string
	Address  string
	Color    string
}

type byStartTime []QueryDetailzRow

func (a byStartTime) Len() int           { return len(a) }
func (a byStartTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byStartTime) Less(i, j int) bool { return a[i].Start.Before(a[j].Start) }

// GetQueryzRows returns a list of QueryDetailzRow sorted by start time
func (qz *Queryz) GetQueryzRows() []QueryDetailzRow {
	qz.mu.RLock()
	rows := []QueryDetailzRow{}
	for _, qd := range qz.queryDetails {
		row := QueryDetailzRow{
			Query:    qd.query,
			Address:  qd.conn.Address(),
			Start:    qd.start,
			Duration: time.Since(qd.start),
			ConnID:   qd.connID,
		}
		if row.Duration < 10*time.Millisecond {
			row.Color = "low"
		} else if row.Duration < 100*time.Millisecond {
			row.Color = "medium"
		} else {
			row.Color = "high"
		}

		rows = append(rows, row)
	}
	qz.mu.RUnlock()
	sort.Sort(byStartTime(rows))
	return rows
}
