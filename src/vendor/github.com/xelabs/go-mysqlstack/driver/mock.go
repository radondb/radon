/*
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package driver

import (
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/xlog"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

func randomPort(min int, max int) int {
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	d, delta := min, (max - min)
	if delta > 0 {
		d += rand.Intn(int(delta))
	}
	return d
}

type exprResult struct {
	expr   *regexp.Regexp
	result *sqltypes.Result
	err    error
}

// CondType used for Condition type.
type CondType int

const (
	// COND_NORMAL enum.
	COND_NORMAL CondType = iota
	// COND_DELAY enum.
	COND_DELAY
	// COND_ERROR enum.
	COND_ERROR
	// COND_PANIC enum.
	COND_PANIC
	// COND_STREAM enum.
	COND_STREAM
)

// Cond presents a condition tuple.
type Cond struct {
	// Cond type.
	Type CondType

	// Query string
	Query string

	// Query results
	Result *sqltypes.Result

	// Panic or Not
	Panic bool

	// Return Error if Error is not nil
	Error error

	// Delay(ms) for results return
	Delay int
}

// CondList presents a list of Cond.
type CondList struct {
	len   int
	idx   int
	conds []Cond
}

// SessionTuple presents a session tuple.
type SessionTuple struct {
	session *Session
	closed  bool
	killed  chan bool
}

// TestHandler is the handler for testing.
type TestHandler struct {
	log      *xlog.Log
	mu       sync.RWMutex
	conds    map[string]*Cond
	condList map[string]*CondList
	ss       map[uint32]*SessionTuple

	// patterns is a list of regexp to results.
	patterns      []exprResult
	patternErrors []exprResult

	// How many times a query was called.
	queryCalled map[string]int
}

// NewTestHandler creates new Handler.
func NewTestHandler(log *xlog.Log) *TestHandler {
	return &TestHandler{
		log:         log,
		ss:          make(map[uint32]*SessionTuple),
		conds:       make(map[string]*Cond),
		queryCalled: make(map[string]int),
		condList:    make(map[string]*CondList),
	}
}

func (th *TestHandler) setCond(cond *Cond) {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.conds[strings.ToLower(cond.Query)] = cond
	th.queryCalled[strings.ToLower(cond.Query)] = 0
}

// ResetAll resets all querys.
func (th *TestHandler) ResetAll() {
	th.mu.Lock()
	defer th.mu.Unlock()
	for k := range th.conds {
		delete(th.conds, k)
	}
	th.patterns = make([]exprResult, 0, 4)
	th.patternErrors = make([]exprResult, 0, 4)
}

// ResetPatternErrors used to reset all the errors pattern.
func (th *TestHandler) ResetPatternErrors() {
	th.patternErrors = make([]exprResult, 0, 4)
}

// ResetErrors used to reset all the errors.
func (th *TestHandler) ResetErrors() {
	for k, v := range th.conds {
		if v.Type == COND_ERROR {
			delete(th.conds, k)
		}
	}
}

// SessionCheck implements the interface.
func (th *TestHandler) SessionCheck(s *Session) error {
	//th.log.Debug("[%s].coming.db[%s].salt[%v].scramble[%v]", s.Addr(), s.Schema(), s.Salt(), s.Scramble())
	return nil
}

// AuthCheck implements the interface.
func (th *TestHandler) AuthCheck(s *Session) error {
	user := s.User()
	if user != "mock" {
		return sqldb.NewSQLErrorf(sqldb.ER_ACCESS_DENIED_ERROR, "Access denied for user '%v'", user)
	}
	return nil
}

// ServerVersion implements the interface.
func (th *TestHandler) ServerVersion() string {
	return "FakeDB"
}

// SetServerVersion implements the interface.
func (th *TestHandler) SetServerVersion() {
	return
}

// NewSession implements the interface.
func (th *TestHandler) NewSession(s *Session) {
	th.mu.Lock()
	defer th.mu.Unlock()
	st := &SessionTuple{
		session: s,
		killed:  make(chan bool, 2),
	}
	th.ss[s.ID()] = st
}

// SessionInc implements the interface.
func (th *TestHandler) SessionInc(s *Session) {

}

// SessionDec implements the interface.
func (th *TestHandler) SessionDec(s *Session) {

}

// SessionClosed implements the interface.
func (th *TestHandler) SessionClosed(s *Session) {
	th.mu.Lock()
	defer th.mu.Unlock()
	delete(th.ss, s.ID())
}

// ComInitDB implements the interface.
func (th *TestHandler) ComInitDB(s *Session, db string) error {
	if strings.HasPrefix(db, "xx") {
		return fmt.Errorf("mock.cominit.db.error: unkonw database[%s]", db)
	}
	return nil
}

// ComQuery implements the interface.
func (th *TestHandler) ComQuery(s *Session, query string, bindVariables map[string]*querypb.BindVariable, callback func(qr *sqltypes.Result) error) error {
	log := th.log
	query = strings.ToLower(query)

	th.mu.Lock()
	th.queryCalled[query]++
	cond := th.conds[query]
	sessTuple := th.ss[s.ID()]
	th.mu.Unlock()

	if cond != nil {
		switch cond.Type {
		case COND_DELAY:
			log.Debug("test.handler.delay:%s,time:%dms", query, cond.Delay)
			select {
			case <-sessTuple.killed:
				sessTuple.closed = true
				return fmt.Errorf("mock.session[%v].query[%s].was.killed", s.ID(), query)
			case <-time.After(time.Millisecond * time.Duration(cond.Delay)):
				log.Debug("mock.handler.delay.done...")
			}
			return callback(cond.Result)
		case COND_ERROR:
			return cond.Error
		case COND_PANIC:
			log.Panic("mock.handler.panic....")
		case COND_NORMAL:
			return callback(cond.Result)
		case COND_STREAM:
			flds := cond.Result.Fields
			// Send Fields for stream.
			qr := &sqltypes.Result{Fields: flds, State: sqltypes.RStateFields}
			if err := callback(qr); err != nil {
				return fmt.Errorf("mock.handler.send.stream.error:%+v", err)
			}

			// Send Row by row for stream.
			for _, row := range cond.Result.Rows {
				qr := &sqltypes.Result{Fields: flds, State: sqltypes.RStateRows}
				qr.Rows = append(qr.Rows, row)
				if err := callback(qr); err != nil {
					return fmt.Errorf("mock.handler.send.stream.error:%+v", err)
				}
			}

			// Send EOF for stream.
			qr = &sqltypes.Result{Fields: flds, State: sqltypes.RStateFinished}
			if err := callback(qr); err != nil {
				return fmt.Errorf("mock.handler.send.stream.error:%+v", err)
			}
			return nil
		}
	}

	// kill filter.
	if strings.HasPrefix(query, "kill") {
		if id, err := strconv.ParseUint(strings.Split(query, " ")[1], 10, 32); err == nil {
			th.mu.Lock()
			if sessTuple, ok := th.ss[uint32(id)]; ok {
				log.Debug("mock.session[%v].to.kill.the.session[%v]...", s.ID(), id)
				if !sessTuple.closed {
					sessTuple.killed <- true
				}
				delete(th.ss, uint32(id))
				sessTuple.session.Close()
			}
			th.mu.Unlock()
		}
		return callback(&sqltypes.Result{})
	}

	th.mu.Lock()
	defer th.mu.Unlock()
	// Check query patterns from AddQueryPattern().
	for _, pat := range th.patternErrors {
		if pat.expr.MatchString(query) {
			return pat.err
		}
	}
	for _, pat := range th.patterns {
		if pat.expr.MatchString(query) {
			return callback(pat.result)
		}
	}

	if v, ok := th.condList[query]; ok {
		idx := 0
		if v.idx >= v.len {
			v.idx = 0
		} else {
			idx = v.idx
			v.idx++
		}
		return callback(v.conds[idx].Result)
	}
	return fmt.Errorf("mock.handler.query[%v].error[can.not.found.the.cond.please.set.first]", query)
}

// AddQuery used to add a query and its expected result.
func (th *TestHandler) AddQuery(query string, result *sqltypes.Result) {
	th.setCond(&Cond{Type: COND_NORMAL, Query: query, Result: result})
}

// AddQuerys used to add new query rule.
func (th *TestHandler) AddQuerys(query string, results ...*sqltypes.Result) {
	cl := &CondList{}
	for _, r := range results {
		cond := Cond{Type: COND_NORMAL, Query: query, Result: r}
		cl.conds = append(cl.conds, cond)
		cl.len++
	}
	th.condList[query] = cl
}

// AddQueryDelay used to add a query and returns the expected result after delay_ms.
func (th *TestHandler) AddQueryDelay(query string, result *sqltypes.Result, delayMs int) {
	th.setCond(&Cond{Type: COND_DELAY, Query: query, Result: result, Delay: delayMs})
}

// AddQueryStream used to add a stream query.
func (th *TestHandler) AddQueryStream(query string, result *sqltypes.Result) {
	th.setCond(&Cond{Type: COND_STREAM, Query: query, Result: result})
}

// AddQueryError used to add a query which will be rejected by a error.
func (th *TestHandler) AddQueryError(query string, err error) {
	th.setCond(&Cond{Type: COND_ERROR, Query: query, Error: err})
}

// AddQueryPanic used to add query but underflying blackhearted.
func (th *TestHandler) AddQueryPanic(query string) {
	th.setCond(&Cond{Type: COND_PANIC, Query: query})
}

// AddQueryPattern adds an expected result for a set of queries.
// These patterns are checked if no exact matches from AddQuery() are found.
// This function forces the addition of begin/end anchors (^$) and turns on
// case-insensitive matching mode.
// This code was derived from https://github.com/youtube/vitess.
func (th *TestHandler) AddQueryPattern(queryPattern string, expectedResult *sqltypes.Result) {
	if len(expectedResult.Rows) > 0 && len(expectedResult.Fields) == 0 {
		panic(fmt.Errorf("Please add Fields to this Result so it's valid: %v", queryPattern))
	}
	expr := regexp.MustCompile("(?is)^" + queryPattern + "$")
	result := *expectedResult
	th.mu.Lock()
	defer th.mu.Unlock()
	th.patterns = append(th.patterns, exprResult{expr, &result, nil})
}

// AddQueryErrorPattern used to add an query pattern with errors.
func (th *TestHandler) AddQueryErrorPattern(queryPattern string, err error) {
	expr := regexp.MustCompile("(?is)^" + queryPattern + "$")
	th.mu.Lock()
	defer th.mu.Unlock()
	th.patternErrors = append(th.patternErrors, exprResult{expr, nil, err})
}

// GetQueryCalledNum returns how many times db executes a certain query.
// This code was derived from https://github.com/youtube/vitess.
func (th *TestHandler) GetQueryCalledNum(query string) int {
	th.mu.Lock()
	defer th.mu.Unlock()
	num, ok := th.queryCalled[strings.ToLower(query)]
	if !ok {
		return 0
	}
	return num
}

// MockMysqlServer creates a new mock mysql server.
func MockMysqlServer(log *xlog.Log, h Handler) (svr *Listener, err error) {
	port := randomPort(10000, 60000)
	return mockMysqlServer(log, port, h)
}

// MockMysqlServerWithPort creates a new mock mysql server with port.
func MockMysqlServerWithPort(log *xlog.Log, port int, h Handler) (svr *Listener, err error) {
	return mockMysqlServer(log, port, h)
}

func mockMysqlServer(log *xlog.Log, port int, h Handler) (svr *Listener, err error) {
	addr := fmt.Sprintf(":%d", port)
	for i := 0; i < 5; i++ {
		if svr, err = NewListener(log, addr, h); err != nil {
			port = randomPort(5000, 20000)
			addr = fmt.Sprintf("127.0.0.1:%d", port)
		} else {
			break
		}
	}
	if err != nil {
		return nil, err
	}

	go func() {
		svr.Accept()
	}()
	time.Sleep(100 * time.Millisecond)
	log.Debug("mock.server[%v].start...", addr)
	return
}
