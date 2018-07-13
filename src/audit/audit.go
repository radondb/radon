/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package audit

import (
	"config"
	"os"
	"path/filepath"
	"sync"
	"time"

	"xbase"

	"github.com/xelabs/go-mysqlstack/xlog"
)

const (
	prefix    = "audit-"
	extension = ".log"
)

const (
	// NULL enum.
	NULL = "N"

	// READ enum.
	READ = "R"

	// WRITE enum.
	WRITE = "W"

	// ALL enum.
	ALL = "A"
)

// easyjson:json
// NOTE:
// if the event changes, we must re-generate the audit_easyjson.go file by 'easyjson src/audit/audit.go' command.
type event struct {
	Start       time.Time     `json:"start"`        // Time the query was start.
	End         time.Time     `json:"end"`          // Time the query was end.
	Cost        time.Duration `json:"cost"`         // Cost.
	User        string        `json:"user"`         // User.
	UserHost    string        `json:"user_host"`    // User and host combination.
	ThreadID    uint32        `json:"thread_id"`    // Thread id.
	CommandType string        `json:"command_type"` // Type of command.
	Argument    string        `json:"argument"`     // Full query.
	QueryRows   uint64        `json:"query_rows"`   // Query rows.
}

// Audit tuple.
type Audit struct {
	log    *xlog.Log
	conf   *config.AuditConfig
	ticker *time.Ticker
	queue  chan *event
	done   chan bool
	rfile  xbase.RotateFile
	wg     sync.WaitGroup
}

// NewAudit creates the new audit.
func NewAudit(log *xlog.Log, conf *config.AuditConfig) *Audit {
	return &Audit{
		log:    log,
		conf:   conf,
		done:   make(chan bool),
		queue:  make(chan *event, 1024),
		ticker: time.NewTicker(time.Duration(time.Second * 300)), // 5 minutes
		rfile:  xbase.NewRotateFile(conf.LogDir, prefix, extension, conf.MaxSize),
	}
}

// Init used to create the log dir, if EXISTS we do onthing.
func (a *Audit) Init() error {
	log := a.log

	log.Info("audit.init.conf:%+v", a.conf)
	if err := os.MkdirAll(a.conf.LogDir, 0744); err != nil {
		return err
	}

	a.wg.Add(1)
	go func(audit *Audit) {
		defer a.wg.Done()
		a.eventConsumer()
	}(a)

	a.wg.Add(1)
	go func(audit *Audit) {
		defer a.wg.Done()
		a.purge()
	}(a)
	log.Info("audit.init.done")
	return nil
}

// LogReadEvent used to handle the read-only event.
func (a *Audit) LogReadEvent(t string, user string, host string, threadID uint32, query string, affected uint64, startTime time.Time) {
	if a.conf.Mode == ALL || a.conf.Mode == READ {
		e := &event{
			Start:       startTime,
			End:         time.Now(),
			Cost:        time.Since(startTime),
			User:        user,
			UserHost:    host,
			ThreadID:    threadID,
			CommandType: t,
			Argument:    query,
			QueryRows:   affected,
		}
		a.queue <- e
	}
}

// LogWriteEvent used to handle the write event.
func (a *Audit) LogWriteEvent(t string, user string, host string, threadID uint32, query string, affected uint64, startTime time.Time) {
	if a.conf.Mode == ALL || a.conf.Mode == WRITE {
		e := &event{
			Start:       startTime,
			End:         time.Now(),
			Cost:        time.Since(startTime),
			User:        user,
			UserHost:    host,
			ThreadID:    threadID,
			CommandType: t,
			Argument:    query,
			QueryRows:   affected,
		}
		a.queue <- e
	}
}

// Close used to close the audit log.
func (a *Audit) Close() {
	// wait the queue event flush to file.
	close(a.done)
	close(a.queue)
	a.wg.Wait()
	a.rfile.Sync()
	a.rfile.Close()
	a.log.Info("audit.closed")
}

func (a *Audit) eventConsumer() {
	for e := range a.queue {
		a.writeEvent(e)
	}
}

func (a *Audit) writeEvent(e *event) {
	log := a.log
	b, err := e.MarshalJSON()
	if err != nil {
		b = []byte(err.Error())
	}
	b = append(b, '\n')

	// write
	_, err = a.rfile.Write(b)
	if err != nil {
		log.Error("audit.write.file.error:%v", err)
	}
}

func (a *Audit) purge() {
	defer a.ticker.Stop()
	for {
		select {
		case <-a.ticker.C:
			a.doPurge()
		case <-a.done:
			return
		}
	}
}

func (a *Audit) doPurge() {
	log := a.log
	if a.conf.ExpireHours == 0 {
		return
	}

	oldLogs, err := a.rfile.GetOldLogInfos()
	if err != nil {
		log.Error("audit.get.old.loginfos.error:%v", err)
		return
	}

	for _, old := range oldLogs {
		diff := time.Now().UTC().Sub(time.Unix(0, old.Ts))
		if int(diff.Hours()) > a.conf.ExpireHours {
			os.Remove(filepath.Join(a.conf.LogDir, old.Name))
		}
	}
}
