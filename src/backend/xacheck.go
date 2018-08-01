/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package backend

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"config"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/xlog"
)

const (
	xacheckJSONFile = "xacheck.json"
)

const (
	xaRedoError = "error"
	xaRedoOk    = "ok"

	txnXACommitErrStateCommit   = "commit"
	txnXACommitErrStateRollback = "rollback"
)

// XaCommitErr tuple.
type XaCommitErr struct {
	Time     string   `json:"time"`
	Xaid     string   `json:"xaid"`
	State    string   `json:"state"`
	Backends []string `json:"backends"`
}

// XaCommitErrs tuple
type XaCommitErrs struct {
	Logs []*XaCommitErr `json:"xacommit-errs"`
}

// XaCheck tuple.
type XaCheck struct {
	log     *xlog.Log
	dir     string
	scatter *Scatter
	retrys  map[string]*XaCommitErr
	done    chan bool
	ticker  *time.Ticker
	wg      sync.WaitGroup
	mu      sync.RWMutex
}

// NewXaCheck creates the XaCheck tuple.
func NewXaCheck(scatter *Scatter, conf *config.ScatterConfig) *XaCheck {
	return &XaCheck{
		log:     scatter.log,
		dir:     conf.XaCheckDir,
		scatter: scatter,
		retrys:  make(map[string]*XaCommitErr),
		done:    make(chan bool),
		ticker:  time.NewTicker(time.Duration(time.Second * time.Duration(conf.XaCheckInterval))),
	}
}

// Init used to init xa check goroutine.
func (xc *XaCheck) Init() error {
	log := xc.log

	// If the xc.dir is already a directory, MkdirAll does nothing
	// if the dir is one file, return err
	if err := os.MkdirAll(xc.dir, 0744); err != nil {
		return err
	}

	if err := xc.LoadXaCommitErrLogs(); err != nil {
		return err
	}

	xc.wg.Add(1)
	go func(dc *XaCheck) {
		defer dc.wg.Done()
		dc.xaCommitcheck()
	}(xc)

	log.Info("xacheck.init.done")
	return nil
}

func (xc *XaCheck) addXaCommitErrLog(new *XaCommitErr) error {
	xc.mu.Lock()
	defer xc.mu.Unlock()

	log := xc.log
	log.Info("xc.addXaCommitErrLog.add:+%v", new)

	if _, ok := xc.retrys[new.Xaid]; ok {
		return errors.Errorf("xacheck.addXACommitErrLog.xaid[%v].backend[%v].duplicate", new.Xaid)
	}

	xc.retrys[new.Xaid] = new
	return nil
}

// flushConfig is used to write the xaCommitErrlogs to the file.
func (xc *XaCheck) flushXaCommitErrLog() error {
	xc.mu.Lock()
	defer xc.mu.Unlock()

	log := xc.log
	file := path.Join(xc.dir, xacheckJSONFile)

	var xaCommitErrs XaCommitErrs
	for _, v := range xc.retrys {
		xaCommitErrs.Logs = append(xaCommitErrs.Logs, v)
	}

	log.Info("xacheck.flush.to.file[%v].backends.conf:%+v", file, xaCommitErrs)
	if err := config.WriteConfig(file, xaCommitErrs); err != nil {
		log.Panicf("xacheck.flush.config.to.file[%v].error:%v", file, err)
		return err
	}
	return nil
}

// WriteXaCommitErrLog is used to write the xaCommitErrLog into the redo file.
func (xc *XaCheck) WriteXaCommitErrLog(txn *Txn, state string) error {
	xaCommitErr := &XaCommitErr{
		Time:     time.Now().Format("20060102150405"),
		Xaid:     txn.xid,
		State:     state,
		Backends: []string{},
	}

	// add the xaCommitErrLog to xacheck
	if err := xc.addXaCommitErrLog(xaCommitErr); err != nil {
		return errors.WithStack(err)
	}

	// TODO if the Radon crash
	// flush the mem to the file
	if err := xc.flushXaCommitErrLog(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// commitRetryBackends in which the Backends in the retrys maybe change
func (xc *XaCheck) commitRetryBackends(query string, scatter *Scatter, xid string) (bool, error) {
	backends := scatter.Backends()
	log := xc.log

	// if the backend is empty, output warning log.
	if len(backends) == 0 {
		log.Error("xacheck.commitRetryBackends.backend.empty.")
		return false, errors.New("xacheck.backend.empty")
	}

	txn, err := scatter.CreateTransaction()
	if err != nil {
		log.Error("xacheck.commitRetryBackends.create.transaction.error:[%v]", err)
		return false, err
	}
	// TODO set state
	txn.state.Set(int32(txnStateCommitting))
	xaRecoverQuery := "xa recover"

	var validBackends []string

	for _, backend := range backends {
		// TODO detail the result and err, speciallly to the error
		result, err := txn.ExecuteOnThisBackend(backend, xaRecoverQuery)
		if result != nil {
			for _, row := range(result.Rows) {

				if (len(result.Fields) != 4) {
					break
				}

				valStr := string(row[3].Raw())
				if strings.EqualFold(valStr, xid) {
					validBackends = append(validBackends, backend)
				}
			}
		} else {
			log.Error("xacheck.commitRetryBackends.recover.un1397error:[%v]", err)
		}
	}
	txn.Finish()

	txn, err = scatter.CreateTransaction()
	if err != nil {
		log.Error("xacheck.commitRetryBackends.create.transaction.error:[%v]", err)
		return false, err
	}
	defer txn.Finish()

	ExecuteOKCount := 0
	for _, backend := range validBackends {
		_, err = txn.ExecuteOnThisBackend(backend, query)
		if err == nil {
			log.Info("xacheck.commitRetryBackends.query[%v].success.backend[%v]", query, backend)
			ExecuteOKCount++
		} else {
			log.Warning("xacheck.commitRetryBackends.query[%v].backend[%v].error[%T]:%+v", query, backend, err, err)
		}
	}

	if ExecuteOKCount == len(validBackends) {
		return true, nil
	}

	return false, nil
}

// xaCommitsRetryMain in which the retrys maybe change
func (xc *XaCheck) xaCommitsRetryMain() error {
	xc.mu.Lock()
	defer xc.mu.Unlock()

	log := xc.log
	retrys := xc.retrys
	if (len(retrys) > 0) {
		log.Info("xacheck.commit.retry %v.", retrys)
	}

	for _, retry := range retrys {
		query := fmt.Sprintf("xa %s '%s' ", retry.State, retry.Xaid)
		committed, err := xc.commitRetryBackends(query, xc.scatter, retry.Xaid);
		if err != nil {
			log.Warning("xacheck.commits.retry failed.")
		}

		if committed {
			delete(xc.retrys, retry.Xaid)
		}
	}
	return nil
}

func (xc *XaCheck) xaCommitsRetry() error {
	xc.mu.Lock()
	oldCountXaRetrys := len(xc.retrys)
	xc.mu.Unlock()

	// xaCommitsRetryMain
	if err := xc.xaCommitsRetryMain(); err != nil {
		return errors.WithStack(err)
	}

	// when the count of the old XaRedos >0, flush to the file
	// to avoid creating the empty file xaredolog.json about "xaredologs": null when the count is 0
	if oldCountXaRetrys > 0 {
		if err := xc.flushXaCommitErrLog(); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (xc *XaCheck) xaCommitcheck() {
	defer xc.ticker.Stop()
	for {
		select {
		case <-xc.ticker.C:
			xc.xaCommitsRetry()
		case <-xc.done:
			return
		}
	}
}

// ReadXaCommitErrLogs is used to read the Xaredologs config from the data.
func (xc *XaCheck) ReadXaCommitErrLogs(data string) (*XaCommitErrs, error) {
	s := &XaCommitErrs{}
	if err := json.Unmarshal([]byte(data), s); err != nil {
		return nil, errors.WithStack(err)
	}
	return s, nil
}

// LoadXaCommitErrLogs is used to load all XaCommitErr from metadir/xacheck.json file.
func (xc *XaCheck) LoadXaCommitErrLogs() error {
	log := xc.log
	metadir := xc.dir
	file := path.Join(metadir, xacheckJSONFile)

	if _, err := os.Stat(file); os.IsNotExist(err) {
		// not Creating it if the xacheck log doesn't exist.
		// to avoid creating the empty file xaredolog.json about "xaredologs": null
		// the xaredolog.json will be created when the xaredolog are generated.
		return nil
	}

	data, err := ioutil.ReadFile(file)
	if err != nil {
		log.Error("xacheck.LoadXaCommitErrLogs.readfile[%v].error:%v", file, err)
		return err
	}

	retrys, err := xc.ReadXaCommitErrLogs(string(data))
	if err != nil {
		log.Error("xacheck.LoadXaCommitErrLogs.readfile.to.xacheck[%v].error:%v", file, err)
		return err
	}

	for _, new := range retrys.Logs {
		if err := xc.addXaCommitErrLog(new); err != nil {
			log.Error("xacheck.add.xaid[%s] on backends[%v].error:%v", new.Xaid, new.Backends, err)
			return err
		}

		log.Info("xacheck.load.xaid:%+v", new.Xaid)
	}
	return nil
}

// Close is used to close the xacheck goroutine
func (xc *XaCheck) Close() {
	close(xc.done)
	xc.wg.Wait()
}

// RemoveXaCommitErrLogs is only used to test to avoid the noise,
// XaCommitErrLogs can not be removed in the production environment, it is so important.
//
func (xc *XaCheck) RemoveXaCommitErrLogs() error {
	return os.RemoveAll(xc.dir)
}
