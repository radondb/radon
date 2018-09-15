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
	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/xlog"
)

const (
	// versionJSONFile version file name.
	xaredologJSONFile = "xaredolog.json"
	//Metadir = "/tmp/"
)

const (
	xaRedoError = "error"
	xaRedoOk    = "ok"
)

// XaCommitErr tuple.
type XaCommitErr struct {
	Ts       string   `json:"ts"`
	Xaid     string   `json:"xaid"`
	Stat     string   `json:"status"`
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

	/*
		// may be deleted?
		if err := xc.xaCommitsRetryMem(); err != nil {
			log.Error("xacheck.init.retry.first.failed.,error:%+v", err)
		}
	*/

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
	log.Info("xacheck.add:+%v", new)

	if old, ok := xc.retrys[new.Xaid]; ok {
		//return errors.Errorf("xacheck.addXaRedo.xaid[%v].duplicate", xaredo.Xaid)
		for _, oldBack := range old.Backends {
			for _, newBack := range new.Backends {
				if strings.EqualFold(oldBack, newBack) {
					return errors.Errorf("xacheck.addXaRedo.xaid[%v].backend[%v].duplicate", new.Xaid, newBack)
				}
			}
		}

		xc.retrys[new.Xaid].Backends = append(xc.retrys[new.Xaid].Backends, new.Backends...)
		return nil
	}

	xc.retrys[new.Xaid] = new
	return nil
}

// FlushConfig used to write the xaCommitErrlogs to the file.
func (xc *XaCheck) flushConfig() error {
	xc.mu.Lock()
	defer xc.mu.Unlock()

	log := xc.log
	file := path.Join(xc.dir, xaredologJSONFile)

	var xaCommitErrs XaCommitErrs
	for _, v := range xc.retrys {
		xaCommitErrs.Logs = append(xaCommitErrs.Logs, v)
	}

	log.Warning("xacheck.flush.to.file[%v].backends.conf:%+v", file, xaCommitErrs)
	if err := config.WriteConfig(file, xaCommitErrs); err != nil {
		log.Panicf("xacheck.flush.config.to.file[%v].error:%v", file, err)
		return err
	}

	/*
		if err := config.UpdateVersion(scatter.metadir); err != nil {
			log.Panicf("scatter.flush.config.update.version.error:%v", err)
			return err
		}
	*/
	return nil
}

// WriteXaLog used to write the xaCommitErrLog into the redo file.
func (xc *XaCheck) WriteXaCommitErrLog(txn *Txn, back string) error {
	xaCommitErr := &XaCommitErr{
		Ts:       time.Now().Format("20060102150405"),
		Xaid:     txn.xid,
		Stat:     xaRedoError,
		Backends: []string{back},
	}

	// add the xaCommitErrLog to xacheck
	if err := xc.addXaCommitErrLog(xaCommitErr); err != nil {
		return errors.WithStack(err)
	}

	// TODO if the Radon crash
	// flush the mem to the file
	if err := xc.flushConfig(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// the Backends in the retrys maybe change
func (xc *XaCheck) commitRetryBackends(query string, scatter *Scatter, xid string) error {
	backends := scatter.Backends()
	log := xc.log

	// if the backend is empty, output warning log.
	if len(backends) == 0 {
		log.Error("xacheck.commitRetryBackends.backend.empty.")
		return errors.New("xacheck.backend.empty")
	}

	txn, err := scatter.CreateTransaction()
	if err != nil {
		log.Error("xacheck.commitRetryBackends.create.transaction.error:[%v]", err)
		return err
	}
	txn.state.Set(int32(txnStateCommitting))

	defer txn.Finish()

	for _, backend := range backends {
		_, err = txn.ExecuteOnThisBackend(backend, query)
		if err != nil {
			log.Warning("xacheck.commitRetryBackends.query[%v].backend[%v].error[%T]:%+v", query, backend, err, err)
			if sqlErr, ok := err.(*sqldb.SQLError); ok {
				// XAE04:
				// https://dev.mysql.com/doc/refman/5.5/en/error-messages-server.html#error_er_xaer_nota
				// Error: 1397 SQLSTATE: XAE04 (ER_XAER_NOTA)
				// Message: XAER_NOTA: Unknown XID
				if sqlErr.Num == 1397 {
					log.Warning("txn.xa.[%v].XAE04.error....", txn.State())
					continue
				}
			}
		} else {
			log.Info("xacheck.commitRetryBackends.query[%v].success.backend[%v]", query, backend)
			for i, xaback := range xc.retrys[xid].Backends {
				// the backends in the xaredo must be in the scatter.backends
				// otherwise the logic need to change
				if strings.EqualFold(backend, xaback) {
					xc.retrys[xid].Backends = append(xc.retrys[xid].Backends[:i], xc.retrys[xid].Backends[i+1:]...)
				}
			}
		}
	}
	return err
}

// the retrys maybe change
func (xc *XaCheck) xaCommitsRetryMem() error {
	xc.mu.Lock()
	defer xc.mu.Unlock()

	log := xc.log
	retrys := xc.retrys
	if (len(retrys) > 0) {
		log.Info("xacheck.commit.retry %v.", retrys)
	}

	for _, xaRedo := range retrys {
		query := fmt.Sprintf("xa commit '%s' ", xaRedo.Xaid)
		if err := xc.commitRetryBackends(query, xc.scatter, xaRedo.Xaid); err != nil {
			log.Warning("xacheck.commits.retry failed.")
			//continue
		}

		if len(xc.retrys[xaRedo.Xaid].Backends) == 0 {
			delete(xc.retrys, xaRedo.Xaid)
		}
	}
	return nil
}

func (xc *XaCheck) xaCommitsRetry() error {
	xc.mu.Lock()
	oldCountXaRedos := len(xc.retrys)
	xc.mu.Unlock()

	// xaCommitsRetryMem
	if err := xc.xaCommitsRetryMem(); err != nil {
		return errors.WithStack(err)
	}

	// when the count of the old XaRedos >0, flush to the file
	// to avoid creating the empty file xaredolog.json about "xaredologs": null when the count is 0
	if oldCountXaRedos > 0 {
		if err := xc.flushConfig(); err != nil {
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

// ReadXaCommitErrLogs used to read the Xaredologs config from the data.
func (xc *XaCheck) ReadXaCommitErrLogs(data string) (*XaCommitErrs, error) {
	s := &XaCommitErrs{}
	if err := json.Unmarshal([]byte(data), s); err != nil {
		return nil, errors.WithStack(err)
	}
	return s, nil
}

// LoadXaCommitErrLogs is used to load all backends from metadir/backend.json file.
func (xc *XaCheck) LoadXaCommitErrLogs() error {

	// Do clear first.
	//xc.clear()

	log := xc.log
	metadir := xc.dir
	file := path.Join(metadir, xaredologJSONFile)

	// Create it if the xacheck log doesn't exist.
	if _, err := os.Stat(file); os.IsNotExist(err) {
		// to avoid creating the empty file xaredolog.json about "xaredologs": null
		// the xaredolog.json will be created when the xaredolog are generated.
		/*
			xaredos := &Xaredologs{}
			if err := config.WriteConfig(file, xaredos); err != nil {
				log.Error("xacheck.flush.xa.to.file[%v].error:%v", file, err)
				return err
			}
		*/
		return nil
	}

	data, err := ioutil.ReadFile(file)
	if err != nil {
		log.Error("xacheck.LoadXaRedoLogs.readfile[%v].error:%v", file, err)
		return err
	}

	retrys, err := xc.ReadXaCommitErrLogs(string(data))
	if err != nil {
		log.Error("xacheck.LoadXaRedoLogs.readfile.to.xaredolog[%v].error:%v", file, err)
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
func (xc *XaCheck) RemoveXaCommitErrLogs() error {
	return os.RemoveAll(xc.dir)
}
