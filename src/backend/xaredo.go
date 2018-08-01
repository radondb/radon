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

// Xaredolog tuple.
type Xaredolog struct {
	Ts       string   `json:"ts"`
	Xaid     string   `json:"xaid"`
	Stat     string   `json:"status"`
	Backends []string `json:"backends"`
}

// Xaredologs tuple
type Xaredologs struct {
	Xaredos []*Xaredolog `json:"xaredologs"`
}

// XaCheck tuple.
type XaCheck struct {
	log     *xlog.Log
	dir     string
	scatter *Scatter
	xaRedos map[string]*Xaredolog
	done    chan bool
	ticker  *time.Ticker
	wg      sync.WaitGroup
	mu      sync.RWMutex
}

// NewXaCheck creates the XaCheck tuple.
func NewXaCheck(scatter *Scatter, conf *config.XaCheckConfig) *XaCheck {
	return &XaCheck{
		log:     scatter.log,
		dir:     conf.Dir,
		scatter: scatter,
		xaRedos: make(map[string]*Xaredolog),
		done:    make(chan bool),
		ticker:  time.NewTicker(time.Duration(time.Second * time.Duration(conf.Interval))),
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

	if err := xc.LoadXaRedoLogs(); err != nil {
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

func (xc *XaCheck) addXaRedo(new *Xaredolog) error {
	xc.mu.Lock()
	defer xc.mu.Unlock()

	log := xc.log
	log.Info("xacheck.add:+%v", new)

	if old, ok := xc.xaRedos[new.Xaid]; ok {
		//return errors.Errorf("xacheck.addXaRedo.xaid[%v].duplicate", xaredo.Xaid)
		for _, oldBack := range old.Backends {
			for _, newBack := range new.Backends {
				if strings.EqualFold(oldBack, newBack) {
					return errors.Errorf("xacheck.addXaRedo.xaid[%v].backend[%v].duplicate", new.Xaid, newBack)
				}
			}
		}

		xc.xaRedos[new.Xaid].Backends = append(xc.xaRedos[new.Xaid].Backends, new.Backends...)
		return nil
	}

	xc.xaRedos[new.Xaid] = new
	return nil
}

// FlushConfig used to write the xaredo to the file.
func (xc *XaCheck) flushConfig() error {
	xc.mu.Lock()
	defer xc.mu.Unlock()

	log := xc.log
	file := path.Join(xc.dir, xaredologJSONFile)

	var xaRedologs Xaredologs
	for _, v := range xc.xaRedos {
		xaRedologs.Xaredos = append(xaRedologs.Xaredos, v)
	}

	log.Warning("xacheck.flush.to.file[%v].backends.conf:%+v", file, xaRedologs)
	if err := config.WriteConfig(file, xaRedologs); err != nil {
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

// WriteXaLog used to write the xa commit infos into the redo file.
func (xc *XaCheck) WriteXaLog(txn *Txn, back string) error {

	xaredo := &Xaredolog{
		Ts:       time.Now().Format("20060102150405"),
		Xaid:     txn.xid,
		Stat:     xaRedoError,
		Backends: []string{back},
	}

	// append to xacheck in the txnmgr
	if err := xc.addXaRedo(xaredo); err != nil {
		return errors.WithStack(err)
	}

	// flush to the file
	if err := xc.flushConfig(); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

//
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
			xc.mu.Lock()
			for i, xaback := range xc.xaRedos[xid].Backends {
				if strings.EqualFold(backend, xaback) {
					xc.xaRedos[xid].Backends = append(xc.xaRedos[xid].Backends[:i], xc.xaRedos[xid].Backends[i+1:]...)
				}
			}
			xc.mu.Unlock()
		}
	}

	return err
}

func (xc *XaCheck) xaCommitsRetryMem() error {
	log := xc.log

	xaRedos := xc.xaRedos
	if len(xaRedos) <= 0 {
		return nil
	}

	log.Info("xacheck.commit.retry %v.", xaRedos)

	for _, xaRedo := range xaRedos {
		query := fmt.Sprintf("xa commit '%s' ", xaRedo.Xaid)

		if err := xc.commitRetryBackends(query, xc.scatter, xaRedo.Xaid); err != nil {
			log.Warning("xacheck.commits.retry failed.")
			continue
		}

		if (len(xc.xaRedos[xaRedo.Xaid].Backends) == 0) {
			xc.mu.Lock()
			delete(xc.xaRedos, xaRedo.Xaid)
			xc.mu.Unlock()
		}
	}

	// flush to the file
	if err := xc.flushConfig(); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (xc *XaCheck) xaCommitcheck() {

	defer xc.ticker.Stop()
	for {
		select {
		case <-xc.ticker.C:
			xc.xaCommitsRetryMem()
		case <-xc.done:
			return
		}
	}
}

// ReadXaRedoLogs used to read the Xaredologs config from the data.
func (xc *XaCheck) ReadXaRedoLogs(data string) (*Xaredologs, error) {
	xaRedos := &Xaredologs{}
	if err := json.Unmarshal([]byte(data), xaRedos); err != nil {
		return nil, errors.WithStack(err)
	}
	return xaRedos, nil
}

// LoadXaRedoLogs is used to load all backends from metadir/backend.json file.
func (xc *XaCheck) LoadXaRedoLogs() error {

	// Do clear first.
	//xc.clear()

	log := xc.log
	metadir := xc.dir
	file := path.Join(metadir, xaredologJSONFile)

	// Create it if the xacheck log doesn't exist.
	if _, err := os.Stat(file); os.IsNotExist(err) {
		xaredos := &Xaredologs{}
		if err := config.WriteConfig(file, xaredos); err != nil {
			log.Error("xacheck.flush.xa.to.file[%v].error:%v", file, err)
			return err
		}
	}

	data, err := ioutil.ReadFile(file)
	if err != nil {
		log.Error("xacheck.LoadXaRedoLogs.readfile[%v].error:%v", file, err)
		return err
	}

	xaRedos, err := xc.ReadXaRedoLogs(string(data))
	if err != nil {
		log.Error("xacheck.LoadXaRedoLogs.readfile.to.xaredolog[%v].error:%v", file, err)
		return err
	}

	for _, xa := range xaRedos.Xaredos {
		if err := xc.addXaRedo(xa); err != nil {
			log.Error("xacheck.add.xaid[%s] on backends[%v].error:%v", xa.Xaid, xa.Backends, err)
			return err
		}

		log.Info("xacheck.load.xaid:%+v", xa.Xaid)
	}
	return nil
}

// Close is used to close the xacheck goroutine
func (xc *XaCheck) Close() {
	close(xc.done)
	xc.wg.Wait()
}

// RemoveXaRedoLogs is only used to test to avoid the noise,
// RedoLogs can not be removed in the production environment, it is so important.
func (xc *XaCheck) RemoveXaRedoLogs() error {
	return os.RemoveAll(xc.dir)
}

/*

func (xc *XaCheck) xaCommitRetryOneBackend(backend string, query string, scatter *Scatter) error {

	txn, err := scatter.CreateTransaction()
	if err != nil {
		scatter.log.Error("xa.Commit.Retry.error:[%v]", err)
		return err
	}

	defer txn.Finish()
	_, err = txn.ExecuteOnThisBackend(backend, query)
	if err != nil {
		return err
	}
	return nil
}

func WriteXaLogs(name string, Xaredos []Xaredolog) error {
	b, err := json.Marshal(Xaredos)
	if err != nil {
		return errors.WithStack(err)
	}

	return xbase.WriteFile(name, b)
}

// readFile used to read file from disk.
func xaReadFile(file string) ([]Xaredolog, error) {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		//log.Error("backend.xaredo.read.file[%s].error:%+v", file, err)
		return nil, err
	}

	var xalogs []Xaredolog

	err = json.Unmarshal(data, &xalogs)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return xalogs, nil
}

func (xc *XaCheck) xaCommitsRetry(log *xlog.Log, scatter *Scatter) error {

	// read file
	metadir := scatter.metadir
	fileName := metadir + xaredologJSONFile
	xaRedos, err := xaReadFile(fileName)
	if err != nil {
		return err
	}

	// commit retry
	log.Info("xa.commit.retry %v.", xaRedos)

	//query := fmt.Sprintf("xa commit '%s' ", xaRedos[0].Xaid)
	//backend := "node2"

	for _, xaRedo := range xaRedos {
		query := fmt.Sprintf("xa commit '%s' ", xaRedo.Xaid)
		backend := xaRedo.Backend

		if err := xc.xaCommitRetry(query, backend, scatter); err != nil {
			log.Warning("xa.commits.retry failed.")
			xaRedo.Stat = xaRedoError
			continue
		}

		xaRedo.Stat = xaRedoOk
	}

	var failedXalogs []Xaredolog
	for _, xaRedo := range xaRedos {
		if xaRedo.Stat == xaRedoError {
			failedXalogs = append(failedXalogs, xaRedo)
		}
	}

	os.Remove(fileName)
	if len(failedXalogs) > 0 {
		WriteXaLogs(fileName, failedXalogs)
	}
	return nil
}

*/
