/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package backend

import (
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"xcontext"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestWriteXaLogAddXidDuplicate(t *testing.T) {

	defer leaktest.Check(t)()

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	scatter, _, cleanup := MockScatter(log, 2)
	defer cleanup()

	txn1, err := scatter.CreateTransaction()
	assert.Nil(t, err)
	defer txn1.Finish()

	txn1.xid = "RXID-20180903103145-1"
	backend := "backend0"

	err = scatter.txnMgr.xaCheck.WriteXaLog(txn1, backend)
	assert.Nil(t, err)
	//scatter.txnMgr.xaCheck.RemoveXaRedoLogs()
	txn2, err := scatter.CreateTransaction()
	assert.Nil(t, err)
	defer txn2.Finish()

	txn2.xid = "RXID-20180903103145-1"
	backend = "backend0"

	err = scatter.txnMgr.xaCheck.WriteXaLog(txn2, backend)
	assert.NotNil(t, err)
}

func TestReadXaRedoLogsWithoutBackend(t *testing.T) {
	defer leaktest.Check(t)()

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	scatter := NewScatter(log, "")
	scatter.Init(MockXaCheckConfigDefault())

	data := `{
    "xaredologs": [
        {
            "ts": "20180903103145",
            "xaid": "RXID-20180903103145-1",
            "status": "error",
            "backends": [
                "backend0",
                "backend1"
            ]
        }
    ]
}`
	file := path.Join(MockXaCheckConfigDefault().Dir, xaredologJSONFile)
	ioutil.WriteFile(file, []byte(data), 0644)
	defer os.RemoveAll(file)

	time.Sleep(1 * time.Second)

	scatter.txnMgr.xaCheck.Close()
}

func TestTxnTwoPCExecuteCommitError(t *testing.T) {
	defer leaktest.Check(t)()

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	scatter, fakedb1, cleanup1 := MockScatter(log, 2)
	defer cleanup1()

	var backend [2]string
	var i int
	for k := range scatter.backends {
		backend[i] = k
		i++
	}

	querys1 := []xcontext.QueryTuple{
		xcontext.QueryTuple{Query: "update", Backend: backend[0]},
		xcontext.QueryTuple{Query: "update", Backend: backend[1]},
	}

	fakedb1.AddQuery(querys1[0].Query, result1)
	fakedb1.AddQueryDelay(querys1[1].Query, result2, 100)

	// Set 2PC conds.
	resetFunc1 := func(txn *Txn) {
		fakedb1.ResetAll()
		fakedb1.AddQuery(querys1[0].Query, result1)
		fakedb1.AddQueryDelay(querys1[1].Query, result2, 100)
		fakedb1.AddQueryPattern("XA .*", result1)
	}

	// XA COMMIT error.
	{
		txn, err := scatter.CreateTransaction()
		assert.Nil(t, err)
		defer txn.Finish()

		resetFunc1(txn)
		fakedb1.AddQueryErrorPattern("XA COMMIT .*", sqldb.NewSQLError1(1397, "XAE04", "XAER_NOTA: Unknown XID"))

		err = txn.Begin()
		assert.Nil(t, err)

		rctx := &xcontext.RequestContext{
			Mode:    xcontext.ReqNormal,
			TxnMode: xcontext.TxnWrite,
			Querys:  querys1,
		}
		_, err = txn.Execute(rctx)
		assert.Nil(t, err)
		err = txn.Commit()
		assert.NotNil(t, err)
		want := "XAER_NOTA: Unknown XID (errno 1397) (sqlstate XAE04)"
		got := err.Error()
		assert.Equal(t, want, got)

		time.Sleep(2 * time.Second)

	}

	scatter.txnMgr.xaCheck.RemoveXaRedoLogs()

}

func TestXaCheckInitWithRedoLogCommitErrorOk(t *testing.T) {

	defer leaktest.Check(t)()

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	/*
		MockXaredologs := []*Xaredolog{
			&Xaredolog{
				Ts:       "20180903103145",
				Xaid:     "RXID-20180903103145-1",
				Stat:     xaRedoError,
				Backends: []string{"backend0", "backend1"},
			},
		}

		file := path.Join(config.DefaultXaCheckConfig().Dir, xaredologJSONFile)
		config.WriteConfig(file, &Xaredologs{Xaredos: MockXaredologs})
		//defer os.RemoveAll(file)

	*/

	scatter, fakedb1, cleanup1 := MockScatter(log, 2)
	defer cleanup1()

	var backend [2]string
	var i int
	for k := range scatter.backends {
		backend[i] = k
		i++
	}

	querys1 := []xcontext.QueryTuple{
		xcontext.QueryTuple{Query: "update", Backend: backend[0]},
		xcontext.QueryTuple{Query: "update", Backend: backend[1]},
	}

	fakedb1.AddQuery(querys1[0].Query, result1)
	fakedb1.AddQueryDelay(querys1[1].Query, result2, 100)

	// Set 2PC conds.
	resetFunc1 := func(txn *Txn) {
		fakedb1.ResetAll()
		fakedb1.AddQuery(querys1[0].Query, result1)
		fakedb1.AddQueryDelay(querys1[1].Query, result2, 100)
		fakedb1.AddQueryPattern("XA .*", result1)
	}

	// XA COMMIT error.
	{
		txn, err := scatter.CreateTransaction()
		assert.Nil(t, err)
		defer txn.Finish()

		resetFunc1(txn)
		fakedb1.AddQueryErrorPattern("XA COMMIT .*", sqldb.NewSQLError1(1397, "XAE04", "XAER_NOTA: Unknown XID"))

		err = txn.Begin()
		assert.Nil(t, err)

		rctx := &xcontext.RequestContext{
			Mode:    xcontext.ReqNormal,
			TxnMode: xcontext.TxnWrite,
			Querys:  querys1,
		}
		_, err = txn.Execute(rctx)
		assert.Nil(t, err)
		err = txn.Commit()
		assert.NotNil(t, err)
		want := "XAER_NOTA: Unknown XID (errno 1397) (sqlstate XAE04)"
		got := err.Error()
		assert.Equal(t, want, got)

		time.Sleep(2 * time.Second)
	}

	// XA COMMIT ok.
	{
		txn, err := scatter.CreateTransaction()
		assert.Nil(t, err)
		defer txn.Finish()

		resetFunc1(txn)
		fakedb1.AddQuery("XA COMMIT .*", result1)

		err = txn.Begin()
		assert.Nil(t, err)

		rctx := &xcontext.RequestContext{
			Mode:    xcontext.ReqNormal,
			TxnMode: xcontext.TxnWrite,
			Querys:  querys1,
		}
		_, err = txn.Execute(rctx)
		assert.Nil(t, err)
		err = txn.Commit()
		assert.Nil(t, err)

		time.Sleep(2 * time.Second)
	}

	//scatter.txnMgr.xaCheck.RemoveXaRedoLogs()
}

func TestXaCheckInit(t *testing.T) {
	defer leaktest.Check(t)()

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	scatter := NewScatter(log, "")

	xaChecker := NewXaCheck(scatter, MockXaCheckConfig2Default())
	defer os.RemoveAll(MockXaCheckConfig2Default().Dir)
	file := MockXaCheckConfig2Default().Dir
	_, err := os.Create(file)
	assert.Nil(t, err)
	err = xaChecker.Init()
	assert.NotNil(t, err)
	err = os.RemoveAll(file)
	assert.Nil(t, err)

	xaChecker.Close()
}

func TestLoadXaRedoLogsError(t *testing.T) {
	defer leaktest.Check(t)()

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	scatter := NewScatter(log, "")

	xaChecker := NewXaCheck(scatter, MockXaCheckConfig2Default())
	defer os.RemoveAll(MockXaCheckConfig2Default().Dir)
	err := xaChecker.Init()
	assert.Nil(t, err)

	err = xaChecker.LoadXaRedoLogs()
	assert.Nil(t, err)
	//xaChecker.RemoveXaRedoLogs()

	//
	file := path.Join(xaChecker.dir, xaredologJSONFile)
	err = os.Chmod(file, 0200)
	err = xaChecker.LoadXaRedoLogs()
	assert.NotNil(t, err)

	xaChecker.Close()
}

func TestReadXaRedoLogsError1(t *testing.T) {
	defer leaktest.Check(t)()

	data := `{
    "xaredologs": [
        {
            "ts": "20180903103145",
            "xaid": "RXID-20180903103145-1",
            "status": "error",
            "backends": [
                "backend0",
                "backend1"
            ]
        }
    ]
}`

	MockXaredologs := []*Xaredolog{
		&Xaredolog{
			Ts:       "20180903103145",
			Xaid:     "RXID-20180903103145-1",
			Stat:     xaRedoError,
			Backends: []string{"backend0", "backend1"},
		},
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	scatter := NewScatter(log, "")

	xaChecker := NewXaCheck(scatter, MockXaCheckConfigDefault())
	err := xaChecker.Init()
	assert.Nil(t, err)

	xaredos, err := xaChecker.ReadXaRedoLogs(string(data))
	assert.Nil(t, err)
	want := &Xaredologs{Xaredos: MockXaredologs}
	got := xaredos
	assert.Equal(t, want, got)
	xaChecker.Close()
	xaChecker.RemoveXaRedoLogs()
}

func TestReadXaRedoLogsError2(t *testing.T) {
	defer leaktest.Check(t)()


	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	scatter := NewScatter(log, "")

	xaChecker := NewXaCheck(scatter, MockXaCheckConfigDefault())
	err := xaChecker.Init()
	assert.Nil(t, err)


	data1 := `{
    "xaredologs": [
		2
    ]
}`

	file := path.Join(MockXaCheckConfigDefault().Dir, xaredologJSONFile)
	ioutil.WriteFile(file, []byte(data1), 0644)
	err = xaChecker.Init()
	assert.NotNil(t, err)

	xaChecker.Close()
	xaChecker.RemoveXaRedoLogs()
}