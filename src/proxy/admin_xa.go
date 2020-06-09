package proxy

import (
	"backend"
	"fmt"
	"router"
	"strconv"
	"strings"
	"time"

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

const (
	// It is more important to find the core cause to the prepared XA,
	// so add thresholds to prevent operating too quickly.
	intervalToNow = 10000 //one hour.
)

type AdminXA struct {
	log     *xlog.Log
	scatter *backend.Scatter
	router  *router.Router
	spanner *Spanner
}

func NewAdminXA(log *xlog.Log, scatter *backend.Scatter, router *router.Router, spanner *Spanner) *AdminXA {
	return &AdminXA{
		log:     log,
		scatter: scatter,
		router:  router,
		spanner: spanner,
	}
}

// Recover used to handle the 'XA RECOVER' to all backends .
func (adminXA *AdminXA) Recover() (*sqltypes.Result, error) {
	rewritten := fmt.Sprintf("XA RECOVER")
	qr, err := adminXA.spanner.ExecuteSingle(rewritten)
	if err != nil {
		return nil, err
	}
	return qr, nil
}

// Commit used to handle the 'XA COMMIT' to all XAIDs an hour ago.
func (adminXA *AdminXA) Commit() (*sqltypes.Result, error) {
	log := adminXA.log
	rewritten := fmt.Sprintf("XA RECOVER")
	qr, err := adminXA.spanner.ExecuteSingle(rewritten)
	if err != nil {
		return nil, err
	}

	for _, row := range qr.Rows {
		// the format of xaid: txn.xid = fmt.Sprintf("RXID-%v-%v", time.Now().Format("20060102150405"), txn.id)
		data := string(row[3].Raw())
		xaid := strings.SplitN(data, "-", 3)
		xaTimeStamp := xaid[1]
		now := time.Now().Format("20060102150405")
		xaTimeStampInt, _ := strconv.ParseInt(xaTimeStamp, 10, 64)
		nowInt, _ := strconv.ParseInt(now, 10, 64)
		interval := nowInt - xaTimeStampInt

		// the time interval > 1 hour, we will execute the cmd,
		// It is more important to find the core cause to the prepared XA.
		if interval > intervalToNow {
			rewritten := fmt.Sprintf("XA COMMIT '%s'", data)
			qr, err := adminXA.spanner.ExecuteScatter(rewritten)
			if err != nil {
				log.Error("proxy.adminXA.commit.xaid[%s].err:%+v", data, err)
				return nil, err
			}

			log.Warning("proxy.adminXA.commit.xaid[%s].succeed", data)
			return qr, err
		}
	}
	return &sqltypes.Result{}, err
}

// Rollback used to handle the 'XA ROLLBACK' to all XAIDs an hour ago.
func (adminXA *AdminXA) Rollback() (*sqltypes.Result, error) {
	log := adminXA.log
	rewritten := fmt.Sprintf("XA RECOVER")
	qr, err := adminXA.spanner.ExecuteSingle(rewritten)
	if err != nil {
		return nil, err
	}

	for _, row := range qr.Rows {
		// the format of xaid: txn.xid = fmt.Sprintf("RXID-%v-%v", time.Now().Format("20060102150405"), txn.id)
		data := string(row[3].Raw())
		xaid := strings.SplitN(data, "-", 3)
		xaTimeStamp := xaid[1]
		now := time.Now().Format("20060102150405")
		xaTimeStampInt, _ := strconv.ParseInt(xaTimeStamp, 10, 64)
		nowInt, _ := strconv.ParseInt(now, 10, 64)
		interval := nowInt - xaTimeStampInt

		// the time interval > 1 hour, we will execute the cmd,
		// It is more important to find the core cause to the prepared XA.
		if interval > intervalToNow {
			rewritten := fmt.Sprintf("XA ROLLBACK '%s'", data)
			qr, err := adminXA.spanner.ExecuteScatter(rewritten)
			if err != nil {
				log.Error("proxy.adminXA.rollback.xaid[%s].err:%+v", data, err)
				return nil, err
			}

			log.Warning("proxy.adminXA.rollback.xaid[%s].succeed", data)
			return qr, err
		}
	}
	return &sqltypes.Result{}, err
}
