package proxy

import (
	"fmt"
	"strings"

	"backend"
	"plugins/shiftmanager"
	"router"

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// Cleanup ...
type Cleanup struct {
	log     *xlog.Log
	scatter *backend.Scatter
	router  *router.Router
	spanner *Spanner
}

// NewCleanup -- creates new Cleanup handler.
func NewCleanup(log *xlog.Log, scatter *backend.Scatter, router *router.Router, spanner *Spanner) *Cleanup {
	return &Cleanup{
		log:     log,
		scatter: scatter,
		router:  router,
		spanner: spanner,
	}
}

// Cleanup used to find and cleanup the old data.
func (c *Cleanup) Cleanup() (*sqltypes.Result, error) {
	backends := c.scatter.AllBackends()
	for _, backend := range backends {
		dbQuery := "show databases"
		qr, err := c.spanner.ExecuteOnThisBackend(backend, dbQuery)
		if err != nil {
			return nil, err
		}

		for _, r := range qr.Rows {
			db := string(r[0].Raw())
			err := c.cleanupHandler(db, backend)
			if err != nil {
				return nil, err
			}
		}
	}

	return &sqltypes.Result{}, nil
}

func (c *Cleanup) cleanupHandler(database string, backend string) error {
	log := c.log
	var err error
	if isSysDB := c.router.IsSystemDB(database); isSysDB {
		return nil
	}

	// 1.Check if the database is in router. If not, drop database.
	if err = c.router.CheckDatabase(database); err != nil {
		sql := fmt.Sprintf("drop database if exists `%s`", database)
		if _, err = c.spanner.ExecuteOnThisBackend(backend, sql); err != nil {
			log.Error("cleanup.drop.database[%s].on.backend[%s].error:%v", database, backend, err)
			return err
		}
		log.Warning("cleanup.database[%s].on.backend[%s].has.been.cleaned", database, backend)
		return nil
	}

	// 2. Find the table with suffix '_cleanup', check whether exist in radon. If not, drop table.
	sql := fmt.Sprintf("select table_name from information_schema.tables where table_schema = '%s' and table_name like '%%_cleanup'", database)
	qr, err := c.spanner.ExecuteOnThisBackend(backend, sql)
	if err != nil {
		return err
	}

	for _, r := range qr.Rows {
		tb := string(r[0].Raw())
		// Because partition table suffix is a 4-digit number,
		// here can only be a global table or a single table.
		// Therefore, we can directly check in the router.
		if isExist, _ := c.router.CheckTable(database, tb); !isExist {
			sql := fmt.Sprintf("drop table if exists `%s`.`%s`", database, tb)
			_, err = c.spanner.ExecuteOnThisBackend(backend, sql)
			if err != nil {
				log.Error("cleanup.drop.table[%s.%s].on.backend[%s].error:%v", database, tb, backend, err)
				return err
			}
			log.Warning("cleanup.table[%s.%s].on.backend[%s].has.been.cleaned", database, tb, backend)
		}
	}

	// 3. Find the table with suffix '_migrate', check whether migrating or exist in radon. If not, drop table.
	sql = fmt.Sprintf("select table_name from information_schema.tables where table_schema = '%s' and table_name like '%%_migrate'", database)
	qr, err = c.spanner.ExecuteOnThisBackend(backend, sql)
	if err != nil {
		return err
	}

	for _, r := range qr.Rows {
		tb := string(r[0].Raw())
		if isExist, _ := c.router.CheckTable(database, tb); !isExist {
			// Check if the table is migrating.
			key := fmt.Sprintf("`%s`.`%s`_%s", database, strings.TrimSuffix(tb, "_migrate"), backend)
			shiftMgr := c.spanner.plugins.PlugShiftMgr()
			status := shiftMgr.GetStatus(key)
			if status == shiftmanager.ShiftStatusMigrating {
				continue
			}

			sql := fmt.Sprintf("drop table if exists `%s`.`%s`", database, tb)
			_, err = c.spanner.ExecuteOnThisBackend(backend, sql)
			if err != nil {
				log.Error("cleanup.drop.table[%s.%s].on.backend[%s].error:%v", database, tb, backend, err)
				return err
			}
			log.Warning("cleanup.table[%s.%s].on.backend[%s].has.been.cleaned", database, tb, backend)
		}
	}
	return nil
}
