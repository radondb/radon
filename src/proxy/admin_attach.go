/*
 * Radon
 *
 * Copyright 2018-2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"fmt"
	"strings"

	"backend"
	"config"
	"router"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

const (
	AttachDatabaseCheckTable = "attach_database_check_table_valid"

	AttachParamsCount = 3
	DetachParamsCount = 1
)

type Attach struct {
	log     *xlog.Log
	scatter *backend.Scatter
	router  *router.Router
	spanner *Spanner
}

type attachParams struct {
	Name           string `json:"name"`
	Address        string `json:"address"`
	User           string `json:"user"`
	Password       string `json:"password"`
	MaxConnections int    `json:"max-connections"`
}

// NewAttach -- creates new Attach handler.
func NewAttach(log *xlog.Log, scatter *backend.Scatter, router *router.Router, spanner *Spanner) *Attach {
	return &Attach{
		log:     log,
		scatter: scatter,
		router:  router,
		spanner: spanner,
	}
}

func (attach *Attach) Attach(node *sqlparser.Radon) (*sqltypes.Result, error) {
	row := node.Row
	log := attach.log
	scatter := attach.scatter

	if len(row) != AttachParamsCount {
		return nil, errors.Errorf("spanner.query.execute.radon.%s.error,", node.Action)
	}
	var p attachParams
	p.Name = common.BytesToString(row[0].(*sqlparser.SQLVal).Val)
	p.Address = common.BytesToString(row[0].(*sqlparser.SQLVal).Val)
	p.User = common.BytesToString(row[1].(*sqlparser.SQLVal).Val)
	p.Password = common.BytesToString(row[2].(*sqlparser.SQLVal).Val)
	p.MaxConnections = 1024

	if err := attach.addAttachHandler(log, scatter, &p); err != nil {
		log.Error("attach[%+v]", p)
		return nil, sqldb.NewSQLErrorf(sqldb.ER_UNKNOWN_ERROR, "attach node[%+v]: %v", p, err)
	}

	log.Warning("attach[%v]", p)
	return &sqltypes.Result{}, nil
}

func (attach *Attach) Detach(attachName string) (*sqltypes.Result, error) {
	log := attach.log
	scatter := attach.scatter
	router := attach.router

	if err := attach.detachHandler(log, scatter, router, attachName); err != nil {
		log.Error("detach[%v]", attachName)
		return nil, sqldb.NewSQLErrorf(sqldb.ER_UNKNOWN_ERROR, "detach node[%+v]: %v", attachName, err)
	}

	log.Warning("detach[%v]", attachName)
	return &sqltypes.Result{}, nil
}

func (attach *Attach) ListAttach() (*sqltypes.Result, error) {
	qr := &sqltypes.Result{}
	qr.Fields = []*querypb.Field{
		{Name: "Name", Type: querypb.Type_VARCHAR},
		{Name: "Address", Type: querypb.Type_VARCHAR},
		{Name: "User", Type: querypb.Type_VARCHAR},
	}

	backendConfigs := attach.scatter.BackendConfigsClone()
	var attachInfos []attachParams

	for _, backendConfig := range backendConfigs {
		if backendConfig.Role == config.AttachBackend {
			attachInfo := attachParams{
				Name:    backendConfig.Name,
				Address: backendConfig.Address,
				User:    backendConfig.User,
			}

			attachInfos = append(attachInfos, attachInfo)
		}
	}

	for _, attach := range attachInfos {
		row := []sqltypes.Value{
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(attach.Name)),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(attach.Address)),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(attach.User)),
		}

		qr.Rows = append(qr.Rows, row)
	}
	return qr, nil
}

func (attach *Attach) addAttachHandler(log *xlog.Log, scatter *backend.Scatter, p *attachParams) (err error) {
	route := attach.router
	routeDBs := make(map[string]struct{})
	attachTableList := make(map[string][]string)

	conf := &config.BackendConfig{
		Name:           p.Name,
		Address:        p.Address,
		User:           p.User,
		Password:       p.Password,
		Charset:        "utf8",
		MaxConnections: p.MaxConnections,
		Role:           config.AttachBackend,
	}

	err = scatter.Add(conf)
	if err != nil {
		log.Error("attach[%+v].error:%+v", conf, err)
		return err
	}

	defer func() {
		if err != nil {
			if err := scatter.Remove(conf); err != nil {
				log.Error("attach.remove.attach.conf[%+v]:%+v", conf, err)
			}

			for db, tables := range attachTableList {
				if _, ok := routeDBs[db]; ok {
					continue
				}

				for _, table := range tables {
					if err := route.DropTable(db, table); err != nil {
						log.Error("attach.route.DropTable[%s.%s]:%+v", db, table, err)
					}
				}
			}

			for db, _ := range routeDBs {
				if err := route.DropDatabase(db); err != nil {
					log.Error("attach.route.DropDatabase[%s]:%+v", db, err)
				}
			}

			return
		}

		err = scatter.FlushConfig()
		if err != nil {
			log.Error("attach.flush.config.error:%+v", err)
			return
		}

		attach.dropAttachCheckDatabase()
	}()

	// Sync table on the attach backend to radon's router as single table
	// just register the info on the radon, it wll be fast.
	attachBackend := []string{conf.Name}
	dbQuery := "show databases"
	qr, err := attach.spanner.ExecuteOnThisBackend(conf.Name, dbQuery)
	if err != nil {
		return err
	}

	tblList := route.Tables()
	for _, r := range qr.Rows {
		db := string(r[0].Raw())
		if isSysDB := route.IsSystemDB(db); isSysDB {
			continue
		}

		if _, ok := tblList[db]; !ok {
			query := fmt.Sprintf("create database IF NOT EXISTS %s", db)
			_, err = attach.spanner.ExecuteScatter(query)
			if err != nil {
				return err
			}

			err = route.CreateDatabase(db)
			if err != nil {
				return err
			}
			routeDBs[db] = struct{}{}
		}

		tableQuery := fmt.Sprintf("show tables from %s", db)
		qr, err := attach.spanner.ExecuteOnThisBackend(conf.Name, tableQuery)
		if err != nil {
			log.Error("attach[%+v].show.tables.error:%+v", conf, err)
			return err
		}

		attach.createAttachCheckDatabase()
		tables := make([]string, 0, 16)
		for _, r := range qr.Rows {
			table := string(r[0].Raw())
			err = attach.checkTableValid(p, db, table, scatter)
			if err != nil {
				log.Error("attach[%+v].checkTable.error:%+v", conf, err)
				return err
			}

			err = route.CreateNonPartTable(db, table, router.TableTypeSingle, attachBackend, nil)
			if err != nil {
				return err
			}

			tables = append(tables, table)
			attachTableList[db] = tables
		}
		attach.dropAttachCheckDatabase()
	}
	return nil
}

func (attach *Attach) detachHandler(log *xlog.Log, scatter *backend.Scatter, router *router.Router, attachName string) (err error) {
	conf := &config.BackendConfig{
		Name: attachName,
	}

	dbQuery := "show databases"
	dbs, err := attach.spanner.ExecuteOnThisBackend(attachName, dbQuery)
	if err != nil {
		return
	}

	tblList := router.Tables()
	for _, r := range dbs.Rows {
		db := string(r[0].Raw())
		if isSysDB := router.IsSystemDB(db); isSysDB {
			continue
		}

		if tables, ok := tblList[db]; ok {
			routerTableLen := len(tables)
			var attachTableCount int
			for _, table := range tables {
				segments, err := router.Lookup(db, table, nil, nil)
				if err != nil {
					continue
				}

				if strings.Compare(segments[0].Backend, attachName) == 0 {
					attachTableCount++
					if err := router.DropTable(db, table); err != nil {
						return err
					}
				}
			}

			// All tables on this database are on the attach backend, detach will drop the db on the normal backend.
			if routerTableLen == attachTableCount {
				sql := fmt.Sprintf("drop database if exists `%s`", db)
				if _, err := attach.spanner.ExecuteScatter(sql); err != nil {
					return err
				}
			}
		}
	}

	tblListNow := router.Tables()
	for db, tables := range tblListNow {
		if len(tables) == 0 {
			if err := router.DropDatabase(db); err != nil {
				return err
			}
		}
	}

	if err := scatter.Remove(conf); err != nil {
		return err
	}

	if err := scatter.FlushConfig(); err != nil {
		return err
	}
	return nil
}

// check the table valid by creating table on the radon to .
func (attach *Attach) checkTableValid(from *attachParams, db string, table string, scatter *backend.Scatter) error {
	log := attach.log
	sql := fmt.Sprintf("show create table `%s`.`%s`", db, table)
	r, err := attach.spanner.ExecuteOnThisBackend(from.Name, sql)
	if err != nil {
		log.Error("check.table.valid.attach[%+v].execute.sql[%v].error:%+v", from, sql, err)
		return err
	}

	if len(r.Rows) > 0 {
		createTableSQL := string(r.Rows[0][1].Raw())
		sql = strings.Replace(createTableSQL, fmt.Sprintf("CREATE TABLE `%s`", table),
			fmt.Sprintf("CREATE TABLE `%s`.`%s`", AttachDatabaseCheckTable, table), 1)
		if _, err := attach.spanner.ExecuteScatter(sql); err != nil {
			log.Error("check.table.valid.attach.[%v].execute.sql[%s].error:%+v", from, sql, err)
			return err
		}
	}
	return nil
}

func (attach *Attach) dropAttachCheckDatabase() error {
	log := attach.log

	sql := fmt.Sprintf("drop database if exists `%s`", AttachDatabaseCheckTable)
	if _, err := attach.spanner.ExecuteScatter(sql); err != nil {
		log.Error("drop.attach.test.database[%v].error:%+v", AttachDatabaseCheckTable, err)
		return err
	}
	return nil
}

func (attach *Attach) createAttachCheckDatabase() error {
	log := attach.log

	sql := fmt.Sprintf("create database if not exists `%s`", AttachDatabaseCheckTable)
	if _, err := attach.spanner.ExecuteScatter(sql); err != nil {
		log.Error("create.attach.test.database[%v].error:%+v", AttachDatabaseCheckTable, err)
		return err
	}
	return nil
}
