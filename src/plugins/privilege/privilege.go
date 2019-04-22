/*
 * Radon
 *
 * Copyright 2018-2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package privilege

import (
	"fmt"
	"sync"
	"time"

	"backend"
	"config"

	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

type privilege struct {
	selectPriv bool
	insertPriv bool
	updatePriv bool
	deletePriv bool
	createPriv bool
	dropPriv   bool
	alterPriv  bool
	indexPriv  bool
	showDBPriv bool
	superPriv  bool
}

type dbPriv struct {
	host string
	user string
	db   string
	priv privilege
}

type userPriv struct {
	host    string
	user    string
	priv    privilege
	dbPrivs map[string]dbPriv
}

// Privilege struct.
type Privilege struct {
	mu        sync.RWMutex
	wg        sync.WaitGroup
	log       *xlog.Log
	conf      *config.Config
	done      chan bool
	userPrivs map[string]userPriv
	scatter   *backend.Scatter
	ticker    *time.Ticker
}

// NewPrivilege -- creates new Privilege.
func NewPrivilege(log *xlog.Log, conf *config.Config, scatter *backend.Scatter) PrivilegeHandler {
	return &Privilege{
		log:       log,
		conf:      conf,
		done:      make(chan bool),
		userPrivs: make(map[string]userPriv),
		scatter:   scatter,
		ticker:    time.NewTicker(time.Duration(time.Second * 5)),
	}
}

// Init -- init the privilege plugin.
func (p *Privilege) Init() error {
	log := p.log

	if err := p.UpdatePrivileges(); err != nil {
		log.Error("plugin.privilege.init.privilege.error:%+v", err)
		return err
	}
	log.Info("privilege.init:%+v", p.userPrivs)

	p.wg.Add(1)
	go func(gp *Privilege) {
		defer gp.ticker.Stop()
		defer gp.wg.Done()
		for {
			select {
			case <-gp.ticker.C:
				if err := gp.UpdatePrivileges(); err != nil {
					log.Error("plugin.privilege.update.privilege.error:%+v", err)
				}
			case <-gp.done:
				return
			}
		}
	}(p)

	log.Info("plugin.privileges.init.done")
	return nil
}

// https://dev.mysql.com/doc/refman/8.0/en/privileges-provided.html
func (p *Privilege) CheckPrivilege(db string, user string, node sqlparser.Statement) bool {
	p.mu.RLock()
	userpriv := p.userPrivs[user]
	dbpriv := userpriv.dbPrivs[db]
	p.mu.RUnlock()

	if node != nil {
		switch node.(type) {
		case *sqlparser.Select:
			return (userpriv.priv.superPriv || userpriv.priv.selectPriv || dbpriv.priv.selectPriv)
		case *sqlparser.Insert:
			return (userpriv.priv.superPriv || userpriv.priv.insertPriv || dbpriv.priv.insertPriv)
		case *sqlparser.Update:
			return (userpriv.priv.superPriv || userpriv.priv.updatePriv || dbpriv.priv.updatePriv)
		case *sqlparser.Delete:
			return (userpriv.priv.superPriv || userpriv.priv.deletePriv || dbpriv.priv.deletePriv)
		case *sqlparser.Show:
			return (userpriv.priv.superPriv || userpriv.priv.showDBPriv || dbpriv.priv.showDBPriv)
		case *sqlparser.DDL:
			user := (userpriv.priv.createPriv && userpriv.priv.dropPriv && userpriv.priv.alterPriv && userpriv.priv.indexPriv)
			db := (dbpriv.priv.createPriv && dbpriv.priv.dropPriv && dbpriv.priv.alterPriv && dbpriv.priv.indexPriv)
			return (userpriv.priv.superPriv || user || db)
		}
	}
	// If node is nil, we must the super privilege.
	return userpriv.priv.superPriv
}

// Check -- checks the session privilege on the database.
func (p *Privilege) Check(session *driver.Session, node sqlparser.Statement) error {
	db := ""
	ok := true
	user := session.User()

	if node != nil {
		sqlparser.Walk(func(nod sqlparser.SQLNode) (kontinue bool, err error) {
			switch nod := nod.(type) {
			case sqlparser.TableName:
				if !nod.Qualifier.IsEmpty() {
					db = nod.Qualifier.String()
					if !p.CheckPrivilege(db, user, node) {
						ok = false
						return false, nil
					}
				}
			}
			return true, nil
		}, node)
	} else {
		db = session.Schema()
		if !p.CheckPrivilege(db, user, nil) {
			ok = false
		}
	}

	if !ok {
		return sqldb.NewSQLErrorf(sqldb.ER_ACCESS_DENIED_ERROR, "Access denied for user '%v'@'%v'", user, db)
	}
	return nil
}

// Close -- close the privilege plugin.
func (p *Privilege) Close() error {
	close(p.done)
	p.wg.Wait()
	return nil
}

// UpdatePrivileges -- used to update the privileges map to latest.
func (p *Privilege) UpdatePrivileges() error {
	userpriv, err := p.loadUserPrivileges()
	if err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.userPrivs = userpriv
	return nil
}

// loadPrivileges -- used to get the backend's user privileges.
// mysql> select Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv, Drop_priv, Grant_priv, Alter_priv from mysql.db;
// +-----------+--------------------+---------------+-------------+-------------+-------------+-------------+-------------+-----------+------------+------------+
// | Host      | Db                 | User          | Select_priv | Insert_priv | Update_priv | Delete_priv | Create_priv | Drop_priv | Grant_priv | Alter_priv |
// +-----------+--------------------+---------------+-------------+-------------+-------------+-------------+-------------+-----------+------------+------------+
// | localhost | performance_schema | mysql.session | Y           | N           | N           | N           | N           | N         | N          | N          |
// | localhost | sys                | mysql.sys     | N           | N           | N           | N           | N           | N         | N          | N          |
// | localhost | test1              | u1            | Y           | Y           | Y           | Y           | Y           | Y         | N          | Y          |
// | %         | db1                | x2            | Y           | Y           | Y           | Y           | Y           | N         | N          | N          |
// | %         | db2                | x2            | Y           | Y           | Y           | Y           | Y           | N         | N          | N          |
// +-----------+--------------------+---------------+-------------+-------------+-------------+-------------+-------------+-----------+------------+------------+
func (p *Privilege) loadDBPrivileges(host string, user string) (map[string]dbPriv, error) {
	privis := make(map[string]dbPriv)

	query := fmt.Sprintf(`select Host, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv, Drop_priv, Grant_priv, Alter_priv, Index_priv, db from mysql.db where Host='%v' and User='%s'`, host, user)
	qr, err := p.execute(query)
	if err != nil {
		return nil, err
	}

	for _, r := range qr.Rows {
		dbpriv := dbPriv{
			host: string(r[0].Raw()),
			user: string(r[1].Raw()),
			priv: privilege{
				selectPriv: string(r[2].Raw()) == "Y",
				insertPriv: string(r[3].Raw()) == "Y",
				updatePriv: string(r[4].Raw()) == "Y",
				deletePriv: string(r[5].Raw()) == "Y",
				createPriv: string(r[6].Raw()) == "Y",
				dropPriv:   string(r[7].Raw()) == "Y",
				alterPriv:  string(r[8].Raw()) == "Y",
				indexPriv:  string(r[9].Raw()) == "Y",
			},
			db: string(r[10].Raw()),
		}
		privis[dbpriv.db] = dbpriv
	}
	return privis, nil
}

// mysql> select Host, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv, Drop_priv, Grant_priv, Alter_priv from mysql.user;
// +-----------+---------------+-------------+-------------+-------------+-------------+-------------+-----------+------------+------------+
// | Host      | User          | Select_priv | Insert_priv | Update_priv | Delete_priv | Create_priv | Drop_priv | Grant_priv | Alter_priv |
// +-----------+---------------+-------------+-------------+-------------+-------------+-------------+-----------+------------+------------+
// | localhost | root          | Y           | Y           | Y           | Y           | Y           | Y         | Y          | Y          |
// | %         | x3            | Y           | Y           | Y           | Y           | Y           | Y         | N          | Y          |
// | %         | x4            | N           | N           | N           | N           | N           | N         | N          | N          |
// | %         | xall          | Y           | Y           | Y           | Y           | Y           | Y         | N          | Y          |
// +-----------+---------------+-------------+-------------+-------------+-------------+-------------+-----------+------------+------------+
func (p *Privilege) loadUserPrivileges() (map[string]userPriv, error) {
	privis := make(map[string]userPriv)

	// user privileges.
	query := "select Host, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv, Drop_priv, Alter_priv, Index_priv, Show_db_priv, Super_priv from mysql.user"
	qr, err := p.execute(query)
	if err != nil {
		return nil, err
	}

	for _, r := range qr.Rows {
		host := string(r[0].Raw())
		user := string(r[1].Raw())
		dbprivs, err := p.loadDBPrivileges(host, user)
		if err != nil {
			return nil, err
		}

		userpriv := userPriv{
			host: host,
			user: user,
			priv: privilege{
				selectPriv: string(r[2].Raw()) == "Y",
				insertPriv: string(r[3].Raw()) == "Y",
				updatePriv: string(r[4].Raw()) == "Y",
				deletePriv: string(r[5].Raw()) == "Y",
				createPriv: string(r[6].Raw()) == "Y",
				dropPriv:   string(r[7].Raw()) == "Y",
				alterPriv:  string(r[8].Raw()) == "Y",
				indexPriv:  string(r[9].Raw()) == "Y",
				showDBPriv: string(r[10].Raw()) == "Y",
				superPriv:  string(r[11].Raw()) == "Y",
			},
			dbPrivs: dbprivs,
		}
		privis[userpriv.user] = userpriv
	}
	return privis, nil
}

func (p *Privilege) getPrivByDbUser(db string, user string) *privilege {
	p.mu.RLock()
	defer p.mu.RUnlock()

	userpriv, ok := p.userPrivs[user]
	// User privi canot be found.
	if !ok {
		return nil
	}

	dbpriv, ok := userpriv.dbPrivs[db]
	if !ok {
		// Db privilege canot be found, returns the user privileges.
		return &userpriv.priv
	} else {
		// Returns db privileges.
		return &dbpriv.priv
	}
}

// execute -- get the result from backend.
func (p *Privilege) execute(query string) (*sqltypes.Result, error) {
	scatter := p.scatter

	txn, err := scatter.CreateTransaction()
	if err != nil {
		return nil, err
	}
	defer txn.Finish()

	return txn.ExecuteSingle(query)
}
