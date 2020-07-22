package proxy

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"backend"
	"config"
	"plugins"
	"plugins/shiftmanager"
	"router"

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	subtable = regexp.MustCompile("_[0-9]{4}$")
)

// SubTableToTable used to determine from is subtable or not; if it is, get the table from the subtable.
func SubTableToTable(from string) (isSub bool, to string) {
	isSub = false
	to = ""

	Suffix := subtable.FindAllStringSubmatch(from, -1)
	lenSuffix := len(Suffix)
	if lenSuffix == 0 {
		return
	}

	isSub = true
	to = strings.TrimSuffix(from, Suffix[0][lenSuffix-1])
	return
}

type BackendSize struct {
	Name    string
	Address string
	Size    float64
	User    string
	Passwd  string
}

func ShardBalanceAdvice(log *xlog.Log, spanner *Spanner, scatter *backend.Scatter, route *router.Router,
	max, min *BackendSize, database, table *string, tableSize *float64) error {
	backends := scatter.Backends()

	for _, backend := range backends {
		query := "select round((sum(data_length) + sum(index_length)) / 1024/ 1024, 0)  as SizeInMB from information_schema.tables"
		qr, err := spanner.ExecuteOnThisBackend(backend, query)
		if err != nil {
			log.Error("admin.rebalance.advice.backend[%s].error:%+v", backend, err)
			return err
		}

		if len(qr.Rows) > 0 {
			valStr := string(qr.Rows[0][0].Raw())
			datasize, err := strconv.ParseFloat(valStr, 64)
			if err != nil {
				log.Error("admin.rebalance.advice.parse.value[%s].error:%+v", valStr, err)
				return err
			}

			if datasize > max.Size {
				max.Name = backend
				max.Size = datasize
			}

			if min.Size == 0 {
				min.Name = backend
				min.Size = datasize
			}
			if datasize < min.Size {
				min.Name = backend
				min.Size = datasize
			}
		}
	}
	log.Warning("admin.rebalance.advice.max:[%+v], min:[%+v]", max, min)

	// The differ must big than 256MB.
	delta := float64(100)
	differ := (max.Size - min.Size)
	if differ < delta {
		log.Warning("admin.rebalance.advice.return.nil.since.differ[%+vMB].less.than.%vMB", differ, delta)
		//err := fmt.Sprintf("admin.rebalance.advice.return.nil.since.differ[%+vMB].less.than.%vMB", differ, delta)
		return nil
	}

	backendConfs := scatter.BackendConfigsClone()
	for _, bconf := range backendConfs {
		if bconf.Name == max.Name {
			max.Address = bconf.Address
			max.User = bconf.User
			max.Passwd = bconf.Password
		} else if bconf.Name == min.Name {
			min.Address = bconf.Address
			min.User = bconf.User
			min.Passwd = bconf.Password
		}
	}

	// 2. Find the best table.
	query := "SELECT table_schema, table_name, ROUND((SUM(data_length+index_length)) / 1024/ 1024, 0) AS sizeMB FROM information_schema.TABLES GROUP BY table_name HAVING SUM(data_length + index_length)>10485760 ORDER BY (data_length + index_length) DESC"
	qr, err := spanner.ExecuteOnThisBackend(max.Name, query)
	if err != nil {
		log.Error("admin.rebalance.advice.get.max[%+v].tables.error:%+v", max, err)
		return err
	}

	for _, row := range qr.Rows {
		db := string(row[0].Raw())
		tbl := string(row[1].Raw())
		valStr := string(row[2].Raw())
		tblSize, err := strconv.ParseFloat(valStr, 64)
		if err != nil {
			log.Error("admin.rebalance.advice.get.tables.parse.value[%s].error:%+v", valStr, err)
			return err
		}

		// Make sure the table is small enough.
		if (min.Size + tblSize) < (max.Size - tblSize) {
			isSub, t := SubTableToTable(tbl)
			if isSub {
				partitionType, err := route.PartitionType(db, t)
				// The advice table just hash, Filter the global/single/list table.
				if err == nil && route.IsPartitionHash(partitionType) {
					//Find the advice table.
					*database = db
					*table = tbl
					*tableSize = tblSize
					break
				}
			}

			log.Warning("admin.rebalance.advice.skip.table[%v]", tbl)
		}
	}
	return nil
}

func RebalanceMigrate(log *xlog.Log, rebalance *Rebalance, max, min *BackendSize, database, table string) error {
	p := &migrateParams{
		From:                   max.Address,
		FromUser:               max.User,
		FromPassword:           max.Passwd,
		FromDatabase:           database,
		FromTable:              table,
		To:                     min.Address,
		ToUser:                 min.User,
		ToPassword:             min.Passwd,
		ToDatabase:             database,
		ToTable:                table,
		RadonURL:               "http://" + rebalance.spanner.conf.Proxy.PeerAddress,
		Rebalance:              false,
		Cleanup:                true,
		MySQLDump:              "mysqldump",
		Threads:                16,
		Behinds:                2048,
		Checksum:               true,
		WaitTimeBeforeChecksum: 10,
	}

	if rebalance.spanner.ReadOnly() {
		log.Error("admin.rebalance.error:The MySQL server is running with the --read-only option")
		err := fmt.Sprintf("admin.rebalance.error:The MySQL server is running with the --read-only option")
		return errors.New(err)
	}

	// check args.
	if len(p.FromUser) == 0 || len(p.FromDatabase) == 0 || len(p.FromTable) == 0 ||
		len(p.ToUser) == 0 || len(p.ToDatabase) == 0 || len(p.ToTable) == 0 {
		log.Error("admin.rebalance[%+v].error:some param is empty", p)
		err := fmt.Sprintf("admin.rebalance[%+v].error:some param is empty", p)
		return errors.New(err)
	}

	// Check the backend name.
	var fromBackend, toBackend string
	backends := rebalance.scatter.BackendConfigsClone()
	for _, backend := range backends {
		if backend.Address == p.From {
			fromBackend = backend.Name
		} else if backend.Address == p.To {
			toBackend = backend.Name
		}
	}
	if fromBackend == "" || toBackend == "" {
		log.Error("admin.rebalance.fromBackend[%s].or.toBackend[%s].is.NULL", fromBackend, toBackend)
		err := fmt.Sprintf("admin.rebalance.fromBackend[%s].or.toBackend[%s].is.NULL", fromBackend, toBackend)
		return errors.New(err)
	}

	cfg := &shiftmanager.ShiftInfo{
		From:                   p.From,
		FromUser:               p.FromUser,
		FromPassword:           p.FromPassword,
		FromDatabase:           p.FromDatabase,
		FromTable:              p.FromTable,
		To:                     p.To,
		ToUser:                 p.ToUser,
		ToPassword:             p.ToPassword,
		ToDatabase:             p.ToDatabase,
		ToTable:                p.ToTable,
		Rebalance:              p.Rebalance,
		Cleanup:                p.Cleanup,
		MysqlDump:              p.MySQLDump,
		Threads:                p.Threads,
		PosBehinds:             p.Behinds,
		RadonURL:               p.RadonURL,
		Checksum:               p.Checksum,
		WaitTimeBeforeChecksum: p.WaitTimeBeforeChecksum,
	}

	shiftMgr := rebalance.plugins.PlugShiftMgr()
	shift, _ := shiftMgr.NewShiftInstance(cfg, shiftmanager.ShiftTypeRebalance)

	key := fmt.Sprintf("`%s`.`%s`_%s", p.ToDatabase, p.ToTable, toBackend)
	err := shiftMgr.StartShiftInstance(key, shift, shiftmanager.ShiftTypeRebalance)
	if err != nil {
		log.Error("shift.start.error:%+v", err)
		return err
	}

	err = shiftMgr.WaitInstanceFinish(key)
	if err != nil {
		log.Error("shift.wait.finish.error:%+v", err)
		return err
	}
	log.Warning("rebalance.migrate.done...")
	return nil
}

// Rebalance ...
type Rebalance struct {
	log     *xlog.Log
	scatter *backend.Scatter
	router  *router.Router
	spanner *Spanner
	conf    *config.Config
	plugins *plugins.Plugin
}

// NewRebalance -- creates new Rebalance handler.
func NewRebalance(log *xlog.Log, scatter *backend.Scatter, router *router.Router, spanner *Spanner, conf *config.Config, plugins *plugins.Plugin) *Rebalance {
	return &Rebalance{
		log:     log,
		scatter: scatter,
		router:  router,
		spanner: spanner,
		conf:    conf,
		plugins: plugins,
	}
}

type migrateParams struct {
	From         string `json:"from"`
	FromUser     string `json:"from-user"`
	FromPassword string `json:"from-password"`
	FromDatabase string `json:"from-database"`
	FromTable    string `json:"from-table"`

	To         string `json:"to"`
	ToUser     string `json:"to-user"`
	ToPassword string `json:"to-password"`
	ToDatabase string `json:"to-database"`
	ToTable    string `json:"to-table"`

	RadonURL               string `json:"radonurl"`
	Rebalance              bool   `json:"rebalance"`
	Cleanup                bool   `json:"cleanup"`
	MySQLDump              string `json:"mysqldump"`
	Threads                int    `json:"threads"`
	Behinds                int    `json:"behinds"`
	Checksum               bool   `json:"checksum"`
	WaitTimeBeforeChecksum int    `json:"wait-time-before-checksum"`
}

type ruleParams struct {
	Database    string `json:"database"`
	Table       string `json:"table"`
	FromAddress string `json:"from-address"`
	ToAddress   string `json:"to-address"`
}

// Rebalance used to Rebalance.
func (r *Rebalance) Rebalance() (*sqltypes.Result, error) {
	log := r.log
	max := &BackendSize{}
	min := &BackendSize{}
	var database, table string
	var tableSize float64

	if err := ShardBalanceAdvice(log, r.spanner, r.scatter, r.router, max, min, &database, &table, &tableSize); err != nil {
		log.Error("admin.rebalance.advice.return.error:%+v", err)
		return &sqltypes.Result{}, err
	}

	if err := RebalanceMigrate(log, r, max, min, database, table); err != nil {
		log.Error("admin.rebalance.migrate.return.error:%+v", err)
		return &sqltypes.Result{}, err
	}
	return &sqltypes.Result{}, nil
}
