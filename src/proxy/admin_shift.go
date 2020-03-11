package proxy

import (
	"runtime"

	"config"

	"github.com/radondb/shift/build"
	"github.com/radondb/shift/shift"
	"github.com/radondb/shift/xlog"
)

const (
	cleanup   = false
	checksum  = true
	mysqlDump = "mysqldump"
	threads   = 16
	behinds   = 2048
	//radonURL               = "http://127.0.0.1:8080"
	waitTimeBeforeChecksum = 10
	toFlavor               = shift.ToRadonDBFlavor
)

type shiftInfo struct {
	From         string
	FromUser     string
	FromPassword string
	FromDatabase string
	FromTable    string

	To         string
	ToUser     string
	ToPassword string
	ToDatabase string
	ToTable    string

	RadonURL string
}

func getShiftInfo(db, srcTable, dstDB, dstTable string, spanner *Spanner, user string, log *xlog.Log) (*shiftInfo, error) {
	route := spanner.router
	scatter := spanner.scatter

	srcTableConfig, err := route.TableConfig(db, srcTable)
	if err != nil {
		log.Error("shift.start.error:%+v", err)
		return nil, err
	}

	srcBackendName := srcTableConfig.Partitions[0].Backend
	BackendConfigs := scatter.BackendConfigsClone()

	var srcInfo *config.BackendConfig
	for _, config := range BackendConfigs {
		if config.Name == srcBackendName {
			srcInfo = config
		}
	}

	var shift shiftInfo

	shift.From = srcInfo.Address
	shift.FromUser = srcInfo.User
	shift.FromPassword = srcInfo.Password
	shift.FromDatabase = db
	shift.FromTable = srcTable

	shift.To = spanner.conf.Proxy.Endpoint
	shift.ToUser = user
	shift.ToPassword = srcInfo.Password
	shift.ToDatabase = dstDB
	shift.ToTable = dstTable

	shift.RadonURL = "http://" + spanner.conf.Proxy.PeerAddress
	return &shift, nil
}

func shiftTableLow(db, srcTable, dstDB, dstTable, user string, spanner *Spanner) error {
	log := xlog.NewStdLog(xlog.Level(xlog.INFO))
	runtime.GOMAXPROCS(runtime.NumCPU())

	build := build.GetInfo()
	log.Warning("shift:[%+v]\n", build)

	//check(log)
	log.Warning(`
           IMPORTANT: Please check that the shift run completes successfully.
           At the end of a successful shift run prints "shift.completed.OK!".`)

	shiftInfo, err := getShiftInfo(db, srcTable, dstDB, dstTable, spanner, user, log)
	if err != nil {
		log.Error("shift.start.error:%+v", err)
		return err
	}

	cfg := &shift.Config{
		From:                   shiftInfo.From,
		FromUser:               shiftInfo.FromUser,
		FromPassword:           shiftInfo.FromPassword,
		FromDatabase:           shiftInfo.FromDatabase,
		FromTable:              shiftInfo.FromTable,
		To:                     shiftInfo.To,
		ToUser:                 shiftInfo.ToUser,
		ToPassword:             shiftInfo.ToPassword,
		ToDatabase:             shiftInfo.ToDatabase,
		ToTable:                shiftInfo.ToTable,
		ToFlavor:               toFlavor,
		Cleanup:                cleanup,
		MySQLDump:              mysqlDump,
		Threads:                threads,
		Behinds:                behinds,
		RadonURL:               shiftInfo.RadonURL,
		Checksum:               checksum,
		WaitTimeBeforeChecksum: waitTimeBeforeChecksum,
	}

	log.Info("shift.cfg:%+v", cfg)

	shift := shift.NewShift(log, cfg).(*shift.Shift)
	if err := shift.Start(); err != nil {
		log.Error("shift.start.error:%+v", err)
		return err
	}

	err = shift.WaitFinish()
	if err != nil {
		log.Error("shift.wait.finish.error:%+v", err)
		return err
	}
	return nil
}
