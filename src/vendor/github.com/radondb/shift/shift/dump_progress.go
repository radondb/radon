package shift

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"github.com/radondb/shift/xbase"
)

const (
	metaDir = "/tmp/radon-meta-shift"
)

// ShiftProgress used to store progress rate msg during shift data
type ShiftProgress struct {
	DumpProgressRate string `json:"dump-progress-rate"`
	DumpRemainTime   string `json:"remain-time"`
	PositionBehinds  string `json:"position-behinds"`
	SynGTID          string `json:"current-sync-gtid"`
	MasterGTID       string `json:"current-src-mysql-gtid"`
	// 1. success 2. fail 3. migrating
	MigrateStatus string `json:"current-migrate-status"`
}

func (shift *Shift) UpdateProgress(rate, time, pos, sync, master, status string) {
	shift.mu.Lock()
	defer shift.mu.Unlock()
	if rate != "" {
		shift.progress.DumpProgressRate = rate
	}
	if time != "" {
		shift.progress.DumpRemainTime = time
	}
	if pos != "" {
		shift.progress.PositionBehinds = pos
	}
	if sync != "" {
		shift.progress.SynGTID = sync
	}
	if master != "" {
		shift.progress.MasterGTID = master
	}
	if status != "" {
		shift.progress.MigrateStatus = status
	}
}

// WriteShiftProgress used to update progress rate during shift data
func (shift *Shift) WriteShiftProgress() error {
	shift.mu.Lock()
	defer shift.mu.Unlock()
	b, err := json.Marshal(shift.progress)
	if err != nil {
		return errors.WithStack(err)
	}
	if err := os.MkdirAll(metaDir, os.ModePerm); err != nil {
		return err
	}
	shiftProgressFile := fmt.Sprintf("%s_%s.json", shift.cfg.FromDatabase, shift.cfg.FromTable)
	file := filepath.Join(metaDir, shiftProgressFile)

	return xbase.WriteFile(file, b)
}

// ReadShiftProgress used to read the config version from the file.
func (shift *Shift) ReadShiftProgress() (*ShiftProgress, error) {
	shift.mu.Lock()
	defer shift.mu.Unlock()

	// generate the file path
	shiftProgressFile := fmt.Sprintf("%s_%s.json", shift.cfg.FromDatabase, shift.cfg.FromTable)
	file := filepath.Join(metaDir, shiftProgressFile)
	b, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// get progress info
	var s ShiftProgress
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, errors.WithStack(err)
	}
	return &s, nil
}

/*
mysql> select table_rows from information_schema.tables where TABLE_SCHEMA = 'db' and table_name='ccs_dev_data_record';
+------------+
| table_rows |
+------------+
|    1415423 |
+------------+
1 row in set (0.00 sec)
*/
/* count dump progress */
const (
	secondsPerMinute = 60
	secondsPerHour   = secondsPerMinute * 60
	secondsSleep     = 30
)

func (shift *Shift) dumpProgress() error {
	go func(s *Shift) {
		cfg := s.cfg
		log := s.log

		// init progress file
		shift.UpdateProgress("0%", "", "", "", "", "migrating")
		shift.WriteShiftProgress()

		// First time to calculate toRows(named baseRows) and flag(named firstCalFlag)
		var dumpTime, baseRows, fromRows, toRows uint64
		firstCalFlag := true

		sqlFrom := fmt.Sprintf("select table_rows from information_schema.tables where TABLE_SCHEMA = '%s' and table_name='%s';", cfg.ToDatabase, cfg.ToTable)
		sqlTo := fmt.Sprintf("select table_rows from information_schema.tables where TABLE_SCHEMA = '%s' and table_name='%s';", cfg.FromDatabase, cfg.FromTable)

		// Get fromRows from fromConn
		fromConn := s.fromPool.Get()
		defer s.fromPool.Put(fromConn)
		if r, err := fromConn.Execute(sqlFrom); err != nil {
			shift.err <- err
		} else {
			fromRows, _ = r.GetUintByName(0, "table_rows")
			log.Info("fromRows when dump begin:%d", fromRows)
		}

		toConn := s.toPool.Get()
		defer s.toPool.Put(toConn)
		for {
			// If first time we execute, skip to add dumpTime to get baseRows first
			// and then we really start to calculate dump progress
			if !firstCalFlag {
				dumpTime += secondsSleep
			}

			// Get to rows from toConn
			if r, err := toConn.Execute(sqlTo); err != nil {
				shift.err <- err
			} else {
				toRows, _ = r.GetUintByName(0, "table_rows")
			}

			// Store rows when execute loop first time
			if firstCalFlag {
				baseRows = toRows
				firstCalFlag = false
				time.Sleep(secondsSleep * time.Second)
				continue
			}

			// Calculate remain time
			rowsInc := toRows - baseRows
			if dumpTime == 0 {
				// dumpTime should not be 0, always set to 1, we'll use it to divide later
				dumpTime = 1
			}
			avgRate := rowsInc / dumpTime
			// like dumpTime, avgRate should not be 0, we'll use it to divide later
			if avgRate == 0 {
				avgRate = 1
			}

			// Unit: second
			remainTime := (fromRows - rowsInc) / avgRate
			seconds := remainTime % secondsPerMinute
			hours := (remainTime - seconds) / secondsPerHour
			minutes := (remainTime - seconds - hours*secondsPerHour) / secondsPerMinute

			// Calculate progress rate
			// If data is not so large, it may be happened that per > 100
			// as the result of "show table status" is estimate in MySQL
			if fromRows == 0 {
				fromRows = 1
			}
			log.Info("fromRows before cal:%+v", fromRows)
			per := uint((float64(toRows) / float64(fromRows)) * 100)
			if per > 100 {
				per = 100
			}

			shift.UpdateProgress(fmt.Sprintf("%v%v", per, "%"), fmt.Sprintf("%v%v %v%v", hours, "hours", minutes, "minutes"), "", "", "", "")

			s.WriteShiftProgress()
			log.Info("dump.phase.progress%+v", shift.progress)

			if per > 98 {
				log.Warning("wait.dump.phase.done.and.now.progress:%+v", shift.progress)
				<-s.canal.WaitDumpDone()
				shift.UpdateProgress("100%", "0", "", "", "", "")
				s.WriteShiftProgress()
				log.Warning("dump.done:%+v", shift.progress)
				break
			}

			time.Sleep(secondsSleep * time.Second)
		}
	}(shift)

	return nil
}
