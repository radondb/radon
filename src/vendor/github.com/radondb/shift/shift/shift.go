/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package shift

import (
	"fmt"
	"strings"
	"time"

	"github.com/radondb/shift/xbase/sync2"
	"github.com/radondb/shift/xlog"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
)

const (
	// Millisecond
	behindsDuration = 5000
)

type Shift struct {
	log           *xlog.Log
	cfg           *Config
	toPool        *Pool
	fromPool      *Pool
	canal         *canal.Canal
	behindsTicker *time.Ticker
	handler       *EventHandler

	err        chan error
	done       chan bool
	allDone    sync2.AtomicBool
	atomicBool sync2.AtomicBool // true: canal run normal; false: canal get some exception
}

func NewShift(log *xlog.Log, cfg *Config) *Shift {
	log.Info("shift.cfg:%#v", cfg)
	return &Shift{
		log:           log,
		cfg:           cfg,
		done:          make(chan bool),
		err:           make(chan error),
		behindsTicker: time.NewTicker(time.Duration(behindsDuration) * time.Millisecond),
		atomicBool:    sync2.NewAtomicBool(true),
	}
}

// Start used to start canal and behinds ticker.
func (shift *Shift) Start() error {
	log := shift.log
	if err := shift.prepareConnection(); err != nil {
		log.Error("shift.prepare.connection.error")
		return errors.Trace(err)
	}
	if err := shift.prepareTable(); err != nil {
		log.Error("shift.prepare.table.error")
		return errors.Trace(err)
	}
	if err := shift.prepareCanal(); err != nil {
		log.Error("shift.prepare.canal.error")
		return errors.Trace(err)
	}
	if err := shift.behindsCheckStart(); err != nil {
		log.Error("shift.start.check.behinds.error")
		return errors.Trace(err)
	}
	return nil
}

// In WaitFinish(), we should add signal kill operation if we use shift as a program.
func (shift *Shift) WaitFinish() error {
	log := shift.log
	// No matter shift table success or not, we do close func
	closeWithDone := func() error {
		if err := shift.close(); err != nil {
			log.Error("shift.do.close.failed:%+v", err)
			return err
		} else {
			log.Info("shift.completed.OK!")
			return nil
		}
	}
	closeWithError := func() error {
		if err := shift.close(); err != nil {
			log.Error("shift.do.close.failed:%+v", err)
			return err
		}
		return nil
	}

	select {
	case <-shift.getDoneCh():
		log.Info("shift.table.done.and.do.close.work.before.return.")
		return closeWithDone()
	case err := <-shift.getErrorCh():
		log.Error("shift.table.got.error.and.do.close.work.before.return:%+v", err)
		_ = closeWithError()
		return err
	}
}

func (shift *Shift) GetCanalStatus() bool {
	return shift.atomicBool.Get()
}

func (shift *Shift) SetCanalStatus(b bool) {
	shift.atomicBool.Set(b)
}

func (shift *Shift) prepareConnection() error {
	log := shift.log
	cfg := shift.cfg

	fromPool, err := NewPool(log, 4, cfg.From, cfg.FromUser, cfg.FromPassword)
	if err != nil {
		log.Error("shift.new.from.connection.pool.error")
		return errors.Trace(err)
	}
	shift.fromPool = fromPool
	log.Info("shift.[%s].connection.done...", cfg.From)

	toPool, err := NewPool(log, cfg.Threads, cfg.To, cfg.ToUser, cfg.ToPassword)
	if err != nil {
		log.Error("shift.new.to.connection.pool.error")
		return errors.Trace(err)
	}
	shift.toPool = toPool
	log.Info("shift.[%s].connection.done...", cfg.To)
	log.Info("shift.prepare.connections.done...")
	return nil
}

func (shift *Shift) prepareTable() error {
	log := shift.log
	cfg := shift.cfg

	// From connection.
	fromConn := shift.fromPool.Get()
	if fromConn == nil {
		return errors.Trace(errors.Errorf("shift.from.conn.get.nil"))
	}
	defer shift.fromPool.Put(fromConn)

	// To connection.
	toConn := shift.toPool.Get()
	if toConn == nil {
		return errors.Trace(errors.Errorf("shift.to.conn.get.nil"))
	}
	defer shift.toPool.Put(toConn)

	// Check the database is not system database and create them.
	if _, isSystem := sysDatabases[strings.ToLower(cfg.ToDatabase)]; !isSystem {
		log.Info("shift.prepare.database[%s]...", cfg.ToDatabase)
		sql := fmt.Sprintf("select * from information_schema.tables where table_schema = '%s' limit 1", cfg.ToDatabase)
		r, err := toConn.Execute(sql)
		if err != nil {
			log.Error("shift.check.database.sql[%s].error", sql)
			return errors.Trace(err)
		}

		if r.RowNumber() == 0 {
			sql := fmt.Sprintf("create database if not exists `%s`", cfg.ToDatabase)
			if _, err := toConn.Execute(sql); err != nil {
				log.Error("shift.create.database.sql[%s].error", sql)
				return errors.Trace(err)
			}
			log.Info("shift.prepare.database.done...")
		} else {
			log.Info("shift.database.exists...")
		}

		log.Info("shift.prepare.table[%s/%s]...", cfg.ToDatabase, cfg.ToTable)
		sql = fmt.Sprintf("show create table `%s`.`%s`", cfg.FromDatabase, cfg.FromTable)
		r, err = fromConn.Execute(sql)
		if err != nil {
			log.Error("shift.show.[%s].create.table.sql[%s].error", cfg.From, sql)
			return errors.Trace(err)
		}
		sql, err = r.GetString(0, 1)
		if err != nil {
			log.Error("shift.show.[%s].create.table.get.error", cfg.From)
			return errors.Trace(err)
		}
		sql = strings.Replace(sql, fmt.Sprintf("CREATE TABLE `%s`", cfg.FromTable), fmt.Sprintf("CREATE TABLE `%s`.`%s`", cfg.ToDatabase, cfg.ToTable), 1)
		if _, err := toConn.Execute(sql); err != nil {
			log.Error("shift.create.[%s].table.sql[%s].error", cfg.To, sql)
			return errors.Trace(err)
		}
		log.Info("shift.prepare.table.done...")
	}
	return nil
}

func (shift *Shift) prepareCanal() error {
	log := shift.log
	conf := shift.cfg
	cfg := canal.NewDefaultConfig()
	cfg.Addr = conf.From
	cfg.User = conf.FromUser
	cfg.Password = conf.FromPassword
	cfg.Dump.ExecutionPath = conf.MySQLDump
	cfg.Dump.DiscardErr = false
	cfg.Dump.TableDB = conf.FromDatabase
	cfg.Dump.Tables = []string{conf.FromTable}

	// canal
	canal, err := canal.NewCanal(cfg)
	if err != nil {
		log.Error("shift.canal.new.error")
		return errors.Trace(err)
	}

	handler := NewEventHandler(log, shift)
	canal.SetEventHandler(handler)
	shift.handler = handler
	shift.canal = canal
	go func() {
		if err := canal.Run(); err != nil {
			if !shift.allDone.Get() {
				shift.SetCanalStatus(false)
				log.Error("shift.canal.running.with.error")
				shift.err <- errors.Trace(err)
			} else {
				log.Info("shift.canal.exit.normal")
			}
		}
	}()
	log.Info("shift.prepare.canal.done...")
	return nil
}

/*
	mysql> checksum table sbtest.sbtest1;
	+----------------+-----------+
	| Table          | Checksum  |
	+----------------+-----------+
	| sbtest.sbtest1 | 410139351 |
	+----------------+-----------+
*/
// ChecksumTable ensure that FromTable and ToTable are consistent
func (shift *Shift) ChecksumTable() error {
	log := shift.log
	var fromchecksum, tochecksum uint64

	if _, isSystem := sysDatabases[strings.ToLower(shift.cfg.FromDatabase)]; isSystem {
		log.Info("shift.checksum.table.skip.system.table[%s.%s]", shift.cfg.FromDatabase, shift.cfg.FromTable)
		return nil
	}

	checksumFunc := func(t string, Conn *client.Conn, Database string, Table string, c chan interface{}) {
		sql := fmt.Sprintf("checksum table %s.%s", Database, Table)
		r, err := Conn.Execute(sql)
		if err != nil {
			log.Error("shift.checksum.%s.table[%s.%s].error", t, Database, Table)
			c <- errors.Trace(err)
		}

		v, err := r.GetUint(0, 1)
		if err != nil {
			log.Error("shift.get.%s.table[%s.%s].checksum.error", t, Database, Table)
			c <- errors.Trace(err)
		}
		c <- v
	}

	fromchan := make(chan interface{}, 1)
	tochan := make(chan interface{}, 1)

	// execute checksum func
	{
		fromConn := shift.fromPool.Get()
		if fromConn == nil {
			return errors.Trace(errors.Errorf("shift.from.conn.get.nil"))
		}
		defer shift.fromPool.Put(fromConn)

		toConn := shift.toPool.Get()
		if toConn == nil {
			return errors.Trace(errors.Errorf("shift.to.conn.get.nil"))
		}
		defer shift.toPool.Put(toConn)

		time.Sleep(time.Second * time.Duration(shift.cfg.WaitTimeBeforeChecksum))
		maxRetryTimes := 30 // max retry 2.5 minutes
		for i := 1; i < maxRetryTimes; i++ {
			go checksumFunc("from", fromConn, shift.cfg.FromDatabase, shift.cfg.FromTable, fromchan)
			go checksumFunc("to", toConn, shift.cfg.ToDatabase, shift.cfg.ToTable, tochan)
			fromc := <-fromchan
			toc := <-tochan
			switch fromc.(type) {
			case error:
				return fromc.(error)
			case uint64:
				fromchecksum = fromc.(uint64)
			default:
				return errors.Trace(errors.Errorf("shift.checksum.chan.got.wrong.type"))
			}
			switch toc.(type) {
			case error:
				return toc.(error)
			case uint64:
				tochecksum = toc.(uint64)
			default:
				return errors.Trace(errors.Errorf("shift.checksum.chan.got.wrong.type"))
			}

			if fromchecksum != tochecksum {
				log.Info("shift.checksum.table.not.eq.and.retry.times[%+v]", i)
				time.Sleep(time.Second * 5)
				continue
			} else {
				log.Info("shift.checksum.table.from[%v.%v, crc:%v].to[%v.%v, crc:%v].ok", shift.cfg.FromDatabase, shift.cfg.FromTable, fromchecksum, shift.cfg.ToDatabase, shift.cfg.ToTable, tochecksum)
				break
			}
		}
	}

	if fromchecksum != tochecksum {
		return errors.Trace(errors.Errorf("checksum not equivalent: from-table[%v.%v] checksum is %v, to-table[%v.%v] checksum is %v", shift.cfg.FromDatabase, shift.cfg.FromTable, fromchecksum, shift.cfg.ToDatabase, shift.cfg.ToTable, tochecksum))
	}
	return nil
}

/*
   mysql> show master status;
   +------------------+-----------+--------------+------------------+------------------------------------------------+
   | File             | Position  | Binlog_Do_DB | Binlog_Ignore_DB | Executed_Gtid_Set                              |
   +------------------+-----------+--------------+------------------+------------------------------------------------+
   | mysql-bin.000002 | 112107994 |              |                  | 4dc59763-5431-11e7-90cb-5254281e57de:1-2561361 |
   +------------------+-----------+--------------+------------------+------------------------------------------------+
*/
func (shift *Shift) masterPosition() (*mysql.Position, error) {
	log := shift.log
	position := &mysql.Position{}

	fromConn := shift.fromPool.Get()
	if fromConn == nil {
		return nil, errors.Trace(errors.Errorf("shift.from.conn.get.nil"))
	}
	defer shift.fromPool.Put(fromConn)

	sql := "show master status"
	r, err := fromConn.Execute(sql)
	if err != nil {
		log.Error("shift.get.master[%s].postion.error", shift.cfg.From)
		return nil, errors.Trace(err)
	}

	file, err := r.GetString(0, 0)
	if err != nil {
		log.Error("shift.get.master[%s].file.error", shift.cfg.From)
		return nil, errors.Trace(err)
	}

	pos, err := r.GetUint(0, 1)
	if err != nil {
		log.Error("shift.get.master[%s].position.error", shift.cfg.From)
		return nil, errors.Trace(err)
	}
	position.Name = file
	position.Pos = uint32(pos)
	return position, nil
}

// 1. check mysqldump worker done
// 2. check sync binlog pos
func (shift *Shift) behindsCheckStart() error {
	go func(s *Shift) {
		log := s.log
		log.Info("shift.dumping...")
		// If some error happened during dumping, wait dump will be still set dump done.
		<-s.canal.WaitDumpDone()
		prePos := s.canal.SyncedPosition()

		for range s.behindsTicker.C {
			// if allDone, the loop should be over and break, otherwise,
			// it will caused fatal exit during waitUtilPositon
			if s.allDone.Get() {
				break
			}
			// If canal get something wrong during dumping or syncing data, we should log error
			if s.GetCanalStatus() {
				masterPos, err := s.masterPosition()
				if err != nil {
					shift.err <- errors.Trace(err)
					break
				}
				syncPos := s.canal.SyncedPosition()
				behinds := int(masterPos.Pos - syncPos.Pos)
				diff := (syncPos.Pos - prePos.Pos)
				speed := diff / (behindsDuration / 1000)
				log.Info("--shift.check.behinds[%d]--master[%+v]--synced[%+v]--speed:%v events/second, diff:%v", behinds, masterPos, syncPos, speed, diff)
				if (masterPos.Name == syncPos.Name) && (behinds <= shift.cfg.Behinds) {
					if err := shift.setRadon(); err != nil {
						shift.err <- errors.Trace(err)
						break
					}
				} else {
					factor := float32(shift.cfg.Behinds+1) / float32(behinds+1)
					log.Info("shift.set.throttle.behinds[%v].cfgbehinds[%v].factor[%v]", behinds, shift.cfg.Behinds, factor)
					if err := shift.setRadonThrottle(factor); err != nil {
						shift.err <- errors.Trace(err)
						break
					}
				}
				prePos = syncPos
			} else {
				log.Error("shift.canal.get.error.during.dump.or.sync.and.behinds.check.should.be.break")
				break
			}
		}
	}(shift)
	return nil
}

// Close used to destroy all the resource.
func (shift *Shift) close() error {
	log := shift.log

	shift.behindsTicker.Stop()
	shift.canal.Close()
	shift.fromPool.Close()
	shift.toPool.Close()
	if err := shift.Cleanup(); err != nil {
		return errors.Trace(err)
	}
	log.Info("shift.do.close.done...")
	return nil
}

func (shift *Shift) getDoneCh() chan bool {
	return shift.done
}

func (shift *Shift) getErrorCh() chan error {
	return shift.err
}
