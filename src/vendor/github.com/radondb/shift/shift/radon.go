/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package shift

import (
	"net/http"
	"strings"
	"time"

	"github.com/radondb/shift/xbase"

	"github.com/juju/errors"
)

func (shift *Shift) setRadonReadOnly(v bool) error {
	log := shift.log
	cfg := shift.cfg
	path := cfg.RadonURL + "/v1/radon/readonly"

	type request struct {
		Readonly bool `json:"readonly"`
	}
	req := &request{
		Readonly: v,
	}
	log.Info("shift.set.radon[%s].readonly.req[%+v]", path, req)

	resp, cleanup, err := xbase.HTTPPut(path, req)
	defer cleanup()
	if err != nil {
		return errors.Trace(err)
	}

	if resp == nil || resp.StatusCode != http.StatusOK {
		return errors.Trace(errors.Errorf("shift.set.radon.readonly[%s].response.error:%+s", path, xbase.HTTPReadBody(resp)))
	}
	return nil
}

func (shift *Shift) setRadonRule() error {
	log := shift.log
	cfg := shift.cfg
	path := cfg.RadonURL + "/v1/shard/shift"

	if _, isSystem := sysDatabases[strings.ToLower(shift.cfg.FromDatabase)]; isSystem {
		log.Info("shift.set.radon.rune.skip.system.table:[%s.%s]", shift.cfg.FromDatabase, shift.cfg.FromTable)
		return nil
	}

	type request struct {
		Database    string `json:"database"`
		Table       string `json:"table"`
		FromAddress string `json:"from-address"`
		ToAddress   string `json:"to-address"`
	}
	req := &request{
		Database:    cfg.FromDatabase,
		Table:       cfg.FromTable,
		FromAddress: cfg.From,
		ToAddress:   cfg.To,
	}
	log.Info("shift.set.radon[%s].rule.req[%+v]", path, req)

	resp, cleanup, err := xbase.HTTPPost(path, req)
	defer cleanup()
	if err != nil {
		return errors.Trace(err)
	}
	if resp == nil || resp.StatusCode != http.StatusOK {
		return errors.Trace(errors.Errorf("shift.set.radon.shard.rule[%s].response.error:%+s", path, xbase.HTTPReadBody(resp)))
	}
	return nil
}

var (
	radon_limits_min = 500
	radon_limits_max = 10000
)

func (shift *Shift) setRadonThrottle(factor float32) error {
	log := shift.log
	cfg := shift.cfg
	path := cfg.RadonURL + "/v1/radon/throttle"

	type request struct {
		Limits int `json:"limits"`
	}

	// limits =0 means unlimits.
	limits := int(float32(radon_limits_max) * factor)
	if limits != 0 && limits < radon_limits_min {
		limits = radon_limits_min
	}
	req := &request{
		Limits: limits,
	}
	log.Info("shift.set.radon[%s].throttle.to.req[%+v].by.factor[%v].limits[%v]", path, req, factor, limits)

	resp, cleanup, err := xbase.HTTPPut(path, req)
	defer cleanup()
	if err != nil {
		return errors.Trace(err)
	}

	if resp == nil || resp.StatusCode != http.StatusOK {
		return errors.Trace(errors.Errorf("shift.set.radon.throttle[%s].response.error:%+s", path, xbase.HTTPReadBody(resp)))
	}
	return nil
}

func (shift *Shift) setRadon() error {
	log := shift.log

	// 1. WaitUntilPos
	{
		masterPos, err := shift.masterPosition()
		if err != nil {
			return errors.Trace(err)
		}
		log.Info("shift.wait.until.pos[%#v]...", masterPos)
		if err := shift.canal.WaitUntilPos(*masterPos, time.Hour*12); err != nil {
			log.Error("shift.set.radon.wait.until.pos[%#v].error", masterPos)
			return errors.Trace(err)
		}
		log.Info("shift.wait.until.pos.done...")
	}

	// 2. Set radon to readonly.
	{
		log.Info("shift.set.radon.readonly...")
		if err := shift.setRadonReadOnly(true); err != nil {
			log.Error("shift.set.radon.readonly.error")
			return errors.Trace(err)
		}
		log.Info("shift.set.radon.readonly.done...")
	}

	// 3. Wait again.
	{
		masterPos, err := shift.masterPosition()
		if err != nil {
			return errors.Trace(err)
		}
		log.Info("shift.wait.until.pos.again[%#v]...", masterPos)
		if err := shift.canal.WaitUntilPos(*masterPos, time.Second*300); err != nil {
			log.Error("shift.wait.until.pos.again[%#v].error", masterPos)
			return errors.Trace(err)
		}
		log.Info("shift.wait.until.pos.again.done...")
	}

	// 4. Checksum table.
	if shift.cfg.Checksum {
		log.Info("shift.checksum.table...")
		if err := shift.ChecksumTable(); err != nil {
			log.Error("shift.checksum.table.error")
			return errors.Trace(err)
		}
		log.Info("shift.checksum.table.done...")
	}

	// 5. Rename ToTable.
	{
		log.Info("shift.rename.totable...")
		if err := shift.renameToTable(); err != nil {
			log.Error("shift.rename.totable.error")
			return errors.Trace(err)
		}
		log.Info("shift.rename.totable.done...")
	}

	// 6. Set radon rule.
	{
		if shift.cfg.ToFlavor == ToMySQLFlavor || shift.cfg.ToFlavor == ToMariaDBFlavor {
			log.Info("shift.set.radon.rule...")
			if err := shift.setRadonRule(); err != nil {
				log.Error("shift.set.radon.rule.error")
				return errors.Trace(err)
			}
			log.Info("shift.set.radon.rule.done...")
		}
	}

	// 7. Set radon to read/write.
	{
		log.Info("shift.set.radon.to.write...")
		if err := shift.setRadonReadOnly(false); err != nil {
			log.Error("shift.set.radon.to.write.error")
			return errors.Trace(err)
		}
		log.Info("shift.set.radon.to.write.done...")
	}

	// 8. Set radon throttle to unlimits.
	{
		log.Info("shift.set.radon.throttle.to.unlimits...")
		if err := shift.setRadonThrottle(0); err != nil {
			log.Error("shift.set.radon.throttle.to.unlimits.error")
			return errors.Trace(err)
		}
		log.Info("shift.set.radon.throttle.to.unlimits.done...")
	}

	// 9. Good, we have all done.
	{
		shift.done <- true
		shift.allDone.Set(true)
		log.Info("shift.all.done...")
	}
	return nil
}
