/*
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package xlog

import (
	"testing"
)

// assert fails the test if the condition is false.
func Assert(tb testing.TB, condition bool, msg string, v ...interface{}) {
	if !condition {
		tb.FailNow()
	}
}

func TestGetLog(t *testing.T) {
	GetLog().Debug("DEBUG")
	log := NewStdLog()
	log.SetLevel("INFO")
	GetLog().Debug("DEBUG")
	GetLog().Info("INFO")
}

func TestSysLog(t *testing.T) {
	log := NewSysLog()

	log.Debug("DEBUG")
	log.Info("INFO")
	log.Warning("WARNING")
	log.Error("ERROR")

	log.SetLevel("DEBUG")
	log.Debug("DEBUG")
	log.Info("INFO")
	log.Warning("WARNING")
	log.Error("ERROR")

	log.SetLevel("INFO")
	log.Debug("DEBUG")
	log.Info("INFO")
	log.Warning("WARNING")
	log.Error("ERROR")

	log.SetLevel("WARNING")
	log.Debug("DEBUG")
	log.Info("INFO")
	log.Warning("WARNING")
	log.Error("ERROR")

	log.SetLevel("ERROR")
	log.Debug("DEBUG")
	log.Info("INFO")
	log.Warning("WARNING")
	log.Error("ERROR")
}

func TestStdLog(t *testing.T) {
	log := NewStdLog()

	log.Println("........DEFAULT........")
	log.Debug("DEBUG")
	log.Info("INFO")
	log.Warning("WARNING")
	log.Error("ERROR")

	log.Println("........DEBUG........")
	log.SetLevel("DEBUG")
	log.Debug("DEBUG")
	log.Info("INFO")
	log.Warning("WARNING")
	log.Error("ERROR")

	log.Println("........INFO........")
	log.SetLevel("INFO")
	log.Debug("DEBUG")
	log.Info("INFO")
	log.Warning("WARNING")
	log.Error("ERROR")

	log.Println("........WARNING........")
	log.SetLevel("WARNING")
	log.Debug("DEBUG")
	log.Info("INFO")
	log.Warning("WARNING")
	log.Error("ERROR")

	log.Println("........ERROR........")
	log.SetLevel("ERROR")
	log.Debug("DEBUG")
	log.Info("INFO")
	log.Warning("WARNING")
	log.Error("ERROR")
}

func TestLogLevel(t *testing.T) {
	log := NewStdLog()
	{
		log.SetLevel("DEBUG")
		want := DEBUG
		got := log.opts.Level
		Assert(t, want == got, "want[%v]!=got[%v]", want, got)
	}

	{
		log.SetLevel("DEBUGX")
		want := DEBUG
		got := log.opts.Level
		Assert(t, want == got, "want[%v]!=got[%v]", want, got)
	}

	{
		log.SetLevel("PANIC")
		want := PANIC
		got := log.opts.Level
		Assert(t, want == got, "want[%v]!=got[%v]", want, got)
	}

	{
		log.SetLevel("WARNING")
		want := WARNING
		got := log.opts.Level
		Assert(t, want == got, "want[%v]!=got[%v]", want, got)
	}
}
