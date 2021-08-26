/*
 * Copyright (c) 2020. Temple3x (temple3x@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"

	"g.tesamc.com/IT/zaipkg/app"
	"g.tesamc.com/IT/zaipkg/config"
	"g.tesamc.com/IT/zaipkg/xbytes"
	"g.tesamc.com/IT/zaipkg/xerrors"
	"g.tesamc.com/IT/zaipkg/xlog"
	"g.tesamc.com/IT/zaipkg/xtime/hlc"
	"g.tesamc.com/IT/zaipkg/xtime/hlc/mhlc"
	"g.tesamc.com/IT/zaipkg/xtime/systimemon"
	"g.tesamc.com/IT/zbuf/metric"
	"g.tesamc.com/IT/zbuf/server"
	scfg "g.tesamc.com/IT/zbuf/server/config"
	"github.com/templexxx/tsc"
)

const _appName = "zbuf"

func main() {

	config.Init(_appName)

	var cfg scfg.Config
	config.Load(&cfg)

	cfg.App.Adjust()

	_, err := cfg.App.Log.MakeLogger(_appName)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		beforeExit()
	}()

	if cfg.Development {
		xbytes.EnableDefault()
	} else {
		xbytes.EnableMax()
	}

	ctx, cancel := context.WithCancel(context.Background())

	svr, err := server.Create(ctx, &cfg)
	if err != nil {
		xlog.Fatal(xerrors.WithMessage(err, "create server failed").Error())
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	var sig os.Signal
	go func() {
		sig = <-sc
		cancel()
	}()

	tsc.ResetEnabled(true) // There is no sequence events in ZBuf server rely on clock.

	rand.Seed(tsc.UnixNano())

	go systimemon.StartMonitor(ctx, tsc.UnixNano, func() { // HLC clock doesn't like backward.
		xlog.Error("system time jumps backward")
		metric.TimeJumpBackCounter.Inc()
	})

	go app.TimeCalibrateLoop(ctx, cfg.App.TimeCalibrateInterval.Duration)

	mh := mhlc.New()
	hlc.InitGlobalHLC(mh)

	if err = svr.Run(); err != nil {
		svr.Close()
		xlog.Fatal(xerrors.WithMessage(err, "run server failed").Error())
	}

	<-ctx.Done()
	xlog.Infof("got signal to exit: %s", sig.String())

	svr.Close()
	switch sig {
	case syscall.SIGTERM:
		beforeExit()
		os.Exit(0)
	default:
		beforeExit()
		os.Exit(1)
	}
}

func beforeExit() {
	_ = xlog.Sync()
	_ = xlog.Close()
}
