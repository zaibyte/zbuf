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
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/zaibyte/pkg/xerrors"
	"github.com/zaibyte/zbuf/server"

	"github.com/zaibyte/pkg/config"
	"github.com/zaibyte/pkg/xlog"
	scfg "github.com/zaibyte/zbuf/server/config"
)

const _appName = "zbuf"

func main() {

	// Store GOMAXPROCS bigger for these reasons:
	//
	// 1. SSD is superb, and assume ZBuf runs on a server with multi-SSD, so there is a problem:
	// SSD's latency is very low, but it will take 20μs-10ms to find a thread blocked in Go.
	// So the block may finish before notice it, the GO Process will be wasted in this situation,
	// That's why we need more process
	// (I found this trick from this discussion: https://groups.google.com/forum/#!topic/golang-nuts/jPb_h3TvlKE/discussion)
	runtime.GOMAXPROCS(128)

	config.Init(_appName)

	var cfg scfg.Config
	config.Load(&cfg)

	_, err := cfg.Log.MakeLogger(_appName)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		beforeExit()
	}()

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

	if err := svr.Run(); err != nil {
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
