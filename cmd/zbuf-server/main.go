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

package zbuf_server

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/zaibyte/pkg/xerrors"
	"github.com/zaibyte/zbuf/server"

	"github.com/zaibyte/pkg/config"
	"github.com/zaibyte/pkg/xlog"
	scfg "github.com/zaibyte/zbuf/server/config"
)

const _appName = "zbuf"

func main() {
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
