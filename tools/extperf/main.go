package main

import (
	"context"
	"log"
	"os"
	"runtime"

	"github.com/zaibyte/zaipkg/config"
	"github.com/zaibyte/zaipkg/xbytes"
	"github.com/zaibyte/zaipkg/xerrors"
	"github.com/zaibyte/zaipkg/xlog/xlogtest"
	"github.com/zaibyte/zbuf/tools/extperf/extperf"
)

const _appName = "zbuf-ext-perf"

func main() {

	// extperf is mainly built for testing one disk performance. 128 is enough.
	runtime.GOMAXPROCS(128)

	config.Init(_appName)

	xbytes.EnableMax()

	var cfg extperf.Config
	config.Load(&cfg)

	xlogtest.New(!cfg.PrintLog)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r, err := extperf.Create(ctx, &cfg)
	if err != nil {
		log.Fatal(xerrors.WithMessage(err, "create failed").Error())
	}

	if err = r.Run(); err != nil {
		log.Fatal(xerrors.WithMessage(err, "run failed").Error())
	}

	os.Exit(0)
}
