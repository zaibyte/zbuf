package main

import (
	"context"
	"log"
	"os"
	"runtime"

	"g.tesamc.com/IT/zaipkg/xbytes"

	"g.tesamc.com/IT/zaipkg/config"
	"g.tesamc.com/IT/zaipkg/xerrors"
	_ "g.tesamc.com/IT/zaipkg/xlog/xlogtest" // Using xlogtest here because we don't really need the log.
	"g.tesamc.com/IT/zbuf/tools/extperf/extperf"
)

const _appName = "zbuf-ext-perf"

func main() {

	runtime.GOMAXPROCS(256)

	config.Init(_appName)

	xbytes.EnableMax()

	var cfg extperf.Config
	config.Load(&cfg)

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
