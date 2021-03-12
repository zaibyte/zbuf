package main

import (
	"context"
	"log"
	"os"
	"runtime"

	"g.tesamc.com/IT/zaipkg/config"
	"g.tesamc.com/IT/zaipkg/xerrors"
	_ "g.tesamc.com/IT/zaipkg/xlog/xlogtest"
	"g.tesamc.com/IT/zbuf/tools/v1perf/v1perf"
)

const _appName = "zbuf-v1-perf"

func main() {

	runtime.GOMAXPROCS(256)

	config.Init(_appName)

	var cfg v1perf.Config
	config.Load(&cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r, err := v1perf.Create(ctx, &cfg)
	if err != nil {
		log.Fatal(xerrors.WithMessage(err, "create zbuf-v1-perf failed").Error())
	}

	if err := r.Run(); err != nil {
		log.Fatal(xerrors.WithMessage(err, "run zbuf-v1-perf failed").Error())
	}

	os.Exit(0)
}
