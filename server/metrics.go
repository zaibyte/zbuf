package server

import (
	"github.com/VictoriaMetrics/metrics"
)

var (
	timeJumpBackCounter = metrics.NewCounter(`zbuf_monitor_time_jump_back_total`)
)

// TODO add disk/extent broken metrics
