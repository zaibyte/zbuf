package metric

import (
	"github.com/VictoriaMetrics/metrics"
)

var (
	TimeJumpBackCounter = metrics.NewCounter(`zbuf_monitor_time_jump_back_total`)
)

// TODO add disk/extent broken metrics
