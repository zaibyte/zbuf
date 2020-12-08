package eva

import (
	"time"

	"g.tesamc.com/IT/zaipkg/config"
	"g.tesamc.com/IT/zbuf/xio"
)

// There are default configs for eva.
const (
	defaultFlushDelay = -1 // Flush immediately. Rely on disk latency.
	defaultPutPending = 64 // Each extent has 64 pending put.
	defaultGetPending = 1024
	defaultGetThread  = 4
)

type ExtentConfig struct {
	Path         string
	SegmentSize  int64
	SizePerWrite int64
	FlushDelay   time.Duration
	GetThread    int
	GetPending   int
	PutPending   int
	InsertOnly   bool
}

func (cfg *ExtentConfig) adjust() {
	config.Adjust(&cfg.SegmentSize, defaultSegmentSize)
	config.Adjust(&cfg.PutPending, defaultPutPending)
	if cfg.FlushDelay == 0 {
		cfg.FlushDelay = defaultFlushDelay
	}
	config.Adjust(&cfg.SizePerWrite, xio.DefaultSizePerWrite)
	config.Adjust(&cfg.GetPending, defaultGetPending)
	config.Adjust(&cfg.GetThread, defaultGetThread)
}
