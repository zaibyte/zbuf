package eva

import (
	"g.tesamc.com/IT/zaipkg/typeutil"

	"g.tesamc.com/IT/zaipkg/config"
	"g.tesamc.com/IT/zbuf/xio"
)

// There are default configs for eva.
const (
	defaultFlushDelay = -1 // Flush immediately. Rely on disk latency.
	defaultPutPending = 64 // Each extent has 64 pending put.
	defaultGetPending = 1024
	defaultGetThread  = 4

	// 128KB is enough for NVMe device read/write sequentially.
	DefaultWriteBufferSize = 128 * 1024
	DefaultReadBufferSize  = 128 * 1024
)

// Config is the configs of EVA.
type Config struct {
	Path         string
	SegmentSize  int64
	SizePerWrite int64
	GetThread    int
	GetPending   int
	PutPending   int
	InsertOnly   bool
	// Delay between request flushes.
	//
	// Negative values lead to immediate requests' sending to the filesystem
	// without their buffering. This minimizes latency at the cost
	// of higher CPU and disk usage.
	//
	// Default value is DefaultFlushDelay.
	FlushDelay typeutil.Duration `toml:"flush_delay"`

	// Size of write buffer per writes in bytes.
	// Default value is DefaultWriteBufferSize.
	WriteBufferSize int `toml:"write_buffer_size"`
	// Size of write buffer per reads in bytes.
	// Default value is DefaultReadBufferSize.
	ReadBufferSize int `toml:"read_buffer_size"`
}

func (cfg *Config) adjust() {
	config.Adjust(&cfg.SegmentSize, defaultSegmentSize)
	config.Adjust(&cfg.PutPending, defaultPutPending)
	config.Adjust(&cfg.FlushDelay, defaultFlushDelay)
	config.Adjust(&cfg.SizePerWrite, xio.DefaultSizePerWrite)
	config.Adjust(&cfg.GetPending, defaultGetPending)
	config.Adjust(&cfg.GetThread, defaultGetThread)
}
