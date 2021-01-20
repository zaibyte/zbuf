package v1

import (
	"g.tesamc.com/IT/zaipkg/config"
	"g.tesamc.com/IT/zaipkg/typeutil"
)

// There are default configs for eva.
const (
	defaultFlushDelay = -1 // Flush immediately. Rely on disk latency.
	defaultPutPending = 64 // Each extent has 64 pending put.
	// 128KB is enough for NVMe device read/write sequentially.
	defaultWriteBufferSize = 128 * 1024
	defaultReadBufferSize  = 128 * 1024
	defaultSegmentSize     = 1024 * 1024 * 1024 // 1GiB.
	// Each extent has the same segment count: 256.
	// Warn:
	// Don't change it, because in present there are some hard codes are using 256 directly.
	// e.g. header.
	segmentCnt = 256
	// There are 16 segments are reserved for GC, in Tesamc, there'll be only few random deletion,
	// 16 segments may be enough.
	defaultReservedSeg = 16
)

// TODO may no need to create get queue
const (
	defaultGetPending = 1024
	defaultGetThread  = 4
)

// Config is the configs of v1 extent.
type Config struct {
	SegmentSize uint32 `toml:"segment_size"`
	// ReservedSeg are the count of segments reserved for GC.
	ReservedSeg float64 `toml:"reserved_seg"`

	// PutPending is put request queue size.
	PutPending int `toml:"put_pending"`
	// Delay between request flushes.
	//
	// Negative values lead to immediate requests' sending to the filesystem
	// without their buffering. This minimizes latency at the cost
	// of higher CPU and disk usage.
	//
	// Default value is defaultFlushDelay.
	FlushDelay typeutil.Duration `toml:"flush_delay"`
	// Size of write buffer per writes in bytes.
	// Default value is defaultWriteBufferSize.
	WriteBufferSize int `toml:"write_buffer_size"`
	// Size of write buffer per reads in bytes.
	// Default value is defaultReadBufferSize.
	ReadBufferSize int `toml:"read_buffer_size"`
}

func (cfg *Config) adjust() {
	config.Adjust(&cfg.SegmentSize, defaultSegmentSize)
	config.Adjust(&cfg.ReservedSeg, defaultReservedSeg)
	config.Adjust(&cfg.PutPending, defaultPutPending)
	config.Adjust(&cfg.FlushDelay, defaultFlushDelay)
	config.Adjust(&cfg.WriteBufferSize, defaultWriteBufferSize)
	config.Adjust(&cfg.ReadBufferSize, defaultReadBufferSize)
}
