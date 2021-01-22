package v1

import (
	"g.tesamc.com/IT/zaipkg/config"
	"g.tesamc.com/IT/zaipkg/typeutil"
)

// There are default configs for eva.
const (
	defaultFlushDelay = -1  // Flush immediately. Rely on disk latency.
	defaultPutPending = 512 // Each extent has 512 pending put, same as default Scheduler pending.
	// 128KB is enough for NVMe device read/write sequentially.
	defaultWriteBufferSize = 128 * 1024
	defaultReadBufferSize  = 128 * 1024
	defaultSegmentSize     = 1024 * 1024 * 1024 // 1GiB.
	// Each extent has the same segment count: 256.
	// Warn:
	// Don't change it, because in present there are some hard codes are using 256 directly.
	// e.g. header.
	segmentCnt = 256
	// There are 48 segments are reserved for GC:
	// 1. The load factor is 0.8125 at most, suitable for the phy_addr algorithm.
	// 2. 81.25% usable storage is quite good for NVMe drivers:
	// NVMe drivers need to scrub before writing when there is a new writing if there is no free space,
	// that's why it's getting slow when the disk is more than 50% full.
	// For extent.v1 almost all writing are sequentially writing, and the garbage will be collected in sequentially
	// way, which means even get 80% full, the performance won't be impacted in theory.
	defaultReservedSeg = 48

	// For the worst cases, 128 means 128*4MB = 512MB, is the half of segment,
	// snapshot still has big chance to catch up the changes enough fast.
	defaultMaxDirtyCount = 128
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

	// MaxDirtyCount is the maximum dirty updates in phy_addr(memory) which we could tolerate,
	// if the dirty_count > MaxDirtyCount we should trigger a snapshot making event.
	MaxDirtyCount int `toml:"max_dirty_count"`
}

func (cfg *Config) adjust() {
	config.Adjust(&cfg.SegmentSize, defaultSegmentSize)
	config.Adjust(&cfg.ReservedSeg, defaultReservedSeg)
	config.Adjust(&cfg.PutPending, defaultPutPending)
	config.Adjust(&cfg.FlushDelay, defaultFlushDelay)
	config.Adjust(&cfg.WriteBufferSize, defaultWriteBufferSize)
	config.Adjust(&cfg.ReadBufferSize, defaultReadBufferSize)
	config.Adjust(&cfg.MaxDirtyCount, defaultMaxDirtyCount)
}
