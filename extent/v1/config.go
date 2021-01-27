package v1

import (
	"time"

	"g.tesamc.com/IT/zaipkg/config"
	"g.tesamc.com/IT/zaipkg/typeutil"
)

// There are default configs for eva.
const (
	defaultUpdatesPending = 512 // Each extent has 512 pending put, same as default Scheduler pending.
	// 128KB is enough for NVMe device read/write sequentially.
	// Too big value may block other requests too long.
	defaultSizePerWrite = typeutil.ByteSize(128 * 1024)
	defaultSizePerRead  = typeutil.ByteSize(128 * 1024)
	defaultSegmentSize  = typeutil.ByteSize(1024 * 1024 * 1024) // 1GiB.
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
	// But for small objects, 128 is really bad, if every 128 updates we will do a snapshot syncing:
	// 1. The updates will be too frequently.
	// 2. The snapshot will be huge because the number of objects is big in a extent.
	// I've implemented a algorithm to measure the opportunity of making snapshot.
	defaultMaxDirtyCount = 128

	defaultGCRatio        = 0.5
	defaultGCInterval     = time.Hour * 24
	defaultGCScanInterval = time.Hour
)

// TODO may no need to create get queue
const (
	defaultGetPending = 1024
	defaultGetThread  = 4
)

// Config is the configs of v1 extent.
type Config struct {
	SegmentSize typeutil.ByteSize `toml:"segment_size"`
	// ReservedSeg are the count of segments reserved for GC.
	ReservedSeg float64 `toml:"reserved_seg"`

	// UpdatesPending is updates request queue size.
	UpdatesPending int `toml:"put_pending"`

	// Size of per writes in bytes.
	SizePerWrite typeutil.ByteSize `toml:"size_per_write"`
	// Size of write buffer per reads in bytes.
	SizePerRead typeutil.ByteSize `toml:"size_per_read"`

	// MaxDirtyCount is the maximum dirty updates in phy_addr(memory) which we could tolerate,
	// if the dirty_count > MaxDirtyCount we should trigger a snapshot making event.
	MaxDirtyCount int64 `toml:"max_dirty_count"`

	// GCRatio is the ratio of garbage which if the segment's garbage is beyond it, the GC will start.
	GCRatio float64 `toml:"gc_ratio"`
	// GCInterval is the interval of two GC job.
	// After GC, the GC worker will sleep for GCInterval for next scan.
	GCInterval typeutil.Duration `toml:"gc_interval"`
	// GCScanInterval is the interval of two GC scan job.
	// If there is nothing to do for GC, the GC worker will sleep for GCScanInterval for next scan.
	GCScanInterval typeutil.Duration `toml:"gc_scan_interval"`
}

func (cfg *Config) adjust() {
	config.Adjust(&cfg.SegmentSize, defaultSegmentSize)
	config.Adjust(&cfg.ReservedSeg, defaultReservedSeg)
	config.Adjust(&cfg.UpdatesPending, defaultUpdatesPending)
	config.Adjust(&cfg.SizePerWrite, defaultSizePerWrite)
	config.Adjust(&cfg.SizePerRead, defaultSizePerRead)
	config.Adjust(&cfg.MaxDirtyCount, defaultMaxDirtyCount)
	config.Adjust(&cfg.GCRatio, defaultGCRatio)
	config.Adjust(&cfg.GCInterval, defaultGCInterval)
	config.Adjust(&cfg.GCScanInterval, defaultGCScanInterval)
}
