package v1

import (
	"time"

	"g.tesamc.com/IT/zaipkg/config"
	"g.tesamc.com/IT/zaipkg/typeutil"
)

// There are default configs for eva.
const (
	defaultUpdatesPending = 512 // Each extent has 512 pending put, same as default Scheduler pending.

	// 4MB size per read is chosen by the testing result.
	// Discussion here: https://g.tesamc.com/IT/zbuf/issues/209#issuecomment-761
	defaultSizePerRead = typeutil.ByteSize(4 * 1024 * 1024)
	// By default, the size of segment is 1GB, which means the extent size is 256GB.
	// For a 8TB NVMe driver(raw capacity), in practice, we'll use about 70% of the capacity
	// (30% for over-provisioning & other things). So we have about 20 extents on each disk.
	//
	// It's obvious that the bigger extent, the lower rate of losing extent when there are broken disks.
	// But we can't make it too bigger either, because we may lose the property of distributed repairing,
	// we hope if there is a broken disk, more disks could help to reconstruct the data, it'll reduce the
	// load of reconstruction on disks in avg. and speeding up the process.
	defaultSegmentSize = typeutil.ByteSize(1024 * 1024 * 1024) // 1GiB.
	// Each extent has the same segment count: 256.
	// Warn:
	// Don't change it, because in present there are some hard codes are using 256 directly.
	// e.g. header.
	segmentCnt = 256
	// There are 1 segments are reserved for GC:
	// 99.6% usable storage is quite good for NVMe drivers:
	// NVMe drivers need to scrub before writing when there is a new writing if there is no free space,
	// that's why it's getting slow when the disk is more than 50% full.
	// We'll set over-provisioning, this could help.
	// For extent.v1 almost all writing are sequentially writing, and the garbage will be collected in sequentially
	// way, which means even get 90% full, the performance won't be impacted in theory.
	defaultReservedSeg = 1

	// For the worst cases, 128 means 128*4MB = 512MB, is the half of segment,
	// snapshot still has big chance to catch up the changes enough fast.
	// But for small objects, 128 is really bad, if every 128 updates we will do a snapshot syncing:
	// 1. The updates will be too frequently.
	// 2. The snapshot will be huge because the number of objects is big in a extent.
	// I've implemented a algorithm to measure the opportunity of making snapshot.
	//
	// If most of objects are small, try to raise this value avoiding creating DMU snapshot too frequently.
	// A good practise is that setting the MaxDirtyCount be the half of possible count of objects in a segment.
	defaultMaxDirtyCount = 128

	// Ensure free speed is faster than the speed of taking reserved segments by GC.
	// The min value should > 0.33, for sure there won't be too much I/O wasting.
	defaultGCRatio        = 0.55
	defaultGCInterval     = time.Hour * 24
	defaultGCScanInterval = time.Hour
	defaultDeepGCInterval = 30 * 24 * time.Hour // One month.
)

// Config is the configs of v1 extent.
type Config struct {
	// Development indicates it's in development mode or not.
	Development bool `toml:"development"`
	// UpdateOrInsert only could be true when Development is true,
	// using it for updating DMU if entry already existed.
	UpdateOrInsert bool `toml:"update_or_insert"`
	// SegmentSize is [16KB, 1GB].
	SegmentSize typeutil.ByteSize `toml:"segment_size"`
	// ReservedSeg are the count of segments reserved for GC.
	ReservedSeg int `toml:"reserved_seg"`

	// UpdatesPending is updates request queue size.
	UpdatesPending int `toml:"put_pending"`

	// Size of write buffer per reads in bytes.
	SizePerRead typeutil.ByteSize `toml:"size_per_read"`

	// MaxDirtyCount is the maximum dirty updates in DMU(memory) which we could tolerate,
	// if the dirty_count > MaxDirtyCount we should trigger a snapshot making event.
	// It should < Min_Objects_Count_in_Segment / 2,
	// e.g. the max size of object is 4MB, and segments size is 1GB,
	// MaxDirtyCount should be < 1GB/4MB/2
	// It ensures that when GCer wants to get the next source,
	// we have high probability of making snapshot.
	MaxDirtyCount int64 `toml:"max_dirty_count"`

	// GCRatio is the ratio of garbage which if the segment's garbage is beyond it, the GC will start.
	GCRatio float64 `toml:"gc_ratio"`
	// GCInterval is the interval of two GC job.
	// After GC, the GC worker will sleep for GCInterval for next scan.
	GCInterval typeutil.Duration `toml:"gc_interval"`
	// GCScanInterval is the interval of two GC scan job.
	// If there is nothing to do for GC, the GC worker will sleep for GCScanInterval for next scan.
	GCScanInterval typeutil.Duration `toml:"gc_scan_interval"`
	// DeepGCInterval is the interval of deep GC, which will traverse the whole DMU to get the accurate removed for
	// each segment.
	// See: https://g.tesamc.com/IT/zbuf/issues/150#issuecomment-578 for deailts.
	DeepGCInterval typeutil.Duration `toml:"deep_gc_interval"`
}

func (cfg *Config) Adjust() {
	if cfg.UpdateOrInsert {
		if !cfg.Development {
			cfg.UpdateOrInsert = false
		}
	}
	config.Adjust(&cfg.SegmentSize, defaultSegmentSize)
	config.Adjust(&cfg.ReservedSeg, defaultReservedSeg)
	config.Adjust(&cfg.UpdatesPending, defaultUpdatesPending)
	config.Adjust(&cfg.SizePerRead, defaultSizePerRead)
	config.Adjust(&cfg.MaxDirtyCount, int64(defaultMaxDirtyCount))
	maxDirtyCount := cfg.SegmentSize / 4 * 1024 * 1024 / 2
	if maxDirtyCount < 1 {
		cfg.MaxDirtyCount = 1 // Only meaningful for testing.
	} else {
		cfg.MaxDirtyCount = int64(maxDirtyCount)
	}
	config.Adjust(&cfg.GCRatio, defaultGCRatio)
	config.Adjust(&cfg.GCInterval, defaultGCInterval)
	config.Adjust(&cfg.GCScanInterval, defaultGCScanInterval)
	config.Adjust(&cfg.DeepGCInterval, defaultDeepGCInterval)
}

// GetDefaultConfig gets Config with default values.
func GetDefaultConfig() *Config {
	c := new(Config)
	c.Adjust()
	return c
}
