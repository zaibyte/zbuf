package extperf

type Config struct {
	Version        int    `toml:"version"`
	ExtentsPerDisk int    `toml:"extents_per_disk"`
	DataRoot       string `toml:"data_root"`

	BlockSize int64  `toml:"block_size"` // KB.
	JobType   string `toml:"job_type"`
	JobTime   int64  `toml:"job_time"` // sec
	// Ignore first SkipTime when collect result.
	SkipTime int64 `toml:"skip_time"`

	// MBPer_XXX_Thread * threads = total_IO.
	MBPerPutThread int `toml:"mb_per_put_thread"`
	MBPerGetThread int `toml:"mb_per_get_thread"`
	PutThreads     int `toml:"put_threads"`
	GetThreads     int `toml:"get_threads"`

	SegmentSize  int64 `toml:"segment_size"` // mb
	PutPending   int   `toml:"put_pending"`  // Extent put chan size.
	SizePerWrite int64 `toml:"size_per_write"`
	SizePerRead  int64 `toml:"size_per_read"`

	// Scheduler configs.
	Nop       bool `toml:"nop"` // Using nop scheduler or not.
	IOThreads int  `toml:"io_threads"`

	// IsRaw indicates testing the raw I/O directly sent to the file system.
	// TODO it only works when JobType is read.
	IsRaw bool `toml:"is_raw"`
	// IsDoNothing will do nothing at all when there should be a I/O request.
	// It's used for measuring the extperf works as expect.
	// TODO it only works when JobType is read.
	IsDoNothing bool `toml:"is_do_nothing"`
}

var jobTypes = map[string]int{
	"read":  Get,
	"write": Put,
	"rw":    PutGet,
}

const (
	Put    = 1
	Get    = 2
	PutGet = 3
)

const DefaultBlockSize = 8 * 1024
