package v1perf

type Config struct {
	Extents  int    `toml:"extents"`
	DataRoot string `toml:"data_root"`

	BlockSize int64  `toml:"block_size"`
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

	// Scheduler configs.
	WriteThreadsPerDisk int `toml:"write_threads_per_disk"`
	ReadThreadsPerDisk  int `toml:"read_threads_per_disk"`
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
