package extperf

import (
	"testing"

	"github.com/elastic/go-hdrhistogram"
)

// BenchmarkP99 tries to test the latency collector's performance.
// About 100ns for single thread.
func BenchmarkP99(b *testing.B) {
	lat := hdrhistogram.New(1, 1000000*10, 3)
	for i := 0; i < b.N; i++ {
		_ = lat.RecordValueAtomic(int64(i))
	}
	_ = lat.Max()
}

func BenchmarkP99Concurrency(b *testing.B) {
	lat := hdrhistogram.New(1, 1000000*10, 3)
	b.SetParallelism(32)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			_ = lat.RecordValueAtomic(int64(i))
		}
	})
}
