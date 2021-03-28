package extperf

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/elastic/go-hdrhistogram"
)

func (r *Runner) printStat(totalCost, putCost, readCost int64) {
	r.printSummary(totalCost)
	r.printIOPS(putCost, readCost)
	r.printLat()
}

func (r *Runner) printSummary(cost int64) {
	fmt.Println("config")
	fmt.Println("-------------")
	fmt.Println(fmt.Sprintf("%#v", r.cfg))
	fmt.Println("-------------")
	fmt.Println("summary")
	fmt.Println("-------------")
	fmt.Printf("job time: %.5fms\n", float64(cost)/float64(time.Millisecond))
	putMb := r.cfg.MBPerPutThread * r.cfg.PutThreads
	if jobTypes[r.cfg.JobType]&1 != Put {
		putMb = 0
	}
	fmt.Printf("put: %dMB\n", putMb)
	getMB := r.cfg.MBPerGetThread * r.cfg.GetThreads
	if jobTypes[r.cfg.JobType]&2 != Get {
		getMB = 0
	}
	fmt.Printf("get: %dMB\n", getMB)
	fmt.Println("-------------")
}

func (r *Runner) printIOPS(putCost, readCost int64) {

	putIO, getIO := atomic.LoadInt64(&r.putIO), atomic.LoadInt64(&r.getIO)
	putIOFailed, getIOFailed := atomic.LoadInt64(&r.putIOFailed), atomic.LoadInt64(&r.getIOFailed)

	fmt.Println(fmt.Sprintf("put ok: %d, failed: %d", putIO, putIOFailed))
	fmt.Println(fmt.Sprintf("get ok: %d, failed: %d", getIO, getIOFailed))

	putAvg := calcIOPS(putIO, putCost)
	getAvg := calcIOPS(getIO, readCost)

	fmt.Println("iops")
	fmt.Println(fmt.Sprintf("put avg: %.2fk/s", float64(putAvg)/1000))
	fmt.Println(fmt.Sprintf("get avg: %.2fk/s", float64(getAvg)/1000))
	fmt.Println("-------------")
}

func calcIOPS(io int64, cost int64) (avg float64) {

	sec := float64(cost) / float64(time.Second)
	return float64(io) / sec
}

func (r *Runner) printLat() {

	fmt.Println("latency")
	fmt.Println("-------------")
	printLat("put", r.putLat)
	printLat("get", r.getLat)
}

func printLat(name string, lats *hdrhistogram.Histogram) {
	fmt.Println(fmt.Sprintf("%s min: %d, avg: %.2f, max: %d",
		name, lats.Min(), lats.Mean(), lats.Max()))
	fmt.Println("percentiles (nsec):")
	fmt.Print(fmt.Sprintf(
		"|  1.00th=[%d],  5.00th=[%d], 10.00th=[%d], 20.00th=[%d],\n"+
			"| 30.00th=[%d], 40.00th=[%d], 50.00th=[%d], 60.00th=[%d],\n"+
			"| 70.00th=[%d], 80.00th=[%d], 90.00th=[%d], 95.00th=[%d],\n"+
			"| 99.00th=[%d], 99.50th=[%d], 99.90th=[%d], 99.95th=[%d],\n"+
			"| 99.99th=[%d]\n",
		lats.ValueAtQuantile(1), lats.ValueAtQuantile(5), lats.ValueAtQuantile(10), lats.ValueAtQuantile(20),
		lats.ValueAtQuantile(30), lats.ValueAtQuantile(40), lats.ValueAtQuantile(50), lats.ValueAtQuantile(60),
		lats.ValueAtQuantile(70), lats.ValueAtQuantile(80), lats.ValueAtQuantile(90), lats.ValueAtQuantile(95),
		lats.ValueAtQuantile(99), lats.ValueAtQuantile(99.5), lats.ValueAtQuantile(99.9), lats.ValueAtQuantile(99.95),
		lats.ValueAtQuantile(99.99)))
}
