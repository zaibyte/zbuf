// Package xio provides io controller for ZBuf:
// 1. Each disk has its own I/O controller, including: Threads management, QoS.
//
// In one word, xio is the guarantee of disk I/O workload conditioning.
// p.s.
// Workload Conditioning is the use of the various metrics that
// the ZBuf has to create control feedback loops that guarantee the system progresses at a good, stable pace.
package xio

// TODO try to write a tool to calculate these if not set in configs.
// The tool also has a table to record results, saving time.
const (
	// DefaultWriteThreadsPerDisk is the single disk concurrent writers.
	// Although NVMe driver has multi queues to handle I/O requests, but the reading is much heavier than writing,
	// leaving more abilities for reading is a better choice.
	//
	// This value is the result of combination of Intel manual & my experience.
	DefaultWriteThreadsPerDisk = 4

	// DefaultReadThreadsPerDisk is the single disk concurrent readers.
	// ZBuf has internal cache, these threads are used for accessing disk.
	// Beyond 64, we may get higher IOPS, but much higher latency.
	//
	// In an enterprise-class TLC/QLC NVMe driver, 32-64 would be a good choice.
	//
	// This value is the result of combination of Intel manual & my experience.
	DefaultReadThreadsPerDisk = 64

	DefaultSizePerWrite = 32 * 1024 // Flush to the disk every DefaultSizePerWrite. Too big will impact latency.

	DefaultWriteDepth = 128
	DefaultReadDepth  = 256
)
