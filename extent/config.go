package extent

import "g.tesamc.com/IT/zaipkg/typeutil"

// Different sizes of objects will be put into different versions of extents for avoiding high hash collision.
// In these settings, we will get 3% - 11.8% hash collision at most when extents are full.
// For details, see issue: https://g.tesamc.com/IT/zai/issues/40
const (
	DefaultV1SegmentSize = typeutil.ByteSize(4 * 1024 * 1024) // 4MB, Low hash collision for (0, 64KB] objects.
	DefaultV2SegmentSize = DefaultV1SegmentSize * 4           // 16MB,  Low hash collision for (64KB, 256KB] objects.
	DefaultV3SegmentSize = DefaultV2SegmentSize * 4           // 64MB, Low hash collision for (256KB, 1MB] objects.
	DefaultV4SegmentSize = DefaultV3SegmentSize * 4           // 256MB, Low hash collision for (1MB, 4MB) objects.
	DefaultV5SegmentSize = DefaultV4SegmentSize * 2           // 512MB, Low hash collision for [4MB] objects.
)
