Extent Performance Testing Tool
====

extperf(Extent Performance Testing Tool) is a tool built for testing all versions
of extent's performance.

It focuses on small objects perf, especially the latency.

That's because the perf is easy to tune when the object is large. For a single
ZBuf server the throughput won't be that high in production env.

Throughput could bring a amazing testing result, but it can fool you around either.

The P999 latency in a certain IOPS really matters.