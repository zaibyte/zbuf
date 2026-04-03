# ZBuf

ZBuf is a local extent-based object storage server used in the Zai stack.

## Overview

ZBuf stores objects into local disks using extent abstractions (`extent/v1` ~ `extent/v5`).
The current server implementation creates extent `v1` and `v2`, and exposes:

- Object service (default `0.0.0.0:8882`)
- Operator HTTP service (default `0.0.0.0:8881`)
- Prometheus metrics endpoint (`GET /v1/metrics`)

## Platform

- Recommended: Linux x86_64
- Development and tests can run on macOS
- Windows is not an actively supported target

## Repository Layout

- `cmd/zbuf-server`: server entrypoint
- `server`: server runtime, heartbeat, handlers, configuration binding
- `extent`: extent abstraction and versioned implementations
- `metric`: exported metrics definitions
- `tools/extperf`: extent performance test tool

## Data Layout On Disk

```text
<data_root>/
  disk_<disk_id0>/
  disk_<disk_id1>/
  disk_<disk_id2>/
    ext/
      <ext_id0>/
      <ext_id1>/
      <ext_id2>/
        boot-sector
        header
        <timestamp>.idx-snap
        segments
```

Notes:

- `header`, `idx-snap`, and `segments` are `extent/v1` concepts
- In production, multiple `disk_<id>` directories are managed by scheduler and heartbeat logic

## Build

```bash
make build
```

Artifacts:

- `bin/zbuf`
- `bin/extperf`

Or build directly:

```bash
go build -o bin/zbuf ./cmd/zbuf-server
go build -o bin/extperf ./tools/extperf
```

## Run ZBuf Server

```bash
./bin/zbuf -c /path/to/zbuf.toml
```

CLI:

- `-c`: config file path (default: `/usr/local/zai/zbuf.toml`)

If specific fields are omitted, several defaults are applied in code:

- operator address: `0.0.0.0:8881`
- object service address: `0.0.0.0:8882`
- data root: `/zai/zbuf/data`

### Minimal Config Skeleton

Use this as a starting point and fill it with your real environment values:

```toml
obj_srv_addr = "0.0.0.0:8882"
data_root = "/zai/zbuf/data"
development = true

[app]
server_addr = "0.0.0.0:8881"
instance_id = "zbuf-dev-1"
keeper_cluster_id = "dev"

[scheduler]

[ext_v1_config]
segment_size = "1GiB"

[ext_v2_config]
segment_size = "1GiB"

[zai_config]
# Fill keeper / service discovery settings required by your Zai deployment.
```

## Metrics

After startup, scrape:

```text
http://<server_addr>/v1/metrics
```

Example with defaults:

```text
http://127.0.0.1:8881/v1/metrics
```

## Testing

Run unit tests:

```bash
go test ./...
```

Run repository Makefile test target (race + coverage with `memfs_test` tag):

```bash
make test
```

## Extent Performance Tool

`tools/extperf` is used to test extent performance and latency characteristics.

- source code: `tools/extperf`
- config example: `tools/extperf/config.toml`
- notes: `tools/extperf/README.md`

Build and run:

```bash
go build -o bin/extperf ./tools/extperf
./bin/extperf -c tools/extperf/config.toml
```

## Development Notes

- `development=true` enables development-oriented behavior in several components
- `extent/v1` contains the most complete implementation details and tests
- Segment size and GC-related knobs are defined under `ext_v*_config`


