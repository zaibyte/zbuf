# ZBuf

## Filesystem Layout

```shell
Extent on local file system:
 .
 ├── <data_root>
 │    ├── disk_<disk_id0>
 │    ├── disk_<disk_id1>
 │    └── disk_<disk_id2>
 │         ├── <digest>.init-block
 │         └── ext
 │              ├── <ext_id0>
 │              ├── <ext_id1>
 │              └── <ext_id2>
 │                      ├── boot-sector
 │                      ├── header
 │                      ├── <timestamp>.idx-snap
 │                      ├── <start-end>.idx-wal
 │                      └── segments
```

