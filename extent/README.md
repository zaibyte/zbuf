Extent
===

Extent is the data structure on local storage device. 

Ext.v1 - Ext.v5 are Ext.v1's derivatives, they share the same mechanism but with different settings for satisfying various
I/O models.

## Performance

hundreds thousands IOPS nearly reach hardware limitation.

Run on a Intel P4510 NVMe device.

Put & Get mixed:

```shell
config
-------------
&extperf.Config{Version:1, ExtentsPerDisk:16, DataRoot:"/home/xxx/zbuf", BlockSize:12, JobType:"rw", JobTime:100000000000, SkipTime:1000000000, MBPerPutThread:1024, MBPerGetThread:1024, PutThreads:16, GetThreads:16, SegmentSize:1073741824, PutPending:0, SizePerRead:0, Nop:false, IOThreads:32, IsRaw:false, IsDoNothing:false}
-------------
summary
-------------
job time: 30344.00710ms
put: 16384MB
get: 16384MB
-------------
put ok: 1398096, failed: 0
get ok: 267555, failed: 0
iops
put avg: 46.08k/s
get avg: 54.18k/s
-------------
latency
-------------
put min: 47488, avg: 344775.65, max: 2633727
percentiles (nsec):
|  1.00th=[87743],  5.00th=[115455], 10.00th=[141951], 20.00th=[188415],
| 30.00th=[238463], 40.00th=[287231], 50.00th=[329215], 60.00th=[368895],
| 70.00th=[410879], 80.00th=[481535], 90.00th=[567807], 95.00th=[653311],
| 99.00th=[815103], 99.50th=[882175], 99.90th=[1017855], 99.95th=[1079295],
| 99.99th=[1234943]
get min: 61376, avg: 294694.05, max: 2334719
percentiles (nsec):
|  1.00th=[81855],  5.00th=[96319], 10.00th=[107711], 20.00th=[132607],
| 30.00th=[159103], 40.00th=[201471], 50.00th=[249855], 60.00th=[307199],
| 70.00th=[375807], 80.00th=[444415], 90.00th=[535551], 95.00th=[627199],
| 99.00th=[843263], 99.50th=[924159], 99.90th=[1161215], 99.95th=[1276927],
| 99.99th=[1635327]
```


Random Get:

```shell
xxx@tes_of03:~$ ./extperf -c extperf.toml
2021/08/05 14:09:58 run on 1 disks
2021/08/05 14:09:58 start to prepare read
2021/08/05 14:10:09 prepare read done, cost: 11.16s
config
-------------
&extperf.Config{Version:1, ExtentsPerDisk:16, DataRoot:"/home/xxx/zbuf", BlockSize:4, JobType:"read", JobTime:100000000000, SkipTime:1000000000, MBPerPutThread:1024, MBPerGetThread:256, PutThreads:16, GetThreads:128, SegmentSize:1073741824, PutPending:0, SizePerRead:0, Nop:false, IOThreads:128, IsRaw:false, IsDoNothing:false, PrintLog:true}
-------------
summary
-------------
job time: 25494.27115ms
put: 0MB
get: 32768MB
-------------
put ok: 0, failed: 0
get ok: 8388608, failed: 0
iops
put avg: NaNk/s
get avg: 585.10k/s
-------------
latency
-------------
put min: 0, avg: 0.00, max: 63
percentiles (nsec):
|  1.00th=[0],  5.00th=[0], 10.00th=[0], 20.00th=[0],
| 30.00th=[0], 40.00th=[0], 50.00th=[0], 60.00th=[0],
| 70.00th=[0], 80.00th=[0], 90.00th=[0], 95.00th=[0],
| 99.00th=[0], 99.50th=[0], 99.90th=[0], 99.95th=[0],
| 99.99th=[0]
get min: 43712, avg: 217581.47, max: 10485759
percentiles (nsec):
|  1.00th=[105151],  5.00th=[127487], 10.00th=[141951], 20.00th=[161407],
| 30.00th=[177023], 40.00th=[191871], 50.00th=[206847], 60.00th=[222975],
| 70.00th=[241663], 80.00th=[265471], 90.00th=[303103], 95.00th=[340735],
| 99.00th=[433407], 99.50th=[476415], 99.90th=[612863], 99.95th=[771583],
| 99.99th=[2549759]
```

details in this [issue](https://g.tesamc.com/IT/zbuf/issues/191)