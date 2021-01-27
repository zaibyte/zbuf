Colf(Colfer)
===

Package Colf provides data structs which could be marshaled/unmarshaled by colfer, helping us to 
flush them to non-volatile devices.

We won't put all datas which need to be persisted into colfer:

1. The limitation of colfer: actually none of serialization implementations are friendly for big data block
   (physical address tables may need quite large data block, e.g. 200MB)
   
2. We're using direct I/O in ZBuf, we can't have the exact size of header/snapshot files, which makes unmarshal unpredictable.

## Physical Address

Physical Address snapshot helping us to reconstruct physical address faster.

It maybe a bit behind than events happens on segments file. What should we do is start from
the snapshot and traverse from the cursor(before the cursor the addresses are reliable) recorded in the snapshot:

1. Writable segment cursor
2. GC src & GC dst cursor
3. Clone job cursor

## Generates Codes

```shell
go get -u github.com/pascaldekloe/colfer/cmd/colf
colf -b ../ Go *.colf
```
