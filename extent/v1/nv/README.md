NV (Non-Volatile) 
===

Package NV provides data structs which could be marshaled/unmarshaled, helping us to 
flush them to non-volatile devices.

There are two kinds of data need to be persisted:

1. Infos of segments (header)

2. Infos of physical address (physical address snapshot)

## Segments

Any segments' changes need to be persisted.

## Physical Address

Physical Address snapshot helping us to reconstruct physical address faster.

It maybe a bit behind than events happens on segments file. What should we do is start from
the snapshot and traverse from the cursor(before the cursor the addresses are reliable) recorded in the snapshot:

1. Writable segment cursor
2. GC src & GC dst cursor
3. Clone job cursor

