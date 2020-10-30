Extent V1
====

##Design Purpose

Extent V1 is designed for lots of small files (~16KB).

##Constraint Conditions

1. NVMe Driver
2. Only servers objects which < 128KB

##Index

Based on Hopscotch Hashing with these optimizations:

1. Wait-free
2. Fixed size
3. Fixed load factor (relies on reserved segments in an extent)

For a server with 8T/disk * 4disk, it will cost 16GB for index.

Which means 0.5GB/TB.