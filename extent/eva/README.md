EVA
====

The first generation of Extent. The Name is from `EVA-01`, a hero in a famous cartoon.

Designed for NVMe Drivers. You could build different versions of extents on EVA by passing different params.

##Constraint Conditions

1. Run on NVMe Drivers
2. Index should search fast and save memory(automatically adjusting its size), at the same time, it's easy to make snapshot of it.
3. Garbage collection friendly: fast and good for SSD framework.
4. Supports various I/O models (could be adjusted by different configs) 

##Design

The struct of extent is mostly come from the Paper: <Reaping the performance of fast NVM storage with uDepot> with these optimizations:

###Index

Based on Hopscotch Hashing with these optimizations:

1. Redesign for Zai's oid, reducing overhead of key.
1. Wait-free Searching
2. Online Scaling

For a server with 8T/disk * 4disk, it will cost up to 16GB for index (with 16KB grain, and each file's size is 16KB).

Which means 0.5GB/TB.

###Cache

1. Combine write buffer & read cache. Only one goroutine could write, read is wait-free.
   
###I/O

1. Use direct I/O saving memory copy cost

###Garbage Collection

1. Algorithm is more like the one in SSD firmware.