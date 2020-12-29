EVA
====

The first generation of Extent. The Name is from `EVA-01`, a hero in a famous cartoon.

Designed for NVMe Drivers.

##Constraint Conditions

1. Run on NVMe Drivers
2. Index should search fast and save memory(automatically adjusting its size), at the same time, it's easy to make snapshot of it.
3. Garbage collection friendly: fast and good for SSD framework.
4. Supports various I/O models (could be adjusted by different configs) 

##Design

###Index

Based on Hopscotch Hashing with these optimizations:

1. Wait-free Searching
2. Online Scaling

For a server with 8T/disk * 4disk, it will cost up to 16GB for index (with 16KB grain, and each file's size is 16KB).

Which means 0.5GB/TB.

###