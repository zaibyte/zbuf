Extent Version 1
====

The first generation of Extent.

Designed for NVMe Drivers. 

##Design Constraint Conditions/Principals

1. Run on NVMe Drivers
2. Index should search fast and save memory(automatically adjusting its size), at the same time, it's easy to make snapshot of it.
3. Garbage collection friendly: fast and good for SSD firmware & its physical properties.
4. Supports various I/O models (could be adjusted by different configs)
5. Consistence model: Read-After-Writer, any read after a writing must be succeeded.

##Details

This Paper: `<Reaping the performance of fast NVM storage with uDepot>` has almost the same idea with me, and it gives 
confidence to make the extent.v1 successfully. 

###Index

Based on Hopscotch Hashing with these optimizations:

1. Redesign for Zai's oid, reducing overhead of key.
2. Wait-free Searching
3. Online Scaling

For a server with 8T/disk * 4disk, it will cost up to 16GB for index (with 16KB grain, and each file's size is 16KB).

Which means 0.5GB/TB.

###I/O

1. Use direct I/O for saving memory copy cost
2. Only one goroutine could write object sequentially, avoiding non-sequential state.(segments file plays the WAL role indeed,
   so we can't tolerate beyond failed write has succeeded one).
3. Read is thread-safe, because it has no side-effect

####Data Integrity

Follow the Data Integrity Design Principle strictly which described in [Docs](https://g.tesamc.com/IT/zai-docs)

###Garbage Collection

In extent.v1, extent is split into several segments. We reclaim segment one by one, if a segment is clean up, it gets
the chance to be written again.

It helps SSD drivers to set blocks invalid sequentially, and no GC inside SSD needed(firmware could erase these blocks directly,
no need to copy-past, then erase):

When the host wants to rewrite to an address, the SSD actually writes to a different, blank page and then updates the 
logical block address (LBA) table (much like the MFT of an HDD). Inside the LBA table, the original page is marked as 
“invalid”, and the new page is marked as the current location for the new data.

####I/O

I/O in GC is been done in a independent goroutine, so it won't block the object uploading requests.

