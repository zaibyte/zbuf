package vdisk

type Disk interface {
}

// enum DiskType {
// Disk_NVMe = 0;
// Disk_Optane = 1;
// Disk_SATA = 2;
// }
//
// message Disk {
// DiskState state = 1;
// uint32 id = 2;
// uint64 size = 3;
// uint64 used = 4;
// double weight = 5;
// DiskType type = 6;
// }
//
// enum DiskState {
// Disk_ReadWrite = 0;
// Disk_Full = 1;
// Disk_Broken = 2;
// Disk_Offline =3;  // See ZBuf_Offline.
// Disk_Tombstone = 4; // See ZBuf_Tombstone.
// }
