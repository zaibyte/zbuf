package vfs

import (
	"os"
	"time"
)

var _speedFile File = new(SpeedFile)

type SpeedFile struct {
	Speed int // MB/s.
}

func (s *SpeedFile) Fdatasync() error {
	return nil
}

func (s *SpeedFile) Truncate(size int64) error {
	return nil
}

func (s *SpeedFile) Fd() uintptr {
	return 0
}

func (s *SpeedFile) Close() error {
	panic("implement me")
}

func (s *SpeedFile) Read(p []byte) (n int, err error) {
	panic("implement me")
}

func (s *SpeedFile) ReadAt(p []byte, off int64) (n int, err error) {

	ns := s.calcSleepTime(len(p))
	time.Sleep(ns)

	return len(p), nil
}

func (s *SpeedFile) calcSleepTime(n int) time.Duration {

	st := float64(n) / float64(s.Speed*1024*1024) // seconds.
	return time.Duration(int64(st * float64(time.Second)))
}

func (s *SpeedFile) Write(p []byte) (n int, err error) {
	panic("implement me")
}

func (s *SpeedFile) WriteAt(p []byte, off int64) (n int, err error) {
	ns := s.calcSleepTime(len(p))
	time.Sleep(ns)

	return len(p), nil
}

func (s *SpeedFile) Stat() (os.FileInfo, error) {
	panic("implement me")
}

func (s *SpeedFile) Sync() error {
	return nil
}
