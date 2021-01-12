package vfs

import (
	"os"
	"time"
)

type SpeedFS struct {
	Speed int // MB/s.
}

func (s *SpeedFS) Close() error {
	panic("implement me")
}

func (s *SpeedFS) Read(p []byte) (n int, err error) {
	panic("implement me")
}

func (s *SpeedFS) ReadAt(p []byte, off int64) (n int, err error) {

	ns := s.calcSleepTime(len(p))
	time.Sleep(ns)

	return len(p), nil
}

func (s *SpeedFS) calcSleepTime(n int) time.Duration {
	st := float64(n) / float64(s.Speed*1024*1024) // seconds.
	return time.Duration(int64(st * float64(time.Second)))
}

func (s *SpeedFS) Write(p []byte) (n int, err error) {
	panic("implement me")
}

func (s *SpeedFS) WriteAt(p []byte, off int64) (n int, err error) {
	ns := s.calcSleepTime(len(p))
	time.Sleep(ns)

	return len(p), nil
}

func (s *SpeedFS) Stat() (os.FileInfo, error) {
	panic("implement me")
}

func (s *SpeedFS) Sync() error {
	panic("implement me")
}
