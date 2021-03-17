package xio

// Scheduler is the ZBuf I/O queue. Each disk/core has one(depends on implementations).
type Scheduler interface {
	Start()
	Close()
	// DoAsync does I/O request async.
	DoAsync(reqType uint64, f File, offset int64, d []byte) (ar *AsyncRequest, err error)
	// DoSync does I/O request waiting until succeed.
	DoSync(reqType uint64, f File, offset int64, d []byte) error
}

// NopScheduler wraps Scheduler but actually no scheduler working just write/read directly.
type NopScheduler struct {
}

func (s *NopScheduler) Start() {
	return
}

func (s *NopScheduler) Close() {
	return
}

func (s *NopScheduler) DoAsync(reqType uint64, f File, offset int64, d []byte) (ar *AsyncRequest, err error) {

	if IsReqRead(reqType) {
		_, err = f.ReadAt(d, offset)
		return nil, err
	}

	_, err = f.WriteAt(d, offset)
	if err != nil {
		return nil, err
	}

	return nil, f.Fdatasync()
}

func (s *NopScheduler) DoSync(reqType uint64, f File, offset int64, d []byte) error {
	_, err := s.DoAsync(reqType, f, offset, d)
	return err
}
