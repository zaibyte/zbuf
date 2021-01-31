package server

func (s *Server) PutObj(reqid, oid uint64, objData []byte) error {

	panic("implement me")
}

func (s *Server) GetObj(reqid, oid uint64) (objData []byte, err error) {
	panic("implement me")
}

func (s *Server) DeleteObj(reqid, oid uint64) error {
	panic("implement me")
}
