package hostserver

func (s *Server) mountPlayground() error {
	if !s.cfg.Playground.Enabled {
		return nil
	}
	panic("hostserver: mountPlayground not implemented yet")
}
