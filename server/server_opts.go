package server

func WithRedis(redis Redis) ServerOption {
	return func(s *Server) {
		s.redis = redis
	}
}
