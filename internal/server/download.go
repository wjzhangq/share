package server

import (
	"net/http"
)

func (s *Server) handleDownload(w http.ResponseWriter, r *http.Request) {
	if s.cfg.Download.Dir == "" {
		http.NotFound(w, r)
		return
	}
	http.StripPrefix("/download", http.FileServer(http.Dir(s.cfg.Download.Dir))).ServeHTTP(w, r)
}
