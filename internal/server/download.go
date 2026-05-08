package server

import (
	"net/http"
	"path"
	"strings"
)

func (s *Server) handleDownload(w http.ResponseWriter, r *http.Request) {
	if s.cfg.Download.Dir == "" {
		http.NotFound(w, r)
		return
	}

	// Strip /download prefix, serve files from configured directory
	filePath := strings.TrimPrefix(r.URL.Path, "/download")
	if filePath == "" || filePath == "/" {
		s.serveDownloadIndex(w, r)
		return
	}

	// Prevent directory traversal
	cleaned := path.Clean(filePath)
	if strings.Contains(cleaned, "..") {
		http.NotFound(w, r)
		return
	}

	http.ServeFile(w, r, path.Join(s.cfg.Download.Dir, cleaned))
}

func (s *Server) serveDownloadIndex(w http.ResponseWriter, r *http.Request) {
	fs := http.Dir(s.cfg.Download.Dir)
	f, err := fs.Open("/")
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer f.Close()

	entries, err := f.Readdir(-1)
	if err != nil {
		http.Error(w, "read dir failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(`<!DOCTYPE html><html><head><meta charset="utf-8"><title>Downloads</title>
<style>body{font-family:sans-serif;max-width:800px;margin:40px auto;padding:0 20px}
a{display:block;padding:8px 0;color:#0066cc;text-decoration:none}a:hover{text-decoration:underline}
.size{color:#666;font-size:0.9em;margin-left:12px}</style></head><body><h2>Client Downloads</h2>`))

	for _, entry := range entries {
		if entry.IsDir() || strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		name := entry.Name()
		size := formatSize(entry.Size())
		w.Write([]byte(`<a href="/download/` + name + `">` + name + `<span class="size">` + size + `</span></a>`))
	}

	w.Write([]byte(`</body></html>`))
}

