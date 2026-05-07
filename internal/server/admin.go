package server

import (
	"fmt"
	"html"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/wjzhangq/share/internal/proto"
)

func (s *Server) handleAdmin(w http.ResponseWriter, r *http.Request) {
	if !s.checkBasicAuth(w, r) {
		return
	}

	path := r.URL.Path
	switch {
	case path == "/" || path == "":
		s.adminOverview(w, r)
	case path == "/clients":
		s.adminClients(w, r)
	case strings.HasPrefix(path, "/clients/c"):
		s.adminClientDetail(w, r, path)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) checkBasicAuth(w http.ResponseWriter, r *http.Request) bool {
	if s.cfg.Admin.User == "" {
		return true
	}
	user, pass, ok := r.BasicAuth()
	if !ok || user != s.cfg.Admin.User || pass != s.cfg.Admin.Password {
		w.Header().Set("WWW-Authenticate", `Basic realm="share admin"`)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}

func (s *Server) adminOverview(w http.ResponseWriter, r *http.Request) {
	clients, _ := s.store.ListClients()
	onlineCount := 0
	totalShares := 0
	for _, c := range clients {
		if c.Online {
			onlineCount++
		}
		shares, _ := s.store.ListActiveSharesByClient(c.UniqueID)
		totalShares += len(shares)
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, `<!DOCTYPE html><html><head><meta charset="utf-8"><title>share admin</title>
<style>body{font-family:sans-serif;margin:2em}table{border-collapse:collapse}td,th{border:1px solid #ddd;padding:8px}th{background:#f5f5f5}</style>
</head><body>
<h1>share admin</h1>
<p>Online clients: %d / %d | Active shares: %d</p>
<p><a href="/clients">View all clients</a></p>
</body></html>`, onlineCount, len(clients), totalShares)
}

func (s *Server) adminClients(w http.ResponseWriter, r *http.Request) {
	clients, _ := s.store.ListClients()

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	var sb strings.Builder
	sb.WriteString(`<!DOCTYPE html><html><head><meta charset="utf-8"><title>clients - share admin</title>
<style>body{font-family:sans-serif;margin:2em}table{border-collapse:collapse;width:100%}td,th{border:1px solid #ddd;padding:8px;text-align:left}th{background:#f5f5f5}.online{color:green}.offline{color:red}</style>
</head><body><h1>Clients</h1><table><tr><th>ID</th><th>Hostname</th><th>OS/Arch</th><th>Version</th><th>Status</th><th>Shares</th></tr>`)

	for _, c := range clients {
		status := `<span class="offline">offline</span>`
		if c.Online {
			status = `<span class="online">online</span>`
		}
		shares, _ := s.store.ListActiveSharesByClient(c.UniqueID)
		sb.WriteString(fmt.Sprintf(`<tr><td><a href="/clients/c%d">c%d</a></td><td>%s</td><td>%s/%s</td><td>%s</td><td>%s</td><td>%d active</td></tr>`,
			c.ShortID, c.ShortID, html.EscapeString(c.Hostname), c.OS, c.Arch, html.EscapeString(c.Version), status, len(shares)))
	}

	sb.WriteString("</table></body></html>")
	w.Write([]byte(sb.String()))
}

func (s *Server) adminClientDetail(w http.ResponseWriter, r *http.Request, path string) {
	parts := strings.Split(strings.TrimPrefix(path, "/clients/c"), "/")
	if len(parts) == 0 {
		http.NotFound(w, r)
		return
	}

	shortID, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	client, err := s.store.GetClientByShortID(shortID)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	if len(parts) >= 3 && parts[1] == "share" && r.Method == "POST" && strings.HasSuffix(path, "/close") {
		shareName := parts[2]
		s.store.CloseShare(client.UniqueID, shareName)
		s.hub.SendToClient(client.UniqueID, proto.ShareClosed{Type: "share.closed", ShareName: shareName, Reason: "admin_close"})
		http.Redirect(w, r, fmt.Sprintf("/clients/c%d", shortID), http.StatusSeeOther)
		return
	}

	shares, _ := s.store.ListSharesByClient(client.UniqueID)

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf(`<!DOCTYPE html><html><head><meta charset="utf-8"><title>c%d - share admin</title>
<style>body{font-family:sans-serif;margin:2em}table{border-collapse:collapse;width:100%%}td,th{border:1px solid #ddd;padding:8px;text-align:left}th{background:#f5f5f5}.active{color:green}.offline{color:orange}.closed{color:red}</style>
</head><body><h1>c%d — %s</h1>
<p>OS: %s/%s | Version: %s</p>
<p>Online at: %s | Offline at: %s</p>
<h2>Shares</h2><table><tr><th>Name</th><th>Kind</th><th>Status</th><th>Host</th><th>Action</th></tr>`,
		shortID, shortID, html.EscapeString(client.Hostname),
		client.OS, client.Arch, html.EscapeString(client.Version),
		formatTime(client.OnlineAt), formatTime(client.OfflineAt)))

	for _, sh := range shares {
		fullHost := fmt.Sprintf("c%d-%s.%s", shortID, sh.ShareName, s.cfg.Domain)
		action := ""
		if sh.Status != "closed" {
			action = fmt.Sprintf(`<form method="POST" action="/clients/c%d/share/%s/close" style="display:inline"><button type="submit">Close</button></form>`, shortID, sh.ShareName)
		}
		sb.WriteString(fmt.Sprintf(`<tr><td>%s</td><td>%s</td><td><span class="%s">%s</span></td><td><a href="https://%s">%s</a></td><td>%s</td></tr>`,
			html.EscapeString(sh.ShareName), sh.Kind, sh.Status, sh.Status, fullHost, fullHost, action))
	}

	sb.WriteString("</table><p><a href=\"/clients\">&larr; Back</a></p></body></html>")
	w.Write([]byte(sb.String()))
}

func formatTime(unix int64) string {
	if unix == 0 {
		return "-"
	}
	return time.Unix(unix, 0).UTC().Format("2006-01-02 15:04:05 UTC")
}
