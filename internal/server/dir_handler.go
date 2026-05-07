package server

import (
	"fmt"
	"html"
	"strings"

	"github.com/wenjin/sharexxx/internal/proto"
)

func renderDirListing(shareName string, entries []proto.DirEntry, truncated bool) string {
	var sb strings.Builder
	sb.WriteString("<!DOCTYPE html><html><head><meta charset=\"utf-8\">")
	sb.WriteString("<title>")
	sb.WriteString(html.EscapeString(shareName))
	sb.WriteString("</title>")
	sb.WriteString("<style>body{font-family:monospace;margin:2em}a{text-decoration:none}a:hover{text-decoration:underline}.dir{color:#0366d6}.file{color:#333}.size{color:#666;margin-left:1em}</style>")
	sb.WriteString("</head><body>")
	sb.WriteString("<h2>")
	sb.WriteString(html.EscapeString(shareName))
	sb.WriteString("</h2><ul>")

	for _, e := range entries {
		name := html.EscapeString(e.Name)
		if e.IsDir {
			sb.WriteString(fmt.Sprintf("<li><a class=\"dir\" href=\"%s/\">%s/</a></li>", name, name))
		} else {
			sb.WriteString(fmt.Sprintf("<li><a class=\"file\" href=\"%s\">%s</a><span class=\"size\">%s</span></li>", name, name, formatSize(e.Size)))
		}
	}

	sb.WriteString("</ul>")
	if truncated {
		sb.WriteString("<p><em>Directory listing truncated (>5000 entries)</em></p>")
	}
	sb.WriteString("</body></html>")
	return sb.String()
}

func formatSize(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(b)/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(b)/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(b)/(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
