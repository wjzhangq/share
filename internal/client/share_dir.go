package client

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/wjzhangq/share/internal/proto"
)

func (d *Daemon) shareDir(path string) proto.IPCResponse {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return proto.IPCResponse{Err: fmt.Sprintf("invalid path: %v", err)}
	}
	info, err := os.Stat(absPath)
	if err != nil || !info.IsDir() {
		return proto.IPCResponse{Err: "path does not exist or is not a directory"}
	}

	hintName := dirHintName(absPath)
	sourceKey := dirSourceKey(absPath)

	ss := ShareState{
		Kind:      "dir",
		ShareName: hintName,
		LocalPath: absPath,
		SourceKey: sourceKey,
	}
	d.state.AddShare(ss)
	d.createShare(ss)

	return proto.IPCResponse{OK: true, Data: map[string]string{"hint": hintName, "path": absPath}}
}

func dirHintName(absPath string) string {
	gitRemote := getGitRemoteRepoName(absPath)
	if gitRemote != "" {
		return cleanName(gitRemote)
	}
	return cleanName(filepath.Base(absPath))
}

func dirSourceKey(absPath string) string {
	gitRemote := getGitRemoteURL(absPath)
	if gitRemote != "" {
		return "git:" + gitRemote
	}
	return "path:" + absPath
}

func getGitRemoteURL(dir string) string {
	gitDir := filepath.Join(dir, ".git")
	if _, err := os.Stat(gitDir); err != nil {
		return ""
	}
	configPath := filepath.Join(gitDir, "config")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return ""
	}
	lines := strings.Split(string(data), "\n")
	inRemoteOrigin := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == `[remote "origin"]` {
			inRemoteOrigin = true
			continue
		}
		if strings.HasPrefix(trimmed, "[") {
			inRemoteOrigin = false
			continue
		}
		if inRemoteOrigin && strings.HasPrefix(trimmed, "url") {
			parts := strings.SplitN(trimmed, "=", 2)
			if len(parts) == 2 {
				return strings.TrimSpace(parts[1])
			}
		}
	}
	return ""
}

func getGitRemoteRepoName(dir string) string {
	url := getGitRemoteURL(dir)
	if url == "" {
		return ""
	}
	url = strings.TrimSuffix(url, ".git")
	if idx := strings.LastIndex(url, "/"); idx >= 0 {
		return url[idx+1:]
	}
	if idx := strings.LastIndex(url, ":"); idx >= 0 {
		return url[idx+1:]
	}
	return ""
}

func (d *Daemon) handleDirList(dl proto.DirList) {
	d.mu.Lock()
	share := d.findShareByName(dl.ShareName)
	d.mu.Unlock()
	if share == nil {
		return
	}

	localPath := share.State.LocalPath
	targetPath := filepath.Join(localPath, filepath.FromSlash(dl.RelPath))

	if !isPathSafe(localPath, targetPath) {
		d.sendDirListResp(dl.ReqID, nil, false, false)
		return
	}

	entries, err := os.ReadDir(targetPath)
	if err != nil {
		d.sendDirListResp(dl.ReqID, nil, false, false)
		return
	}

	hasIndex := false
	var result []proto.DirEntry
	for i, e := range entries {
		if i >= 5000 {
			d.sendDirListResp(dl.ReqID, result, hasIndex, true)
			return
		}
		info, _ := e.Info()
		size := int64(0)
		if info != nil {
			size = info.Size()
		}
		name := e.Name()
		if !e.IsDir() && (name == "index.html" || name == "index.htm") {
			hasIndex = true
		}
		result = append(result, proto.DirEntry{Name: name, IsDir: e.IsDir(), Size: size})
	}
	d.sendDirListResp(dl.ReqID, result, hasIndex, false)
}

func (d *Daemon) sendDirListResp(reqID string, entries []proto.DirEntry, hasIndex, truncated bool) {
	st := d.state.Get()
	fullHost := fmt.Sprintf("c%d-%s.%s", st.ShortID, "", d.getShareDomain())

	resp := proto.DirListResp{
		Type:      "dir.list.resp",
		ReqID:     reqID,
		Entries:   entries,
		HasIndex:  hasIndex,
		Truncated: truncated,
	}

	_ = fullHost
	d.sendOriginResp(reqID, proto.OpDirListResp, resp)
}

func (d *Daemon) handleDirRead(dr proto.DirRead) {
	d.mu.Lock()
	share := d.findShareByName(dr.ShareName)
	d.mu.Unlock()
	if share == nil {
		return
	}

	localPath := share.State.LocalPath
	targetPath := filepath.Join(localPath, filepath.FromSlash(dr.RelPath))

	if !isPathSafe(localPath, targetPath) {
		d.sendErrorResp(dr.ReqID, dr.ShareName, 403, "forbidden")
		return
	}

	f, err := os.Open(targetPath)
	if err != nil {
		d.sendErrorResp(dr.ReqID, dr.ShareName, 404, "not found")
		return
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		d.sendErrorResp(dr.ReqID, dr.ShareName, 500, "stat error")
		return
	}

	contentType := mime.TypeByExtension(filepath.Ext(targetPath))
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	st := d.state.Get()
	threshold := 1048576

	if info.Size() <= int64(threshold) {
		data, err := io.ReadAll(f)
		if err != nil {
			d.sendErrorResp(dr.ReqID, dr.ShareName, 500, "read error")
			return
		}
		d.sendInlineResp(dr.ReqID, dr.ShareName, st, 200, map[string]string{
			"Content-Type": contentType,
		}, data)
	} else {
		d.sendStreamResp(dr.ReqID, dr.ShareName, st, 200, map[string]string{
			"Content-Type": contentType,
		}, info.Size(), f)
	}
}

func (d *Daemon) sendInlineResp(reqID, shareName string, st State, status int, headers map[string]string, body []byte) {
	host := d.buildShareHost(shareName, st)
	url := fmt.Sprintf("%s://%s/", d.scheme(), host)

	payload := map[string]any{
		"status":   status,
		"headers":  headers,
		"body_b64": base64.StdEncoding.EncodeToString(body),
	}
	data, _ := json.Marshal(payload)

	req, _ := http.NewRequest("POST", url, bytes.NewReader(data))
	req.Header.Set(proto.HeaderOp, proto.OpRespInline)
	req.Header.Set(proto.HeaderReqID, reqID)
	req.Header.Set(proto.HeaderClientToken, st.UniqueID)
	req.Header.Set("Content-Type", "application/json")

	d.httpCli.Do(req)
}

func (d *Daemon) sendStreamResp(reqID, shareName string, st State, status int, headers map[string]string, contentLength int64, body io.Reader) {
	host := d.buildShareHost(shareName, st)
	url := fmt.Sprintf("%s://%s/", d.scheme(), host)

	headPayload := map[string]any{
		"status":         status,
		"headers":        headers,
		"content_length": contentLength,
	}
	headData, _ := json.Marshal(headPayload)

	headReq, _ := http.NewRequest("POST", url, bytes.NewReader(headData))
	headReq.Header.Set(proto.HeaderOp, proto.OpRespHead)
	headReq.Header.Set(proto.HeaderReqID, reqID)
	headReq.Header.Set(proto.HeaderClientToken, st.UniqueID)
	headReq.Header.Set("Content-Type", "application/json")
	d.httpCli.Do(headReq)

	streamReq, _ := http.NewRequest("POST", url, body)
	streamReq.Header.Set(proto.HeaderOp, proto.OpRespStream)
	streamReq.Header.Set(proto.HeaderReqID, reqID)
	streamReq.Header.Set(proto.HeaderClientToken, st.UniqueID)
	d.httpCli.Do(streamReq)
}

func (d *Daemon) sendOriginResp(reqID, op string, payload any) {
	st := d.state.Get()
	host := d.getAnyShareHost(st)
	url := fmt.Sprintf("%s://%s/", d.scheme(), host)

	data, _ := json.Marshal(payload)
	req, _ := http.NewRequest("POST", url, bytes.NewReader(data))
	req.Header.Set(proto.HeaderOp, op)
	req.Header.Set(proto.HeaderReqID, reqID)
	req.Header.Set(proto.HeaderClientToken, st.UniqueID)
	req.Header.Set("Content-Type", "application/json")
	d.httpCli.Do(req)
}

func (d *Daemon) sendErrorResp(reqID, shareName string, status int, msg string) {
	st := d.state.Get()
	d.sendInlineResp(reqID, shareName, st, status, map[string]string{
		"Content-Type": "text/plain",
	}, []byte(msg))
}

func (d *Daemon) buildShareHost(shareName string, st State) string {
	domain := d.getDomain()
	return fmt.Sprintf("c%d-%s.%s", st.ShortID, shareName, domain)
}

func (d *Daemon) getAnyShareHost(st State) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, s := range d.shares {
		return s.FullHost
	}
	domain := d.getDomain()
	return fmt.Sprintf("c%d-x.%s", st.ShortID, domain)
}

func (d *Daemon) getDomain() string {
	st := d.state.Get()
	url := st.ServerURL
	url = strings.TrimPrefix(url, "wss://")
	url = strings.TrimPrefix(url, "ws://")
	url = strings.TrimSuffix(url, "/ws")
	url = strings.TrimSuffix(url, "/")
	return url
}

func (d *Daemon) getShareDomain() string {
	return d.getDomain()
}

func (d *Daemon) scheme() string {
	st := d.state.Get()
	if strings.HasPrefix(st.ServerURL, "wss://") {
		return "https"
	}
	return "http"
}

func (d *Daemon) findShareByName(name string) *ActiveShare {
	for _, s := range d.shares {
		if s.ShareName == name {
			return s
		}
	}
	return nil
}

func isPathSafe(root, target string) bool {
	absRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return false
	}
	absTarget, err := filepath.EvalSymlinks(target)
	if err != nil {
		absTarget = target
	}
	return strings.HasPrefix(absTarget, absRoot)
}

func cleanName(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	for _, c := range s {
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' {
			b.WriteRune(c)
		} else {
			b.WriteRune('-')
		}
	}
	result := b.String()
	for strings.Contains(result, "--") {
		result = strings.ReplaceAll(result, "--", "-")
	}
	result = strings.Trim(result, "-")
	if result == "" {
		result = "share"
	}
	return result
}
