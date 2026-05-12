package server

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"net/http"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	"github.com/wjzhangq/share/internal/proto"
	"github.com/wjzhangq/share/internal/server/store"
)

type pendingRequest struct {
	reqID     string
	clientUID string
	shortID   int64
	shareName string
	w         http.ResponseWriter
	done      chan struct{}
	body      io.ReadCloser

	mu       sync.Mutex
	status   int
	headers  map[string]string
	bodyData []byte
	stream   io.ReadCloser
	hasHead  bool
	headCh   chan struct{}
	streamCh chan io.ReadCloser
}

// wsSession holds state for a proxied WebSocket connection.
type wsSession struct {
	connID    string
	clientUID string
	openedCh  chan struct{}
	errorCh   chan string
	// frames from client -> browser
	fromClient chan proto.WSFrame
	// conn is the browser-side WebSocket
	conn *websocket.Conn
	mu   sync.Mutex
}

type Forwarder struct {
	srv     *Server
	mu      sync.RWMutex
	pending map[string]*pendingRequest

	wsMu      sync.RWMutex
	wsSessions map[string]*wsSession
}

func NewForwarder(srv *Server) *Forwarder {
	return &Forwarder{
		srv:        srv,
		pending:    make(map[string]*pendingRequest),
		wsSessions: make(map[string]*wsSession),
	}
}

func (f *Forwarder) Handle(w http.ResponseWriter, r *http.Request, hi hostInfo) {
	op := r.Header.Get(proto.HeaderOp)
	if op != "" {
		f.handleOriginAPI(w, r, op)
		return
	}
	f.handlePublicRequest(w, r, hi)
}

func (f *Forwarder) handlePublicRequest(w http.ResponseWriter, r *http.Request, hi hostInfo) {
	start := time.Now()
	f.srv.logger.Info("req start", "method", r.Method, "path", r.URL.Path, "host", r.Host, "short_id", hi.shortID, "share", hi.shareName, "remote", r.RemoteAddr)

	client := f.srv.hub.GetClientByShortID(hi.shortID)
	if client == nil {
		cl, err := f.srv.store.GetClientByShortID(hi.shortID)
		if err != nil {
			http.Error(w, "share not found", http.StatusNotFound)
			f.srv.logger.Info("req end", "path", r.URL.Path, "status", 404, "reason", "client not found", "duration", time.Since(start))
			return
		}
		f.writeOfflinePage(w, cl, nil, "client offline")
		f.srv.logger.Info("req end", "path", r.URL.Path, "status", 503, "reason", "client offline", "duration", time.Since(start))
		return
	}

	share, err := f.srv.store.GetShare(client.UniqueID, hi.shareName)
	if err != nil || share == nil {
		http.Error(w, "share not found", http.StatusNotFound)
		f.srv.logger.Info("req end", "path", r.URL.Path, "status", 404, "reason", "share not found", "duration", time.Since(start))
		return
	}
	if share.Status == "closed" {
		http.Error(w, "share closed", http.StatusGone)
		f.srv.logger.Info("req end", "path", r.URL.Path, "status", 410, "reason", "share closed", "duration", time.Since(start))
		return
	}
	if share.Status == "offline" {
		cl, _ := f.srv.store.GetClientByShortID(hi.shortID)
		f.writeOfflinePage(w, cl, share, "process exited")
		f.srv.logger.Info("req end", "path", r.URL.Path, "status", 503, "reason", "process offline", "duration", time.Since(start))
		return
	}

	if r.Header.Get("Upgrade") == "websocket" {
		f.handleWSProxy(w, r, hi, client, start)
		return
	}

	f.handleHTTPForward(w, r, hi, client, share, start)
}

func (f *Forwarder) handleHTTPForward(w http.ResponseWriter, r *http.Request, hi hostInfo, client *ConnectedClient, share *store.Share, start time.Time) {
	reqID := uuid.New().String()
	pr := &pendingRequest{
		reqID:     reqID,
		clientUID: client.UniqueID,
		shortID:   hi.shortID,
		shareName: hi.shareName,
		w:         w,
		done:      make(chan struct{}),
		headCh:    make(chan struct{}),
		streamCh:  make(chan io.ReadCloser, 1),
	}

	bodyMode := "empty"
	if r.ContentLength > 0 || r.TransferEncoding != nil {
		bodyMode = "stream"
		pr.body = r.Body
	}

	f.mu.Lock()
	f.pending[reqID] = pr
	f.mu.Unlock()

	defer func() {
		f.mu.Lock()
		delete(f.pending, reqID)
		f.mu.Unlock()
	}()

	headers := make(map[string]string)
	for k := range r.Header {
		if k == proto.HeaderClientToken || k == proto.HeaderReqID || k == proto.HeaderOp {
			continue
		}
		headers[k] = r.Header.Get(k)
	}

	var fwdMsg any
	if share.Kind == "dir" {
		cleanPath := path.Clean("/" + r.URL.Path)
		if isDir(r.URL.Path) {
			fwdMsg = proto.DirList{Type: "dir.list", ReqID: reqID, ShareName: hi.shareName, RelPath: cleanPath}
		} else {
			fwdMsg = proto.DirRead{Type: "dir.read", ReqID: reqID, ShareName: hi.shareName, RelPath: cleanPath, Range: r.Header.Get("Range")}
		}
	} else {
		fwdMsg = proto.ForwardReq{
			Type:      "forward.req",
			ReqID:     reqID,
			ShareName: hi.shareName,
			Method:    r.Method,
			Path:      r.URL.RequestURI(),
			Headers:   headers,
			BodyMode:  bodyMode,
		}
	}

	if err := f.srv.hub.SendToClient(client.UniqueID, fwdMsg); err != nil {
		http.Error(w, "client unreachable", http.StatusBadGateway)
		f.srv.logger.Info("req end", "path", r.URL.Path, "status", 502, "reason", "client unreachable", "duration", time.Since(start))
		return
	}

	timeout := f.srv.cfg.RequestTimeoutDuration()
	select {
	case <-pr.done:
	case <-time.After(timeout):
		http.Error(w, "gateway timeout", http.StatusGatewayTimeout)
		f.srv.logger.Info("req end", "path", r.URL.Path, "status", 504, "reason", "timeout", "duration", time.Since(start))
		return
	}

	pr.mu.Lock()
	defer pr.mu.Unlock()
	if pr.status == 0 {
		f.srv.logger.Info("req end", "path", r.URL.Path, "status", 0, "reason", "no response", "duration", time.Since(start))
		return
	}
	for k, v := range pr.headers {
		w.Header().Set(k, v)
	}
	w.WriteHeader(pr.status)
	if pr.bodyData != nil {
		w.Write(pr.bodyData)
	}
	f.srv.logger.Info("req end", "path", r.URL.Path, "status", pr.status, "duration", time.Since(start))
}

func (f *Forwarder) handleWSProxy(w http.ResponseWriter, r *http.Request, hi hostInfo, client *ConnectedClient, start time.Time) {
	connID := uuid.New().String()

	sess := &wsSession{
		connID:     connID,
		clientUID:  client.UniqueID,
		openedCh:   make(chan struct{}),
		errorCh:    make(chan string, 1),
		fromClient: make(chan proto.WSFrame, 64),
	}

	f.wsMu.Lock()
	f.wsSessions[connID] = sess
	f.wsMu.Unlock()

	defer func() {
		f.wsMu.Lock()
		delete(f.wsSessions, connID)
		f.wsMu.Unlock()
	}()

	headers := make(map[string]string)
	for k := range r.Header {
		if k == proto.HeaderClientToken || k == proto.HeaderReqID || k == proto.HeaderOp {
			continue
		}
		headers[k] = r.Header.Get(k)
	}

	openMsg := proto.WSOpen{
		Type:      "ws.open",
		ConnID:    connID,
		ShareName: hi.shareName,
		Path:      r.URL.RequestURI(),
		Headers:   headers,
	}
	if err := f.srv.hub.SendToClient(client.UniqueID, openMsg); err != nil {
		http.Error(w, "client unreachable", http.StatusBadGateway)
		f.srv.logger.Info("ws proxy end", "path", r.URL.Path, "status", 502, "reason", "client unreachable", "duration", time.Since(start))
		return
	}

	select {
	case <-sess.openedCh:
	case errMsg := <-sess.errorCh:
		http.Error(w, errMsg, http.StatusBadGateway)
		f.srv.logger.Info("ws proxy end", "path", r.URL.Path, "status", 502, "reason", errMsg, "duration", time.Since(start))
		return
	case <-time.After(15 * time.Second):
		http.Error(w, "ws open timeout", http.StatusGatewayTimeout)
		f.srv.logger.Info("ws proxy end", "path", r.URL.Path, "status", 504, "reason", "open timeout", "duration", time.Since(start))
		return
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true,
	})
	if err != nil {
		f.srv.logger.Error("ws accept browser", "err", err)
		f.srv.hub.SendToClient(client.UniqueID, proto.WSClose{Type: "ws.close", ConnID: connID})
		return
	}
	sess.mu.Lock()
	sess.conn = conn
	sess.mu.Unlock()

	f.srv.logger.Info("ws proxy open", "conn_id", connID, "path", r.URL.Path)

	ctx := r.Context()
	done := make(chan struct{})

	// browser -> client
	go func() {
		defer close(done)
		for {
			msgType, data, err := conn.Read(ctx)
			if err != nil {
				break
			}
			mt := int(msgType)
			frame := proto.WSFrame{
				Type:    "ws.frame",
				ConnID:  connID,
				MsgType: mt,
				DataB64: base64.StdEncoding.EncodeToString(data),
			}
			if sendErr := f.srv.hub.SendToClient(client.UniqueID, frame); sendErr != nil {
				break
			}
		}
		f.srv.hub.SendToClient(client.UniqueID, proto.WSClose{Type: "ws.close", ConnID: connID})
	}()

	// client -> browser
	go func() {
		for {
			select {
			case <-done:
				return
			case frame, ok := <-sess.fromClient:
				if !ok {
					conn.Close(websocket.StatusNormalClosure, "")
					return
				}
				data, err := base64.StdEncoding.DecodeString(frame.DataB64)
				if err != nil {
					continue
				}
				writeCtx := ctx
				conn.Write(writeCtx, websocket.MessageType(frame.MsgType), data)
			}
		}
	}()

	<-done
	f.srv.logger.Info("ws proxy closed", "conn_id", connID, "duration", time.Since(start))
}

func (f *Forwarder) handleOriginAPI(w http.ResponseWriter, r *http.Request, op string) {
	token := r.Header.Get(proto.HeaderClientToken)
	reqID := r.Header.Get(proto.HeaderReqID)

	if token == "" || reqID == "" {
		http.Error(w, "missing headers", http.StatusBadRequest)
		return
	}

	cc := f.srv.hub.GetClient(token)
	if cc == nil {
		http.Error(w, "unauthorized", http.StatusForbidden)
		return
	}

	switch op {
	case proto.OpWSOpened:
		f.handleWSOpened(w, r, cc, reqID)
		return
	case proto.OpWSOpenError:
		f.handleWSOpenError(w, r, cc, reqID)
		return
	case proto.OpWSFrameToServer:
		f.handleWSFrameToServer(w, r, cc, reqID)
		return
	case proto.OpWSCloseFromClient:
		f.handleWSCloseFromClient(w, cc, reqID)
		return
	}

	f.mu.RLock()
	pr, ok := f.pending[reqID]
	f.mu.RUnlock()
	if !ok {
		http.Error(w, "request not found", http.StatusNotFound)
		return
	}

	if pr.clientUID != cc.UniqueID {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	switch op {
	case proto.OpPullReqBody:
		f.handlePullReqBody(w, pr)
	case proto.OpRespInline:
		f.handleRespInline(w, r, pr)
	case proto.OpRespHead:
		f.handleRespHead(w, r, pr)
	case proto.OpRespStream:
		f.handleRespStream(w, r, pr)
	case proto.OpDirListResp:
		f.handleDirListResp(w, r, pr)
	default:
		http.Error(w, "unknown op", http.StatusBadRequest)
	}
}

func (f *Forwarder) handleWSOpened(w http.ResponseWriter, _ *http.Request, cc *ConnectedClient, connID string) {
	f.wsMu.RLock()
	sess, ok := f.wsSessions[connID]
	f.wsMu.RUnlock()
	if !ok {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	if sess.clientUID != cc.UniqueID {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	select {
	case <-sess.openedCh:
	default:
		close(sess.openedCh)
	}
	w.WriteHeader(http.StatusOK)
}

func (f *Forwarder) handleWSOpenError(w http.ResponseWriter, r *http.Request, cc *ConnectedClient, connID string) {
	f.wsMu.RLock()
	sess, ok := f.wsSessions[connID]
	f.wsMu.RUnlock()
	if !ok {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	if sess.clientUID != cc.UniqueID {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	var body struct {
		Message string `json:"message"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	msg := body.Message
	if msg == "" {
		msg = "upstream ws connect failed"
	}
	select {
	case sess.errorCh <- msg:
	default:
	}
	w.WriteHeader(http.StatusOK)
}

func (f *Forwarder) handleWSFrameToServer(w http.ResponseWriter, r *http.Request, cc *ConnectedClient, connID string) {
	f.wsMu.RLock()
	sess, ok := f.wsSessions[connID]
	f.wsMu.RUnlock()
	if !ok {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	if sess.clientUID != cc.UniqueID {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	var frame proto.WSFrame
	if err := json.NewDecoder(r.Body).Decode(&frame); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	frame.ConnID = connID
	select {
	case sess.fromClient <- frame:
	default:
	}
	w.WriteHeader(http.StatusOK)
}

func (f *Forwarder) handleWSCloseFromClient(w http.ResponseWriter, cc *ConnectedClient, connID string) {
	f.wsMu.RLock()
	sess, ok := f.wsSessions[connID]
	f.wsMu.RUnlock()
	if !ok {
		w.WriteHeader(http.StatusOK)
		return
	}
	if sess.clientUID != cc.UniqueID {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	sess.mu.Lock()
	if sess.conn != nil {
		sess.conn.Close(websocket.StatusNormalClosure, "")
	}
	sess.mu.Unlock()
	close(sess.fromClient)
	w.WriteHeader(http.StatusOK)
}

func (f *Forwarder) handlePullReqBody(w http.ResponseWriter, pr *pendingRequest) {
	if pr.body == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("Transfer-Encoding", "chunked")
	w.WriteHeader(http.StatusOK)
	io.Copy(w, pr.body)
	pr.body.Close()
}

type inlineResponse struct {
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers"`
	BodyB64 string            `json:"body_b64"`
}

func (f *Forwarder) handleRespInline(w http.ResponseWriter, r *http.Request, pr *pendingRequest) {
	var resp inlineResponse
	if err := json.NewDecoder(r.Body).Decode(&resp); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	body, err := base64.StdEncoding.DecodeString(resp.BodyB64)
	if err != nil {
		http.Error(w, "bad base64", http.StatusBadRequest)
		return
	}

	pr.mu.Lock()
	pr.status = resp.Status
	pr.headers = resp.Headers
	pr.bodyData = body
	pr.mu.Unlock()
	close(pr.done)

	w.WriteHeader(http.StatusOK)
}

type headResponse struct {
	Status        int               `json:"status"`
	Headers       map[string]string `json:"headers"`
	ContentLength int64             `json:"content_length,omitempty"`
}

func (f *Forwarder) handleRespHead(w http.ResponseWriter, r *http.Request, pr *pendingRequest) {
	var resp headResponse
	if err := json.NewDecoder(r.Body).Decode(&resp); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	for k, v := range resp.Headers {
		pr.w.Header().Set(k, v)
	}
	if resp.ContentLength > 0 {
		pr.w.Header().Set("Content-Length", strconv.FormatInt(resp.ContentLength, 10))
	}
	pr.w.WriteHeader(resp.Status)

	pr.mu.Lock()
	pr.hasHead = true
	pr.status = resp.Status
	pr.mu.Unlock()
	close(pr.headCh)

	w.WriteHeader(http.StatusOK)
}

func (f *Forwarder) handleRespStream(w http.ResponseWriter, r *http.Request, pr *pendingRequest) {
	select {
	case <-pr.headCh:
	case <-time.After(30 * time.Second):
		http.Error(w, "timeout waiting for response head", http.StatusGatewayTimeout)
		return
	}

	if flusher, ok := pr.w.(http.Flusher); ok {
		buf := make([]byte, 4096)
		for {
			n, err := r.Body.Read(buf)
			if n > 0 {
				pr.w.Write(buf[:n])
				flusher.Flush()
			}
			if err != nil {
				break
			}
		}
	} else {
		io.Copy(pr.w, r.Body)
	}
	r.Body.Close()

	pr.mu.Lock()
	if pr.bodyData == nil {
		pr.bodyData = []byte{}
	}
	pr.mu.Unlock()
	close(pr.done)

	w.WriteHeader(http.StatusOK)
}

func (f *Forwarder) handleDirListResp(w http.ResponseWriter, r *http.Request, pr *pendingRequest) {
	var resp proto.DirListResp
	if err := json.NewDecoder(r.Body).Decode(&resp); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	if resp.HasIndex {
		pr.mu.Lock()
		pr.status = http.StatusFound
		pr.headers = map[string]string{"Location": "index.html"}
		pr.bodyData = []byte{}
		pr.mu.Unlock()
		close(pr.done)
		w.WriteHeader(http.StatusOK)
		return
	}

	html := renderDirListing(pr.shareName, resp.Entries, resp.Truncated)
	pr.mu.Lock()
	pr.status = http.StatusOK
	pr.headers = map[string]string{"Content-Type": "text/html; charset=utf-8"}
	pr.bodyData = []byte(html)
	pr.mu.Unlock()
	close(pr.done)

	w.WriteHeader(http.StatusOK)
}

func (f *Forwarder) writeOfflinePage(w http.ResponseWriter, cl *store.Client, sh *store.Share, reason string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusServiceUnavailable)

	hostname := "unknown"
	clientLabel := "unknown"
	lastIP := ""
	offlineAt := ""
	if cl != nil {
		hostname = html.EscapeString(cl.Hostname)
		clientLabel = fmt.Sprintf("c%d", cl.ShortID)
		lastIP = cl.LastIP
		if cl.OfflineAt > 0 {
			offlineAt = time.Unix(cl.OfflineAt, 0).UTC().Format("2006-01-02 15:04:05 UTC")
		}
	}

	var shareDetail string
	if sh != nil {
		switch sh.Kind {
		case "dir":
			shareDetail = html.EscapeString(sh.LocalPath)
		case "port":
			shareDetail = fmt.Sprintf(":%d", sh.LocalPort)
			if sh.ProcessCwd != "" {
				shareDetail += " &nbsp;·&nbsp; " + html.EscapeString(sh.ProcessCwd)
			}
		}
	}

	fmt.Fprintf(w, `<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Share Offline</title>
<style>
body{font-family:sans-serif;margin:0;background:#f5f5f5;display:flex;align-items:center;justify-content:center;min-height:100vh}
.card{background:#fff;border-radius:8px;box-shadow:0 2px 8px rgba(0,0,0,.12);padding:2em 2.5em;max-width:480px;width:100%%}
h2{margin:0 0 .5em;color:#c0392b}
.row{margin:.4em 0;color:#555;font-size:.95em}
.label{display:inline-block;width:7em;color:#888}
.reason{margin-top:1.2em;padding:.6em 1em;background:#fef9e7;border-left:4px solid #f39c12;font-size:.9em;color:#7d6608}
</style>
</head><body><div class="card">
<h2>Share Unavailable</h2>
<div class="row"><span class="label">Client</span>%s (%s)</div>`,
		clientLabel, hostname)

	if lastIP != "" {
		fmt.Fprintf(w, `<div class="row"><span class="label">Last IP</span>%s</div>`, html.EscapeString(lastIP))
	}
	if offlineAt != "" {
		fmt.Fprintf(w, `<div class="row"><span class="label">Offline at</span>%s</div>`, offlineAt)
	}
	if sh != nil {
		fmt.Fprintf(w, `<div class="row"><span class="label">Share</span>%s (%s)</div>`, html.EscapeString(sh.ShareName), sh.Kind)
		if shareDetail != "" {
			fmt.Fprintf(w, `<div class="row"><span class="label">Detail</span>%s</div>`, shareDetail)
		}
	}

	fmt.Fprintf(w, `<div class="reason">%s</div>
</div></body></html>`, html.EscapeString(reason))
}

func isDir(path string) bool {
	return path == "" || path == "/" || path[len(path)-1] == '/'
}
