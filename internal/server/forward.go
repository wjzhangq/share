package server

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/wjzhangq/share/internal/proto"
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

type Forwarder struct {
	srv      *Server
	mu       sync.RWMutex
	pending  map[string]*pendingRequest
}

func NewForwarder(srv *Server) *Forwarder {
	return &Forwarder{
		srv:     srv,
		pending: make(map[string]*pendingRequest),
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

	if r.Header.Get("Upgrade") == "websocket" {
		http.Error(w, "WebSocket proxy not supported", http.StatusNotImplemented)
		f.srv.logger.Info("req end", "path", r.URL.Path, "status", 501, "duration", time.Since(start))
		return
	}

	client := f.srv.hub.GetClientByShortID(hi.shortID)
	if client == nil {
		cl, err := f.srv.store.GetClientByShortID(hi.shortID)
		if err != nil {
			http.Error(w, "share not found", http.StatusNotFound)
			f.srv.logger.Info("req end", "path", r.URL.Path, "status", 404, "reason", "client not found", "duration", time.Since(start))
			return
		}
		f.writeOfflinePage(w, cl.Hostname, fmt.Sprintf("c%d", hi.shortID), hi.shareName, "client offline")
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
		f.writeOfflinePage(w, "", fmt.Sprintf("c%d", hi.shortID), hi.shareName, "offline (process exited)")
		f.srv.logger.Info("req end", "path", r.URL.Path, "status", 503, "reason", "process offline", "duration", time.Since(start))
		return
	}

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
		if isDir(r.URL.Path) {
			fwdMsg = proto.DirList{Type: "dir.list", ReqID: reqID, ShareName: hi.shareName, RelPath: r.URL.Path}
		} else {
			fwdMsg = proto.DirRead{Type: "dir.read", ReqID: reqID, ShareName: hi.shareName, RelPath: r.URL.Path, Range: r.Header.Get("Range")}
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

	io.Copy(pr.w, r.Body)
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
		pr.status = 0
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

func (f *Forwarder) writeOfflinePage(w http.ResponseWriter, hostname, clientLabel, shareName, reason string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusServiceUnavailable)
	fmt.Fprintf(w, "client: %s (%s)\nshare : %s\nstatus: %s\n", clientLabel, hostname, shareName, reason)
}

func isDir(path string) bool {
	return path == "" || path == "/" || path[len(path)-1] == '/'
}
