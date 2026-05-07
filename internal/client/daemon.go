package client

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/wenjin/sharexxx/internal/client/ipc"
	"github.com/wenjin/sharexxx/internal/client/paths"
	"github.com/wenjin/sharexxx/internal/proto"
)

type Daemon struct {
	state    *StateManager
	ws       *WSClient
	ipcSrv   *ipc.Server
	httpCli  *http.Client
	logger   *slog.Logger
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	shares   map[string]*ActiveShare
}

type ActiveShare struct {
	State     ShareState
	ShareName string
	FullHost  string
}

func NewDaemon(logger *slog.Logger) *Daemon {
	ctx, cancel := context.WithCancel(context.Background())
	return &Daemon{
		state:  NewStateManager(),
		httpCli: &http.Client{Timeout: 300 * time.Second},
		logger: logger,
		ctx:    ctx,
		cancel: cancel,
		shares: make(map[string]*ActiveShare),
	}
}

func (d *Daemon) Run() error {
	if err := d.state.Load(); err != nil {
		d.logger.Warn("load state", "err", err)
	}

	st := d.state.Get()
	if st.UniqueID == "" {
		uid := generateUID()
		d.state.SetUniqueID(uid)
		st.UniqueID = uid
	}

	if st.ServerURL == "" {
		return fmt.Errorf("server URL not configured, run: sharexxx login <server-url>")
	}

	ipcAddr := paths.IPCAddr(st.UniqueID)
	hostname, _ := os.Hostname()

	d.ws = NewWSClient(st.ServerURL, st.UniqueID, hostname, runtime.GOOS, runtime.GOARCH, d.logger)
	d.ws.SetMessageHandler(d.handleWSMessage)

	ipcSrv, err := ipc.NewServer(ipcAddr, d.handleIPC)
	if err != nil {
		return fmt.Errorf("ipc listen: %w", err)
	}
	d.ipcSrv = ipcSrv
	go ipcSrv.Serve()

	d.connectWithRetry()
	return nil
}

func (d *Daemon) connectWithRetry() {
	backoff := time.Second
	for {
		select {
		case <-d.ctx.Done():
			return
		default:
		}

		welcome, err := d.ws.Connect(d.ctx)
		if err != nil {
			d.logger.Error("ws connect", "err", err, "retry_in", backoff)
			time.Sleep(backoff)
			backoff *= 2
			if backoff > 60*time.Second {
				backoff = 60 * time.Second
			}
			continue
		}

		backoff = time.Second
		d.state.SetShortID(welcome.ShortID)
		d.logger.Info("connected", "short_id", welcome.ShortID, "label", welcome.ClientLabel)

		d.restoreShares()

		d.ws.StartPing(d.ctx)
		d.ws.ReadLoop(d.ctx)

		d.logger.Warn("ws disconnected, reconnecting...")
	}
}

func (d *Daemon) restoreShares() {
	st := d.state.Get()
	for _, s := range st.Shares {
		d.createShare(s)
	}
}

func (d *Daemon) createShare(s ShareState) {
	msg := proto.ShareCreate{
		Type:       "share.create",
		Kind:       s.Kind,
		HintName:   s.ShareName,
		SourceKey:  s.SourceKey,
		LocalPath:  s.LocalPath,
		LocalPort:  s.LocalPort,
		ProcessExe: s.ProcessExe,
	}
	d.ws.SendJSON(d.ctx, msg)
}

func (d *Daemon) handleWSMessage(raw json.RawMessage) {
	var msg proto.WSMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}

	switch msg.Type {
	case "ping":
		d.ws.SendJSON(d.ctx, proto.Pong{Type: "pong"})
	case "share.created":
		var sc proto.ShareCreated
		json.Unmarshal(raw, &sc)
		d.mu.Lock()
		d.shares[sc.ShareName] = &ActiveShare{ShareName: sc.ShareName, FullHost: sc.FullHost}
		d.mu.Unlock()
		d.logger.Info("share active", "name", sc.ShareName, "host", sc.FullHost)
	case "share.closed":
		var sc proto.ShareClosed
		json.Unmarshal(raw, &sc)
		d.mu.Lock()
		delete(d.shares, sc.ShareName)
		d.mu.Unlock()
		d.state.RemoveShare(sc.ShareName)
		d.logger.Info("share closed", "name", sc.ShareName, "reason", sc.Reason)
	case "share.error":
		var se proto.ShareError
		json.Unmarshal(raw, &se)
		d.logger.Error("share error", "code", se.Code, "msg", se.Message)
	case "forward.req":
		var fr proto.ForwardReq
		json.Unmarshal(raw, &fr)
		go d.handleForwardReq(fr)
	case "dir.list":
		var dl proto.DirList
		json.Unmarshal(raw, &dl)
		go d.handleDirList(dl)
	case "dir.read":
		var dr proto.DirRead
		json.Unmarshal(raw, &dr)
		go d.handleDirRead(dr)
	}
}

func (d *Daemon) handleIPC(req proto.IPCRequest) proto.IPCResponse {
	switch req.Cmd {
	case "share.create":
		return d.ipcShareCreate(req.Args)
	case "share.list":
		return d.ipcShareList()
	case "share.close":
		return d.ipcShareClose(req.Args)
	case "status":
		return d.ipcStatus()
	case "quit":
		go d.Shutdown()
		return proto.IPCResponse{OK: true}
	default:
		return proto.IPCResponse{Err: "unknown command"}
	}
}

func (d *Daemon) ipcShareCreate(args map[string]any) proto.IPCResponse {
	kind, _ := args["kind"].(string)
	switch kind {
	case "dir":
		path, _ := args["path"].(string)
		return d.shareDir(path)
	case "port":
		portF, _ := args["port"].(float64)
		return d.sharePort(int(portF))
	default:
		return proto.IPCResponse{Err: "invalid kind"}
	}
}

func (d *Daemon) ipcShareList() proto.IPCResponse {
	d.mu.Lock()
	defer d.mu.Unlock()
	var list []map[string]string
	for _, s := range d.shares {
		list = append(list, map[string]string{
			"name": s.ShareName,
			"host": s.FullHost,
		})
	}
	return proto.IPCResponse{OK: true, Data: list}
}

func (d *Daemon) ipcShareClose(args map[string]any) proto.IPCResponse {
	name, _ := args["name"].(string)
	all, _ := args["all"].(bool)

	if all {
		d.mu.Lock()
		for n := range d.shares {
			d.ws.SendJSON(d.ctx, proto.ShareClose{Type: "share.close", ShareName: n})
		}
		d.mu.Unlock()
		d.state.ClearShares()
		return proto.IPCResponse{OK: true}
	}

	if name == "" {
		return proto.IPCResponse{Err: "share name required"}
	}
	d.ws.SendJSON(d.ctx, proto.ShareClose{Type: "share.close", ShareName: name})
	d.state.RemoveShare(name)
	return proto.IPCResponse{OK: true}
}

func (d *Daemon) ipcStatus() proto.IPCResponse {
	st := d.state.Get()
	return proto.IPCResponse{OK: true, Data: map[string]any{
		"unique_id":  st.UniqueID,
		"short_id":   st.ShortID,
		"server_url": st.ServerURL,
		"shares":     len(d.shares),
	}}
}

func (d *Daemon) Shutdown() {
	d.cancel()
	if d.ipcSrv != nil {
		d.ipcSrv.Close()
	}
	d.ws.Close()
}

func generateUID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b)
}
