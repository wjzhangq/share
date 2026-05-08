package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/wjzhangq/share/internal/proto"
)

type ConnectedClient struct {
	UniqueID string
	ShortID  int64
	Conn     *websocket.Conn
	cancel   context.CancelFunc
}

type Hub struct {
	srv     *Server
	mu      sync.RWMutex
	clients map[string]*ConnectedClient // unique_id -> client
}

func NewHub(srv *Server) *Hub {
	return &Hub{
		srv:     srv,
		clients: make(map[string]*ConnectedClient),
	}
}

func (h *Hub) GetClient(uniqueID string) *ConnectedClient {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.clients[uniqueID]
}

func (h *Hub) GetClientByShortID(shortID int64) *ConnectedClient {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, c := range h.clients {
		if c.ShortID == shortID {
			return c
		}
	}
	return nil
}

func (h *Hub) HandleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{})
	if err != nil {
		h.srv.logger.Error("ws accept", "err", err)
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	helloCtx, helloCancel := context.WithTimeout(ctx, 10*time.Second)
	defer helloCancel()

	var hello proto.Hello
	if err := h.readJSON(helloCtx, conn, &hello); err != nil {
		h.srv.logger.Error("ws read hello", "err", err)
		conn.Close(websocket.StatusProtocolError, "expected hello")
		return
	}
	if hello.Type != "hello" || hello.UniqueID == "" {
		conn.Close(websocket.StatusProtocolError, "invalid hello")
		return
	}

	client, err := h.srv.store.RegisterClient(hello.UniqueID, hello.Hostname, hello.OS, hello.Arch, hello.Version)
	if err != nil {
		h.srv.logger.Error("register client", "err", err)
		conn.Close(websocket.StatusInternalError, "register failed")
		return
	}

	h.mu.Lock()
	if old, ok := h.clients[hello.UniqueID]; ok {
		old.cancel()
		old.Conn.Close(websocket.StatusGoingAway, "replaced")
	}
	cc := &ConnectedClient{
		UniqueID: hello.UniqueID,
		ShortID:  client.ShortID,
		Conn:     conn,
		cancel:   cancel,
	}
	h.clients[hello.UniqueID] = cc
	h.mu.Unlock()

	welcome := proto.Welcome{
		Type:        "welcome",
		ShortID:     client.ShortID,
		ClientLabel: fmt.Sprintf("c%d", client.ShortID),
		ServerTime:  time.Now().Unix(),
	}
	if err := h.writeJSON(ctx, conn, welcome); err != nil {
		h.srv.logger.Error("ws write welcome", "err", err)
		h.removeClient(hello.UniqueID)
		return
	}

	h.srv.logger.Info("client connected",
		"uid", hello.UniqueID,
		"short_id", client.ShortID,
		"hostname", hello.Hostname,
		"os", hello.OS,
		"arch", hello.Arch,
		"version", hello.Version,
	)

	h.readLoop(ctx, cc)

	h.removeClient(hello.UniqueID)
	h.srv.store.SetClientOffline(hello.UniqueID)
	h.srv.store.SetSharesOfflineByClient(hello.UniqueID)
	h.srv.logger.Info("client disconnected", "uid", hello.UniqueID)
}

func (h *Hub) removeClient(uid string) {
	h.mu.Lock()
	delete(h.clients, uid)
	h.mu.Unlock()
}

func (h *Hub) readLoop(ctx context.Context, cc *ConnectedClient) {
	pingTicker := time.NewTicker(30 * time.Second)
	defer pingTicker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-pingTicker.C:
				h.writeJSON(ctx, cc.Conn, proto.Ping{Type: "ping"})
			}
		}
	}()

	for {
		readCtx, readCancel := context.WithTimeout(ctx, 60*time.Second)
		var raw json.RawMessage
		if err := h.readJSON(readCtx, cc.Conn, &raw); err != nil {
			readCancel()
			return
		}
		readCancel()

		var msg proto.WSMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			continue
		}

		switch msg.Type {
		case "pong":
			// heartbeat ok
		case "share.create":
			h.handleShareCreate(ctx, cc, raw)
		case "share.list":
			h.handleShareList(ctx, cc)
		case "share.close":
			h.handleShareClose(ctx, cc, raw)
		case "share.process_down":
			h.handleProcessDown(ctx, cc, raw)
		case "share.process_up":
			h.handleProcessUp(ctx, cc, raw)
		case "forward.resp.ack":
			// informational
		default:
			h.srv.logger.Debug("unknown ws message type", "type", msg.Type)
		}
	}
}

func (h *Hub) handleShareCreate(ctx context.Context, cc *ConnectedClient, raw json.RawMessage) {
	var req proto.ShareCreate
	if err := json.Unmarshal(raw, &req); err != nil {
		return
	}

	shareName, err := h.srv.store.ResolveShareName(cc.UniqueID, req.SourceKey, req.HintName)
	if err != nil {
		h.writeJSON(ctx, cc.Conn, proto.ShareError{Type: "share.error", Code: "name_resolve", Message: err.Error()})
		return
	}

	existing, _ := h.srv.store.GetShare(cc.UniqueID, shareName)
	if existing != nil && existing.Status != "closed" {
		h.srv.store.ReactivateShare(cc.UniqueID, shareName)
		fullHost := fmt.Sprintf("c%d-%s.%s", cc.ShortID, shareName, h.srv.cfg.Domain)
		h.writeJSON(ctx, cc.Conn, proto.ShareCreated{
			Type: "share.created", ShareName: shareName, FullHost: fullHost, ShareID: existing.ID,
		})
		return
	}

	share, err := h.srv.store.CreateShare(cc.UniqueID, shareName, req.Kind, req.LocalPath, req.LocalPort, req.ProcessPID, req.ProcessExe, req.ProcessCwd)
	if err != nil {
		h.writeJSON(ctx, cc.Conn, proto.ShareError{Type: "share.error", Code: "create_failed", Message: err.Error()})
		return
	}

	fullHost := fmt.Sprintf("c%d-%s.%s", cc.ShortID, shareName, h.srv.cfg.Domain)
	h.writeJSON(ctx, cc.Conn, proto.ShareCreated{
		Type: "share.created", ShareName: shareName, FullHost: fullHost, ShareID: share.ID,
	})
	h.srv.logger.Info("share created",
		"client", cc.UniqueID,
		"short_id", cc.ShortID,
		"name", shareName,
		"kind", req.Kind,
		"local_port", req.LocalPort,
		"local_path", req.LocalPath,
		"process_exe", req.ProcessExe,
		"url", "https://"+fullHost,
	)
}

func (h *Hub) handleShareList(ctx context.Context, cc *ConnectedClient) {
	shares, err := h.srv.store.ListActiveSharesByClient(cc.UniqueID)
	if err != nil {
		return
	}
	var items []proto.ShareListItem
	for _, sh := range shares {
		items = append(items, proto.ShareListItem{
			ShareName: sh.ShareName,
			FullHost:  fmt.Sprintf("c%d-%s.%s", cc.ShortID, sh.ShareName, h.srv.cfg.Domain),
			Kind:      sh.Kind,
			Status:    sh.Status,
		})
	}
	h.writeJSON(ctx, cc.Conn, proto.ShareListResult{Type: "share.list.result", Shares: items})
}

func (h *Hub) handleShareClose(ctx context.Context, cc *ConnectedClient, raw json.RawMessage) {
	var req proto.ShareClose
	if err := json.Unmarshal(raw, &req); err != nil {
		return
	}
	h.srv.store.CloseShare(cc.UniqueID, req.ShareName)
	h.writeJSON(ctx, cc.Conn, proto.ShareClosed{Type: "share.closed", ShareName: req.ShareName, Reason: "client_close"})
}

func (h *Hub) handleProcessDown(_ context.Context, cc *ConnectedClient, raw json.RawMessage) {
	var req proto.ShareProcessDown
	if err := json.Unmarshal(raw, &req); err != nil {
		return
	}
	h.srv.store.SetShareOffline(cc.UniqueID, req.ShareName)
	h.srv.logger.Info("process down", "client", cc.UniqueID, "share", req.ShareName)
}

func (h *Hub) handleProcessUp(_ context.Context, cc *ConnectedClient, raw json.RawMessage) {
	var req proto.ShareProcessUp
	if err := json.Unmarshal(raw, &req); err != nil {
		return
	}
	h.srv.store.ReactivateShare(cc.UniqueID, req.ShareName)
	h.srv.logger.Info("process up", "client", cc.UniqueID, "share", req.ShareName, "pid", req.NewPID)
}

func (h *Hub) SendToClient(uid string, msg any) error {
	h.mu.RLock()
	cc, ok := h.clients[uid]
	h.mu.RUnlock()
	if !ok {
		return fmt.Errorf("client not connected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return h.writeJSON(ctx, cc.Conn, msg)
}

func (h *Hub) readJSON(ctx context.Context, conn *websocket.Conn, v any) error {
	_, data, err := conn.Read(ctx)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

func (h *Hub) writeJSON(ctx context.Context, conn *websocket.Conn, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return conn.Write(writeCtx, websocket.MessageText, data)
}

