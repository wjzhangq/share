package client

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/wjzhangq/share/internal/proto"
	"github.com/wjzhangq/share/internal/version"
)

type WSClient struct {
	serverURL string
	uniqueID  string
	hostname  string
	os        string
	arch      string
	conn      *websocket.Conn
	mu        sync.Mutex
	onMessage func(json.RawMessage)
	logger    *slog.Logger
}

func NewWSClient(serverURL, uniqueID, hostname, goos, goarch string, logger *slog.Logger) *WSClient {
	return &WSClient{
		serverURL: serverURL,
		uniqueID:  uniqueID,
		hostname:  hostname,
		os:        goos,
		arch:      goarch,
		logger:    logger,
	}
}

func (w *WSClient) SetMessageHandler(fn func(json.RawMessage)) {
	w.onMessage = fn
}

func (w *WSClient) Connect(ctx context.Context) (*proto.Welcome, error) {
	dialCtx, dialCancel := context.WithTimeout(ctx, 15*time.Second)
	defer dialCancel()

	conn, _, err := websocket.Dial(dialCtx, w.serverURL, nil)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	w.mu.Lock()
	w.conn = conn
	w.mu.Unlock()

	writeCtx, writeCancel := context.WithTimeout(ctx, 10*time.Second)
	defer writeCancel()

	hello := proto.Hello{
		Type:     "hello",
		UniqueID: w.uniqueID,
		Hostname: w.hostname,
		OS:       w.os,
		Arch:     w.arch,
		Version:  version.Version,
	}
	if err := w.writeJSON(writeCtx, hello); err != nil {
		conn.Close(websocket.StatusInternalError, "")
		return nil, err
	}

	readCtx, readCancel := context.WithTimeout(ctx, 10*time.Second)
	defer readCancel()

	var welcome proto.Welcome
	if err := w.readJSON(readCtx, &welcome); err != nil {
		conn.Close(websocket.StatusInternalError, "")
		return nil, err
	}
	if welcome.Type != "welcome" {
		conn.Close(websocket.StatusProtocolError, "expected welcome")
		return nil, fmt.Errorf("unexpected message type: %s", welcome.Type)
	}

	return &welcome, nil
}

func (w *WSClient) ReadLoop(ctx context.Context) {
	for {
		_, data, err := w.conn.Read(ctx)
		if err != nil {
			w.logger.Error("ws read error", "err", err)
			return
		}
		if w.onMessage != nil {
			w.onMessage(json.RawMessage(data))
		}
	}
}

func (w *WSClient) SendJSON(ctx context.Context, v any) error {
	return w.writeJSON(ctx, v)
}

func (w *WSClient) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.conn != nil {
		w.conn.Close(websocket.StatusNormalClosure, "")
		w.conn = nil
	}
}

func (w *WSClient) writeJSON(ctx context.Context, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.conn == nil {
		return fmt.Errorf("not connected")
	}
	writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return w.conn.Write(writeCtx, websocket.MessageText, data)
}

func (w *WSClient) readJSON(ctx context.Context, v any) error {
	_, data, err := w.conn.Read(ctx)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

func (w *WSClient) StartPing(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				w.SendJSON(ctx, proto.Pong{Type: "pong"})
			}
		}
	}()
}
