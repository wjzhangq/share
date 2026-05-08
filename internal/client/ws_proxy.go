package client

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/coder/websocket"

	"github.com/wjzhangq/share/internal/proto"
)

type wsProxyConn struct {
	connID string
	conn   *websocket.Conn
	cancel context.CancelFunc
	// frames from server -> local ws
	toLocal chan proto.WSFrame
	mu      sync.Mutex
}

func (d *Daemon) handleWSOpen(wo proto.WSOpen) {
	d.mu.Lock()
	share := d.findShareByName(wo.ShareName)
	d.mu.Unlock()
	if share == nil {
		d.notifyWSOpenError(wo.ConnID, wo.ShareName, "share not found")
		return
	}

	st := d.state.Get()
	localPort := share.State.LocalPort
	scheme := "ws"
	if d.scheme() == "https" {
		scheme = "wss"
	}
	localURL := fmt.Sprintf("%s://127.0.0.1:%d%s", scheme, localPort, wo.Path)

	dialHeaders := http.Header{}
	for k, v := range wo.Headers {
		dialHeaders.Set(k, v)
	}
	// remove hop-by-hop headers
	dialHeaders.Del("Upgrade")
	dialHeaders.Del("Connection")
	dialHeaders.Del("Sec-Websocket-Key")
	dialHeaders.Del("Sec-Websocket-Version")
	dialHeaders.Del("Sec-Websocket-Extensions")

	ctx, cancel := context.WithCancel(d.ctx)

	conn, _, err := websocket.Dial(ctx, localURL, &websocket.DialOptions{
		HTTPHeader: dialHeaders,
	})
	if err != nil {
		cancel()
		d.notifyWSOpenError(wo.ConnID, wo.ShareName, err.Error())
		return
	}

	pc := &wsProxyConn{
		connID:  wo.ConnID,
		conn:    conn,
		cancel:  cancel,
		toLocal: make(chan proto.WSFrame, 64),
	}

	d.wsMu.Lock()
	d.wsConns[wo.ConnID] = pc
	d.wsMu.Unlock()

	d.notifyWSOpened(wo.ConnID, wo.ShareName, st)

	// local -> server
	go func() {
		defer func() {
			d.wsMu.Lock()
			delete(d.wsConns, wo.ConnID)
			d.wsMu.Unlock()
			cancel()
			d.notifyWSClose(wo.ConnID, wo.ShareName, st)
		}()

		for {
			msgType, data, err := conn.Read(ctx)
			if err != nil {
				break
			}
			frame := proto.WSFrame{
				Type:    "ws.frame",
				ConnID:  wo.ConnID,
				MsgType: int(msgType),
				DataB64: base64.StdEncoding.EncodeToString(data),
			}
			d.sendWSFrameToServer(wo.ConnID, wo.ShareName, frame, st)
		}
	}()

	// server -> local
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case frame, ok := <-pc.toLocal:
				if !ok {
					return
				}
				data, err := base64.StdEncoding.DecodeString(frame.DataB64)
				if err != nil {
					continue
				}
				conn.Write(ctx, websocket.MessageType(frame.MsgType), data)
			}
		}
	}()
}

func (d *Daemon) dispatchWSFrame(wf proto.WSFrame) {
	d.wsMu.Lock()
	pc, ok := d.wsConns[wf.ConnID]
	d.wsMu.Unlock()
	if !ok {
		return
	}
	select {
	case pc.toLocal <- wf:
	default:
		d.logger.Warn("ws frame dropped", "conn_id", wf.ConnID)
	}
}

func (d *Daemon) closeWSConn(connID string) {
	d.wsMu.Lock()
	pc, ok := d.wsConns[connID]
	if ok {
		delete(d.wsConns, connID)
	}
	d.wsMu.Unlock()
	if ok {
		pc.cancel()
		pc.conn.Close(websocket.StatusNormalClosure, "")
	}
}

func (d *Daemon) notifyWSOpened(connID, shareName string, st State) {
	host := d.buildShareHost(shareName, st)
	url := fmt.Sprintf("%s://%s/", d.scheme(), host)

	req, _ := http.NewRequest("POST", url, nil)
	req.Header.Set(proto.HeaderOp, proto.OpWSOpened)
	req.Header.Set(proto.HeaderReqID, connID)
	req.Header.Set(proto.HeaderClientToken, st.UniqueID)

	resp, err := d.httpCli.Do(req)
	if err != nil {
		d.logger.Error("ws opened notify", "err", err)
		return
	}
	resp.Body.Close()
}

func (d *Daemon) notifyWSOpenError(connID, shareName, msg string) {
	st := d.state.Get()
	host := d.buildShareHost(shareName, st)
	url := fmt.Sprintf("%s://%s/", d.scheme(), host)

	body, _ := json.Marshal(map[string]string{"message": msg})
	req, _ := http.NewRequestWithContext(d.ctx, "POST", url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(proto.HeaderOp, proto.OpWSOpenError)
	req.Header.Set(proto.HeaderReqID, connID)
	req.Header.Set(proto.HeaderClientToken, st.UniqueID)

	resp, err := d.httpCli.Do(req)
	if err != nil {
		d.logger.Error("ws open error notify", "err", err)
		return
	}
	resp.Body.Close()
}

func (d *Daemon) sendWSFrameToServer(connID, shareName string, frame proto.WSFrame, st State) {
	host := d.buildShareHost(shareName, st)
	url := fmt.Sprintf("%s://%s/", d.scheme(), host)

	body, _ := json.Marshal(frame)
	req, _ := http.NewRequestWithContext(d.ctx, "POST", url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(proto.HeaderOp, proto.OpWSFrameToServer)
	req.Header.Set(proto.HeaderReqID, connID)
	req.Header.Set(proto.HeaderClientToken, st.UniqueID)

	resp, err := d.httpCli.Do(req)
	if err != nil {
		d.logger.Error("ws frame send", "err", err)
		return
	}
	resp.Body.Close()
}

func (d *Daemon) notifyWSClose(connID, shareName string, st State) {
	host := d.buildShareHost(shareName, st)
	url := fmt.Sprintf("%s://%s/", d.scheme(), host)

	req, _ := http.NewRequestWithContext(d.ctx, "POST", url, nil)
	req.Header.Set(proto.HeaderOp, proto.OpWSCloseFromClient)
	req.Header.Set(proto.HeaderReqID, connID)
	req.Header.Set(proto.HeaderClientToken, st.UniqueID)

	resp, err := d.httpCli.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close()
}
