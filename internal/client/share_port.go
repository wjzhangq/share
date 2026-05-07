package client

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/wenjin/sharexxx/internal/client/procmon"
	"github.com/wenjin/sharexxx/internal/proto"
)

func (d *Daemon) sharePort(port int) proto.IPCResponse {
	proc, err := procmon.FindListeningProcess(port)
	if err != nil {
		return proto.IPCResponse{Err: err.Error()}
	}

	exeBase := filepath.Base(proc.Exe)
	hintName := cleanName(fmt.Sprintf("%s-%d", exeBase, port))
	sourceKey := fmt.Sprintf("port:%s:%d", proc.Exe, port)

	ss := ShareState{
		Kind:       "port",
		ShareName:  hintName,
		LocalPort:  port,
		SourceKey:  sourceKey,
		ProcessExe: proc.Exe,
	}
	d.state.AddShare(ss)

	msg := proto.ShareCreate{
		Type:       "share.create",
		Kind:       "port",
		HintName:   hintName,
		SourceKey:  sourceKey,
		LocalPort:  port,
		ProcessPID: int(proc.PID),
		ProcessExe: proc.Exe,
	}
	d.ws.SendJSON(d.ctx, msg)

	go d.watchProcess(hintName, proc.PID, proc.Exe, port)

	return proto.IPCResponse{OK: true, Data: map[string]any{
		"hint": hintName,
		"port": port,
		"pid":  proc.PID,
		"exe":  proc.Exe,
	}}
}

func (d *Daemon) watchProcess(shareName string, pid int32, exe string, port int) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	alive := true
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-ticker.C:
		}

		d.mu.Lock()
		_, exists := d.shares[shareName]
		d.mu.Unlock()
		if !exists {
			return
		}

		if alive {
			if !procmon.IsProcessAlive(pid, exe) || !procmon.IsListening(pid, port) {
				alive = false
				d.ws.SendJSON(d.ctx, proto.ShareProcessDown{
					Type:      "share.process_down",
					ShareName: shareName,
					ExitAt:    time.Now().Unix(),
				})
				d.logger.Info("process down", "share", shareName, "pid", pid)
			}
		} else {
			newProc, err := procmon.FindListeningProcess(port)
			if err == nil && newProc.Exe == exe {
				alive = true
				pid = newProc.PID
				d.ws.SendJSON(d.ctx, proto.ShareProcessUp{
					Type:      "share.process_up",
					ShareName: shareName,
					NewPID:    int(newProc.PID),
					LocalPort: port,
				})
				d.logger.Info("process up", "share", shareName, "pid", newProc.PID)
			}
		}
	}
}

func (d *Daemon) handleForwardReq(fr proto.ForwardReq) {
	d.mu.Lock()
	share := d.findShareByName(fr.ShareName)
	d.mu.Unlock()
	if share == nil {
		return
	}

	st := d.state.Get()
	localPort := share.State.LocalPort
	localURL := fmt.Sprintf("http://127.0.0.1:%d%s", localPort, fr.Path)

	var reqBody io.Reader
	if fr.BodyMode == "stream" {
		body, err := d.pullReqBody(fr.ReqID, fr.ShareName, st)
		if err != nil {
			d.sendErrorResp(fr.ReqID, fr.ShareName, 502, "failed to pull request body")
			return
		}
		reqBody = body
	}

	localReq, err := http.NewRequest(fr.Method, localURL, reqBody)
	if err != nil {
		d.sendErrorResp(fr.ReqID, fr.ShareName, 502, "bad request")
		return
	}
	for k, v := range fr.Headers {
		if strings.EqualFold(k, "Host") {
			continue
		}
		localReq.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(localReq)
	if err != nil {
		d.sendErrorResp(fr.ReqID, fr.ShareName, 502, "upstream error")
		return
	}
	defer resp.Body.Close()

	respHeaders := make(map[string]string)
	for k := range resp.Header {
		respHeaders[k] = resp.Header.Get(k)
	}

	threshold := int64(1048576)
	if resp.ContentLength >= 0 && resp.ContentLength <= threshold {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return
		}
		d.sendInlineResp(fr.ReqID, fr.ShareName, st, resp.StatusCode, respHeaders, body)
	} else if resp.ContentLength > threshold {
		d.sendStreamResp(fr.ReqID, fr.ShareName, st, resp.StatusCode, respHeaders, resp.ContentLength, resp.Body)
	} else {
		var buf bytes.Buffer
		n, _ := io.CopyN(&buf, resp.Body, threshold+1)
		if n <= threshold {
			d.sendInlineResp(fr.ReqID, fr.ShareName, st, resp.StatusCode, respHeaders, buf.Bytes())
		} else {
			combined := io.MultiReader(&buf, resp.Body)
			d.sendStreamResp(fr.ReqID, fr.ShareName, st, resp.StatusCode, respHeaders, -1, combined)
		}
	}
}

func (d *Daemon) pullReqBody(reqID, shareName string, st State) (io.ReadCloser, error) {
	host := d.buildShareHost(shareName, st)
	url := fmt.Sprintf("%s://%s/", d.scheme(), host)

	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set(proto.HeaderOp, proto.OpPullReqBody)
	req.Header.Set(proto.HeaderReqID, reqID)
	req.Header.Set(proto.HeaderClientToken, st.UniqueID)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		resp.Body.Close()
		return nil, fmt.Errorf("pull body: status %d", resp.StatusCode)
	}
	return resp.Body, nil
}
