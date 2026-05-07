//go:build windows

package ipc

import (
	"net"
	"time"

	"github.com/Microsoft/go-winio"
)

func listen(addr string) (net.Listener, error) {
	return winio.ListenPipe(addr, nil)
}

func dial(addr string) (net.Conn, error) {
	return winio.DialPipe(addr, (*time.Duration)(nil))
}
