//go:build !windows

package ipc

import (
	"net"
	"os"
)

func listen(addr string) (net.Listener, error) {
	os.Remove(addr)
	return net.Listen("unix", addr)
}

func dial(addr string) (net.Conn, error) {
	return net.Dial("unix", addr)
}
