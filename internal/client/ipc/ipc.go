package ipc

import (
	"bufio"
	"encoding/json"
	"net"
	"time"

	"github.com/wenjin/sharexxx/internal/proto"
)

type Server struct {
	listener net.Listener
	handler  func(proto.IPCRequest) proto.IPCResponse
}

func NewServer(addr string, handler func(proto.IPCRequest) proto.IPCResponse) (*Server, error) {
	ln, err := listen(addr)
	if err != nil {
		return nil, err
	}
	return &Server{listener: ln, handler: handler}, nil
}

func (s *Server) Serve() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.handle(conn)
	}
}

func (s *Server) Close() error {
	return s.listener.Close()
}

func (s *Server) handle(conn net.Conn) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(30 * time.Second))
	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		return
	}
	var req proto.IPCRequest
	if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
		return
	}
	resp := s.handler(req)
	data, _ := json.Marshal(resp)
	data = append(data, '\n')
	conn.Write(data)
}

func Dial(addr string) (net.Conn, error) {
	return dial(addr)
}

func SendCommand(addr string, req proto.IPCRequest) (*proto.IPCResponse, error) {
	conn, err := Dial(addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(30 * time.Second))

	data, _ := json.Marshal(req)
	data = append(data, '\n')
	if _, err := conn.Write(data); err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		return nil, scanner.Err()
	}
	var resp proto.IPCResponse
	if err := json.Unmarshal(scanner.Bytes(), &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}
