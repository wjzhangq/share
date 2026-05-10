package procmon

import (
	"fmt"

	"github.com/shirou/gopsutil/v3/net"
	"github.com/shirou/gopsutil/v3/process"
)

type ProcessInfo struct {
	PID int32
	Exe string
	Cwd string
}

func FindListeningProcess(port int) (*ProcessInfo, error) {
	conns, err := net.Connections("tcp")
	if err != nil {
		return nil, fmt.Errorf("list connections: %w", err)
	}

	seen := make(map[int32]bool)
	var result *ProcessInfo

	for _, c := range conns {
		if c.Status != "LISTEN" {
			continue
		}
		if c.Laddr.Port != uint32(port) {
			continue
		}
		if seen[c.Pid] {
			continue
		}
		seen[c.Pid] = true

		p, err := process.NewProcess(c.Pid)
		if err != nil {
			continue
		}
		exe, err := p.Exe()
		if err != nil {
			continue
		}
		cwd, _ := p.Cwd()

		if result != nil && result.PID != c.Pid {
			return nil, fmt.Errorf("multiple processes listening on port %d", port)
		}
		result = &ProcessInfo{PID: c.Pid, Exe: exe, Cwd: cwd}
	}

	if result == nil {
		return nil, fmt.Errorf("no process listening on port %d", port)
	}
	return result, nil
}

func IsProcessAlive(pid int32, expectedExe string) bool {
	p, err := process.NewProcess(pid)
	if err != nil {
		return false
	}
	exe, err := p.Exe()
	if err != nil {
		return false
	}
	return exe == expectedExe
}

func IsPortListening(port int) bool {
	conns, err := net.Connections("tcp")
	if err != nil {
		return false
	}
	for _, c := range conns {
		if c.Laddr.Port == uint32(port) && c.Status == "LISTEN" {
			return true
		}
	}
	return false
}
