package procmon

import (
	"fmt"
	"os/exec"
	"runtime"
	"strings"

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
		cwd := getCwd(c.Pid)

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

func getCwd(pid int32) string {
	if runtime.GOOS == "darwin" {
		out, err := exec.Command("lsof", "-p", fmt.Sprintf("%d", pid), "-a", "-d", "cwd", "-Fn").Output()
		if err == nil {
			for _, line := range strings.Split(string(out), "\n") {
				if strings.HasPrefix(line, "n") {
					return line[1:]
				}
			}
		}
	}
	p, err := process.NewProcess(pid)
	if err != nil {
		return ""
	}
	cwd, _ := p.Cwd()
	return cwd
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

func IsListening(pid int32, port int) bool {
	conns, err := net.Connections("tcp")
	if err != nil {
		return false
	}
	for _, c := range conns {
		if c.Pid == pid && c.Laddr.Port == uint32(port) && c.Status == "LISTEN" {
			return true
		}
	}
	return false
}
