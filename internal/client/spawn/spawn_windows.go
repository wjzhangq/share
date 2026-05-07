//go:build windows

package spawn

import (
	"os"
	"syscall"
)

func spawnDaemon(exe string) error {
	attr := &os.ProcAttr{
		Dir:   ".",
		Env:   os.Environ(),
		Files: []*os.File{os.Stdin, os.Stdout, os.Stderr},
		Sys: &syscall.SysProcAttr{
			CreationFlags: 0x00000008, // DETACHED_PROCESS
		},
	}
	p, err := os.StartProcess(exe, []string{exe, "daemon"}, attr)
	if err != nil {
		return err
	}
	p.Release()
	return nil
}
