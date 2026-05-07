//go:build windows

package paths

import (
	"os"
	"path/filepath"
)

func ipcAddr(uid string) string {
	return `\\.\pipe\share-` + uid
}

func configDir() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".share-cli")
}
