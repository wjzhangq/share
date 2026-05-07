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
	return filepath.Join(os.Getenv("APPDATA"), "share")
}
