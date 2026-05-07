//go:build !windows

package paths

import (
	"os"
	"path/filepath"
)

func ipcAddr(uid string) string {
	if r := os.Getenv("XDG_RUNTIME_DIR"); r != "" {
		return filepath.Join(r, "share-"+uid+".sock")
	}
	return filepath.Join(os.TempDir(), "share-"+uid+".sock")
}

func configDir() string {
	if c := os.Getenv("XDG_CONFIG_HOME"); c != "" {
		return filepath.Join(c, "share")
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".config", "share")
}
