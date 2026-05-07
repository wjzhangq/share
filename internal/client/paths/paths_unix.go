//go:build !windows

package paths

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
)

func ipcAddr(uid string) string {
	if r := os.Getenv("XDG_RUNTIME_DIR"); r != "" {
		return filepath.Join(r, "share-"+uid+".sock")
	}
	return filepath.Join(os.TempDir(), "share-"+uid+".sock")
}

func configDir() string {
	home, _ := homeDir()
	return filepath.Join(home, ".share-cli")
}

func homeDir() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil || homeDir == "" {
		if u, uerr := user.Current(); uerr == nil && u.HomeDir != "" {
			homeDir = u.HomeDir
		} else if u, uerr := user.LookupId(fmt.Sprintf("%d", os.Getuid())); uerr == nil && u.HomeDir != "" {
			homeDir = u.HomeDir
		} else {
			return "", fmt.Errorf("get home dir error: %w", err)
		}
	}
	return homeDir, nil
}
