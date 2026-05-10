package paths

import "path/filepath"

func IPCAddr(uid string) string {
	return ipcAddr(uid)
}

func ConfigDir() string {
	return configDir()
}

func LogFile() string {
	return filepath.Join(configDir(), "daemon.log")
}
