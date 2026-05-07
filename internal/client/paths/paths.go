package paths

func IPCAddr(uid string) string {
	return ipcAddr(uid)
}

func ConfigDir() string {
	return configDir()
}
