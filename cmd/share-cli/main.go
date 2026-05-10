package main

import (
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/wjzhangq/share/internal/client"
	"github.com/wjzhangq/share/internal/client/paths"
)

const maxLogSize = 2 * 1024 * 1024 // 2 MB

func main() {
	args := os.Args[1:]

	if len(args) > 0 && args[0] == "daemon" {
		runDaemon()
		return
	}

	client.RunCLI(args)
}

func runDaemon() {
	logPath := paths.LogFile()
	os.MkdirAll(paths.ConfigDir(), 0700)

	logWriter := openLogWriter(logPath)
	w := io.MultiWriter(os.Stderr, logWriter)

	logger := slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo}))
	d := client.NewDaemon(logger)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigCh
		d.Shutdown()
		os.Exit(0)
	}()

	if err := d.Run(); err != nil {
		logger.Error("daemon failed", "err", err)
		os.Exit(1)
	}
}

// openLogWriter opens the log file, truncating it first if it exceeds maxLogSize.
func openLogWriter(path string) *os.File {
	if fi, err := os.Stat(path); err == nil && fi.Size() >= maxLogSize {
		os.Truncate(path, 0)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return os.Stderr
	}
	return f
}
