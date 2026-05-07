package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/wenjin/sharexxx/internal/client"
)

func main() {
	args := os.Args[1:]

	if len(args) > 0 && args[0] == "daemon" {
		runDaemon()
		return
	}

	client.RunCLI(args)
}

func runDaemon() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
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
