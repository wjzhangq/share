package client

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/wjzhangq/share/internal/client/ipc"
	"github.com/wjzhangq/share/internal/client/paths"
	"github.com/wjzhangq/share/internal/client/spawn"
	"github.com/wjzhangq/share/internal/proto"
	"github.com/wjzhangq/share/internal/version"
)

func RunCLI(args []string) {
	var serverURL string
	args = extractGlobalFlags(args, &serverURL)

	if len(args) < 1 {
		printUsage()
		os.Exit(1)
	}

	cmd := args[0]
	switch cmd {
	case "dir":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "usage: share-cli dir <path>")
			os.Exit(1)
		}
		dirPath, err := filepath.Abs(args[1])
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid path: %v\n", err)
			os.Exit(1)
		}
		ensureServerURL(serverURL)
		resp := sendToDeamon(proto.IPCRequest{
			Cmd:  "share.create",
			Args: map[string]any{"kind": "dir", "path": dirPath},
		})
		if !resp.OK {
			fmt.Fprintf(os.Stderr, "error: %s\n", resp.Err)
			os.Exit(1)
		}
		fmt.Printf("Shared directory: %v\n", resp.Data)

	case "port":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "usage: share-cli port <localPort>")
			os.Exit(1)
		}
		ensureServerURL(serverURL)
		var port int
		fmt.Sscanf(args[1], "%d", &port)
		if port <= 0 {
			fmt.Fprintln(os.Stderr, "invalid port")
			os.Exit(1)
		}
		resp := sendToDeamon(proto.IPCRequest{
			Cmd:  "share.create",
			Args: map[string]any{"kind": "port", "port": float64(port)},
		})
		if !resp.OK {
			fmt.Fprintf(os.Stderr, "error: %s\n", resp.Err)
			os.Exit(1)
		}
		fmt.Printf("Shared port: %v\n", resp.Data)

	case "ls":
		resp := sendToDeamon(proto.IPCRequest{Cmd: "share.list"})
		if !resp.OK {
			fmt.Fprintf(os.Stderr, "error: %s\n", resp.Err)
			os.Exit(1)
		}
		fmt.Printf("Shares: %v\n", resp.Data)

	case "close":
		args := parseCloseArgs(args[1:])
		resp := sendToDeamon(proto.IPCRequest{Cmd: "share.close", Args: args})
		if !resp.OK {
			fmt.Fprintf(os.Stderr, "error: %s\n", resp.Err)
			os.Exit(1)
		}
		fmt.Println("OK")

	case "status":
		resp := sendToDeamon(proto.IPCRequest{Cmd: "status"})
		if !resp.OK {
			fmt.Fprintf(os.Stderr, "error: %s\n", resp.Err)
			os.Exit(1)
		}
		fmt.Printf("Status: %v\n", resp.Data)

	case "login":
		url := serverURL
		if len(args) >= 2 {
			url = args[1]
		}
		if url == "" {
			url = version.DefaultServerURL
		}
		if url == "" {
			fmt.Fprintln(os.Stderr, "usage: share-cli login <server-url>")
			os.Exit(1)
		}
		sm := NewStateManager()
		sm.Load()
		sm.SetServerURL(url)
		fmt.Printf("Server URL set to: %s\n", url)

	case "version":
		fmt.Printf("share-cli %s\n", version.Version)
		if version.DefaultServerURL != "" {
			fmt.Printf("default server: %s\n", version.DefaultServerURL)
		}

	case "stop":
		resp := sendToDeamon(proto.IPCRequest{Cmd: "quit"})
		if !resp.OK {
			fmt.Fprintf(os.Stderr, "error: %s\n", resp.Err)
			os.Exit(1)
		}
		fmt.Println("daemon stopped")

	case "daemon":
		// handled in main
		fmt.Fprintln(os.Stderr, "daemon should be started internally")
		os.Exit(1)

	default:
		printUsage()
		os.Exit(1)
	}
}

func sendToDeamon(req proto.IPCRequest) proto.IPCResponse {
	sm := NewStateManager()
	sm.Load()
	st := sm.Get()

	if st.UniqueID == "" {
		uid := generateUID()
		sm.SetUniqueID(uid)
		st.UniqueID = uid
	}

	addr := paths.IPCAddr(st.UniqueID)

	resp, err := ipc.SendCommand(addr, req)
	if err == nil {
		return *resp
	}

	exe, _ := os.Executable()
	if err := spawn.Daemon(exe); err != nil {
		return proto.IPCResponse{Err: fmt.Sprintf("failed to start daemon: %v", err)}
	}

	for i := 0; i < 30; i++ {
		time.Sleep(200 * time.Millisecond)
		resp, err = ipc.SendCommand(addr, req)
		if err == nil {
			return *resp
		}
	}
	return proto.IPCResponse{Err: "daemon did not start in time"}
}

func extractGlobalFlags(args []string, serverURL *string) []string {
	var remaining []string
	for i := 0; i < len(args); i++ {
		if args[i] == "--url" && i+1 < len(args) {
			*serverURL = args[i+1]
			i++
		} else {
			remaining = append(remaining, args[i])
		}
	}
	return remaining
}

func ensureServerURL(urlOverride string) {
	if urlOverride == "" {
		return
	}
	sm := NewStateManager()
	sm.Load()
	st := sm.Get()
	if st.ServerURL != urlOverride {
		sm.SetServerURL(urlOverride)
	}
}

func parseCloseArgs(args []string) map[string]any {
	if len(args) > 0 && args[0] == "--all" {
		return map[string]any{"all": true}
	}
	if len(args) > 0 {
		return map[string]any{"name": args[0]}
	}
	return map[string]any{}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `share-cli - share local resources to the internet

Usage:
  share-cli [--url <server-url>] <command> [args]

Commands:
  dir <path>           Share a directory
  port <localPort>     Share a local HTTP port
  ls                   List all shares
  close <share-name>   Close a share
  close --all          Close all shares
  status               Show daemon status
  stop                 Stop the daemon
  login [server-url]   Set server URL (uses default if omitted)
  version              Print version

Global Flags:
  --url <server-url>   Override server URL for this session
`)
}
