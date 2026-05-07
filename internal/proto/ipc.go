package proto

type IPCRequest struct {
	Cmd  string         `json:"cmd"`
	Args map[string]any `json:"args,omitempty"`
}

type IPCResponse struct {
	OK   bool   `json:"ok"`
	Data any    `json:"data,omitempty"`
	Err  string `json:"error,omitempty"`
}
