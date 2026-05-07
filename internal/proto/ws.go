package proto

type WSMessage struct {
	Type string `json:"type"`
}

type Hello struct {
	Type     string `json:"type"`
	UniqueID string `json:"unique_id"`
	Hostname string `json:"hostname"`
	OS       string `json:"os"`
	Arch     string `json:"arch"`
	Version  string `json:"version"`
}

type Welcome struct {
	Type        string `json:"type"`
	ShortID     int64  `json:"short_id"`
	ClientLabel string `json:"client_label"`
	ServerTime  int64  `json:"server_time"`
}

type ShareCreate struct {
	Type       string `json:"type"`
	Kind       string `json:"kind"`
	HintName   string `json:"hint_name"`
	SourceKey  string `json:"source_key"`
	LocalPath  string `json:"local_path,omitempty"`
	LocalPort  int    `json:"local_port,omitempty"`
	ProcessPID int    `json:"process_pid,omitempty"`
	ProcessExe string `json:"process_exe,omitempty"`
}

type ShareCreated struct {
	Type      string `json:"type"`
	ShareName string `json:"share_name"`
	FullHost  string `json:"full_host"`
	ShareID   int64  `json:"share_id"`
}

type ShareError struct {
	Type       string `json:"type"`
	RequestRef string `json:"request_ref,omitempty"`
	Code       string `json:"code"`
	Message    string `json:"message"`
}

type ShareList struct {
	Type string `json:"type"`
}

type ShareListItem struct {
	ShareName string `json:"share_name"`
	FullHost  string `json:"full_host"`
	Kind      string `json:"kind"`
	Status    string `json:"status"`
}

type ShareListResult struct {
	Type   string          `json:"type"`
	Shares []ShareListItem `json:"shares"`
}

type ShareClose struct {
	Type      string `json:"type"`
	ShareName string `json:"share_name"`
}

type ShareClosed struct {
	Type      string `json:"type"`
	ShareName string `json:"share_name"`
	Reason    string `json:"reason"`
}

type ShareProcessDown struct {
	Type      string `json:"type"`
	ShareName string `json:"share_name"`
	ExitAt    int64  `json:"exit_at"`
}

type ShareProcessUp struct {
	Type      string `json:"type"`
	ShareName string `json:"share_name"`
	NewPID    int    `json:"new_pid"`
	LocalPort int    `json:"local_port"`
}

type ForwardReq struct {
	Type      string            `json:"type"`
	ReqID     string            `json:"req_id"`
	ShareName string            `json:"share_name"`
	Method    string            `json:"method"`
	Path      string            `json:"path"`
	Headers   map[string]string `json:"headers"`
	BodyMode  string            `json:"body_mode"`
}

type ForwardRespAck struct {
	Type  string `json:"type"`
	ReqID string `json:"req_id"`
}

type DirList struct {
	Type      string `json:"type"`
	ReqID     string `json:"req_id"`
	ShareName string `json:"share_name"`
	RelPath   string `json:"rel_path"`
}

type DirEntry struct {
	Name  string `json:"name"`
	IsDir bool   `json:"is_dir"`
	Size  int64  `json:"size"`
}

type DirListResp struct {
	Type      string     `json:"type"`
	ReqID     string     `json:"req_id"`
	Entries   []DirEntry `json:"entries"`
	HasIndex  bool       `json:"has_index"`
	Truncated bool       `json:"truncated,omitempty"`
}

type DirRead struct {
	Type      string `json:"type"`
	ReqID     string `json:"req_id"`
	ShareName string `json:"share_name"`
	RelPath   string `json:"rel_path"`
	Range     string `json:"range,omitempty"`
}

type Ping struct {
	Type string `json:"type"`
}

type Pong struct {
	Type string `json:"type"`
}
