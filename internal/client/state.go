package client

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"

	"github.com/wjzhangq/share/internal/client/paths"
)

type ShareState struct {
	Kind       string `json:"kind"`
	ShareName  string `json:"share_name"`
	LocalPath  string `json:"local_path,omitempty"`
	LocalPort  int    `json:"local_port,omitempty"`
	SourceKey  string `json:"source_key"`
	ProcessExe string `json:"process_exe,omitempty"`
	ProcessCwd string `json:"process_cwd,omitempty"`
}

type State struct {
	UniqueID  string       `json:"unique_id"`
	ShortID   int64        `json:"short_id"`
	ServerURL string       `json:"server_url"`
	Shares    []ShareState `json:"shares"`
}

type StateManager struct {
	mu   sync.Mutex
	path string
	data State
}

func NewStateManager() *StateManager {
	dir := paths.ConfigDir()
	return &StateManager{
		path: filepath.Join(dir, "state.json"),
	}
}

func (sm *StateManager) Load() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	data, err := os.ReadFile(sm.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return json.Unmarshal(data, &sm.data)
}

func (sm *StateManager) Save() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.save()
}

func (sm *StateManager) save() error {
	dir := filepath.Dir(sm.path)
	os.MkdirAll(dir, 0700)

	data, err := json.MarshalIndent(sm.data, "", "  ")
	if err != nil {
		return err
	}

	tmp := sm.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, sm.path)
}

func (sm *StateManager) Get() State {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.data
}

func (sm *StateManager) SetUniqueID(id string) {
	sm.mu.Lock()
	sm.data.UniqueID = id
	sm.save()
	sm.mu.Unlock()
}

func (sm *StateManager) SetShortID(id int64) {
	sm.mu.Lock()
	sm.data.ShortID = id
	sm.save()
	sm.mu.Unlock()
}

func (sm *StateManager) SetServerURL(url string) {
	sm.mu.Lock()
	sm.data.ServerURL = url
	sm.save()
	sm.mu.Unlock()
}

func (sm *StateManager) AddShare(s ShareState) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	for i, existing := range sm.data.Shares {
		if existing.SourceKey == s.SourceKey {
			sm.data.Shares[i] = s
			sm.save()
			return
		}
	}
	sm.data.Shares = append(sm.data.Shares, s)
	sm.save()
}

func (sm *StateManager) RemoveShare(shareName string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	for i, s := range sm.data.Shares {
		if s.ShareName == shareName {
			sm.data.Shares = append(sm.data.Shares[:i], sm.data.Shares[i+1:]...)
			sm.save()
			return
		}
	}
}

func (sm *StateManager) ClearShares() {
	sm.mu.Lock()
	sm.data.Shares = nil
	sm.save()
	sm.mu.Unlock()
}
