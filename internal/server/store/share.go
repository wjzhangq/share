package store

import (
	"database/sql"
	"fmt"
	"time"
)

type Share struct {
	ID           int64
	ClientUID    string
	ShareName    string
	Kind         string
	LocalPath    string
	LocalPort    int
	ProcessPID   int
	ProcessExe   string
	ProcessCwd   string
	ProcessAlive bool
	Status       string
	OnlineAt     int64
	OfflineAt    int64
	ClosedAt     int64
}

func (s *Store) CreateShare(clientUID, shareName, kind, localPath string, localPort, processPID int, processExe, processCwd string) (*Share, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().Unix()
	// reuse a closed row if one exists for this name, otherwise insert
	res, err := s.db.Exec(`UPDATE shares SET kind=?, local_path=?, local_port=?, process_pid=?, process_exe=?, process_cwd=?,
		process_alive=1, status='active', online_at=?, offline_at=0, closed_at=0
		WHERE client_uid=? AND share_name=? AND status='closed'`,
		kind, localPath, localPort, processPID, processExe, processCwd, now, clientUID, shareName)
	if err != nil {
		return nil, err
	}
	if n, _ := res.RowsAffected(); n > 0 {
		var id int64
		s.db.QueryRow("SELECT id FROM shares WHERE client_uid=? AND share_name=?", clientUID, shareName).Scan(&id)
		return &Share{
			ID: id, ClientUID: clientUID, ShareName: shareName, Kind: kind,
			LocalPath: localPath, LocalPort: localPort, ProcessPID: processPID, ProcessExe: processExe, ProcessCwd: processCwd,
			ProcessAlive: true, Status: "active", OnlineAt: now,
		}, nil
	}
	res, err = s.db.Exec(`INSERT INTO shares (client_uid, share_name, kind, local_path, local_port, process_pid, process_exe, process_cwd, process_alive, status, online_at)
		VALUES (?,?,?,?,?,?,?,?,1,'active',?)`,
		clientUID, shareName, kind, localPath, localPort, processPID, processExe, processCwd, now)
	if err != nil {
		return nil, err
	}
	id, _ := res.LastInsertId()
	return &Share{
		ID:           id,
		ClientUID:    clientUID,
		ShareName:    shareName,
		Kind:         kind,
		LocalPath:    localPath,
		LocalPort:    localPort,
		ProcessPID:   processPID,
		ProcessExe:   processExe,
		ProcessCwd:   processCwd,
		ProcessAlive: true,
		Status:       "active",
		OnlineAt:     now,
	}, nil
}

func (s *Store) ReactivateShare(clientUID, shareName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().Unix()
	_, err := s.db.Exec("UPDATE shares SET status='active', online_at=?, process_alive=1 WHERE client_uid=? AND share_name=? AND status != 'closed'",
		now, clientUID, shareName)
	return err
}

func (s *Store) SetShareOffline(clientUID, shareName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().Unix()
	_, err := s.db.Exec("UPDATE shares SET status='offline', offline_at=?, process_alive=0 WHERE client_uid=? AND share_name=? AND status='active'",
		now, clientUID, shareName)
	return err
}

func (s *Store) CloseShare(clientUID, shareName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().Unix()
	_, err := s.db.Exec("UPDATE shares SET status='closed', closed_at=? WHERE client_uid=? AND share_name=?",
		now, clientUID, shareName)
	return err
}

func (s *Store) CloseAllShares(clientUID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().Unix()
	_, err := s.db.Exec("UPDATE shares SET status='closed', closed_at=? WHERE client_uid=? AND status != 'closed'",
		now, clientUID)
	return err
}

func (s *Store) SetSharesOfflineByClient(clientUID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().Unix()
	_, err := s.db.Exec("UPDATE shares SET status='offline', offline_at=? WHERE client_uid=? AND status='active'",
		now, clientUID)
	return err
}

func (s *Store) GetActiveShare(clientUID, shareName string) (*Share, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.getShare(clientUID, shareName, "active")
}

func (s *Store) GetShare(clientUID, shareName string) (*Share, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var sh Share
	var processAlive int
	err := s.db.QueryRow(`SELECT id, client_uid, share_name, kind, COALESCE(local_path,''), COALESCE(local_port,0),
		COALESCE(process_pid,0), COALESCE(process_exe,''), COALESCE(process_cwd,''), process_alive, status, COALESCE(online_at,0), COALESCE(offline_at,0), COALESCE(closed_at,0)
		FROM shares WHERE client_uid=? AND share_name=?`, clientUID, shareName).
		Scan(&sh.ID, &sh.ClientUID, &sh.ShareName, &sh.Kind, &sh.LocalPath, &sh.LocalPort,
			&sh.ProcessPID, &sh.ProcessExe, &sh.ProcessCwd, &processAlive, &sh.Status, &sh.OnlineAt, &sh.OfflineAt, &sh.ClosedAt)
	if err != nil {
		return nil, err
	}
	sh.ProcessAlive = processAlive == 1
	return &sh, nil
}

func (s *Store) getShare(clientUID, shareName, status string) (*Share, error) {
	var sh Share
	var processAlive int
	err := s.db.QueryRow(`SELECT id, client_uid, share_name, kind, COALESCE(local_path,''), COALESCE(local_port,0),
		COALESCE(process_pid,0), COALESCE(process_exe,''), COALESCE(process_cwd,''), process_alive, status, COALESCE(online_at,0), COALESCE(offline_at,0), COALESCE(closed_at,0)
		FROM shares WHERE client_uid=? AND share_name=? AND status=?`, clientUID, shareName, status).
		Scan(&sh.ID, &sh.ClientUID, &sh.ShareName, &sh.Kind, &sh.LocalPath, &sh.LocalPort,
			&sh.ProcessPID, &sh.ProcessExe, &sh.ProcessCwd, &processAlive, &sh.Status, &sh.OnlineAt, &sh.OfflineAt, &sh.ClosedAt)
	if err != nil {
		return nil, err
	}
	sh.ProcessAlive = processAlive == 1
	return &sh, nil
}

func (s *Store) ListSharesByClient(clientUID string) ([]Share, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows, err := s.db.Query(`SELECT id, client_uid, share_name, kind, COALESCE(local_path,''), COALESCE(local_port,0),
		COALESCE(process_pid,0), COALESCE(process_exe,''), COALESCE(process_cwd,''), process_alive, status, COALESCE(online_at,0), COALESCE(offline_at,0), COALESCE(closed_at,0)
		FROM shares WHERE client_uid=? ORDER BY id`, clientUID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var shares []Share
	for rows.Next() {
		var sh Share
		var processAlive int
		if err := rows.Scan(&sh.ID, &sh.ClientUID, &sh.ShareName, &sh.Kind, &sh.LocalPath, &sh.LocalPort,
			&sh.ProcessPID, &sh.ProcessExe, &sh.ProcessCwd, &processAlive, &sh.Status, &sh.OnlineAt, &sh.OfflineAt, &sh.ClosedAt); err != nil {
			return nil, err
		}
		sh.ProcessAlive = processAlive == 1
		shares = append(shares, sh)
	}
	return shares, rows.Err()
}

func (s *Store) ListActiveSharesByClient(clientUID string) ([]Share, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows, err := s.db.Query(`SELECT id, client_uid, share_name, kind, COALESCE(local_path,''), COALESCE(local_port,0),
		COALESCE(process_pid,0), COALESCE(process_exe,''), COALESCE(process_cwd,''), process_alive, status, COALESCE(online_at,0), COALESCE(offline_at,0), COALESCE(closed_at,0)
		FROM shares WHERE client_uid=? AND status='active' ORDER BY id`, clientUID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var shares []Share
	for rows.Next() {
		var sh Share
		var processAlive int
		if err := rows.Scan(&sh.ID, &sh.ClientUID, &sh.ShareName, &sh.Kind, &sh.LocalPath, &sh.LocalPort,
			&sh.ProcessPID, &sh.ProcessExe, &sh.ProcessCwd, &processAlive, &sh.Status, &sh.OnlineAt, &sh.OfflineAt, &sh.ClosedAt); err != nil {
			return nil, err
		}
		sh.ProcessAlive = processAlive == 1
		shares = append(shares, sh)
	}
	return shares, rows.Err()
}

func (s *Store) ResolveShareName(clientUID, sourceKey, hintName string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var existing string
	err := s.db.QueryRow("SELECT share_name FROM share_name_map WHERE client_uid=? AND source_key=?", clientUID, sourceKey).Scan(&existing)
	if err == nil {
		return existing, nil
	}
	if err != sql.ErrNoRows {
		return "", err
	}

	name := hintName
	for i := 2; ; i++ {
		var count int
		s.db.QueryRow("SELECT COUNT(*) FROM shares WHERE client_uid=? AND share_name=? AND status != 'closed'", clientUID, name).Scan(&count)
		if count == 0 {
			break
		}
		name = fmt.Sprintf("%s-%d", hintName, i)
	}

	_, err = s.db.Exec("INSERT INTO share_name_map (client_uid, source_key, share_name) VALUES (?,?,?)", clientUID, sourceKey, name)
	if err != nil {
		return "", err
	}
	return name, nil
}
