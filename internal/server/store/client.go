package store

import (
	"database/sql"
	"time"
)

type Client struct {
	UniqueID  string
	ShortID   int64
	Hostname  string
	OS        string
	Arch      string
	Version   string
	Online    bool
	OnlineAt  int64
	OfflineAt int64
}

func (s *Store) nextShortID() (int64, error) {
	var maxID sql.NullInt64
	err := s.db.QueryRow("SELECT MAX(short_id) FROM clients").Scan(&maxID)
	if err != nil {
		return 0, err
	}
	if !maxID.Valid {
		return 1, nil
	}
	return maxID.Int64 + 1, nil
}

func (s *Store) RegisterClient(uniqueID, hostname, os, arch, version string) (*Client, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var c Client
	err := s.db.QueryRow("SELECT unique_id, short_id, hostname, os, arch, version, online, online_at, offline_at FROM clients WHERE unique_id = ?", uniqueID).
		Scan(&c.UniqueID, &c.ShortID, &c.Hostname, &c.OS, &c.Arch, &c.Version, &c.Online, &c.OnlineAt, &c.OfflineAt)
	if err == nil {
		now := time.Now().Unix()
		_, err = s.db.Exec("UPDATE clients SET hostname=?, os=?, arch=?, version=?, online=1, online_at=? WHERE unique_id=?",
			hostname, os, arch, version, now, uniqueID)
		if err != nil {
			return nil, err
		}
		c.Hostname = hostname
		c.OS = os
		c.Arch = arch
		c.Version = version
		c.Online = true
		c.OnlineAt = now
		return &c, nil
	}
	if err != sql.ErrNoRows {
		return nil, err
	}

	shortID, err := s.nextShortID()
	if err != nil {
		return nil, err
	}
	now := time.Now().Unix()
	_, err = s.db.Exec("INSERT INTO clients (unique_id, short_id, hostname, os, arch, version, online, online_at) VALUES (?,?,?,?,?,?,1,?)",
		uniqueID, shortID, hostname, os, arch, version, now)
	if err != nil {
		return nil, err
	}
	return &Client{
		UniqueID: uniqueID,
		ShortID:  shortID,
		Hostname: hostname,
		OS:       os,
		Arch:     arch,
		Version:  version,
		Online:   true,
		OnlineAt: now,
	}, nil
}

func (s *Store) SetClientOffline(uniqueID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().Unix()
	_, err := s.db.Exec("UPDATE clients SET online=0, offline_at=? WHERE unique_id=?", now, uniqueID)
	return err
}

func (s *Store) GetClientByShortID(shortID int64) (*Client, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var c Client
	var online int
	err := s.db.QueryRow("SELECT unique_id, short_id, hostname, os, arch, version, online, COALESCE(online_at,0), COALESCE(offline_at,0) FROM clients WHERE short_id=?", shortID).
		Scan(&c.UniqueID, &c.ShortID, &c.Hostname, &c.OS, &c.Arch, &c.Version, &online, &c.OnlineAt, &c.OfflineAt)
	if err != nil {
		return nil, err
	}
	c.Online = online == 1
	return &c, nil
}

func (s *Store) ListClients() ([]Client, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows, err := s.db.Query("SELECT unique_id, short_id, hostname, os, arch, version, online, COALESCE(online_at,0), COALESCE(offline_at,0) FROM clients ORDER BY short_id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var clients []Client
	for rows.Next() {
		var c Client
		var online int
		if err := rows.Scan(&c.UniqueID, &c.ShortID, &c.Hostname, &c.OS, &c.Arch, &c.Version, &online, &c.OnlineAt, &c.OfflineAt); err != nil {
			return nil, err
		}
		c.Online = online == 1
		clients = append(clients, c)
	}
	return clients, rows.Err()
}
