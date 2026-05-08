package store

import (
	"database/sql"
	"fmt"
	"sync"

	_ "modernc.org/sqlite"
)

type Store struct {
	db *sql.DB
	mu sync.RWMutex
}

func New(dbPath string) (*Store, error) {
	db, err := sql.Open("sqlite", dbPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return s, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) migrate() error {
	if _, err := s.db.Exec(schema); err != nil {
		return err
	}
	// add process_cwd column if it doesn't exist (migration for existing DBs)
	_, _ = s.db.Exec(`ALTER TABLE shares ADD COLUMN process_cwd TEXT`)
	return nil
}

const schema = `
CREATE TABLE IF NOT EXISTS clients (
  unique_id   TEXT PRIMARY KEY,
  short_id    INTEGER UNIQUE NOT NULL,
  hostname    TEXT,
  os          TEXT,
  arch        TEXT,
  version     TEXT,
  online      INTEGER NOT NULL DEFAULT 0,
  online_at   INTEGER,
  offline_at  INTEGER
);

CREATE TABLE IF NOT EXISTS shares (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  client_uid     TEXT NOT NULL REFERENCES clients(unique_id),
  share_name     TEXT NOT NULL,
  kind           TEXT NOT NULL,
  local_path     TEXT,
  local_port     INTEGER,
  process_pid    INTEGER,
  process_exe    TEXT,
  process_cwd    TEXT,
  process_alive  INTEGER NOT NULL DEFAULT 1,
  status         TEXT NOT NULL,
  online_at      INTEGER,
  offline_at     INTEGER,
  closed_at      INTEGER,
  UNIQUE(client_uid, share_name)
);

CREATE INDEX IF NOT EXISTS idx_shares_client ON shares(client_uid, status);

CREATE TABLE IF NOT EXISTS share_name_map (
  client_uid     TEXT NOT NULL,
  source_key     TEXT NOT NULL,
  share_name     TEXT NOT NULL,
  PRIMARY KEY (client_uid, source_key)
);
`
