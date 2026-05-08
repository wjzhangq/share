package server

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/wjzhangq/share/internal/server/store"
)

type Config struct {
	Listen   string         `yaml:"listen"`
	Domain   string         `yaml:"domain"`
	DB       DBConfig       `yaml:"db"`
	Admin    AdminConfig    `yaml:"admin"`
	Forward  ForwardConfig  `yaml:"forward"`
	Download DownloadConfig `yaml:"download"`
	Log      LogConfig      `yaml:"log"`
}

type DownloadConfig struct {
	Dir string `yaml:"dir"`
}

type DBConfig struct {
	Path string `yaml:"path"`
}

type AdminConfig struct {
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

type ForwardConfig struct {
	InlineThresholdBytes int    `yaml:"inline_threshold_bytes"`
	RequestTimeout       string `yaml:"request_timeout"`
	UpstreamIdleTimeout  string `yaml:"upstream_idle_timeout"`
}

type LogConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	if cfg.Listen == "" {
		cfg.Listen = ":8080"
	}
	if cfg.Domain == "" {
		cfg.Domain = "share.example.com"
	}
	if cfg.DB.Path == "" {
		cfg.DB.Path = "data.db"
	}
	if cfg.Forward.InlineThresholdBytes == 0 {
		cfg.Forward.InlineThresholdBytes = 1048576
	}
	if cfg.Forward.RequestTimeout == "" {
		cfg.Forward.RequestTimeout = "300s"
	}
	return &cfg, nil
}

func (c *Config) RequestTimeoutDuration() time.Duration {
	d, _ := time.ParseDuration(c.Forward.RequestTimeout)
	if d == 0 {
		d = 300 * time.Second
	}
	return d
}

type Server struct {
	cfg    *Config
	store  *store.Store
	hub    *Hub
	fwd    *Forwarder
	logger *slog.Logger
}

func New(cfg *Config) (*Server, error) {
	st, err := store.New(cfg.DB.Path)
	if err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}

	level := slog.LevelInfo
	switch strings.ToLower(cfg.Log.Level) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}

	var handler slog.Handler
	if cfg.Log.Format == "json" {
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	} else {
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	}
	logger := slog.New(handler)

	srv := &Server{
		cfg:    cfg,
		store:  st,
		logger: logger,
	}
	srv.hub = NewHub(srv)
	srv.fwd = NewForwarder(srv)
	return srv, nil
}

func (s *Server) Run() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.rootHandler)

	s.logger.Info("server starting", "listen", s.cfg.Listen, "domain", s.cfg.Domain)
	return http.ListenAndServe(s.cfg.Listen, mux)
}

func (s *Server) Close() {
	s.store.Close()
}

var reShareLabel = regexp.MustCompile(`^c(\d+)-(.+)$`)

type hostKind int

const (
	hostKindUnknown hostKind = iota
	hostKindMain
	hostKindAdmin
	hostKindShare
)

type hostInfo struct {
	kind      hostKind
	shortID   int64
	shareName string
}

func (s *Server) parseHost(host string) hostInfo {
	if idx := strings.LastIndex(host, ":"); idx != -1 {
		host = host[:idx]
	}

	suffix := "." + s.cfg.Domain
	if host == s.cfg.Domain {
		return hostInfo{kind: hostKindMain}
	}
	if !strings.HasSuffix(host, suffix) {
		return hostInfo{kind: hostKindUnknown}
	}
	label := strings.TrimSuffix(host, suffix)

	if label == "admin" {
		return hostInfo{kind: hostKindAdmin}
	}

	m := reShareLabel.FindStringSubmatch(label)
	if m != nil {
		var id int64
		fmt.Sscanf(m[1], "%d", &id)
		return hostInfo{kind: hostKindShare, shortID: id, shareName: m[2]}
	}
	return hostInfo{kind: hostKindUnknown}
}

func (s *Server) rootHandler(w http.ResponseWriter, r *http.Request) {
	hi := s.parseHost(r.Host)

	switch hi.kind {
	case hostKindMain:
		if r.URL.Path == "/ws" {
			s.hub.HandleWS(w, r)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/download") {
			s.handleDownload(w, r)
			return
		}
		http.NotFound(w, r)

	case hostKindAdmin:
		s.handleAdmin(w, r)

	case hostKindShare:
		s.fwd.Handle(w, r, hi)

	default:
		http.NotFound(w, r)
	}
}
