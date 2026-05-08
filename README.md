# share

把本地目录或本地 HTTP 端口暴露到公网，通过 `c<N>-<共享名>.share.example.com` 子域名访问。

## 架构

```
公网用户 → Caddy(TLS) → Server(HTTP+WS+SQLite) ←WS→ Client(常驻进程) → 本地目录/端口
```

- 控制信令走 WebSocket，数据走 HTTP（可流式）
- Server 通过 Host 头路由到具体 share
- Client 既是 CLI 也是常驻进程，通过 IPC 通信

## 快速开始

### Server

```bash
# 在运行目录创建 config.yaml
cat > config.yaml <<EOF
listen: ":8080"
domain: "share.example.com"
db:
  path: "/var/lib/share/data.db"
admin:
  user: "admin"
  password: "your-strong-password"
download:
  dir: "/var/lib/share/downloads"
EOF

# 启动（默认读取当前目录 config.yaml）
./share-server

# 或指定配置文件路径
./share-server /etc/share/config.yaml
```

前置 Caddy 负责 TLS 终结和 `*.share.example.com` 通配证书签发。

### Client

```bash
# 首次设置 server 地址
share-cli login wss://share.example.com/ws

# 共享目录
share-cli dir /path/to/project

# 共享本地端口
share-cli port 3000

# 查看所有共享
share-cli ls

# 关闭共享
share-cli close myproject
share-cli close --all

# 查看状态
share-cli status
```

## 功能

- **目录共享**: 将本地目录暴露为 web 文件浏览器，支持 index.html 自动展示
- **端口共享**: 将本地 HTTP 服务暴露到公网，支持 GET/POST 及流式传输
- **进程监控**: 端口共享自动检测进程退出/重启，状态实时同步
- **断线重连**: Client 指数退避自动重连，share 自动恢复
- **管理后台**: `admin.<domain>` 查看所有 client 和 share 状态
- **Client 下载**: `<domain>/download` 提供各平台 client 二进制文件下载
- **跨平台**: Windows / macOS / Linux 全平台支持

## 构建

```bash
# 构建 server
go build -ldflags "-X github.com/wjzhangq/share/internal/version.Version=$(git describe --tags --always)" -o share-server ./cmd/share-server/

# 构建 client
go build -ldflags "-X github.com/wjzhangq/share/internal/version.Version=$(git describe --tags --always)" -o share-cli ./cmd/share-cli/
```

### 交叉编译

```bash
GOOS=linux GOARCH=amd64 go build -o share-cli-linux-amd64 ./cmd/share-cli/
GOOS=darwin GOARCH=arm64 go build -o share-cli-darwin-arm64 ./cmd/share-cli/
GOOS=windows GOARCH=amd64 go build -o share-cli-windows-amd64.exe ./cmd/share-cli/
```

编译产物放入 server 配置的 `download.dir` 目录，用户即可通过 `https://share.example.com/download` 下载对应平台的 client。

## 项目结构

```
share/
├── cmd/
│   ├── share-cli/main.go        # client 入口
│   └── share-server/main.go     # server 入口
├── internal/
│   ├── proto/                  # 共享协议定义
│   ├── client/                 # client 实现
│   │   ├── ipc/               # 跨平台 IPC
│   │   ├── paths/             # 跨平台路径
│   │   ├── spawn/             # 跨平台进程启动
│   │   └── procmon/           # 进程监控
│   ├── server/                # server 实现
│   │   └── store/             # SQLite 存储层
│   └── version/               # 版本号注入
├── go.mod
└── dev.md                     # 详细设计文档
```

## 技术栈

- Go 1.22+
- SQLite (modernc.org/sqlite, 纯 Go)
- WebSocket (github.com/coder/websocket)
- 标准库 net/http

## License

MIT
