package proto

const (
	HeaderClientToken = "X-Share-Client-Token"
	HeaderReqID       = "X-Share-Req-Id"
	HeaderOp          = "X-Share-Op"
)

const (
	OpPullReqBody      = "pull-req-body"
	OpRespInline       = "resp-inline"
	OpRespHead         = "resp-head"
	OpRespStream       = "resp-stream"
	OpDirListResp      = "dir-list-resp"
	OpWSOpened         = "ws-opened"
	OpWSOpenError      = "ws-open-error"
	OpWSFrameToServer  = "ws-frame"
	OpWSCloseFromClient = "ws-close"
)
