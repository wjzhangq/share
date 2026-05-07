package proto

const (
	HeaderClientToken = "X-Sharexxx-Client-Token"
	HeaderReqID       = "X-Sharexxx-Req-Id"
	HeaderOp          = "X-Sharexxx-Op"
)

const (
	OpPullReqBody = "pull-req-body"
	OpRespInline  = "resp-inline"
	OpRespHead    = "resp-head"
	OpRespStream  = "resp-stream"
	OpDirListResp = "dir-list-resp"
)
