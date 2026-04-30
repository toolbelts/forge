package errkit

import (
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// defaultGrpcCode 系统码到 gRPC codes.Code 的内置映射。业务码不在此表的统一回 Internal。
var defaultGrpcCode = map[Code]codes.Code{
	CodeOK:                 codes.OK,
	CodeUnknown:            codes.Unknown,
	CodeCanceled:           codes.Canceled,
	CodeDeadlineExceeded:   codes.DeadlineExceeded,
	CodeInvalidArgument:    codes.InvalidArgument,
	CodeNotFound:           codes.NotFound,
	CodeAlreadyExists:      codes.AlreadyExists,
	CodePermissionDenied:   codes.PermissionDenied,
	CodeUnauthenticated:    codes.Unauthenticated,
	CodeResourceExhausted:  codes.ResourceExhausted,
	CodeFailedPrecondition: codes.FailedPrecondition,
	CodeAborted:            codes.Aborted,
	CodeUnavailable:        codes.Unavailable,
	CodeInternal:           codes.Internal,
	CodeDataLoss:           codes.DataLoss,
	CodePanic:              codes.Internal,
}

var (
	grpcCodeMu       sync.RWMutex
	grpcCodeOverride = make(map[Code]codes.Code)
)

// RegisterGrpcCodeMapping 注册业务码到 gRPC code 的映射,可覆盖内置表。
//
// 应用启动时为业务码注入映射:
//
//	for code, gc := range bizGrpcMapping {
//	    errkit.RegisterGrpcCodeMapping(errkit.Code(code), gc)
//	}
func RegisterGrpcCodeMapping(c Code, gc codes.Code) {
	grpcCodeMu.Lock()
	defer grpcCodeMu.Unlock()
	grpcCodeOverride[c] = gc
}

// ToGrpcCode 业务码 → gRPC codes.Code:override 优先,其次内置表,无匹配回 Internal。
func ToGrpcCode(c Code) codes.Code {
	grpcCodeMu.RLock()
	if gc, ok := grpcCodeOverride[c]; ok {
		grpcCodeMu.RUnlock()
		return gc
	}
	grpcCodeMu.RUnlock()
	if gc, ok := defaultGrpcCode[c]; ok {
		return gc
	}
	return codes.Internal
}

// FromGrpcCode gRPC codes.Code → 一个合适的 Code 的反向映射。
//
// 仅做内置系统码反查;业务码无法反推。中间件归一化外部 status.Errorf 的错误时调用,
// 尽量保留语义,而不是一律退化为 Internal。
func FromGrpcCode(c codes.Code) Code {
	switch c {
	case codes.OK:
		return CodeOK
	case codes.Canceled:
		return CodeCanceled
	case codes.DeadlineExceeded:
		return CodeDeadlineExceeded
	case codes.InvalidArgument, codes.OutOfRange:
		return CodeInvalidArgument
	case codes.FailedPrecondition:
		return CodeFailedPrecondition
	case codes.NotFound:
		return CodeNotFound
	case codes.AlreadyExists:
		return CodeAlreadyExists
	case codes.ResourceExhausted:
		return CodeResourceExhausted
	case codes.Aborted:
		return CodeAborted
	case codes.Unauthenticated:
		return CodeUnauthenticated
	case codes.PermissionDenied:
		return CodePermissionDenied
	case codes.Unavailable:
		return CodeUnavailable
	case codes.Unimplemented:
		return CodeNotFound
	case codes.DataLoss:
		return CodeDataLoss
	case codes.Internal:
		return CodeInternal
	default:
		return CodeUnknown
	}
}

// Encoder 应用层把 errkit.Error 编码进 gRPC Status.details 的钩子。
//
// forge 自身不依赖任何 proto;encoder 由应用启动时通过 RegisterEncoder 注入,
// 把 errkit.Error 转为一个或多个可序列化的 proto.Message。
type Encoder func(Error) []proto.Message

var encoder atomic.Pointer[Encoder]

// RegisterEncoder 注册或替换 Encoder。nil 表示清空。
func RegisterEncoder(enc Encoder) {
	if enc == nil {
		encoder.Store(nil)
		return
	}
	encoder.Store(&enc)
}

// Encode 调用已注册的 Encoder,无注册时返回 nil。中间件用本函数构造 Status.details。
func Encode(e Error) []proto.Message {
	if e == nil {
		return nil
	}
	if enc := encoder.Load(); enc != nil {
		return (*enc)(e)
	}
	return nil
}
