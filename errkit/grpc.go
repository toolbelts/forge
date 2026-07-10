package errkit

import (
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
// c 接受任意 ~int32 类型,内部规范化为 Code 入表。
//
// 应用启动时为业务码注入映射:
//
//	for code, gc := range bizGrpcMapping {
//	    errkit.RegisterGrpcCodeMapping(code, gc)
//	}
func RegisterGrpcCodeMapping[C Codeish](c C, gc codes.Code) {
	grpcCodeMu.Lock()
	defer grpcCodeMu.Unlock()
	grpcCodeOverride[Code(c)] = gc
}

// ToGrpcCode 业务码 → gRPC codes.Code:override 优先,其次内置表,无匹配回 Internal。
func ToGrpcCode[C Codeish](c C) codes.Code {
	key := Code(c)
	grpcCodeMu.RLock()
	if gc, ok := grpcCodeOverride[key]; ok {
		grpcCodeMu.RUnlock()
		return gc
	}
	grpcCodeMu.RUnlock()
	if gc, ok := defaultGrpcCode[key]; ok {
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

// Decoder 应用层把 gRPC Status.details 还原成 errkit.Error 的钩子,与 Encoder 对称。
//
// forge 自身不依赖任何 proto;decoder 由应用启动时通过 RegisterDecoder 注入,
// 在 details 里识别出应用错误 proto 时返回 (Error, true),否则返回 (nil, false)。
type Decoder func(details []proto.Message) (Error, bool)

var decoder atomic.Pointer[Decoder]

// RegisterDecoder 注册或替换 Decoder。nil 表示清空。
func RegisterDecoder(dec Decoder) {
	if dec == nil {
		decoder.Store(nil)
		return
	}
	decoder.Store(&dec)
}

// FromGrpcError 从 gRPC 客户端错误恢复 errkit.Error:
// 先 errors.As(同进程/已包装场景),再解 status details(跨进程场景,需应用 RegisterDecoder)。
// 都失败返回 (nil, false),由调用方决定兜底策略。
//
// grpc-gateway 与服务间调用的 client 侧统一走本函数,业务码与 metadata 得以跨进程保留;
// 未注册 Decoder 时行为与 FromError 一致,存量应用不受影响。
func FromGrpcError(err error) (Error, bool) {
	if e, ok := FromError(err); ok {
		return e, true
	}
	dec := decoder.Load()
	if dec == nil || err == nil {
		return nil, false
	}
	st, ok := status.FromError(err)
	if !ok {
		return nil, false
	}
	rawDetails := st.Details()
	details := make([]proto.Message, 0, len(rawDetails))
	for _, d := range rawDetails {
		// grpc-go Details() 已按全局 proto registry 反序列化,失败项是 error 值,跳过。
		if m, ok := d.(proto.Message); ok {
			details = append(details, m)
		}
	}
	if len(details) == 0 {
		return nil, false
	}
	return (*dec)(details)
}
