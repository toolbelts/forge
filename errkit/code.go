// Package errkit 定义 forge 与应用之间唯一稳定的错误契约。
//
// 设计目标:
//   - 公共库内部不依赖任何 proto / 业务码,可被任何应用引入
//   - 应用层 errorpb.BizError 通过实现 errkit.Error 与本包互通
//   - 中间件统一用 errkit.Error 抽取 code/message/metadata,序列化细节由应用通过 Encoder 注入
package errkit

import (
	"fmt"
	"sync/atomic"
)

// Code 业务错误码,int32 与 errorpb proto 的 code 同型,跨边界零拷贝。
//
// 数值分区(全部等宽 5 位,便于日志/检索/可视化):
//   - 0:               OK,唯一保留
//   - 10001-10099:     forge 系统码(语义对齐 gRPC,但不强制数值相等)
//   - 10100-19999:     forge 框架细分扩展(预留)
//   - 20000+:          应用业务码,每业务模块预留 1000 步进
type Code int32

// 系统级码,数值 10001-10015。后续 10100-19999 留给框架扩展。
const (
	CodeOK                 Code = 0
	CodeUnknown            Code = 10001
	CodeCanceled           Code = 10002
	CodeDeadlineExceeded   Code = 10003
	CodeInvalidArgument    Code = 10004
	CodeNotFound           Code = 10005
	CodeAlreadyExists      Code = 10006
	CodePermissionDenied   Code = 10007
	CodeUnauthenticated    Code = 10008
	CodeResourceExhausted  Code = 10009
	CodeFailedPrecondition Code = 10010
	CodeAborted            Code = 10011
	CodeUnavailable        Code = 10012
	CodeInternal           Code = 10013
	CodeDataLoss           Code = 10014
	CodePanic              Code = 10015
)

// 内置码可读名,系统码全集。
var builtinCodeNames = map[Code]string{
	CodeOK:                 "OK",
	CodeUnknown:            "UNKNOWN",
	CodeCanceled:           "CANCELED",
	CodeDeadlineExceeded:   "DEADLINE_EXCEEDED",
	CodeInvalidArgument:    "INVALID_ARGUMENT",
	CodeNotFound:           "NOT_FOUND",
	CodeAlreadyExists:      "ALREADY_EXISTS",
	CodePermissionDenied:   "PERMISSION_DENIED",
	CodeUnauthenticated:    "UNAUTHENTICATED",
	CodeResourceExhausted:  "RESOURCE_EXHAUSTED",
	CodeFailedPrecondition: "FAILED_PRECONDITION",
	CodeAborted:            "ABORTED",
	CodeUnavailable:        "UNAVAILABLE",
	CodeInternal:           "INTERNAL",
	CodeDataLoss:           "DATA_LOSS",
	CodePanic:              "PANIC",
}

// codeNamer 应用层注入的码名解析器,用于把业务码(20000+)映射到可读名。
// 返回空串表示该码不属于本应用,落回内置表或 CODE_<n> 兜底。
var codeNamer atomic.Pointer[func(Code) string]

// RegisterCodeNamer 注册应用码名解析器。重复注册以最后一次为准。
//
// 典型用法(应用启动时):
//
//	errkit.RegisterCodeNamer(func(c errkit.Code) string {
//	    return errorpb.ErrorCode(c).String()
//	})
func RegisterCodeNamer(fn func(Code) string) {
	if fn == nil {
		codeNamer.Store(nil)
		return
	}
	codeNamer.Store(&fn)
}

// String 优先用应用注入的 namer,其次内置表,最后 CODE_<n> 兜底。
func (c Code) String() string {
	if fn := codeNamer.Load(); fn != nil {
		if name := (*fn)(c); name != "" {
			return name
		}
	}
	if name, ok := builtinCodeNames[c]; ok {
		return name
	}
	return fmt.Sprintf("CODE_%d", c)
}
