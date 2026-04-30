package errkit

import (
	"errors"
	"fmt"
	"maps"

	"google.golang.org/grpc/codes"
)

// Error forge 与应用之间唯一稳定的错误契约。
//
// 任何想被 forge 中间件识别的错误必须实现本接口。pkg/* 内部用 errkit.New 构造,
// 应用业务码通过 errorpb.BizError 实现接口即可参与统一归一化与序列化。
type Error interface {
	error
	Code() Code
	Message() string
	Metadata() map[string]string
	Unwrap() error
}

// simpleError errkit 默认实现,链式 API 兼容 errorpb.BizError 的使用习惯。
//
// 与 *errorpb.BizError 的差异:
//   - 不持有 proto,序列化由应用注册的 Encoder 决定
//   - grpcCode 默认通过 ToGrpcCode 计算,仅在 WithGrpcCode 显式覆盖时持久化
type simpleError struct {
	code     Code
	message  string
	metadata map[string]string
	cause    error

	grpcCode    codes.Code
	grpcCodeSet bool
}

// New 构造新错误,gRPC code 由 ToGrpcCode(code) 默认推导。
func New(code Code, message string) *simpleError {
	return &simpleError{code: code, message: message}
}

// Newf 同 New,支持 fmt 风格格式化。
func Newf(code Code, format string, args ...any) *simpleError {
	return &simpleError{code: code, message: fmt.Sprintf(format, args...)}
}

// Wrap 用 cause 包装新 code/message,链上保留 cause 供 errors.Is/As 查找。
func Wrap(cause error, code Code, message string) *simpleError {
	return &simpleError{code: code, message: message, cause: cause}
}

// Wrapf 同 Wrap,支持 fmt 风格格式化。
func Wrapf(cause error, code Code, format string, args ...any) *simpleError {
	return &simpleError{code: code, message: fmt.Sprintf(format, args...), cause: cause}
}

// WithMetadata 添加单条元数据,链式调用。
func (e *simpleError) WithMetadata(key, value string) *simpleError {
	if e.metadata == nil {
		e.metadata = make(map[string]string)
	}
	e.metadata[key] = value
	return e
}

// WithMetadataMap 批量合并元数据,已存在的 key 被覆盖。
func (e *simpleError) WithMetadataMap(m map[string]string) *simpleError {
	if len(m) == 0 {
		return e
	}
	if e.metadata == nil {
		e.metadata = make(map[string]string, len(m))
	}
	maps.Copy(e.metadata, m)
	return e
}

// WithCause 挂载内层 error,被 Unwrap 返回,errors.Is/As 会沿链查找。
func (e *simpleError) WithCause(err error) *simpleError {
	e.cause = err
	return e
}

// WithGrpcCode 显式覆盖 gRPC code,用于业务码与默认映射不符的特殊场景。
func (e *simpleError) WithGrpcCode(c codes.Code) *simpleError {
	e.grpcCode = c
	e.grpcCodeSet = true
	return e
}

// Code 返回业务错误码。
func (e *simpleError) Code() Code { return e.code }

// Message 返回错误描述。
func (e *simpleError) Message() string { return e.message }

// Metadata 返回 metadata 副本,外部修改不影响原对象。
func (e *simpleError) Metadata() map[string]string {
	if len(e.metadata) == 0 {
		return nil
	}
	return maps.Clone(e.metadata)
}

// Error 实现 error 接口,链上有 cause 时附加显示。
func (e *simpleError) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("errkit %d %s: %s: %v", int32(e.code), e.code, e.message, e.cause)
	}
	return fmt.Sprintf("errkit %d %s: %s", int32(e.code), e.code, e.message)
}

// Unwrap 返回 WithCause 挂载的内层错误。
func (e *simpleError) Unwrap() error { return e.cause }

// Is 实现 errors.Is 协议:target 是 errkit.Error 且 code 相等即匹配。
func (e *simpleError) Is(target error) bool {
	var t Error
	if errors.As(target, &t) {
		return e.code == t.Code()
	}
	return false
}

// GrpcCode 返回最终 gRPC code:显式覆盖优先,否则按 ToGrpcCode 推导。
func (e *simpleError) GrpcCode() codes.Code {
	if e.grpcCodeSet {
		return e.grpcCode
	}
	return ToGrpcCode(e.code)
}

// FromError 抽取链上第一个 errkit.Error。
//
// 与 errors.As 的差异:本函数不要求调用方声明目标类型,直接返回 (Error, bool)。
// 应用业务码 BizError 实现 errkit.Error 后,本函数也能抽到它。
func FromError(err error) (Error, bool) {
	if err == nil {
		return nil, false
	}
	var e Error
	if errors.As(err, &e) {
		return e, true
	}
	return nil, false
}

// IsCode 判断 err 是否承载指定 code。
func IsCode(err error, code Code) bool {
	e, ok := FromError(err)
	return ok && e.Code() == code
}

// CodeOf 提取 err 的 Code,无法识别时返回 CodeUnknown。
func CodeOf(err error) Code {
	if e, ok := FromError(err); ok {
		return e.Code()
	}
	return CodeUnknown
}
