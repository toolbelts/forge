package errkit

import (
	"net/http"
	"sync"
)

// defaultHttpStatus 系统码到 HTTP 状态码的内置映射。
var defaultHttpStatus = map[Code]int{
	CodeOK:                 http.StatusOK,
	CodeUnknown:            http.StatusInternalServerError,
	CodeCanceled:           499, // nginx 风格 client closed request
	CodeDeadlineExceeded:   http.StatusGatewayTimeout,
	CodeInvalidArgument:    http.StatusBadRequest,
	CodeNotFound:           http.StatusNotFound,
	CodeAlreadyExists:      http.StatusConflict,
	CodePermissionDenied:   http.StatusForbidden,
	CodeUnauthenticated:    http.StatusUnauthorized,
	CodeResourceExhausted:  http.StatusTooManyRequests,
	CodeFailedPrecondition: http.StatusBadRequest,
	CodeAborted:            http.StatusConflict,
	CodeUnavailable:        http.StatusServiceUnavailable,
	CodeInternal:           http.StatusInternalServerError,
	CodeDataLoss:           http.StatusInternalServerError,
	CodePanic:              http.StatusInternalServerError,
}

var (
	httpStatusMu       sync.RWMutex
	httpStatusOverride = make(map[Code]int)
)

// RegisterHttpStatusMapping 注册业务码到 HTTP 状态码的映射,可覆盖内置表。
func RegisterHttpStatusMapping(c Code, status int) {
	httpStatusMu.Lock()
	defer httpStatusMu.Unlock()
	httpStatusOverride[c] = status
}

// ToHttpStatus 业务码 → HTTP 状态码:override 优先,其次内置表,无匹配回 500。
func ToHttpStatus(c Code) int {
	httpStatusMu.RLock()
	if s, ok := httpStatusOverride[c]; ok {
		httpStatusMu.RUnlock()
		return s
	}
	httpStatusMu.RUnlock()
	if s, ok := defaultHttpStatus[c]; ok {
		return s
	}
	return http.StatusInternalServerError
}
