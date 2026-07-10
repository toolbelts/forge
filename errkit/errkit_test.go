package errkit_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/toolbelts/forge/errkit"
)

// 编译期断言:simpleError 必须满足 errkit.Error 接口。
var _ errkit.Error = errkit.New(errkit.CodeInternal, "")

func TestNewAndCode(t *testing.T) {
	e := errkit.New(errkit.CodeNotFound, "user not found")
	if e.Code() != errkit.CodeNotFound {
		t.Fatalf("Code = %d, want %d", e.Code(), errkit.CodeNotFound)
	}
	if e.Message() != "user not found" {
		t.Fatalf("Message = %q", e.Message())
	}
	if got := e.Error(); got == "" {
		t.Fatal("Error() is empty")
	}
}

func TestNewfFormatting(t *testing.T) {
	e := errkit.Newf(errkit.CodeInvalidArgument, "field %s missing", "email")
	if e.Message() != "field email missing" {
		t.Fatalf("Message = %q", e.Message())
	}
}

func TestWithMetadata(t *testing.T) {
	e := errkit.New(errkit.CodeInvalidArgument, "bad").
		WithMetadata("field", "email").
		WithMetadataMap(map[string]string{"retry_after": "30s"})

	md := e.Metadata()
	if md["field"] != "email" || md["retry_after"] != "30s" {
		t.Fatalf("metadata = %v", md)
	}

	// 副本语义:外部修改不污染原对象
	md["field"] = "tampered"
	if e.Metadata()["field"] != "email" {
		t.Fatal("metadata not cloned")
	}
}

func TestWithCauseAndUnwrap(t *testing.T) {
	cause := errors.New("io failure")
	e := errkit.Wrap(cause, errkit.CodeInternal, "save failed")

	if !errors.Is(e, cause) {
		t.Fatal("errors.Is should find cause")
	}
	if e.Unwrap() != cause {
		t.Fatal("Unwrap should return cause")
	}
}

func TestIsCode(t *testing.T) {
	e := errkit.New(errkit.CodeUnauthenticated, "no token")
	if !errkit.IsCode(e, errkit.CodeUnauthenticated) {
		t.Fatal("IsCode mismatch")
	}
	// errors.Is 链式判断:简单同 code 即匹配
	other := errkit.New(errkit.CodeUnauthenticated, "expired")
	if !errors.Is(e, other) {
		t.Fatal("errors.Is by Code failed")
	}
}

func TestFromError(t *testing.T) {
	want := errkit.New(errkit.CodeNotFound, "x")
	wrapped := errors.Join(errors.New("wrap"), want)
	got, ok := errkit.FromError(wrapped)
	if !ok {
		t.Fatal("FromError did not extract")
	}
	if got.Code() != errkit.CodeNotFound {
		t.Fatalf("got code %d", got.Code())
	}
}

func TestCodeOfFallback(t *testing.T) {
	if got := errkit.CodeOf(errors.New("plain")); got != errkit.CodeUnknown {
		t.Fatalf("plain error should be Unknown, got %d", got)
	}
	if got := errkit.CodeOf(nil); got != errkit.CodeUnknown {
		t.Fatalf("nil should be Unknown, got %d", got)
	}
}

func TestToGrpcCodeBuiltin(t *testing.T) {
	cases := map[errkit.Code]codes.Code{
		errkit.CodeOK:       codes.OK,
		errkit.CodeNotFound: codes.NotFound,
		errkit.CodePanic:    codes.Internal,
		errkit.Code(99999):  codes.Internal, // 未知码兜底
	}
	for c, want := range cases {
		if got := errkit.ToGrpcCode(c); got != want {
			t.Errorf("ToGrpcCode(%d) = %v, want %v", c, got, want)
		}
	}
}

func TestRegisterGrpcCodeMappingOverride(t *testing.T) {
	const bizCode errkit.Code = 20001
	errkit.RegisterGrpcCodeMapping(bizCode, codes.AlreadyExists)
	if got := errkit.ToGrpcCode(bizCode); got != codes.AlreadyExists {
		t.Fatalf("override not picked up: %v", got)
	}
}

func TestWithGrpcCodeOverride(t *testing.T) {
	e := errkit.New(errkit.CodeInternal, "x").WithGrpcCode(codes.FailedPrecondition)
	if got := e.GrpcCode(); got != codes.FailedPrecondition {
		t.Fatalf("WithGrpcCode not honored: %v", got)
	}
	// 未覆盖回到 ToGrpcCode 默认
	plain := errkit.New(errkit.CodeInternal, "x")
	if got := plain.GrpcCode(); got != codes.Internal {
		t.Fatalf("default GrpcCode wrong: %v", got)
	}
}

func TestFromGrpcCode(t *testing.T) {
	if got := errkit.FromGrpcCode(codes.Canceled); got != errkit.CodeCanceled {
		t.Errorf("Canceled mismatch: %v", got)
	}
	if got := errkit.FromGrpcCode(codes.DeadlineExceeded); got != errkit.CodeDeadlineExceeded {
		t.Errorf("DeadlineExceeded mismatch: %v", got)
	}
	if got := errkit.FromGrpcCode(codes.OK); got != errkit.CodeOK {
		t.Errorf("OK mismatch: %v", got)
	}
}

func TestToHttpStatus(t *testing.T) {
	cases := map[errkit.Code]int{
		errkit.CodeOK:                http.StatusOK,
		errkit.CodeNotFound:          http.StatusNotFound,
		errkit.CodeUnauthenticated:   http.StatusUnauthorized,
		errkit.CodePermissionDenied:  http.StatusForbidden,
		errkit.CodeResourceExhausted: http.StatusTooManyRequests,
		errkit.Code(99999):           http.StatusInternalServerError,
	}
	for c, want := range cases {
		if got := errkit.ToHttpStatus(c); got != want {
			t.Errorf("ToHttpStatus(%d) = %d, want %d", c, got, want)
		}
	}
}

func TestRegisterHttpStatusMappingOverride(t *testing.T) {
	const bizCode errkit.Code = 20002
	errkit.RegisterHttpStatusMapping(bizCode, http.StatusTeapot)
	if got := errkit.ToHttpStatus(bizCode); got != http.StatusTeapot {
		t.Fatalf("override not picked up: %d", got)
	}
}

func TestRegisterEncoder(t *testing.T) {
	defer errkit.RegisterEncoder(nil)

	pb := structpb.NewStringValue("payload")
	called := 0
	errkit.RegisterEncoder(func(e errkit.Error) []proto.Message {
		called++
		return []proto.Message{pb}
	})

	got := errkit.Encode(errkit.New(errkit.CodeInternal, "x"))
	if called != 1 || len(got) != 1 || got[0] != pb {
		t.Fatalf("encoder not invoked properly, called=%d got=%v", called, got)
	}

	// nil 解注册后,Encode 返回空
	errkit.RegisterEncoder(nil)
	if got := errkit.Encode(errkit.New(errkit.CodeInternal, "x")); len(got) != 0 {
		t.Fatalf("encoder should be cleared, got %v", got)
	}
}

func TestRegisterCodeNamer(t *testing.T) {
	defer errkit.RegisterCodeNamer(nil)

	if got := errkit.CodeNotFound.String(); got != "NOT_FOUND" {
		t.Errorf("builtin namer: %q", got)
	}
	if got := errkit.Code(99999).String(); got != "CODE_99999" {
		t.Errorf("fallback namer: %q", got)
	}

	errkit.RegisterCodeNamer(func(c errkit.Code) string {
		if c == 99999 {
			return "BIZ_FAKE"
		}
		return ""
	})
	if got := errkit.Code(99999).String(); got != "BIZ_FAKE" {
		t.Errorf("custom namer: %q", got)
	}
	// 内置码应用 namer 返回空时,落回内置表
	if got := errkit.CodeNotFound.String(); got != "NOT_FOUND" {
		t.Errorf("namer fallback to builtin: %q", got)
	}
}

func TestErrorStringNoLeak(t *testing.T) {
	// 没 cause 与有 cause 两种格式
	plain := errkit.New(errkit.CodeNotFound, "x")
	if got := plain.Error(); got == "" {
		t.Fatal("plain Error empty")
	}
	wrapped := errkit.Wrap(context.Canceled, errkit.CodeCanceled, "y")
	if got := wrapped.Error(); got == "" {
		t.Fatal("wrapped Error empty")
	}
}

func TestFromGrpcErrorDecodesDetails(t *testing.T) {
	// 模拟应用注册的 Decoder:从 structpb.Struct detail 恢复业务码 + metadata
	errkit.RegisterDecoder(func(details []proto.Message) (errkit.Error, bool) {
		for _, d := range details {
			s, ok := d.(*structpb.Struct)
			if !ok {
				continue
			}
			e := errkit.New(errkit.Code(s.Fields["code"].GetNumberValue()), s.Fields["message"].GetStringValue())
			if v := s.Fields["remaining"].GetStringValue(); v != "" {
				e = e.WithMetadata("remaining", v)
			}
			return e, true
		}
		return nil, false
	})
	t.Cleanup(func() { errkit.RegisterDecoder(nil) })

	detail, err := structpb.NewStruct(map[string]any{
		"code": 20010, "message": "user banned", "remaining": "3",
	})
	if err != nil {
		t.Fatal(err)
	}
	st, err := status.New(codes.PermissionDenied, "user banned").WithDetails(detail)
	if err != nil {
		t.Fatal(err)
	}

	// 跨进程场景:client 侧只有 *status.Error,errors.As 不可达,必须走 details 解码
	e, ok := errkit.FromGrpcError(st.Err())
	if !ok {
		t.Fatal("FromGrpcError = false, want decoded error")
	}
	if e.Code() != errkit.Code(20010) {
		t.Errorf("Code = %d, want 20010", e.Code())
	}
	if e.Message() != "user banned" {
		t.Errorf("Message = %q", e.Message())
	}
	if e.Metadata()["remaining"] != "3" {
		t.Errorf("Metadata[remaining] = %q, want 3", e.Metadata()["remaining"])
	}
}

func TestFromGrpcErrorPassthroughAndFallback(t *testing.T) {
	// errors.As 直通:链上已是 errkit.Error 时不需要 Decoder
	src := errkit.New(errkit.CodeNotFound, "missing")
	if e, ok := errkit.FromGrpcError(src); !ok || e.Code() != errkit.CodeNotFound {
		t.Fatalf("passthrough failed: ok=%v", ok)
	}

	// 未注册 Decoder:status error 无法恢复,返回 false 由调用方兜底
	st := status.New(codes.PermissionDenied, "denied")
	if _, ok := errkit.FromGrpcError(st.Err()); ok {
		t.Fatal("FromGrpcError = true without decoder, want false")
	}

	// nil error
	if _, ok := errkit.FromGrpcError(nil); ok {
		t.Fatal("FromGrpcError(nil) = true, want false")
	}
}
