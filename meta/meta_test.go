package meta

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestAttachFromIncomingMetadata(t *testing.T) {
	base := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		MetaRequestPath, "/v1/profile",
		MetaUserId, "42",
		MetaToken, "token-1",
	))

	ctx := Attach(base)
	if got := String(ctx, MetaRequestPath); got != "/v1/profile" {
		t.Fatalf("String(path) = %q; want /v1/profile", got)
	}
	if got := UserId(ctx); got != 42 {
		t.Fatalf("UserId = %d; want 42", got)
	}
	if got := Token(ctx); got != "token-1" {
		t.Fatalf("Token = %q; want token-1", got)
	}
}

func TestSetReadAndDelete(t *testing.T) {
	ctx := context.Background()
	ctx = Set(ctx, "string", "hello")
	ctx = Set(ctx, "int64", int64(123))
	ctx = Set(ctx, "int", 456)
	ctx = Set(ctx, "float", 1.5)
	ctx = Set(ctx, "bool", true)
	ctx = Set(ctx, "nil", nil)

	if !Has(ctx, "string") {
		t.Fatal("Has(string) = false; want true")
	}
	raw, ok := Raw(ctx, "int64")
	if !ok || raw != int64(123) {
		t.Fatalf("Raw(int64) = (%v, %v); want (123, true)", raw, ok)
	}
	if got := String(ctx, "int"); got != "456" {
		t.Fatalf("String(int) = %q; want 456", got)
	}
	if got := Int64(ctx, "int"); got != 456 {
		t.Fatalf("Int64(int) = %d; want 456", got)
	}
	if got := Float64(ctx, "float"); got != 1.5 {
		t.Fatalf("Float64(float) = %v; want 1.5", got)
	}
	if got := Bool(ctx, "bool"); !got {
		t.Fatal("Bool(bool) = false; want true")
	}
	if !Has(ctx, "nil") {
		t.Fatal("Has(nil) = false; want true")
	}
	if got := String(ctx, "nil"); got != "" {
		t.Fatalf("String(nil) = %q; want empty", got)
	}

	ctx = Delete(ctx, "string")
	if Has(ctx, "string") {
		t.Fatal("Delete should remove string")
	}
}

func TestSetMutatesAttachedStore(t *testing.T) {
	ctx := Attach(context.Background())
	_ = Set(ctx, "k", "v")
	if got := String(ctx, "k"); got != "v" {
		t.Fatalf("String(k) = %q; want v", got)
	}
}

func TestDeleteIncomingMetadata(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("k", "v"))
	ctx = Delete(ctx, "k")
	if Has(ctx, "k") {
		t.Fatal("Delete should hide incoming metadata key")
	}
}

func TestConversions(t *testing.T) {
	ctx := context.Background()
	ctx = Set(ctx, "s_i64", "123")
	ctx = Set(ctx, "s_float", "1.25")
	ctx = Set(ctx, "s_true", "true")
	ctx = Set(ctx, "s_bad", "bad")
	ctx = Set(ctx, "num_bool", int64(-1))
	ctx = Set(ctx, "bool_bad", "yes")
	ctx = Set(ctx, "i8", int8(7))
	ctx = Set(ctx, "u64", uint64(8))
	ctx = Set(ctx, "f32", float32(2.5))
	ctx = Set(ctx, "zero_f32", float32(0))

	if got := Int64(ctx, "s_i64"); got != 123 {
		t.Fatalf("Int64(s_i64) = %d; want 123", got)
	}
	if got := Float64(ctx, "s_float"); got != 1.25 {
		t.Fatalf("Float64(s_float) = %v; want 1.25", got)
	}
	if got := Bool(ctx, "s_true"); !got {
		t.Fatal("Bool(s_true) = false; want true")
	}
	if got := Bool(ctx, "num_bool"); !got {
		t.Fatal("Bool(num_bool) = false; want true")
	}
	if got := Int64(ctx, "s_bad"); got != 0 {
		t.Fatalf("Int64(s_bad) = %d; want 0", got)
	}
	if got := Bool(ctx, "bool_bad"); got {
		t.Fatal("Bool(bool_bad) = true; want false")
	}
	if got := Int64(ctx, "i8"); got != 7 {
		t.Fatalf("Int64(i8) = %d; want 7", got)
	}
	if got := Float64(ctx, "u64"); got != 8 {
		t.Fatalf("Float64(u64) = %v; want 8", got)
	}
	if got := Float64(ctx, "f32"); got != 2.5 {
		t.Fatalf("Float64(f32) = %v; want 2.5", got)
	}
	if got := Bool(ctx, "zero_f32"); got {
		t.Fatal("Bool(zero_f32) = true; want false")
	}
}

func TestRequestMetaAndShortcuts(t *testing.T) {
	ctx := context.Background()
	ctx = Set(ctx, MetaUserAgent, "ua")
	ctx = Set(ctx, MetaRequestMethod, http.MethodPost)
	ctx = Set(ctx, MetaRequestPath, "/v1/order")
	ctx = Set(ctx, MetaRequestUri, "/v1/order?id=1")
	ctx = Set(ctx, MetaRequestHost, "example.com")
	ctx = Set(ctx, MetaToken, "token-2")
	ctx = Set(ctx, MetaUserId, "99")
	ctx = Set(ctx, MetaUserType, "2")
	ctx = Set(ctx, MetaUserIp, "127.0.0.1")
	ctx = Set(ctx, MetaUserCountry, "SG")
	ctx = Set(ctx, MetaDeviceId, "device-1")
	ctx = Set(ctx, MetaLanguage, "en")
	ctx = Set(ctx, MetaVersion, "1.0.0")
	ctx = Set(ctx, MetaPlatform, "IOS")

	rm := Request(ctx)
	if rm.UserAgent != "ua" || rm.Method != http.MethodPost || rm.Path != "/v1/order" ||
		rm.Uri != "/v1/order?id=1" || rm.Host != "example.com" || rm.Token != "token-2" ||
		rm.UserId != 99 || rm.UserType != 2 ||
		rm.UserIp != "127.0.0.1" || rm.UserCountry != "SG" || rm.DeviceId != "device-1" ||
		rm.Language != "en" || rm.Version != "1.0.0" || rm.Platform != "IOS" {
		t.Fatalf("Request = %+v; want populated snapshot", rm)
	}
	if got := UserId(ctx); got != 99 {
		t.Fatalf("UserId = %d; want 99", got)
	}
	if got := Token(ctx); got != "token-2" {
		t.Fatalf("Token = %q; want token-2", got)
	}
}

func TestOutgoingContextMergesMetadata(t *testing.T) {
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(
		"keep", "yes",
		MetaUserId, "old",
	))
	ctx = Set(ctx, MetaUserId, int64(42))
	ctx = Set(ctx, "skip", nil)

	out := OutgoingContext(ctx)
	md, ok := metadata.FromOutgoingContext(out)
	if !ok {
		t.Fatal("expected outgoing metadata")
	}
	if got := md.Get("keep"); len(got) != 1 || got[0] != "yes" {
		t.Fatalf("keep = %v; want [yes]", got)
	}
	if got := md.Get(MetaUserId); len(got) != 1 || got[0] != "42" {
		t.Fatalf("user_id = %v; want [42]", got)
	}
	if got := md.Get("skip"); len(got) != 0 {
		t.Fatalf("skip = %v; want absent", got)
	}
}

func TestOutgoingContextWithoutExistingMetadata(t *testing.T) {
	ctx := Set(context.Background(), "k", "v")
	out := OutgoingContext(ctx)
	md, ok := metadata.FromOutgoingContext(out)
	if !ok {
		t.Fatal("expected outgoing metadata")
	}
	if got := md.Get("k"); len(got) != 1 || got[0] != "v" {
		t.Fatalf("k = %v; want [v]", got)
	}
}

func TestNormalizeClientIP(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"1.2.3.4", "1.2.3.4"},
		{"1.2.3.4:8080", "1.2.3.4"},
		{"2001:db8::1", "2001:db8::1"},
		{"[2001:db8::1]", "2001:db8::1"},
		{"[2001:db8::1]:8080", "2001:db8::1"},
		// CloudFront-Viewer-Address 的 IPv6 形态：不带括号直接拼端口
		{"2001:0db8:85a3:8d3:1319:8a2e:370:7348:46532", "2001:db8:85a3:8d3:1319:8a2e:370:7348"},
		{"2001:db8::1:46532", "2001:db8::1"},
		{"fe80::1%eth0", "fe80::1"},
		{" 1.2.3.4 ", "1.2.3.4"},
		{"", ""},
		{"not-an-ip", ""},
		{"[garbage]", ""},
		{"@", ""}, // unix socket 握手地址
	}
	for _, c := range cases {
		if got := normalizeClientIP(c.in); got != c.want {
			t.Errorf("normalizeClientIP(%q) = %q; want %q", c.in, got, c.want)
		}
	}
}

func TestAnnotatorUserIpFallsBackOnInvalidHeader(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://example.com/v1/a", nil)
	req.Header.Set("X-Real-Ip", "not-an-ip")
	req.RemoteAddr = "[2001:db8::1]:52642"

	md := Annotator(context.Background(), req)
	if got := md.Get(MetaUserIp); len(got) != 1 || got[0] != "2001:db8::1" {
		t.Fatalf("user_ip = %v; want [2001:db8::1]", got)
	}
}

func TestAnnotatorKeepsNonBearerAuthorizationBehavior(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://example.com/v1/a", nil)
	req.Header.Set("Authorization", "Basic abc")
	req.Header.Set(MetaToken, "fallback")

	md := Annotator(context.Background(), req)
	if got := md.Get(MetaToken); len(got) != 1 || got[0] != "Basic abc" {
		t.Fatalf("token = %v; want [Basic abc]", got)
	}
}
