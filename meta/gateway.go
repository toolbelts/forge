package meta

import (
	"cmp"
	"context"
	"net"
	"net/http"
	"net/netip"
	"strings"

	"google.golang.org/grpc/metadata"
)

// Meta key 常量。Annotator 阶段只填充 HTTP 请求头里能直接拿到的部分；
// MetaUserType、MetaUserId 等"身份相关" key 由后续业务拦截器
// （如认证、token 解析）在 gRPC server 侧补齐。
const (
	MetaUserAgent     = "x-meta-user-agent"
	MetaRequestMethod = "x-meta-request-method"
	MetaRequestPath   = "x-meta-request-path"
	MetaRequestUri    = "x-meta-request-uri"
	MetaRequestHost   = "x-meta-request-host"
	MetaToken         = "x-meta-token"
	MetaUserId        = "x-meta-user-id"
	MetaUserType      = "x-meta-user-type"
	MetaUserIp        = "x-meta-user-ip"
	MetaUserCountry   = "x-meta-user-country"
	MetaDeviceId      = "x-meta-device-id"
	MetaLanguage      = "x-meta-language"
	MetaVersion       = "x-meta-version"
	MetaPlatform      = "x-meta-platform"
)

// Annotator 是 gRPC-Gateway 中间件，用于从 HTTP 请求提取元数据并转化为 gRPC metadata。
func Annotator(ctx context.Context, req *http.Request) metadata.MD {
	md := make(metadata.MD, 15)

	// 辅助函数：仅当值不为空时才写入，避免分配多余空切片
	set := func(key, val string) {
		if val != "" {
			md[key] = []string{val}
		}
	}

	// 从请求头或上下文中提取标准元数据
	set(MetaUserAgent, req.Header.Get("User-Agent"))
	set(MetaRequestMethod, req.Method)
	set(MetaRequestPath, req.URL.Path)
	set(MetaRequestUri, req.RequestURI)
	set(MetaRequestHost, req.Host)

	set(MetaLanguage, cmp.Or(req.Header.Get(MetaLanguage), req.Header.Get("Accept-Language")))
	set(MetaVersion, req.Header.Get(MetaVersion))
	set(MetaPlatform, cmp.Or(req.Header.Get(MetaPlatform), "web"))
	set(MetaDeviceId, req.Header.Get(MetaDeviceId))

	// 获取用户登录令牌（支持 Authorization: Bearer <token> 或自定义 Header）
	token := cmp.Or(strings.TrimPrefix(req.Header.Get("Authorization"), "Bearer "), req.Header.Get(MetaToken))
	set(MetaToken, token)

	// 获取真实用户 IP：优先读取 CDN/Nginx 的代理头，最后回退到握手地址。
	// 逐个校验合法性，某个头存在但值非法时继续回退，避免下游拿到无法解析的值。
	var ip string
	for _, candidate := range []string{
		req.Header.Get("Cf-Connecting-Ip"),
		req.Header.Get("CloudFront-Viewer-Address"),
		req.Header.Get("X-Real-Ip"),
		req.RemoteAddr,
	} {
		if ip = normalizeClientIP(candidate); ip != "" {
			break
		}
	}
	set(MetaUserIp, ip)

	// 获取用户国家区号并转换为大写
	country := cmp.Or(req.Header.Get("Cf-Ipcountry"), req.Header.Get("CloudFront-Viewer-Country"))
	set(MetaUserCountry, strings.ToUpper(country))

	return md
}

// normalizeClientIP 把代理头或握手地址里的原始值规整为纯 IP 字符串，无法解析时返回空。
// 兼容以下形态：
//   - "1.2.3.4" / "1.2.3.4:port"
//   - "2001:db8::1" / "[2001:db8::1]" / "[2001:db8::1]:port"
//   - CloudFront-Viewer-Address 的 IPv6 形态 "2001:db8::1:port"（不带括号直接拼端口）
//   - 带 zone 的链路本地地址 "fe80::1%eth0"（zone 会被剥离）
func normalizeClientIP(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if addr, err := netip.ParseAddr(s); err == nil {
		return addr.WithZone("").String()
	}
	if host, _, err := net.SplitHostPort(s); err == nil {
		if addr, err := netip.ParseAddr(host); err == nil {
			return addr.WithZone("").String()
		}
		return ""
	}
	// "[::1]"：有括号但没端口，SplitHostPort 会报错，手动去括号
	if inner := strings.TrimSuffix(strings.TrimPrefix(s, "["), "]"); inner != s {
		if addr, err := netip.ParseAddr(inner); err == nil {
			return addr.WithZone("").String()
		}
		return ""
	}
	// "2001:db8::1:46532"：CloudFront 的 IPv6+端口，按最后一个冒号切开重试
	if i := strings.LastIndexByte(s, ':'); i > 0 {
		if addr, err := netip.ParseAddr(s[:i]); err == nil {
			return addr.WithZone("").String()
		}
	}
	return ""
}
