package meta

import (
	"cmp"
	"context"
	"net"
	"net/http"
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

	// 获取真实用户 IP：优先读取 CDN/Nginx 的代理头，最后回退到握手地址
	ip := cmp.Or(
		req.Header.Get("Cf-Connecting-Ip"),
		req.Header.Get("CloudFront-Viewer-Address"),
		req.Header.Get("X-Real-Ip"),
		req.RemoteAddr,
	)
	if host, _, err := net.SplitHostPort(ip); err == nil {
		ip = host
	}
	set(MetaUserIp, ip)

	// 获取用户国家区号并转换为大写
	country := cmp.Or(req.Header.Get("Cf-Ipcountry"), req.Header.Get("CloudFront-Viewer-Country"))
	set(MetaUserCountry, strings.ToUpper(country))

	return md
}
