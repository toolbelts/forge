package provider

import (
	"context"
	"errors"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"

	"github.com/toolbelts/forge/errkit"
	"github.com/toolbelts/forge/ioc"
	"github.com/toolbelts/forge/meta"
	"github.com/toolbelts/forge/token"
)

// tokenDefaultRedisName 是 token 管理器默认使用的 Redis 实例名,可通过 token.redis 覆盖。
const tokenDefaultRedisName = "default"

// TokenProvider 基于 Redis 的 access/refresh token 管理器 + 一元鉴权拦截器。
//
// 编排约定:排在 ValidateProvider 之后(更内层)。
// 原因:Validate 的 protovalidate 是纯内存 CEL,token 校验要查 Redis;
// 让畸形请求先被 Validate 砍掉,Redis 流量更省,鉴权日志/告警的信号更纯。
//
// 流式 RPC:本期不挂载流式拦截器(参考 ValidateProvider 的取舍);
// 长连接的 token 续期/失效语义需要单独设计,先聚焦 unary。
//
// 白名单两档:
//   - skips         完全跳过 token 校验,handler 收到的 ctx 中 user_id == 0
//   - optional_skips 软鉴权:命中后,缺 token 或凭证类校验失败均静默降级为
//     匿名(handler 收到 user_id == 0),token 有效则正常写
//     user_id;Redis/网络等系统错误不被静默,继续透传。
//     适用于"既可登录调、也可匿名调"的 RPC(如发送验证码)。
//
// 同一 RPC 同时命中 skips 与 optional_skips 时,skips 优先(更宽松,不查 token)。
//
// 隐含契约:本拦截器假设 token.Manager 不签发 user_id == 0 的 token,因此
// user_id == 0 可作为"匿名"的可靠信号。后续若引入 user_id == 0 的合法用户,
// 需重新设计匿名信号(例如 meta 增设布尔位)。
type TokenProvider struct {
	enabled       bool
	skips         map[string]struct{}
	optionalSkips map[string]struct{}
	tm            *token.Manager
}

// Register 读 token.enabled / token.skips / token.optional_skips。
// disabled 时 Setup 直接跳过,不创建 Manager 也不挂拦截器。
func (p *TokenProvider) Register(ctx context.Context) error {
	v := MustGetViper(ctx)
	p.enabled = v.GetBool("token.enabled")
	if !p.enabled {
		log.Ctx(ctx).Info().Str("provider", "token").Msg("token disabled, skip")
		return nil
	}

	p.skips = meta.BuildSkips(v.GetStringSlice("token.skips"))
	p.optionalSkips = meta.BuildSkips(v.GetStringSlice("token.optional_skips"))
	return nil
}

// Setup 取出 Redis 客户端并构造 Manager,把 Manager 注入容器、把鉴权拦截器加进 chain。
func (p *TokenProvider) Setup(ctx context.Context) error {
	if !p.enabled {
		return nil
	}

	v := MustGetViper(ctx)
	redisName := v.GetString("token.redis")
	if redisName == "" {
		redisName = tokenDefaultRedisName
	}

	rdb, err := GetRedis(ctx, redisName)
	if err != nil {
		return err
	}

	opts := make([]token.Option, 0, 6)
	if prefix := v.GetString("token.prefix"); prefix != "" {
		opts = append(opts, token.WithPrefix(prefix))
	}
	if d := v.GetDuration("token.access_ttl"); d > 0 {
		opts = append(opts, token.WithAccessTtl(d))
	}
	if d := v.GetDuration("token.access_save_ttl"); d > 0 {
		opts = append(opts, token.WithAccessSaveTtl(d))
	}
	if d := v.GetDuration("token.refresh_ttl"); d > 0 {
		opts = append(opts, token.WithRefreshTtl(d))
	}
	if d := v.GetDuration("token.refresh_save_ttl"); d > 0 {
		opts = append(opts, token.WithRefreshSaveTtl(d))
	}
	// GetBool 在键缺失时返回 false,会误关默认开启的旋转策略,所以必须 IsSet 守卫。
	if v.IsSet("token.refresh_rotation") {
		opts = append(opts, token.WithRefreshRotation(v.GetBool("token.refresh_rotation")))
	}

	tm, err := token.NewManager(rdb, opts...)
	if err != nil {
		return err
	}
	p.tm = tm
	ioc.MustInstance(ctx, p.tm)

	chain := ioc.MustGet[*InterceptorChain](ctx)
	chain.Use(tokenUnaryInterceptor(p.tm, p.skips, p.optionalSkips))

	log.Ctx(ctx).Info().
		Str("provider", "token").
		Str("redis", redisName).
		Int("skips", len(p.skips)).
		Int("optional_skips", len(p.optionalSkips)).
		Msg("token interceptor registered")
	return nil
}

// tokenUnaryInterceptor 拦截除 skips 外的全部 unary RPC,
// 校验 access_token 后把 user_id 写进 meta,业务 handler 通过
// meta.UserId(ctx) 拿到当前用户。
//
// skips / optional_skips 都接受 HTTP path(/v1/auth/login)或 gRPC FullMethod
// (/api.Auth/Login),任一命中即放行。直连 gRPC 入口若需要放行,把 FullMethod
// 写进对应名单即可。两个名单同时命中时 skips 优先(更宽松,不查 token)。
//
// 错误映射(统一对外暴露 UNAUTHENTICATED,前端只需一个"重新登录"分支):
//   - 缺 token / 不存在 / 过期 / 载荷损坏  → UNAUTHENTICATED(strict 模式)
//     → 匿名放行(optional 模式)
//   - 其它(Redis/网络等)                   → 透传给 ErrorProvider 归一化为 INTERNAL
//     (无论 strict 还是 optional 都不静默)
//
// 真实 token 失败原因仍通过 WithCause 挂在 BizError 上,服务端日志可排查,但不向客户端暴露。
func tokenUnaryInterceptor(
	tm *token.Manager,
	skips, optionalSkips map[string]struct{},
) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if meta.MatchSkips(ctx, skips) {
			return handler(ctx, req)
		}

		optional := meta.MatchSkips(ctx, optionalSkips)

		rm := meta.Request(ctx)
		logger := log.Ctx(ctx).With().
			Str("full_method", info.FullMethod).
			Str("http_method", rm.Method).
			Str("http_path", rm.Path).
			Bool("optional", optional).
			Logger()

		accessToken := rm.Token
		if accessToken == "" {
			if optional {
				logger.Debug().Msg("token optional: no token, anonymous pass")
				return handler(ctx, req)
			}
			logger.Info().Msg("token auth rejected: missing token")
			return nil, errkit.New(errkit.CodeUnauthenticated, "unauthenticated")
		}

		tk, err := tm.Validate(ctx, accessToken)
		if err != nil {
			credErr := errors.Is(err, token.ErrTokenNotFound) ||
				errors.Is(err, token.ErrEmptyToken) ||
				errors.Is(err, token.ErrTokenExpired) ||
				errors.Is(err, token.ErrTokenCorrupted)
			switch {
			case credErr && optional:
				logger.Debug().Err(err).Msg("token optional: invalid token, anonymous pass")
				return handler(ctx, req)
			case credErr:
				logger.Info().Err(err).Msg("token auth rejected")
				return nil, errkit.New(errkit.CodeUnauthenticated, "unauthenticated").WithCause(err)
			default:
				logger.Warn().Err(err).Msg("token validate failed")
				return nil, err
			}
		}

		ctx = meta.Set(ctx, meta.MetaUserId, tk.UserId)
		logger.Info().Int64("user_id", tk.UserId).Msg("token auth ok")
		return handler(ctx, req)
	}
}

// MustGetTokenManager 从容器获取 token 管理器,缺失时 panic。
// 业务方在登录/退出/续期 handler 中调 Create/Refresh/Delete 等。
func MustGetTokenManager(ctx context.Context) *token.Manager {
	return ioc.MustGet[*token.Manager](ctx)
}

// GetTokenManager 从容器获取 token 管理器。
func GetTokenManager(ctx context.Context) (*token.Manager, error) {
	return ioc.Get[*token.Manager](ctx)
}
