package provider

import (
	"context"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"

	"github.com/toolbelts/forge/errkit"
	"github.com/toolbelts/forge/ioc"
	"github.com/toolbelts/forge/ratelimit"
)

// ratelimitDefaultRedisName 是限速器默认使用的 Redis 实例名,可通过 ratelimit.redis 覆盖。
const ratelimitDefaultRedisName = "default"

// RateLimitProvider 注册基于 Redis 滑动窗口的限速拦截器到 InterceptorChain。
//
// 编排约定:排在 ErrorProvider 之后、ValidateProvider 之前(更外层)。
// 原因:Limiter.Allow 只读 meta(path/method/ip 等),不依赖 proto 消息;
// protovalidate 跑 CEL 规则 CPU 开销大,提前限流能在畸形请求消耗 CPU 之前砍掉。
//
// 流式 RPC:本期不挂载流式拦截器,一次性 Allow 与流式语义不匹配(参考 ValidateProvider 的取舍)。
type RateLimitProvider struct {
	enabled bool
	rl      *ratelimit.RedisRateLimiter
}

// Register 读 ratelimit.enabled。disabled 时 Setup/Shutdown 都直接跳过,不占用 Redis 资源。
func (p *RateLimitProvider) Register(ctx context.Context) error {
	v := MustGetViper(ctx)
	p.enabled = v.GetBool("ratelimit.enabled")
	if !p.enabled {
		log.Ctx(ctx).Info().Str("provider", "ratelimit").Msg("ratelimit disabled, skip")
	}
	return nil
}

// Setup 取出 Redis 客户端并构造 RedisRateLimiter(启动加载 + 后台周期 reload),把一元拦截器加进 chain。
// 必须排在 RedisProvider 之后,GetRedis 才能取到客户端。
func (p *RateLimitProvider) Setup(ctx context.Context) error {
	if !p.enabled {
		return nil
	}

	v := MustGetViper(ctx)
	redisName := v.GetString("ratelimit.redis")
	if redisName == "" {
		redisName = ratelimitDefaultRedisName
	}

	rdb, err := GetRedis(ctx, redisName)
	if err != nil {
		return err
	}

	opts := make([]ratelimit.Option, 0, 3)
	if prefix := v.GetString("ratelimit.prefix"); prefix != "" {
		opts = append(opts, ratelimit.WithPrefix(prefix))
	}
	if d := v.GetDuration("ratelimit.reload_period"); d > 0 {
		opts = append(opts, ratelimit.WithReloadPeriod(d))
	}
	if pol := v.GetString("ratelimit.failure_policy"); pol != "" {
		opts = append(opts, ratelimit.WithFailurePolicy(ratelimit.FailurePolicy(pol)))
	}

	rl, err := ratelimit.NewRedisRateLimiter(ctx, rdb, opts...)
	if err != nil {
		return err
	}
	p.rl = rl
	ioc.MustInstance(ctx, p.rl)

	chain := ioc.MustGet[*InterceptorChain](ctx)
	chain.Use(rateLimitUnaryInterceptor(p.rl))

	log.Ctx(ctx).Info().
		Str("provider", "ratelimit").
		Str("redis", redisName).
		Msg("ratelimit interceptor registered")
	return nil
}

// Shutdown 停掉 RedisRateLimiter 内部的 reload goroutine,避免泄漏。
func (p *RateLimitProvider) Shutdown(ctx context.Context) error {
	if p.rl != nil {
		p.rl.Close()
	}
	return nil
}

// rateLimitUnaryInterceptor 调用 Allow 拿 Quota:被拒返回 ErrorCode_RATE_LIMITED。
// Limiter 内部已按 failurePolicy 决定 quota.Allowed,interceptor 只看该字段;
// Allow 返回 error 时只写日志辅助排查,不重复决策。
func rateLimitUnaryInterceptor(rl *ratelimit.RedisRateLimiter) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		quota, err := rl.Allow(ctx)
		if err != nil {
			log.Ctx(ctx).Warn().
				Err(err).
				Str("method", info.FullMethod).
				Msg("ratelimit allow returned error")
		}
		if quota != nil && !quota.Allowed {
			be := errkit.New(errkit.CodeResourceExhausted, "rate limit exceeded").
				WithMetadata("rule", quota.RuleName).
				WithMetadata("retry_after", quota.ResetAfter.String())
			if err != nil {
				be = be.WithCause(err)
			}
			return nil, be
		}
		return handler(ctx, req)
	}
}

// MustGetRateLimiter 从容器获取限速器,缺失时 panic。
// 业务方在 admin handler 中调 SetRule/DeleteRule 动态调整规则。
func MustGetRateLimiter(ctx context.Context) *ratelimit.RedisRateLimiter {
	return ioc.MustGet[*ratelimit.RedisRateLimiter](ctx)
}

// GetRateLimiter 从容器获取限速器。
func GetRateLimiter(ctx context.Context) (*ratelimit.RedisRateLimiter, error) {
	return ioc.Get[*ratelimit.RedisRateLimiter](ctx)
}
