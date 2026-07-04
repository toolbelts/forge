package registry

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	json "github.com/goccy/go-json"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/resolver"
)

// scanCount SCAN 单次返回的提示批量,实例数 <100 时一次拿完。
const scanCount = 100

// Instance 描述一个服务实例的注册元数据。
// addr 为路由可达的 host:port,客户端按此拨号。
type Instance struct {
	Id       string            `json:"id"`
	Service  string            `json:"service"`
	Addr     string            `json:"addr"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// Manager 服务注册发现工厂,持有 Redis 客户端与默认配置。
type Manager struct {
	rdb redis.UniversalClient
	opt options
}

// NewManager 创建注册发现工厂,rdb 为 nil 或 ttl/heartbeat 非正时返回错误。
func NewManager(rdb redis.UniversalClient, opts ...Option) (*Manager, error) {
	if rdb == nil {
		return nil, ErrNilRedisClient
	}
	o := defaultOptions
	for _, opt := range opts {
		opt(&o)
	}
	if o.ttl <= 0 {
		return nil, fmt.Errorf("%w: ttl must be positive", ErrInvalidOption)
	}
	if o.heartbeat <= 0 {
		return nil, fmt.Errorf("%w: heartbeat must be positive", ErrInvalidOption)
	}
	if o.resolveInterval <= 0 {
		return nil, fmt.Errorf("%w: resolve_interval must be positive", ErrInvalidOption)
	}
	if o.heartbeat >= o.ttl {
		return nil, fmt.Errorf("%w: heartbeat must be less than ttl", ErrInvalidOption)
	}
	return &Manager{rdb: rdb, opt: o}, nil
}

// Resolve 一次性拉取指定 service 当前活跃的全部实例。
// 内部走 SCAN MATCH + MGET,死实例靠 TTL 自动消失,无需显式清理。
// service 为空返回 ErrEmptyService。
func (m *Manager) Resolve(ctx context.Context, service string) ([]Instance, error) {
	if service == "" {
		return nil, ErrEmptyService
	}

	pattern := m.servicePattern(service)
	keys, err := m.scanKeys(ctx, pattern)
	if err != nil {
		return nil, fmt.Errorf("registry: scan failed: %w", err)
	}
	if len(keys) == 0 {
		return nil, nil
	}

	vals, err := m.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("registry: mget failed: %w", err)
	}

	instances := make([]Instance, 0, len(vals))
	for _, v := range vals {
		raw, ok := v.(string)
		if !ok {
			// SCAN 与 MGET 之间 key 过期会拿到 nil,跳过即可。
			continue
		}
		var inst Instance
		if err := json.Unmarshal([]byte(raw), &inst); err != nil {
			log.Ctx(ctx).Warn().Err(err).Str("service", service).Msg("registry instance decode failed")
			continue
		}
		instances = append(instances, inst)
	}

	// 按 id 排序,保证调用方比较前后两次结果时不受 SCAN 顺序抖动影响。
	slices.SortFunc(instances, func(a, b Instance) int {
		return strings.Compare(a.Id, b.Id)
	})
	return instances, nil
}

// Watch 周期轮询 Redis,把命中的实例列表通过返回的 channel 推送出去。
// channel 仅在结果与上次推送不同时发送(去抖);ctx 取消时 channel 关闭、内部 goroutine 退出。
// 首次拉取在调用线程执行,失败时返回 error,channel 仍可能被关闭。
func (m *Manager) Watch(ctx context.Context, service string) (<-chan []Instance, error) {
	if service == "" {
		return nil, ErrEmptyService
	}

	first, err := m.Resolve(ctx, service)
	if err != nil {
		return nil, err
	}

	ch := make(chan []Instance, 1)
	go m.watchLoop(ctx, service, first, ch)
	return ch, nil
}

// ResolverBuilder 返回 gRPC resolver builder,可用于 grpc.WithResolvers 单测注入或多 manager 场景。
func (m *Manager) ResolverBuilder() resolver.Builder {
	return &builder{mgr: m}
}

// RegisterResolver 把 ResolverBuilder 注册到 grpc 全局 resolver 表,
// 之后业务方调 grpc.NewClient("redis:///<service>") 即可解析。
// 进程内只应调用一次,重复注册由 grpc 用最后一次覆盖。
func (m *Manager) RegisterResolver() {
	resolver.Register(m.ResolverBuilder())
}

// instanceKey 拼接单实例 Redis key。
func (m *Manager) instanceKey(service, id string) string {
	return m.opt.prefix + ":" + service + ":" + id
}

// servicePattern 拼接 SCAN MATCH 用的通配模式。
func (m *Manager) servicePattern(service string) string {
	return m.opt.prefix + ":" + service + ":*"
}

// scanKeys 用 SCAN 迭代列出全部命中的 key。
// 单实例 Redis 下行为稳定;cluster 模式需要逐槽位扫描,本期不支持。
func (m *Manager) scanKeys(ctx context.Context, pattern string) ([]string, error) {
	var keys []string
	iter := m.rdb.Scan(ctx, 0, pattern, scanCount).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return keys, nil
}

// watchLoop Watch 内部轮询循环,前置实例由 Watch 提供避免重复一次 RTT。
func (m *Manager) watchLoop(ctx context.Context, service string, last []Instance, ch chan<- []Instance) {
	defer close(ch)

	// 首次推送,让订阅方拿到初始快照。
	select {
	case ch <- last:
	case <-ctx.Done():
		return
	}

	ticker := time.NewTicker(m.opt.resolveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			instances, err := m.Resolve(ctx, service)
			if err != nil {
				log.Ctx(ctx).Warn().Err(err).Str("service", service).Msg("registry watch resolve failed")
				continue
			}
			if instancesEqual(last, instances) {
				continue
			}
			last = instances
			select {
			case ch <- instances:
			case <-ctx.Done():
				return
			}
		}
	}
}

// instancesEqual 判断两组实例是否完全相同,Resolve 已按 id 排好序。
func instancesEqual(a, b []Instance) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Id != b[i].Id || a[i].Addr != b[i].Addr {
			return false
		}
	}
	return true
}
