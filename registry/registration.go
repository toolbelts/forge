package registry

import (
	"context"
	"errors"
	"fmt"
	"time"

	json "github.com/goccy/go-json"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// Registration 一次成功注册的句柄,持有心跳 goroutine 的取消函数。
// Deregister 后不可重用,需重新调用 Manager.Register 派生新句柄。
type Registration struct {
	mgr      *Manager
	instance Instance
	key      string
	cancel   context.CancelFunc
}

// Register 把实例写入 Redis 并启动心跳续约。
// 校验:service/id/addr 均不可为空。
// 冲突:同 service+id 已存在 addr 不同的活跃实例时返回 ErrInstanceConflict,
// 防 hostname 冲突的双副本互踩心跳。
func (m *Manager) Register(ctx context.Context, instance Instance) (*Registration, error) {
	if instance.Service == "" {
		return nil, ErrEmptyService
	}
	if instance.Id == "" {
		return nil, ErrEmptyInstanceId
	}
	if instance.Addr == "" {
		return nil, ErrEmptyAddr
	}

	key := m.instanceKey(instance.Service, instance.Id)

	existing, err := m.rdb.Get(ctx, key).Result()
	switch {
	case errors.Is(err, redis.Nil):
		// key 不存在,正常路径。
	case err != nil:
		return nil, fmt.Errorf("registry: precheck get failed: %w", err)
	default:
		var prev Instance
		if jerr := json.Unmarshal([]byte(existing), &prev); jerr == nil && prev.Addr != instance.Addr {
			return nil, fmt.Errorf("%w: service=%s id=%s prev_addr=%s new_addr=%s",
				ErrInstanceConflict, instance.Service, instance.Id, prev.Addr, instance.Addr)
		}
		// 同 addr 视为重启复活,允许覆盖续约。
	}

	value, err := json.Marshal(&instance)
	if err != nil {
		return nil, fmt.Errorf("registry: marshal failed: %w", err)
	}
	if err := m.rdb.Set(ctx, key, value, m.opt.ttl).Err(); err != nil {
		return nil, fmt.Errorf("registry: setex failed: %w", err)
	}

	hbCtx, cancel := context.WithCancel(context.Background())
	r := &Registration{
		mgr:      m,
		instance: instance,
		key:      key,
		cancel:   cancel,
	}
	go r.heartbeatLoop(hbCtx, value)

	log.Ctx(ctx).Info().
		Str("service", instance.Service).
		Str("instance_id", instance.Id).
		Str("addr", instance.Addr).
		Dur("ttl", m.opt.ttl).
		Msg("registry instance registered")
	return r, nil
}

// Deregister 取消心跳并主动 DEL 该实例 key。
// 调用方在 Shutdown 阶段建议传 context.WithoutCancel(ctx) + 短超时。
func (r *Registration) Deregister(ctx context.Context) error {
	r.cancel()
	if err := r.mgr.rdb.Del(ctx, r.key).Err(); err != nil {
		return fmt.Errorf("registry: del failed: %w", err)
	}
	log.Ctx(ctx).Info().
		Str("service", r.instance.Service).
		Str("instance_id", r.instance.Id).
		Msg("registry instance deregistered")
	return nil
}

// Key 返回写入 Redis 的完整 key(含 prefix)。
func (r *Registration) Key() string {
	return r.key
}

// Instance 返回注册时的实例元数据快照。
func (r *Registration) Instance() Instance {
	return r.instance
}

// heartbeatLoop 周期 SET 重置 TTL 续约,失败仅 Warn 不退出。
// 单次失败靠后续心跳兜底;Redis 长时间不可用时实例会因 TTL 过期被自动剔除。
func (r *Registration) heartbeatLoop(ctx context.Context, value []byte) {
	ticker := time.NewTicker(r.mgr.opt.heartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.mgr.rdb.Set(ctx, r.key, value, r.mgr.opt.ttl).Err(); err != nil {
				log.Ctx(ctx).Warn().
					Err(err).
					Str("service", r.instance.Service).
					Str("instance_id", r.instance.Id).
					Msg("registry heartbeat failed")
			}
		}
	}
}
