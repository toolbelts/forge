package token

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"time"

	json "github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// Generator 定义 token 字符串生成器。
// 默认实现使用 UUID v4,可读性高且 128 bit 熵足够抵御暴力枚举。
type Generator func() (string, error)

// Token 表示一对完整的访问凭证,access_token 与 refresh_token 共享同一份业务上下文。
type Token struct {
	AccessToken    string            `json:"access_token"`
	RefreshToken   string            `json:"refresh_token"`
	UserId         int64             `json:"user_id"`
	AccessExpires  time.Time         `json:"access_expires"`
	RefreshExpires time.Time         `json:"refresh_expires"`
	CreatedAt      time.Time         `json:"created_at"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

// AccessValid 判断 access_token 当前是否仍处于逻辑有效期内。
func (t *Token) AccessValid() bool {
	return time.Now().Before(t.AccessExpires)
}

// RefreshValid 判断 refresh_token 当前是否仍处于逻辑有效期内。
func (t *Token) RefreshValid() bool {
	return time.Now().Before(t.RefreshExpires)
}

// GetMeta 读取指定 key 的 metadata 值;不存在或 Metadata 为空时返回空串。
func (t *Token) GetMeta(key string) string {
	return t.Metadata[key]
}

// SetMeta 写入或覆盖指定 key 的 metadata 值,Metadata 为 nil 时自动初始化。
// 仅修改内存中的 Token 实例,如需持久化请重新通过 Manager 写入。
func (t *Token) SetMeta(key, value string) {
	if t.Metadata == nil {
		t.Metadata = make(map[string]string)
	}
	t.Metadata[key] = value
}

type options struct {
	prefix          string
	accessTtl       time.Duration
	accessSaveTtl   time.Duration
	refreshTtl      time.Duration
	refreshSaveTtl  time.Duration
	refreshRotation bool
	generator       Generator
}

// defaultOptions 给出 Manager 在未显式配置时的默认参数。
// saveTtl 严格大于对应的 Ttl,提供过期排查缓冲。
var defaultOptions = options{
	prefix:          "token",
	accessTtl:       2 * time.Hour,
	accessSaveTtl:   2*time.Hour + 10*time.Minute,
	refreshTtl:      30 * 24 * time.Hour,
	refreshSaveTtl:  30*24*time.Hour + 24*time.Hour,
	refreshRotation: true,
	generator:       defaultGenerator,
}

// Option 定义 Manager 的可选配置。
type Option func(*options)

// WithPrefix 设置 Redis key 前缀。空串保留默认值。
func WithPrefix(prefix string) Option {
	return func(o *options) {
		if prefix != "" {
			o.prefix = prefix
		}
	}
}

// WithAccessTtl 设置 access_token 的逻辑有效期。
func WithAccessTtl(ttl time.Duration) Option {
	return func(o *options) {
		if ttl > 0 {
			o.accessTtl = ttl
		}
	}
}

// WithAccessSaveTtl 设置 access_token 在 Redis 中的保存时间,用于过期后短期排查。
func WithAccessSaveTtl(ttl time.Duration) Option {
	return func(o *options) {
		if ttl > 0 {
			o.accessSaveTtl = ttl
		}
	}
}

// WithRefreshTtl 设置 refresh_token 的逻辑有效期。
func WithRefreshTtl(ttl time.Duration) Option {
	return func(o *options) {
		if ttl > 0 {
			o.refreshTtl = ttl
		}
	}
}

// WithRefreshSaveTtl 设置 refresh_token 在 Redis 中的保存时间,用于过期后短期排查。
func WithRefreshSaveTtl(ttl time.Duration) Option {
	return func(o *options) {
		if ttl > 0 {
			o.refreshSaveTtl = ttl
		}
	}
}

// WithGenerator 替换 token 字符串生成器。传入 nil 保留默认生成器。
func WithGenerator(g Generator) Option {
	return func(o *options) {
		if g != nil {
			o.generator = g
		}
	}
}

// WithRefreshRotation 控制 Refresh 时是否旋转 refresh_token。
// 开启(默认)更安全:旧 refresh_token 在一次成功 Refresh 后立即失效。
func WithRefreshRotation(enable bool) Option {
	return func(o *options) {
		o.refreshRotation = enable
	}
}

// Manager 提供基于 Redis 的 token 全生命周期管理。
type Manager struct {
	rdb redis.UniversalClient
	opt options
}

// luaRenew 在 Redis 端原子完成 access entry 的存在性校验与覆盖写入。
// 返回 -1 表示 access entry 不存在,1 表示成功。
const luaRenew = `
local existing = redis.call('GET', KEYS[1])
if not existing then return -1 end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
return 1
`

// luaRefresh 在 Redis 端原子完成"校验旧 refresh -> 失效旧 access -> 写入新凭证 -> 维护用户索引"。
// KEYS[1] 新 access key, KEYS[2] 新 refresh key, KEYS[3] 旧 access key,
// KEYS[4] 旧 refresh key, KEYS[5] 用户索引 key。
// ARGV[1] 新 Token JSON, ARGV[2] access_save_ms, ARGV[3] refresh_save_ms(同时用于 user 索引),
// ARGV[4] 旧 access_token 字符串, ARGV[5] 新 access_token 字符串, ARGV[6] 旋转标志("1" 旋转)。
// 返回 -1 表示旧 refresh 已不存在,1 表示成功。
const luaRefresh = `
local existing = redis.call('GET', KEYS[4])
if not existing then return -1 end
redis.call('DEL', KEYS[3])
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
redis.call('SREM', KEYS[5], ARGV[4])
redis.call('SADD', KEYS[5], ARGV[5])
redis.call('PEXPIRE', KEYS[5], ARGV[3])
if ARGV[6] == "1" then
    redis.call('DEL', KEYS[4])
    redis.call('SET', KEYS[2], ARGV[1], 'PX', ARGV[3])
end
return 1
`

var (
	renewScript   = redis.NewScript(luaRenew)
	refreshScript = redis.NewScript(luaRefresh)
)

// NewManager 创建一个 token 管理器,并在启动期一次性校验配置合法性。
// Ttl 顺序约束:accessTtl < accessSaveTtl,refreshTtl < refreshSaveTtl,且 refreshTtl >= accessTtl。
func NewManager(rdb redis.UniversalClient, opts ...Option) (*Manager, error) {
	if rdb == nil {
		return nil, ErrNilRedisClient
	}
	o := defaultOptions
	for _, opt := range opts {
		opt(&o)
	}
	if o.accessTtl <= 0 || o.refreshTtl <= 0 {
		return nil, fmt.Errorf("%w: ttl must be positive", ErrInvalidOption)
	}
	if o.accessSaveTtl <= o.accessTtl {
		return nil, fmt.Errorf("%w: access save ttl must be greater than access ttl", ErrInvalidOption)
	}
	if o.refreshSaveTtl <= o.refreshTtl {
		return nil, fmt.Errorf("%w: refresh save ttl must be greater than refresh ttl", ErrInvalidOption)
	}
	if o.refreshTtl < o.accessTtl {
		return nil, fmt.Errorf("%w: refresh ttl must not be shorter than access ttl", ErrInvalidOption)
	}
	if o.generator == nil {
		o.generator = defaultGenerator
	}
	return &Manager{rdb: rdb, opt: o}, nil
}

// Create 为指定用户生成一对全新的 access + refresh token,并写入 Redis 与用户索引。
func (m *Manager) Create(ctx context.Context, userId int64, metadata map[string]string) (*Token, error) {
	if userId <= 0 {
		return nil, ErrInvalidUserId
	}
	accessToken, err := m.opt.generator()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrGeneratorFailed, err)
	}
	refreshToken, err := m.opt.generator()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrGeneratorFailed, err)
	}
	now := time.Now()
	token := &Token{
		AccessToken:    accessToken,
		RefreshToken:   refreshToken,
		UserId:         userId,
		AccessExpires:  now.Add(m.opt.accessTtl),
		RefreshExpires: now.Add(m.opt.refreshTtl),
		CreatedAt:      now,
		Metadata:       maps.Clone(metadata),
	}
	payload, err := json.Marshal(token)
	if err != nil {
		return nil, err
	}
	pipe := m.rdb.TxPipeline()
	pipe.Set(ctx, m.accessKey(accessToken), payload, m.opt.accessSaveTtl)
	pipe.Set(ctx, m.refreshKey(refreshToken), payload, m.opt.refreshSaveTtl)
	pipe.SAdd(ctx, m.userKey(userId), accessToken)
	pipe.PExpire(ctx, m.userKey(userId), m.opt.refreshSaveTtl)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	return token, nil
}

// Validate 校验 access_token 是否有效。
// Redis 中没有 → ErrTokenNotFound;载荷损坏 → ErrTokenCorrupted;逻辑过期 → ErrTokenExpired(数据仍可查)。
func (m *Manager) Validate(ctx context.Context, accessToken string) (*Token, error) {
	if accessToken == "" {
		return nil, ErrEmptyToken
	}
	raw, err := m.rdb.Get(ctx, m.accessKey(accessToken)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrTokenNotFound
		}
		return nil, err
	}
	token := &Token{}
	if err := json.Unmarshal(raw, token); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrTokenCorrupted, err)
	}
	if !token.AccessValid() {
		return nil, ErrTokenExpired
	}
	return token, nil
}

// Renew 重置已有 access_token 的逻辑过期时间与 Redis 保存时间,但不更换 token 字符串。
// 不延长 refresh_token 的过期时间 —— refresh 始终反映会话最长生命边界。
func (m *Manager) Renew(ctx context.Context, accessToken string) (*Token, error) {
	if accessToken == "" {
		return nil, ErrEmptyToken
	}
	// 先校验 token 当前仍逻辑有效,过期 token 不再允许续期。
	existing, err := m.Validate(ctx, accessToken)
	if err != nil {
		return nil, err
	}
	existing.AccessExpires = time.Now().Add(m.opt.accessTtl)
	payload, err := json.Marshal(existing)
	if err != nil {
		return nil, err
	}
	keys := []string{m.accessKey(accessToken)}
	args := []any{payload, m.opt.accessSaveTtl.Milliseconds()}
	result, err := renewScript.Run(ctx, m.rdb, keys, args...).Int64()
	if err != nil {
		return nil, err
	}
	if result == -1 {
		// Validate 与 Renew 之间被并发删除,语义上视为不存在。
		return nil, ErrTokenNotFound
	}
	return existing, nil
}

// Refresh 用 refresh_token 换取新的 access_token,旧 access_token 立即失效。
// 当 refreshRotation=true 时同步替换 refresh_token,旧 refresh_token 一并失效。
func (m *Manager) Refresh(ctx context.Context, refreshToken string) (*Token, error) {
	if refreshToken == "" {
		return nil, ErrEmptyToken
	}
	raw, err := m.rdb.Get(ctx, m.refreshKey(refreshToken)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrTokenNotFound
		}
		return nil, err
	}
	existing := &Token{}
	if err := json.Unmarshal(raw, existing); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrTokenCorrupted, err)
	}
	if !existing.RefreshValid() {
		return nil, ErrTokenExpired
	}
	newAccess, err := m.opt.generator()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrGeneratorFailed, err)
	}
	newRefresh := refreshToken
	if m.opt.refreshRotation {
		newRefresh, err = m.opt.generator()
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrGeneratorFailed, err)
		}
	}
	next := &Token{
		AccessToken:    newAccess,
		RefreshToken:   newRefresh,
		UserId:         existing.UserId,
		AccessExpires:  time.Now().Add(m.opt.accessTtl),
		RefreshExpires: existing.RefreshExpires,
		CreatedAt:      existing.CreatedAt,
		Metadata:       maps.Clone(existing.Metadata),
	}
	payload, err := json.Marshal(next)
	if err != nil {
		return nil, err
	}
	rotationFlag := "0"
	if m.opt.refreshRotation {
		rotationFlag = "1"
	}
	keys := []string{
		m.accessKey(newAccess),
		m.refreshKey(newRefresh),
		m.accessKey(existing.AccessToken),
		m.refreshKey(refreshToken),
		m.userKey(existing.UserId),
	}
	args := []any{
		payload,
		m.opt.accessSaveTtl.Milliseconds(),
		m.opt.refreshSaveTtl.Milliseconds(),
		existing.AccessToken,
		newAccess,
		rotationFlag,
	}
	result, err := refreshScript.Run(ctx, m.rdb, keys, args...).Int64()
	if err != nil {
		return nil, err
	}
	if result == -1 {
		// 并发场景下旋转模式仅有一个 Refresh 能成功,后到者命中此分支。
		return nil, ErrTokenNotFound
	}
	return next, nil
}

// Delete 删除单个 access_token 及其对应 refresh entry 与用户索引引用。
// 当 token 已不存在时返回 nil(幂等)。
func (m *Manager) Delete(ctx context.Context, accessToken string) error {
	if accessToken == "" {
		return ErrEmptyToken
	}
	raw, err := m.rdb.Get(ctx, m.accessKey(accessToken)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil
		}
		return err
	}
	token := &Token{}
	if err := json.Unmarshal(raw, token); err != nil {
		// 载荷损坏时仍尝试清理 access entry,refresh 与索引留待后续惰性清理。
		log.Ctx(ctx).Warn().Err(err).Msg("token corrupted on delete")
		return m.rdb.Del(ctx, m.accessKey(accessToken)).Err()
	}
	pipe := m.rdb.TxPipeline()
	pipe.Del(ctx, m.accessKey(accessToken))
	pipe.Del(ctx, m.refreshKey(token.RefreshToken))
	pipe.SRem(ctx, m.userKey(token.UserId), accessToken)
	_, err = pipe.Exec(ctx)
	return err
}

// DeleteByUser 删除某用户的全部 access/refresh entries 与用户索引本身。
func (m *Manager) DeleteByUser(ctx context.Context, userId int64) error {
	if userId <= 0 {
		return ErrInvalidUserId
	}
	members, err := m.rdb.SMembers(ctx, m.userKey(userId)).Result()
	if err != nil {
		return err
	}
	if len(members) == 0 {
		return m.rdb.Del(ctx, m.userKey(userId)).Err()
	}
	accessKeys := make([]string, 0, len(members))
	for _, member := range members {
		accessKeys = append(accessKeys, m.accessKey(member))
	}
	payloads, err := m.rdb.MGet(ctx, accessKeys...).Result()
	if err != nil {
		return err
	}
	pipe := m.rdb.TxPipeline()
	for _, key := range accessKeys {
		pipe.Del(ctx, key)
	}
	for _, payload := range payloads {
		raw, ok := payload.(string)
		if !ok {
			continue
		}
		token := &Token{}
		if err := json.Unmarshal([]byte(raw), token); err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("token corrupted on delete by user")
			continue
		}
		pipe.Del(ctx, m.refreshKey(token.RefreshToken))
	}
	pipe.Del(ctx, m.userKey(userId))
	_, err = pipe.Exec(ctx)
	return err
}

// ListByUser 返回某用户当前持有的全部 token,包含 Redis 中尚存但已逻辑过期的条目,
// 调用方可根据 AccessExpires 自行判断状态。同时惰性清理用户索引中的幽灵成员。
func (m *Manager) ListByUser(ctx context.Context, userId int64) ([]*Token, error) {
	if userId <= 0 {
		return nil, ErrInvalidUserId
	}
	members, err := m.rdb.SMembers(ctx, m.userKey(userId)).Result()
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return []*Token{}, nil
	}
	accessKeys := make([]string, 0, len(members))
	for _, member := range members {
		accessKeys = append(accessKeys, m.accessKey(member))
	}
	payloads, err := m.rdb.MGet(ctx, accessKeys...).Result()
	if err != nil {
		return nil, err
	}
	tokens := make([]*Token, 0, len(payloads))
	ghosts := make([]any, 0)
	for i, payload := range payloads {
		if payload == nil {
			// 用户索引中残留但 access entry 已自然过期,顺手清理避免索引膨胀。
			ghosts = append(ghosts, members[i])
			continue
		}
		raw, ok := payload.(string)
		if !ok {
			continue
		}
		token := &Token{}
		if err := json.Unmarshal([]byte(raw), token); err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("token corrupted on list by user")
			continue
		}
		tokens = append(tokens, token)
	}
	if len(ghosts) > 0 {
		if err := m.rdb.SRem(ctx, m.userKey(userId), ghosts...).Err(); err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("token ghost cleanup failed")
		}
	}
	return tokens, nil
}

// accessKey 拼接 access_token 在 Redis 中的 key。
func (m *Manager) accessKey(accessToken string) string {
	return m.opt.prefix + ":access:" + accessToken
}

// refreshKey 拼接 refresh_token 在 Redis 中的 key。
func (m *Manager) refreshKey(refreshToken string) string {
	return m.opt.prefix + ":refresh:" + refreshToken
}

// userKey 拼接用户索引在 Redis 中的 SET key。
func (m *Manager) userKey(userId int64) string {
	return m.opt.prefix + ":user:" + strconv.FormatInt(userId, 10)
}

// defaultGenerator 使用 UUID v4 生成 token 字符串。
func defaultGenerator() (string, error) {
	u, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	return u.String(), nil
}
