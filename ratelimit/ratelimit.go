package ratelimit

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	json "github.com/goccy/go-json"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"

	"github.com/toolbelts/forge/meta"
)

const (
	PathMatchExact = "exact"
	PathMatchRegex = "regex"
)

const luaIncrWindow = `
local current = redis.call('INCR', KEYS[1])
if current == 1 then
    redis.call('PEXPIRE', KEYS[1], ARGV[1])
end
local ttl = redis.call('PTTL', KEYS[1])
return {current, ttl}
`

var counterScript = redis.NewScript(luaIncrWindow)

type FailurePolicy string

const (
	FailurePolicyFailOpen   FailurePolicy = "fail_open"
	FailurePolicyFailClosed FailurePolicy = "fail_closed"
)

// Rule 定义一条限速规则。
type Rule struct {
	Name      string   `json:"name"`
	Key       string   `json:"key"`
	Path      string   `json:"path"`
	PathMatch string   `json:"path_match"`
	Methods   []string `json:"methods"`
	Total     int64    `json:"total"`
	Window    string   `json:"window"`
}

// Quota 描述一次限速检查后的配额信息。
type Quota struct {
	Allowed    bool
	Remaining  int64
	Total      int64
	ResetAfter time.Duration
	RuleName   string
}

// RuleStore 定义规则存储的统一接口。
type RuleStore interface {
	LoadRules(ctx context.Context) ([]Rule, error)
	SetRule(ctx context.Context, rule Rule) error
	DeleteRule(ctx context.Context, name string) error
}

type options struct {
	prefix        string
	reloadPeriod  time.Duration
	failurePolicy FailurePolicy
}

var defaultOptions = options{
	prefix:        "ratelimit",
	reloadPeriod:  30 * time.Second,
	failurePolicy: FailurePolicyFailOpen,
}

// Option 定义限速器的可选配置。
type Option func(*options)

// WithPrefix 设置 Redis key 前缀。
func WithPrefix(prefix string) Option {
	return func(o *options) {
		if strings.TrimSpace(prefix) != "" {
			o.prefix = prefix
		}
	}
}

// WithReloadPeriod 设置自动刷新周期。
func WithReloadPeriod(period time.Duration) Option {
	return func(o *options) {
		if period > 0 {
			o.reloadPeriod = period
		}
	}
}

// WithFailurePolicy 设置 Redis 失败时的处理策略。
func WithFailurePolicy(policy FailurePolicy) Option {
	return func(o *options) {
		switch policy {
		case FailurePolicyFailClosed, FailurePolicyFailOpen:
			o.failurePolicy = policy
		}
	}
}

type compiledRule struct {
	rule    Rule
	metaKey string
	window  time.Duration
	methods map[string]struct{}
	regex   *regexp.Regexp
}

// Limiter 表示 Redis 驱动的限速执行器。
type Limiter struct {
	rdb   redis.UniversalClient
	opt   options
	rules atomic.Value
}

// NewLimiter 创建一个新的限速执行器。
func NewLimiter(rdb redis.UniversalClient, opts ...Option) *Limiter {
	o := defaultOptions
	for _, opt := range opts {
		opt(&o)
	}

	limiter := &Limiter{
		rdb: rdb,
		opt: o,
	}
	limiter.rules.Store([]compiledRule{})

	return limiter
}

// Reload 重新编译并替换全部规则快照。
// 单条规则编译失败时会记 warn 日志并跳过，不影响其它规则生效，整体不再返回错误。
func (l *Limiter) Reload(ctx context.Context, rules []Rule) error {
	compiled := make([]compiledRule, 0, len(rules))
	for _, rule := range rules {
		// 先将外部规则编译为运行时结构，避免请求路径上重复做解析工作。
		cr, err := compileRule(rule)
		if err != nil {
			log.Ctx(ctx).
				Warn().
				Err(err).
				Str("rule_name", rule.Name).
				Msg("ratelimit rule compile failed, skip")
			continue
		}
		compiled = append(compiled, *cr)
	}

	l.rules.Store(compiled)
	return nil
}

// Allow 根据当前规则检查本次请求是否允许通过。
func (l *Limiter) Allow(ctx context.Context) (*Quota, error) {
	if l.rdb == nil {
		return nil, ErrNilRedisClient
	}

	// 先从上下文中提取当前请求的匹配维度。
	rm := meta.Request(ctx)
	path := rm.Path
	method := strings.ToUpper(rm.Method)

	quota := &Quota{
		Allowed:   true,
		Remaining: -1,
	}

	rules := l.snapshotRules()
	if len(rules) == 0 {
		return quota, nil
	}

	matched := make([]matchedRule, 0, 2)
	for _, rule := range rules {
		// 只保留路径、方法、主体值都满足条件的规则。
		if !rule.matchPath(path) || !rule.matchMethod(method) {
			continue
		}

		subject := meta.String(ctx, rule.metaKey)
		if subject == "" {
			continue
		}

		matched = append(matched, matchedRule{
			rule:    rule,
			subject: subject,
		})
	}

	if len(matched) == 0 {
		return quota, nil
	}

	var (
		firstErr  error
		hasResult bool
	)

	for _, item := range matched {
		// 每条规则独立计数，并选择剩余配额最少的一条作为最终返回。
		currentQuota, err := l.evalRule(ctx, item)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			log.Ctx(ctx).
				Warn().
				Err(err).
				Str("rule_name", item.rule.rule.Name).
				Msg("ratelimit counter failed")
			continue
		}

		if !hasResult || currentQuota.Remaining < quota.Remaining {
			quota = currentQuota
			hasResult = true
		}

		if !currentQuota.Allowed {
			quota.Allowed = false
		}
	}

	if firstErr == nil {
		return quota, nil
	}

	// 发生 Redis 异常时，根据失败策略决定是放行还是拒绝。
	if l.opt.failurePolicy == FailurePolicyFailClosed {
		if hasResult {
			quota.Allowed = false
			return quota, firstErr
		}
		return &Quota{
			Allowed:   false,
			Remaining: 0,
		}, firstErr
	}

	if hasResult {
		return quota, firstErr
	}

	return &Quota{
		Allowed:   true,
		Remaining: -1,
	}, firstErr
}

type matchedRule struct {
	rule    compiledRule
	subject string
}

// evalRule 执行单条命中规则的 Redis 计数，并转换为配额结果。
func (l *Limiter) evalRule(ctx context.Context, item matchedRule) (*Quota, error) {
	key := l.counterKey(item.rule.rule.Name, item.subject)
	windowMs := item.rule.window.Milliseconds()
	values, err := counterScript.Run(ctx, l.rdb, []string{key}, windowMs).Int64Slice()
	if err != nil {
		return nil, err
	}
	if len(values) != 2 {
		return nil, ErrInvalidCounterResponse
	}

	remaining := item.rule.rule.Total - values[0]
	resetAfter := time.Duration(values[1]) * time.Millisecond
	if values[1] < 0 {
		resetAfter = 0
	}

	return &Quota{
		Allowed:    remaining >= 0,
		Remaining:  remaining,
		Total:      item.rule.rule.Total,
		ResetAfter: resetAfter,
		RuleName:   item.rule.rule.Name,
	}, nil
}

// counterKey 生成规则主体维度的计数 key。
func (l *Limiter) counterKey(ruleName string, subject string) string {
	return fmt.Sprintf("%s:counter:%s:%s", l.opt.prefix, ruleName, hashSubject(subject))
}

// snapshotRules 读取当前生效的不可变规则快照。
func (l *Limiter) snapshotRules() []compiledRule {
	stored := l.rules.Load()
	if stored == nil {
		return nil
	}
	rules, _ := stored.([]compiledRule)
	return rules
}

// matchPath 判断请求路径是否满足规则定义的匹配方式。
func (cr *compiledRule) matchPath(path string) bool {
	switch cr.rule.PathMatch {
	case PathMatchExact:
		return path == cr.rule.Path
	case PathMatchRegex:
		return cr.regex.MatchString(path)
	default:
		return false
	}
}

// matchMethod 判断请求方法是否命中规则的方法过滤。
func (cr *compiledRule) matchMethod(method string) bool {
	if len(cr.methods) == 0 {
		return true
	}
	_, ok := cr.methods[strings.ToUpper(method)]
	return ok
}

// compileRule 将外部规则校验并编译为运行时结构。
func compileRule(rule Rule) (*compiledRule, error) {
	rule.Name = strings.TrimSpace(rule.Name)
	rule.Key = strings.TrimSpace(rule.Key)
	rule.Path = strings.TrimSpace(rule.Path)
	rule.PathMatch = strings.ToLower(strings.TrimSpace(rule.PathMatch))

	if rule.Name == "" {
		return nil, fmt.Errorf("%w: empty name", ErrInvalidRule)
	}
	if rule.Key == "" {
		return nil, fmt.Errorf("%w: empty key", ErrInvalidRule)
	}
	if rule.Path == "" {
		return nil, fmt.Errorf("%w: empty path", ErrInvalidRule)
	}
	if rule.Total <= 0 {
		return nil, fmt.Errorf("%w: total must be greater than 0", ErrInvalidRule)
	}
	if rule.PathMatch != PathMatchExact && rule.PathMatch != PathMatchRegex {
		return nil, fmt.Errorf("%w: invalid path match %q", ErrInvalidRule, rule.PathMatch)
	}

	window, err := time.ParseDuration(rule.Window)
	if err != nil || window <= 0 {
		return nil, fmt.Errorf("%w: invalid window %q", ErrInvalidRule, rule.Window)
	}

	// 统一方法大小写和去重，减少请求路径上的重复处理。
	methods := normalizeMethods(rule.Methods)
	compiled := &compiledRule{
		rule:    rule,
		metaKey: resolveMetaKey(rule.Key),
		window:  window,
		methods: make(map[string]struct{}, len(methods)),
	}

	for _, method := range methods {
		compiled.methods[method] = struct{}{}
	}
	rule.Methods = methods
	compiled.rule.Methods = methods

	if rule.PathMatch == PathMatchRegex {
		// 正则规则在加载阶段预编译，避免 Allow 时重复编译。
		regex, err := regexp.Compile("^" + rule.Path + "$")
		if err != nil {
			return nil, fmt.Errorf("%w: invalid regex path %q", ErrInvalidRule, rule.Path)
		}
		compiled.regex = regex
	}

	return compiled, nil
}

var keyMapping = map[string]string{
	"ip":        meta.MetaUserIp,
	"device_id": meta.MetaDeviceId,
	"token":     meta.MetaToken,
	"user_id":   meta.MetaUserId,
	"platform":  meta.MetaPlatform,
}

// resolveMetaKey 将规则中的短 key 映射为上下文中的标准元数据 key。
func resolveMetaKey(key string) string {
	normalized := strings.ToLower(strings.TrimSpace(key))
	if mapped, ok := keyMapping[normalized]; ok {
		return mapped
	}
	return normalized
}

// normalizeMethods 对方法列表进行清洗、转大写和去重。
func normalizeMethods(methods []string) []string {
	normalized := make([]string, 0, len(methods))
	seen := make(map[string]struct{}, len(methods))
	for _, method := range methods {
		method = strings.ToUpper(strings.TrimSpace(method))
		if method == "" {
			continue
		}
		if _, ok := seen[method]; ok {
			continue
		}
		seen[method] = struct{}{}
		normalized = append(normalized, method)
	}
	slices.Sort(normalized)
	return normalized
}

// hashSubject 对主体值做稳定哈希，避免 Redis key 过长。
func hashSubject(subject string) string {
	sum := sha1.Sum([]byte(subject))
	return hex.EncodeToString(sum[:])
}

// RedisRuleStore 提供基于 Redis Hash 的规则存储实现。
type RedisRuleStore struct {
	rdb redis.UniversalClient
	opt options
}

// NewRedisRuleStore 创建一个 Redis 规则存储。
func NewRedisRuleStore(rdb redis.UniversalClient, opts ...Option) *RedisRuleStore {
	o := defaultOptions
	for _, opt := range opts {
		opt(&o)
	}

	return &RedisRuleStore{
		rdb: rdb,
		opt: o,
	}
}

// LoadRules 从 Redis 中加载全部规则。
func (s *RedisRuleStore) LoadRules(ctx context.Context) ([]Rule, error) {
	if s.rdb == nil {
		return nil, ErrNilRedisClient
	}

	values, err := s.rdb.HGetAll(ctx, s.rulesKey()).Result()
	if err != nil {
		return nil, err
	}

	names := slices.Sorted(maps.Keys(values))

	rules := make([]Rule, 0, len(values))
	for _, name := range names {
		// 单条坏数据只跳过自身，避免影响整批规则加载。
		raw := values[name]
		var rule Rule
		if err := json.Unmarshal([]byte(raw), &rule); err != nil {
			log.Ctx(ctx).
				Warn().
				Err(err).
				Str("rule_name", name).
				Msg("ratelimit rule decode failed")
			continue
		}
		// 语义校验交给 Limiter.Reload，避免双重 compile 与重复 warn。
		rules = append(rules, rule)
	}

	return rules, nil
}

// SetRule 将规则写入 Redis Hash。
func (s *RedisRuleStore) SetRule(ctx context.Context, rule Rule) error {
	if s.rdb == nil {
		return ErrNilRedisClient
	}

	compiled, err := compileRule(rule)
	if err != nil {
		return err
	}

	payload, err := json.Marshal(compiled.rule)
	if err != nil {
		return err
	}

	return s.rdb.HSet(ctx, s.rulesKey(), compiled.rule.Name, string(payload)).Err()
}

// DeleteRule 从 Redis Hash 中删除指定规则。
func (s *RedisRuleStore) DeleteRule(ctx context.Context, name string) error {
	if s.rdb == nil {
		return ErrNilRedisClient
	}

	return s.rdb.HDel(ctx, s.rulesKey(), name).Err()
}

// rulesKey 返回规则集合在 Redis 中的 Hash key。
func (s *RedisRuleStore) rulesKey() string {
	return s.opt.prefix + ":rules"
}

// RedisRateLimiter 组合规则存储、执行器与自动刷新逻辑。
type RedisRateLimiter struct {
	limiter *Limiter
	store   *RedisRuleStore
	opt     options
	cancel  context.CancelFunc
}

// NewRedisRateLimiter 创建一个带自动加载能力的限速器。
func NewRedisRateLimiter(ctx context.Context, rdb redis.UniversalClient, opts ...Option) (*RedisRateLimiter, error) {
	o := defaultOptions
	for _, opt := range opts {
		opt(&o)
	}

	limiter := &Limiter{rdb: rdb, opt: o}
	limiter.rules.Store([]compiledRule{})

	rr := &RedisRateLimiter{
		limiter: limiter,
		store:   &RedisRuleStore{rdb: rdb, opt: o},
		opt:     o,
	}

	if err := rr.Reload(ctx); err != nil {
		return nil, err
	}

	reloadCtx, cancel := context.WithCancel(context.Background())
	rr.cancel = cancel
	go rr.reloadLoop(reloadCtx)

	return rr, nil
}

// Allow 调用内部执行器进行限速判断。
func (r *RedisRateLimiter) Allow(ctx context.Context) (*Quota, error) {
	return r.limiter.Allow(ctx)
}

// Reload 从存储层重新加载规则。
func (r *RedisRateLimiter) Reload(ctx context.Context) error {
	// 先从存储层取回原始规则，再交给执行器统一编译并替换快照。
	rules, err := r.store.LoadRules(ctx)
	if err != nil {
		return err
	}
	return r.limiter.Reload(ctx, rules)
}

// SetRule 写入规则并立即刷新快照。
func (r *RedisRateLimiter) SetRule(ctx context.Context, rule Rule) error {
	if err := r.store.SetRule(ctx, rule); err != nil {
		return err
	}
	return r.Reload(ctx)
}

// DeleteRule 删除规则并立即刷新快照。
func (r *RedisRateLimiter) DeleteRule(ctx context.Context, name string) error {
	if err := r.store.DeleteRule(ctx, name); err != nil {
		return err
	}
	return r.Reload(ctx)
}

// Close 停止自动刷新协程。
func (r *RedisRateLimiter) Close() {
	if r.cancel != nil {
		r.cancel()
	}
}

// reloadLoop 按配置周期刷新规则快照。
func (r *RedisRateLimiter) reloadLoop(ctx context.Context) {
	ticker := time.NewTicker(r.opt.reloadPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 定时刷新失败只记录日志，不主动中断已有规则的服务能力。
			if err := r.Reload(ctx); err != nil {
				log.Ctx(ctx).Warn().Err(err).Msg("ratelimit reload failed")
			}
		}
	}
}
