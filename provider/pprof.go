package provider

import (
	"cmp"
	"context"
	nhpprof "net/http/pprof"
	"runtime"
	runtimepprof "runtime/pprof"

	"github.com/rs/zerolog/log"
)

// PprofProvider 性能分析提供者，在业务 *http.ServeMux 上挂载 net/http/pprof 路由，
// 同时按配置打开 mutex / block profile 采样。
//
// 编排约定：
//   - 实际生效条件是 http.enabled && pprof.enabled，任一为 false 即跳过挂载。
//     pprof 复用 HttpProvider 的 mux 与 server，http 未启用时静默禁用而非 panic，
//     便于不同环境共用同一份配置。
//   - 必须排在 HttpProvider 之后：先有 mux，才能挂 pprof。HttpProvider 要读中间件链，
//     不会被 MustGetHttpMux 提前拉起，顺序不对时启动直接报 binding not found。
//
// 安全提示：pprof 与业务 HTTP 共端口，生产环境应通过反向代理 / ACL 屏蔽
// pprof.path_prefix 配置的路径，或通过 pprof.enabled 关闭。
//
// 路由：挂载点完全由 pprof.path_prefix 决定（默认 /debug/pprof/）。不使用
// `import _ "net/http/pprof"` 副作用注册到 http.DefaultServeMux，避免污染全局 mux。
// 因为 nhpprof.Index 内部硬编码 /debug/pprof/ 来分发子 profile，自定义前缀时需要
// 显式给每个 runtime/pprof 注册的 profile（heap / goroutine / allocs / threadcreate /
// block / mutex 及业务自定义）单独 mount，再单独挂 cmdline / profile / symbol / trace。
// Index 渲染的 HTML 使用相对链接，所以在任意前缀下都能跳转到对应子路由。
//
// 默认采样：mutex_fraction=100、block_rate=10000(ns)，开箱可用且开销 < 1%。
// 显式写 0 可关闭对应采样。threadcreate 路由由 runtime/pprof 默认注册但数据始终
// 为空（golang/go#6104，Go runtime 自身缺陷，与本框架无关）。
type PprofProvider struct{}

// Setup 读 pprof.* 与 http.enabled，两者都为 true 时拿 *http.ServeMux 注册 pprof handler，并按需打开 runtime 采样。
// http 未启用时直接静默跳过，不做副作用；启用时 HttpProvider 已把 mux 注入容器，MustGetHttpMux 必能命中。
func (p *PprofProvider) Setup(ctx context.Context) error {
	v := MustGetViper(ctx)
	pprofOn := v.GetBool("pprof.enabled")
	httpOn := v.GetBool("http.enabled")
	if !pprofOn || !httpOn {
		log.Ctx(ctx).Info().
			Str("provider", "pprof").
			Bool("pprof_enabled", pprofOn).
			Bool("http_enabled", httpOn).
			Msg("pprof disabled, skip register")
		return nil
	}

	mountPrefix := cmp.Or(v.GetString("pprof.path_prefix"), "/debug/pprof/")
	// GetInt 缺失时返回 0 会误关采样,所以默认非零的开关必须 IsSet 守卫;
	// 显式配 0 表示用户主动关闭采样,予以保留。
	mutexFraction, blockRate := 100, 10000
	if v.IsSet("pprof.mutex_fraction") {
		mutexFraction = v.GetInt("pprof.mutex_fraction")
	}
	if v.IsSet("pprof.block_rate") {
		blockRate = v.GetInt("pprof.block_rate")
	}

	mux := MustGetHttpMux(ctx)
	mux.HandleFunc(mountPrefix, nhpprof.Index)
	mux.HandleFunc(mountPrefix+"cmdline", nhpprof.Cmdline)
	mux.HandleFunc(mountPrefix+"profile", nhpprof.Profile)
	mux.HandleFunc(mountPrefix+"symbol", nhpprof.Symbol)
	mux.HandleFunc(mountPrefix+"trace", nhpprof.Trace)
	for _, prof := range runtimepprof.Profiles() {
		name := prof.Name()
		mux.Handle(mountPrefix+name, nhpprof.Handler(name))
	}

	if mutexFraction > 0 {
		runtime.SetMutexProfileFraction(mutexFraction)
	}
	if blockRate > 0 {
		runtime.SetBlockProfileRate(blockRate)
	}

	log.Ctx(ctx).Info().
		Str("mount", mountPrefix).
		Int("mutex_fraction", mutexFraction).
		Int("block_rate", blockRate).
		Msg("pprof routes registered")
	return nil
}
