package provider

import (
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
//   - 排在 HttpProvider 之后，语义上"先有 mux，后挂 pprof"。功能上无强约束，
//     因为 ioc 是 RegisterAll → SetupAll 两阶段（见 pkg/ioc/lifecycle_test.go）。
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
type PprofProvider struct {
	enabled       bool
	mountPrefix   string
	mutexFraction int
	blockRate     int
}

// Register 读 pprof.* 与 http.enabled，仅当两者都为 true 时标记自身启用。
// http 未启用时直接静默跳过，不做副作用。
func (p *PprofProvider) Register(ctx context.Context) error {
	v := MustGetViper(ctx)
	pprofOn := v.GetBool("pprof.enabled")
	httpOn := v.GetBool("http.enabled")
	p.enabled = pprofOn && httpOn
	if !p.enabled {
		log.Ctx(ctx).Info().
			Str("provider", "pprof").
			Bool("pprof_enabled", pprofOn).
			Bool("http_enabled", httpOn).
			Msg("pprof disabled, skip register")
		return nil
	}

	p.mountPrefix = v.GetString("pprof.path_prefix")
	if p.mountPrefix == "" {
		p.mountPrefix = "/debug/pprof/"
	}
	p.mutexFraction = v.GetInt("pprof.mutex_fraction")
	p.blockRate = v.GetInt("pprof.block_rate")
	return nil
}

// Setup 拿 *http.ServeMux 注册 pprof handler；按需打开 runtime 采样。
// p.enabled 已在 Register 阶段保证 http.enabled=true，MustGetHttpMux 必能命中。
func (p *PprofProvider) Setup(ctx context.Context) error {
	if !p.enabled {
		return nil
	}

	mux := MustGetHttpMux(ctx)
	mux.HandleFunc(p.mountPrefix, nhpprof.Index)
	mux.HandleFunc(p.mountPrefix+"cmdline", nhpprof.Cmdline)
	mux.HandleFunc(p.mountPrefix+"profile", nhpprof.Profile)
	mux.HandleFunc(p.mountPrefix+"symbol", nhpprof.Symbol)
	mux.HandleFunc(p.mountPrefix+"trace", nhpprof.Trace)
	for _, prof := range runtimepprof.Profiles() {
		name := prof.Name()
		mux.Handle(p.mountPrefix+name, nhpprof.Handler(name))
	}

	if p.mutexFraction > 0 {
		runtime.SetMutexProfileFraction(p.mutexFraction)
	}
	if p.blockRate > 0 {
		runtime.SetBlockProfileRate(p.blockRate)
	}

	log.Ctx(ctx).Info().
		Str("mount", p.mountPrefix).
		Int("mutex_fraction", p.mutexFraction).
		Int("block_rate", p.blockRate).
		Msg("pprof routes registered")
	return nil
}
