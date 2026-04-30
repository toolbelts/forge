package provider

import (
	"context"
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/toolbelts/forge/ioc"
)

// BuildInfo 构建信息，从 Go 运行时动态读取 VCS 元数据。
type BuildInfo struct {
	GoVersion string     // Go 编译器版本，如 "go1.26"
	Module    string     // 主模块路径，如 "gp"
	Version   string     // VCS 标签或 "(devel)"
	Revision  string     // git commit hash
	Time      *time.Time // commit 时间
	Dirty     bool       // 工作区是否有未提交修改
}

// Short 返回简短版本字符串，如 "v1.0.0 (abc1234)" 或 "(devel) (abc1234 dirty)"。
func (b *BuildInfo) Short() string {
	version := b.Version
	if version == "" {
		version = "(unknown)"
	}
	if b.Revision == "" {
		return version
	}
	rev := b.Revision
	if len(rev) > 7 {
		rev = rev[:7]
	}
	if b.Dirty {
		return fmt.Sprintf("%s (%s dirty)", version, rev)
	}
	return fmt.Sprintf("%s (%s)", version, rev)
}

// String 返回完整构建信息字符串。
func (b *BuildInfo) String() string {
	return fmt.Sprintf("%s %s/%s dirty=%t go=%s",
		b.Version, b.Module, b.Revision, b.Dirty, b.GoVersion)
}

// InstanceId 返回服务实例标识，拼接 hostname 与 commit short，二者均缺失时用 uuid 兜底。
// 同 hostname 多副本部署时 commit short 一致也可接受，主要靠 service.name 和时间在 UI 上区分。
func (b *BuildInfo) InstanceId() string {
	host, _ := os.Hostname()
	host = strings.TrimSpace(host)
	rev := b.Revision
	if len(rev) > 7 {
		rev = rev[:7]
	}
	switch {
	case host != "" && rev != "":
		return host + "@" + rev
	case host != "":
		return host
	case rev != "":
		return rev
	default:
		return uuid.NewString()
	}
}

// BuildProvider 构建信息提供者，在 Register 阶段读取 VCS 信息并注册到容器。
type BuildProvider struct{}

// Register 读取 runtime/debug 中的构建信息并绑定到容器。
func (p *BuildProvider) Register(ctx context.Context) error {
	bi := &BuildInfo{}
	if info, ok := debug.ReadBuildInfo(); ok {
		bi.GoVersion = info.GoVersion
		bi.Module = info.Main.Path
		bi.Version = info.Main.Version
		for _, s := range info.Settings {
			switch s.Key {
			case "vcs.revision":
				bi.Revision = s.Value
			case "vcs.time":
				if t, err := time.Parse(time.RFC3339, s.Value); err == nil {
					bi.Time = &t
				}
			case "vcs.modified":
				bi.Dirty = s.Value == "true"
			}
		}
	}

	ioc.MustInstance(ctx, bi)

	ev := log.Info().
		Str("version", bi.Version).
		Str("revision", bi.Revision).
		Str("go", bi.GoVersion).
		Bool("dirty", bi.Dirty)
	if bi.Time != nil {
		ev = ev.Time("build_time", *bi.Time)
	}
	ev.Msg("build info loaded")

	return nil
}

// Setup 无操作。
func (p *BuildProvider) Setup(ctx context.Context) error {
	return nil
}

// MustGetBuildInfo 从容器获取构建信息，缺失时 panic。
func MustGetBuildInfo(ctx context.Context) *BuildInfo {
	return ioc.MustGet[*BuildInfo](ctx)
}
