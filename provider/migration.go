package provider

import (
	"context"
	"fmt"
	"io/fs"

	"github.com/rs/zerolog/log"

	"github.com/toolbelts/forge/ioc"
	"github.com/toolbelts/forge/migration"
)

// MigrationProvider 迁移集合提供者。Register 阶段从 fs.FS 发现按 db 名分组的迁移,
// 校验子目录与 database.<name> 配置一致后,把 *migration.Set 注入到容器。
//
// 仅在 cmd/gpd/migrate.go 子命令路径下被使用 —— 正常 serve 流程不会注册本 Provider,
// 因为 Set 跟运行期服务无关,留着只是死代码 + 多一次 embed 扫描。要做管理端
// /admin/migrations/status 之类端点时再把本 Provider 加回 main.go 的 Use 列表。
//
// 编排约定:
//   - 排在 DatabaseProvider 之后,语义上 Set 与 db 是配套的。功能上无强约束,因
//     Register 不真正访问 db,只读 viper + embed FS。
//   - 没有 Shutdowner —— Set 持有的是纯内存数据,无资源需要释放。
//
// 跨项目复用:本 Provider 用 gp 内部的 ioc / config 包,无法直接照搬。其它项目按
// 需要复用 pkg/migration 自行实现一个等效 Provider 即可。
type MigrationProvider struct {
	fsys fs.FS
	set  *migration.Set
}

// NewMigrationProvider 用给定 fs.FS(通常是项目根 migrations.FS)构造 Provider。
// fsys 为 nil 会在 Register 阶段失败,而非构造时,以便集中由生命周期日志报错。
func NewMigrationProvider(fsys fs.FS) *MigrationProvider {
	return &MigrationProvider{fsys: fsys}
}

// Register 解析 fsys 构建 Set,并校验每个迁移子目录都对应一个已配置的
// database.<name>。若有 SQL 子目录但 database 里没配,直接报错 —— 这是常见的
// 拼写错误兜底(目录名与配置 key 必须严格一致)。反向缺失(database 配了但还
// 没写迁移)不报错,允许新 db 处于"空迁移"过渡态。
func (p *MigrationProvider) Register(ctx context.Context) error {
	set, err := migration.New(p.fsys)
	if err != nil {
		return fmt.Errorf("migration: build set: %w", err)
	}

	v := MustGetViper(ctx)
	dbMap := v.GetStringMap("database")
	for _, name := range set.Names() {
		if _, ok := dbMap[name]; !ok {
			return fmt.Errorf("migration: subdir %q has no matching database.%s config", name, name)
		}
	}

	p.set = set
	ioc.MustInstance(ctx, set)
	log.Ctx(ctx).Info().Strs("dbs", set.Names()).Msg("migration set registered")
	return nil
}

// Setup 无操作,Set 已在 Register 阶段注入容器。
func (p *MigrationProvider) Setup(ctx context.Context) error {
	return nil
}

// MustGetMigrationSet 容器获取迁移集合,缺失时 panic。
func MustGetMigrationSet(ctx context.Context) *migration.Set {
	return ioc.MustGet[*migration.Set](ctx)
}
