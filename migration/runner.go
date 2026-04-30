package migration

import (
	"context"
	"fmt"
	"io"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/migrate"
)

// 支持的 action 名称。migrate↔up / rollback↔down 是别名 —— 前者是 bun 原生用词,
// 后者是业内更习惯的写法,行为完全等价。未知 action 返回 error,不 fallback,避免误操作。
const (
	ActionInit     = "init"
	ActionMigrate  = "migrate"
	ActionUp       = "up"
	ActionRollback = "rollback"
	ActionDown     = "down"
	ActionStatus   = "status"
)

// IsAction 判断字符串是否是 Run 可识别的 action。CLI dispatcher 用它做白名单,
// 决定 argv 是迁移子命令还是别的什么。
func IsAction(action string) bool {
	switch action {
	case ActionInit, ActionMigrate, ActionUp, ActionRollback, ActionDown, ActionStatus:
		return true
	}
	return false
}

// Run 对给定 db 执行一个迁移动作,文本结果写到 out(通常是 os.Stdout)。
//
// 调用契约:dbName 仅用于错误信息,*bun.DB 由调用方按 dbName 从外部容器/配置取出,
// 本函数只负责"对这个 db 跑这个 action"。
//
// 未注册的 dbName、未知 action 都返回 error,不静默跳过。
func (s *Set) Run(ctx context.Context, db *bun.DB, dbName, action string, out io.Writer) error {
	ms, ok := s.For(dbName)
	if !ok {
		return fmt.Errorf("migration: no migrations registered for db %q (available: %v)", dbName, s.Names())
	}
	m := migrate.NewMigrator(db, ms)

	switch action {
	case ActionInit:
		if err := m.Init(ctx); err != nil {
			return fmt.Errorf("init: %w", err)
		}
		fmt.Fprintln(out, "migrations table initialized")

	case ActionMigrate, ActionUp:
		group, err := m.Migrate(ctx)
		if err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
		if group.IsZero() {
			fmt.Fprintln(out, "nothing to migrate")
			return nil
		}
		fmt.Fprintf(out, "migrated: %s\n", group)

	case ActionRollback, ActionDown:
		group, err := m.Rollback(ctx)
		if err != nil {
			return fmt.Errorf("rollback: %w", err)
		}
		if group.IsZero() {
			fmt.Fprintln(out, "nothing to rollback")
			return nil
		}
		fmt.Fprintf(out, "rolled back: %s\n", group)

	case ActionStatus:
		ms, err := m.MigrationsWithStatus(ctx)
		if err != nil {
			return fmt.Errorf("status: %w", err)
		}
		fmt.Fprintf(out, "applied:   %s\n", ms.Applied())
		fmt.Fprintf(out, "unapplied: %s\n", ms.Unapplied())
		fmt.Fprintf(out, "last:      %s\n", ms.LastGroup())

	default:
		return fmt.Errorf("migration: unknown action %q (supported: init, migrate/up, rollback/down, status)", action)
	}
	return nil
}
