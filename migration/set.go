package migration

import (
	"errors"
	"fmt"
	"io/fs"
	"sort"

	"github.com/uptrace/bun/migrate"
)

// ErrEmpty fs 顶层没有任何子目录时返回。空集合属于配置错误而非"什么都没要做",
// 显式报错让调用方第一时间发现 embed 路径写错或 SQL 文件忘了 commit。
var ErrEmpty = errors.New("migration: no db subdirectories found")

// Set 按 db 名映射到该 db 的迁移集合。零值不可用,必须经 New 构造。
type Set struct {
	byName map[string]*migrate.Migrations
}

// New 扫描 fsys 顶层的每个目录,把目录名当作 db 名构造 Set。
//
// 每个子目录走 bun migrate.Discover —— 内部按文件名正则
// `^(\d{1,14})_([0-9a-z_\-]+)\.(up|down)\.sql$` 提取版本号和方向。
// 命名不合法、up/down 不配对都会让 Discover 返回错误,New 透传。
//
// 顶层非目录条目(README、.gitkeep 等)被忽略,允许 embed 时混入说明文件。
// fsys 没有任何子目录时返回 ErrEmpty,见包级注释。
func New(fsys fs.FS) (*Set, error) {
	if fsys == nil {
		return nil, errors.New("migration: fsys is nil")
	}

	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("migration: read fs root: %w", err)
	}

	byName := make(map[string]*migrate.Migrations)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		sub, err := fs.Sub(fsys, e.Name())
		if err != nil {
			return nil, fmt.Errorf("migration: sub %s: %w", e.Name(), err)
		}
		ms := migrate.NewMigrations()
		if err := ms.Discover(sub); err != nil {
			return nil, fmt.Errorf("migration: discover %s: %w", e.Name(), err)
		}
		byName[e.Name()] = ms
	}

	if len(byName) == 0 {
		return nil, ErrEmpty
	}

	return &Set{byName: byName}, nil
}

// For 返回给定 db 名的迁移集合;未注册时 ok=false。
func (s *Set) For(dbName string) (*migrate.Migrations, bool) {
	ms, ok := s.byName[dbName]
	return ms, ok
}

// Names 返回已注册的 db 名,字典序排序,便于错误信息和日志稳定输出。
func (s *Set) Names() []string {
	out := make([]string, 0, len(s.byName))
	for name := range s.byName {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
