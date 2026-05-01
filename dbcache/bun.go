package dbcache

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"github.com/uptrace/bun"
)

// NewBun 是 bun ORM 的便利入口:从 *bun.DB 自动反射 V 的主键,
// 用 bun 占位符 ?PKs 拼出 LoaderFunc 与 BatchLoaderFunc,免去用户手写。
//
// 使用约束:
//   - V 必须已注册到 bun(即 db.RegisterModel((*V)(nil)) 或被某次 SQL 自动注册)。
//   - V 必须是单主键模型;复合主键直接 panic。复合主键场景一般不走 cache,
//     业务应另行设计。
//
// 行为:
//   - LoaderFunc:SELECT * FROM <table> WHERE <pk> = ?
//   - BatchLoaderFunc:SELECT * FROM <table> WHERE <pk> IN (?)
//   - sql.ErrNoRows 自动映射为 dbcache.ErrNotFound;其它错原样传播(不静默)。
//
// 失效:用户业务侧的失效仍然在自家 model 的 AfterUpdate/AfterDelete hook 里调 Cache.Delete。
// bun hook 是 model 上的方法,无法非侵入挂载。
func NewBun[K comparable, V any](db *bun.DB, opts ...Option) *Cache[K, V] {
	if db == nil {
		panic("dbcache: NewBun: nil db")
	}

	var zero V
	t := reflect.TypeOf(zero)
	if t == nil {
		panic("dbcache: NewBun: V must be a concrete type")
	}
	table := db.Table(t)
	if table == nil {
		panic(fmt.Sprintf("dbcache: NewBun: model %s is not registered with bun", t.String()))
	}
	if len(table.PKs) == 0 {
		panic(fmt.Sprintf("dbcache: NewBun: model %s has no primary key", t.String()))
	}
	if len(table.PKs) > 1 {
		panic(fmt.Sprintf("dbcache: NewBun: model %s has composite primary key (%d cols), unsupported",
			t.String(), len(table.PKs)))
	}
	pkField := table.PKs[0]

	loader := func(ctx context.Context, key K) (V, error) {
		var v V
		err := db.NewSelect().Model(&v).Where("?PKs = ?", key).Scan(ctx)
		if errors.Is(err, sql.ErrNoRows) {
			return v, ErrNotFound
		}
		if err != nil {
			return v, err
		}
		return v, nil
	}

	batchLoader := func(ctx context.Context, keys []K) (map[K]V, error) {
		if len(keys) == 0 {
			return map[K]V{}, nil
		}
		var vs []V
		if err := db.NewSelect().Model(&vs).Where("?PKs IN (?)", bun.List(keys)).Scan(ctx); err != nil {
			return nil, err
		}
		out := make(map[K]V, len(vs))
		for i := range vs {
			pkVal := pkField.Value(reflect.ValueOf(&vs[i]).Elem()).Interface()
			k, ok := pkVal.(K)
			if !ok {
				return nil, fmt.Errorf("dbcache: NewBun: pk type %T does not match generic K", pkVal)
			}
			out[k] = vs[i]
		}
		return out, nil
	}

	full := make([]Option, 0, len(opts)+1)
	full = append(full, WithBatchLoader(batchLoader))
	full = append(full, opts...)
	return New(loader, full...)
}
