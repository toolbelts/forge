package dbcache

import (
	"database/sql"
	"testing"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

// 测试模型,只用于 schema 反射:不会真实跑 SQL。
type testModelSinglePK struct {
	bun.BaseModel `bun:"table:t_single"`

	ID   int64  `bun:",pk,autoincrement"`
	Name string `bun:"name"`
}

type testModelStrPK struct {
	bun.BaseModel `bun:"table:t_str"`

	Code string `bun:",pk"`
	Val  int    `bun:"val"`
}

type testModelComposite struct {
	bun.BaseModel `bun:"table:t_comp"`

	A int `bun:",pk"`
	B int `bun:",pk"`
}

type testModelNoPK struct {
	bun.BaseModel `bun:"table:t_nopk"`

	Foo string `bun:"foo"`
}

// newSchemaDB 构造一个仅用于 schema 反射的 *bun.DB。底层 *sql.DB 为 nil,
// 不会真正查询;NewBun 在反射阶段不需要 sql 连接。
func newSchemaDB(t *testing.T, models ...any) *bun.DB {
	t.Helper()
	db := bun.NewDB((*sql.DB)(nil), pgdialect.New())
	db.RegisterModel(models...)
	return db
}

func TestNewBun_SinglePK(t *testing.T) {
	t.Parallel()
	db := newSchemaDB(t, (*testModelSinglePK)(nil))
	cache := NewBun[int64, testModelSinglePK](db)
	if cache == nil {
		t.Fatal("got nil cache")
	}
	if cache.batchLoader == nil {
		t.Fatal("NewBun should set BatchLoader")
	}
}

func TestNewBun_StringPK(t *testing.T) {
	t.Parallel()
	db := newSchemaDB(t, (*testModelStrPK)(nil))
	cache := NewBun[string, testModelStrPK](db)
	if cache == nil {
		t.Fatal("got nil cache")
	}
}

func TestNewBun_NilDBPanics(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on nil db")
		}
	}()
	NewBun[int64, testModelSinglePK](nil)
}

func TestNewBun_CompositePKPanics(t *testing.T) {
	t.Parallel()
	db := newSchemaDB(t, (*testModelComposite)(nil))
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on composite primary key")
		}
	}()
	NewBun[int, testModelComposite](db)
}

func TestNewBun_NoPKPanics(t *testing.T) {
	t.Parallel()
	db := newSchemaDB(t, (*testModelNoPK)(nil))
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on no primary key")
		}
	}()
	NewBun[int, testModelNoPK](db)
}

func TestNewBun_OptionsApplied(t *testing.T) {
	t.Parallel()
	db := newSchemaDB(t, (*testModelSinglePK)(nil))
	store := NewMemoryStore(50)
	cache := NewBun[int64, testModelSinglePK](db,
		WithStore(store),
		WithKeyPrefix("t:"),
	)
	if cache.keyPrefix != "t:" {
		t.Fatalf("keyPrefix not applied: %q", cache.keyPrefix)
	}
	if cache.store != store {
		t.Fatal("store not applied")
	}
}
