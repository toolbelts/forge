package migration

import (
	"errors"
	"reflect"
	"testing"
	"testing/fstest"
)

// validUp / validDown 一个可被 bun migrate.Discover 识别的最小 up/down 对。
// bun 的版本号正则要求 1~14 位数字 + 下划线 + 描述,描述只能 [0-9a-z_\-]。
const (
	validUpName   = "20260429120000_init.up.sql"
	validDownName = "20260429120000_init.down.sql"
)

func TestNew_DiscoverMultipleDbs(t *testing.T) {
	t.Parallel()

	fsys := fstest.MapFS{
		"default/" + validUpName:   {Data: []byte("SELECT 1;")},
		"default/" + validDownName: {Data: []byte("SELECT 1;")},
		"user/" + validUpName:      {Data: []byte("SELECT 2;")},
		"user/" + validDownName:    {Data: []byte("SELECT 2;")},
		"README.md":                {Data: []byte("doc")}, // 顶层非目录条目应被忽略
	}

	set, err := New(fsys)
	if err != nil {
		t.Fatalf("New: unexpected error: %v", err)
	}

	got := set.Names()
	want := []string{"default", "user"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Names = %v, want %v", got, want)
	}

	for _, name := range want {
		ms, ok := set.For(name)
		if !ok {
			t.Errorf("For(%q): missing", name)
			continue
		}
		if got, want := len(ms.Sorted()), 1; got != want {
			t.Errorf("For(%q): got %d migrations, want %d", name, got, want)
		}
	}

	if _, ok := set.For("nope"); ok {
		t.Errorf("For(\"nope\"): expected ok=false")
	}
}

func TestNew_EmptyReturnsErrEmpty(t *testing.T) {
	t.Parallel()

	// 顶层只有非目录条目,没有任何 db 子目录。
	fsys := fstest.MapFS{
		"README.md": {Data: []byte("doc")},
	}

	_, err := New(fsys)
	if !errors.Is(err, ErrEmpty) {
		t.Fatalf("New: got %v, want ErrEmpty", err)
	}
}

func TestNew_NilFsys(t *testing.T) {
	t.Parallel()

	if _, err := New(nil); err == nil {
		t.Fatalf("New(nil): expected error, got nil")
	}
}

func TestNew_BadFilenameSurfaces(t *testing.T) {
	t.Parallel()

	// 文件名不符合 bun migrate 的命名约束(无版本号前缀),Discover 应报错。
	fsys := fstest.MapFS{
		"default/init.up.sql":   {Data: []byte("SELECT 1;")},
		"default/init.down.sql": {Data: []byte("SELECT 1;")},
	}

	if _, err := New(fsys); err == nil {
		t.Fatalf("New: expected discover error for malformed filename, got nil")
	}
}

func TestIsAction(t *testing.T) {
	t.Parallel()

	cases := map[string]bool{
		"init":     true,
		"migrate":  true,
		"up":       true,
		"rollback": true,
		"down":     true,
		"status":   true,
		"":         false,
		"foo":      false,
		"INIT":     false, // 大小写敏感,避免吞掉用户笔误
	}
	for action, want := range cases {
		if got := IsAction(action); got != want {
			t.Errorf("IsAction(%q) = %v, want %v", action, got, want)
		}
	}
}
