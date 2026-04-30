package ioc_test

import (
	"context"
	"errors"
	"testing"

	"github.com/toolbelts/forge/ioc"
)

// TestExportedErrorsCanBeMatched 验证外部包可以用 errors.Is 判断 ioc 错误。
func TestExportedErrorsCanBeMatched(t *testing.T) {
	if _, err := ioc.Get[int](context.Background()); !errors.Is(err, ioc.ErrContainerNotFound) {
		t.Fatalf("expected exported container error, got %v", err)
	}

	ctx := ioc.NewContainer().WithContext(context.Background())
	if _, err := ioc.Get[int](ctx); !errors.Is(err, ioc.ErrBindingNotFound) {
		t.Fatalf("expected exported binding error, got %v", err)
	}
}
