package ioc

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

// TestContainerWithContextAndInstance 验证容器上下文、默认实例和命名实例。
func TestContainerWithContextAndInstance(t *testing.T) {
	container := NewContainer()
	ctx := container.WithContext(context.Background())

	gotContainer, ok := FromContext(ctx)
	if !ok || gotContainer != container {
		t.Fatal("expected container from context")
	}

	if err := Instance(ctx, containerTestService{id: 10}); err != nil {
		t.Fatalf("bind instance failed: %v", err)
	}
	if err := InstanceNamed(ctx, "primary", containerTestService{id: 20}); err != nil {
		t.Fatalf("bind named instance failed: %v", err)
	}
	if err := InstanceNamed[*containerTestService](ctx, "nil", nil); err != nil {
		t.Fatalf("bind typed nil instance failed: %v", err)
	}

	if !Has[containerTestService](ctx) {
		t.Fatal("expected default binding")
	}
	if !HasNamed[containerTestService](ctx, "primary") {
		t.Fatal("expected named binding")
	}

	defaultValue, err := Get[containerTestService](ctx)
	if err != nil {
		t.Fatalf("get default failed: %v", err)
	}
	if defaultValue.id != 10 {
		t.Fatalf("expected default id 10, got %d", defaultValue.id)
	}

	namedValue, err := GetNamed[containerTestService](ctx, "primary")
	if err != nil {
		t.Fatalf("get named failed: %v", err)
	}
	if namedValue.id != 20 {
		t.Fatalf("expected named id 20, got %d", namedValue.id)
	}

	nilValue, err := GetNamed[*containerTestService](ctx, "nil")
	if err != nil {
		t.Fatalf("get typed nil failed: %v", err)
	}
	if nilValue != nil {
		t.Fatalf("expected typed nil, got %#v", nilValue)
	}
}

// TestContainerInstanceUsesInstanceKind 验证实例绑定走独立的无锁解析分支。
func TestContainerInstanceUsesInstanceKind(t *testing.T) {
	container := NewContainer()
	ctx := container.WithContext(context.Background())

	if err := Instance(ctx, containerTestService{id: 10}); err != nil {
		t.Fatalf("bind instance failed: %v", err)
	}

	item, ok := container.getBinding(keyOf[containerTestService](""))
	if !ok {
		t.Fatal("expected instance binding")
	}
	if item.kind != bindingInstance {
		t.Fatalf("expected instance binding kind, got %v", item.kind)
	}
}

// TestContainerFactories 验证 transient 每次执行、singleton 只执行一次。
func TestContainerFactories(t *testing.T) {
	ctx := NewContainer().WithContext(context.Background())

	transientCalls := 0
	if err := Bind(ctx, func(ctx context.Context) (int, error) {
		transientCalls++
		return transientCalls, nil
	}); err != nil {
		t.Fatalf("bind transient failed: %v", err)
	}

	firstTransient, err := Get[int](ctx)
	if err != nil {
		t.Fatalf("get first transient failed: %v", err)
	}
	secondTransient, err := Get[int](ctx)
	if err != nil {
		t.Fatalf("get second transient failed: %v", err)
	}
	if firstTransient != 1 || secondTransient != 2 || transientCalls != 2 {
		t.Fatalf("expected transient calls 1/2, got values %d/%d calls %d", firstTransient, secondTransient, transientCalls)
	}

	singletonCalls := 0
	if err := Singleton(ctx, func(ctx context.Context) (string, error) {
		singletonCalls++
		return "cached", nil
	}); err != nil {
		t.Fatalf("bind singleton failed: %v", err)
	}

	firstSingleton, err := Get[string](ctx)
	if err != nil {
		t.Fatalf("get first singleton failed: %v", err)
	}
	secondSingleton, err := Get[string](ctx)
	if err != nil {
		t.Fatalf("get second singleton failed: %v", err)
	}
	if firstSingleton != "cached" || secondSingleton != "cached" || singletonCalls != 1 {
		t.Fatalf("expected singleton cached once, got values %q/%q calls %d", firstSingleton, secondSingleton, singletonCalls)
	}
}

// TestContainerConcurrentSingletonFactoryCalledOnce 验证并发解析单例时工厂只会执行一次。
func TestContainerConcurrentSingletonFactoryCalledOnce(t *testing.T) {
	ctx := NewContainer().WithContext(context.Background())
	var calls atomic.Int32

	if err := Singleton(ctx, func(ctx context.Context) (*containerTestService, error) {
		calls.Add(1)
		return &containerTestService{id: 99}, nil
	}); err != nil {
		t.Fatalf("bind singleton failed: %v", err)
	}

	const goroutines = 64
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()

			value, err := Get[*containerTestService](ctx)
			if err != nil {
				t.Errorf("get singleton failed: %v", err)
				return
			}
			if value.id != 99 {
				t.Errorf("expected id 99, got %d", value.id)
			}
		}()
	}
	wg.Wait()

	if calls.Load() != 1 {
		t.Fatalf("expected singleton factory once, got %d", calls.Load())
	}
}

// TestContainerSingletonCachesError 验证 singleton 会缓存第一次解析错误。
func TestContainerSingletonCachesError(t *testing.T) {
	ctx := NewContainer().WithContext(context.Background())
	wantErr := errors.New("dependency failed")
	calls := 0

	if err := Singleton(ctx, func(ctx context.Context) (float64, error) {
		calls++
		return 0, wantErr
	}); err != nil {
		t.Fatalf("bind singleton failed: %v", err)
	}

	_, firstErr := Get[float64](ctx)
	_, secondErr := Get[float64](ctx)
	if !errors.Is(firstErr, wantErr) || !errors.Is(secondErr, wantErr) {
		t.Fatalf("expected cached error, got %v and %v", firstErr, secondErr)
	}
	if calls != 1 {
		t.Fatalf("expected one singleton call, got %d", calls)
	}
}

// TestContainerErrors 验证缺失容器、缺失绑定、重复绑定和空工厂错误。
func TestContainerErrors(t *testing.T) {
	if Has[int](context.Background()) {
		t.Fatal("expected missing container to report false")
	}
	if _, err := Get[int](context.Background()); !errors.Is(err, ErrContainerNotFound) {
		t.Fatalf("expected container not found, got %v", err)
	}
	if err := Bind[int](context.Background(), func(ctx context.Context) (int, error) {
		return 1, nil
	}); !errors.Is(err, ErrContainerNotFound) {
		t.Fatalf("expected bind without container to fail, got %v", err)
	}

	ctx := NewContainer().WithContext(context.Background())
	if _, err := Get[int](ctx); !errors.Is(err, ErrBindingNotFound) {
		t.Fatalf("expected binding not found, got %v", err)
	}
	if err := Bind[int](ctx, nil); !errors.Is(err, ErrFactoryNil) {
		t.Fatalf("expected nil factory error, got %v", err)
	}
	if err := Instance(ctx, 1); err != nil {
		t.Fatalf("bind instance failed: %v", err)
	}
	if err := Instance(ctx, 2); !errors.Is(err, ErrBindingAlreadyExists) {
		t.Fatalf("expected duplicate binding error, got %v", err)
	}
	if err := InstanceNamed(ctx, "two", 2); err != nil {
		t.Fatalf("bind named instance failed: %v", err)
	}
}

// TestContainerMustFunctions 验证 Must 系列成功返回并在失败时 panic。
func TestContainerMustFunctions(t *testing.T) {
	ctx := NewContainer().WithContext(context.Background())
	if err := InstanceNamed(ctx, "answer", 42); err != nil {
		t.Fatalf("bind named instance failed: %v", err)
	}
	if got := MustGetNamed[int](ctx, "answer"); got != 42 {
		t.Fatalf("expected 42, got %d", got)
	}

	requirePanic(t, func() {
		MustGet[int](context.Background())
	})
	requirePanic(t, func() {
		MustFromContext(context.Background())
	})

	regCtx := NewContainer().WithContext(context.Background())
	MustInstance(regCtx, 1)
	if got := MustGet[int](regCtx); got != 1 {
		t.Fatalf("expected 1, got %d", got)
	}
	MustInstanceNamed(regCtx, "two", 2)
	if got := MustGetNamed[int](regCtx, "two"); got != 2 {
		t.Fatalf("expected 2, got %d", got)
	}
	MustBindNamed(regCtx, "factory", func(ctx context.Context) (int, error) { return 7, nil })
	if got := MustGetNamed[int](regCtx, "factory"); got != 7 {
		t.Fatalf("expected 7, got %d", got)
	}
	MustSingletonNamed(regCtx, "single", func(ctx context.Context) (int, error) { return 9, nil })
	if got := MustGetNamed[int](regCtx, "single"); got != 9 {
		t.Fatalf("expected 9, got %d", got)
	}

	requirePanic(t, func() { MustInstance(regCtx, 99) })
	requirePanic(t, func() {
		MustBind(context.Background(), func(ctx context.Context) (int, error) { return 0, nil })
	})
	requirePanic(t, func() { MustSingleton[int](regCtx, nil) })
}

// TestContainerCircularDependency 验证循环依赖会返回错误而不是死锁。
func TestContainerCircularDependency(t *testing.T) {
	ctx := NewContainer().WithContext(context.Background())

	if err := Singleton(ctx, func(ctx context.Context) (containerTestService, error) {
		return Get[containerTestService](ctx)
	}); err != nil {
		t.Fatalf("bind singleton failed: %v", err)
	}

	_, err := Get[containerTestService](ctx)
	if !errors.Is(err, ErrCircularDependency) {
		t.Fatalf("expected circular dependency error, got %v", err)
	}
}
