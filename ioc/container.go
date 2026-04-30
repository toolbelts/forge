package ioc

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
)

// Factory 定义容器绑定的实例创建函数。
type Factory[T any] func(ctx context.Context) (T, error)

type bindingKind int

const (
	bindingTransient bindingKind = iota
	bindingSingleton
	bindingInstance
)

type bindingKey struct {
	typ  reflect.Type
	name string
}

type binding struct {
	kind     bindingKind
	factory  any
	value    any
	err      error
	mu       sync.Mutex
	resolved atomic.Bool
}

// Container 保存应用内的类型绑定关系。
type Container struct {
	mu       sync.RWMutex
	bindings map[bindingKey]*binding
}

// NewContainer 创建一个空的容器实例。
func NewContainer() *Container {
	return &Container{
		bindings: make(map[bindingKey]*binding),
	}
}

// Bind 注册一个每次解析都会重新执行的类型工厂。
func Bind[T any](ctx context.Context, factory Factory[T]) error {
	return bindFactory(ctx, keyOf[T](""), bindingTransient, factory)
}

// MustBind 同 Bind，失败时直接 panic。
func MustBind[T any](ctx context.Context, factory Factory[T]) {
	if err := Bind(ctx, factory); err != nil {
		panic(err)
	}
}

// BindNamed 注册一个每次解析都会重新执行的命名类型工厂。
func BindNamed[T any](ctx context.Context, name string, factory Factory[T]) error {
	return bindFactory(ctx, keyOf[T](name), bindingTransient, factory)
}

// MustBindNamed 同 BindNamed，失败时直接 panic。
func MustBindNamed[T any](ctx context.Context, name string, factory Factory[T]) {
	if err := BindNamed(ctx, name, factory); err != nil {
		panic(err)
	}
}

// Singleton 注册一个只解析一次并缓存结果的类型工厂。
func Singleton[T any](ctx context.Context, factory Factory[T]) error {
	return bindFactory(ctx, keyOf[T](""), bindingSingleton, factory)
}

// MustSingleton 同 Singleton，失败时直接 panic。
func MustSingleton[T any](ctx context.Context, factory Factory[T]) {
	if err := Singleton(ctx, factory); err != nil {
		panic(err)
	}
}

// SingletonNamed 注册一个只解析一次并缓存结果的命名类型工厂。
func SingletonNamed[T any](ctx context.Context, name string, factory Factory[T]) error {
	return bindFactory(ctx, keyOf[T](name), bindingSingleton, factory)
}

// MustSingletonNamed 同 SingletonNamed，失败时直接 panic。
func MustSingletonNamed[T any](ctx context.Context, name string, factory Factory[T]) {
	if err := SingletonNamed(ctx, name, factory); err != nil {
		panic(err)
	}
}

// Instance 注册一个已构造好的类型实例。
func Instance[T any](ctx context.Context, instance T) error {
	return bindInstance(ctx, keyOf[T](""), instance)
}

// MustInstance 同 Instance，失败时直接 panic。
func MustInstance[T any](ctx context.Context, instance T) {
	if err := Instance(ctx, instance); err != nil {
		panic(err)
	}
}

// InstanceNamed 注册一个已构造好的命名类型实例。
func InstanceNamed[T any](ctx context.Context, name string, instance T) error {
	return bindInstance(ctx, keyOf[T](name), instance)
}

// MustInstanceNamed 同 InstanceNamed，失败时直接 panic。
func MustInstanceNamed[T any](ctx context.Context, name string, instance T) {
	if err := InstanceNamed(ctx, name, instance); err != nil {
		panic(err)
	}
}

// Get 从上下文容器解析一个类型实例。
func Get[T any](ctx context.Context) (T, error) {
	return get[T](ctx, keyOf[T](""))
}

// GetNamed 从上下文容器解析一个命名类型实例。
func GetNamed[T any](ctx context.Context, name string) (T, error) {
	return get[T](ctx, keyOf[T](name))
}

// MustGet 从上下文容器解析一个类型实例，失败时直接 panic。
func MustGet[T any](ctx context.Context) T {
	value, err := Get[T](ctx)
	if err != nil {
		panic(err)
	}

	return value
}

// MustGetNamed 从上下文容器解析一个命名类型实例，失败时直接 panic。
func MustGetNamed[T any](ctx context.Context, name string) T {
	value, err := GetNamed[T](ctx, name)
	if err != nil {
		panic(err)
	}

	return value
}

// Has 判断上下文容器是否存在指定类型绑定。
func Has[T any](ctx context.Context) bool {
	return has(ctx, keyOf[T](""))
}

// HasNamed 判断上下文容器是否存在指定命名类型绑定。
func HasNamed[T any](ctx context.Context, name string) bool {
	return has(ctx, keyOf[T](name))
}

// bindFactory 将类型工厂写入上下文容器。
func bindFactory[T any](ctx context.Context, key bindingKey, kind bindingKind, factory Factory[T]) error {
	if factory == nil {
		return ErrFactoryNil
	}

	return bind(ctx, key, &binding{
		kind:    kind,
		factory: factory,
	})
}

// bindInstance 将已构造实例写入上下文容器。
func bindInstance[T any](ctx context.Context, key bindingKey, instance T) error {
	return bind(ctx, key, &binding{
		kind:  bindingInstance,
		value: func() T { return instance },
	})
}

// bind 将绑定写入上下文容器。
func bind(ctx context.Context, key bindingKey, item *binding) error {
	container, ok := FromContext(ctx)
	if !ok {
		return ErrContainerNotFound
	}

	return container.bind(key, item)
}

// get 从上下文容器解析指定 key 的实例。
func get[T any](ctx context.Context, key bindingKey) (T, error) {
	var zero T

	container, ok := FromContext(ctx)
	if !ok {
		return zero, ErrContainerNotFound
	}

	nextCtx, err := enterResolution(ctx, key)
	if err != nil {
		return zero, err
	}

	return resolveFromContainer[T](nextCtx, container, key)
}

// has 判断上下文容器是否存在指定绑定。
func has(ctx context.Context, key bindingKey) bool {
	container, ok := FromContext(ctx)
	return ok && container.has(key)
}

// bind 将绑定写入容器，重复绑定直接报错。
func (c *Container) bind(key bindingKey, item *binding) error {
	if c == nil {
		return ErrContainerNotFound
	}
	if item == nil {
		return ErrBindingTypeMismatch
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.bindings[key]; ok {
		return wrapError(ErrBindingAlreadyExists, key.String())
	}

	c.bindings[key] = item
	return nil
}

// has 判断容器中是否存在指定绑定。
func (c *Container) has(key bindingKey) bool {
	if c == nil {
		return false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	_, ok := c.bindings[key]
	return ok
}

// resolveFromContainer 按 key 从容器中解析指定类型实例。
func resolveFromContainer[T any](ctx context.Context, c *Container, key bindingKey) (T, error) {
	var zero T

	item, ok := c.getBinding(key)
	if !ok {
		return zero, wrapError(ErrBindingNotFound, key.String())
	}

	switch item.kind {
	case bindingTransient:
		return callFactory[T](ctx, item)
	case bindingSingleton:
		return resolveSingleton[T](ctx, item)
	case bindingInstance:
		return readValue[T](item)
	default:
		return zero, ErrBindingTypeMismatch
	}
}

// getBinding 从容器读取绑定项。
func (c *Container) getBinding(key bindingKey) (*binding, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	item, ok := c.bindings[key]
	return item, ok
}

// resolveSingleton 解析单例实例，并缓存第一次解析结果或错误。
func resolveSingleton[T any](ctx context.Context, item *binding) (t T, err error) {
	if item.resolved.Load() {
		return readResolved[T](item)
	}

	item.mu.Lock()
	defer item.mu.Unlock()

	if item.resolved.Load() {
		return readResolved[T](item)
	}

	value, err := callFactory[T](ctx, item)
	if err != nil {
		item.err = err
		item.resolved.Store(true)
		return t, err
	}

	item.value = func() T { return value }
	item.resolved.Store(true)
	return value, nil
}

// readResolved 读取已完成解析的单例结果。
func readResolved[T any](item *binding) (T, error) {
	var zero T

	if item.err != nil {
		return zero, item.err
	}

	return readValue[T](item)
}

// callFactory 调用绑定中的类型化工厂函数。
func callFactory[T any](ctx context.Context, item *binding) (T, error) {
	var zero T

	factory, ok := item.factory.(Factory[T])
	if !ok {
		return zero, ErrBindingTypeMismatch
	}

	return factory(ctx)
}

// readValue 读取已缓存的类型化实例。
func readValue[T any](item *binding) (T, error) {
	var zero T

	getter, ok := item.value.(func() T)
	if !ok {
		return zero, ErrBindingTypeMismatch
	}

	return getter(), nil
}

// keyOf 返回类型和命名共同组成的容器 key。
func keyOf[T any](name string) bindingKey {
	return bindingKey{
		typ:  reflect.TypeFor[T](),
		name: strings.TrimSpace(name),
	}
}

// String 返回绑定 key 的可读名称。
func (k bindingKey) String() string {
	if k.name == "" {
		return typeName(k.typ)
	}

	return typeName(k.typ) + ":" + k.name
}
