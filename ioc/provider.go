package ioc

import (
	"context"
	"reflect"
	"strings"
)

// Provider 定义应用启动阶段必须具备的服务提供者能力。
type Provider interface {
	Register(ctx context.Context) error
	Setup(ctx context.Context) error
}

// Namer 定义服务提供者的稳定名称。
type Namer interface {
	Name() string
}

// Server 定义需要在应用运行期阻塞执行的服务能力。
type Server interface {
	Serve(ctx context.Context) error
}

// Shutdowner 定义应用退出时可选的资源释放能力。
type Shutdowner interface {
	Shutdown(ctx context.Context) error
}

// TypeNameOf 返回泛型类型参数对应的容器默认名称。
func TypeNameOf[T any]() string {
	return typeName(reflect.TypeFor[T]())
}

// providerName 返回 provider 的注册名称，优先使用 Namer。
func providerName(provider Provider) (string, error) {
	if provider == nil || isNilProvider(provider) {
		return "", ErrProviderNil
	}

	if named, ok := provider.(Namer); ok {
		name := strings.TrimSpace(named.Name())
		if name == "" {
			return "", ErrProviderNameEmpty
		}

		return name, nil
	}

	name := typeName(reflect.TypeOf(provider))
	if name == "" {
		return "", ErrProviderNameEmpty
	}

	return name, nil
}

// typeName 返回类型的稳定字符串名称，指针类型会归一化到元素类型。
func typeName(typ reflect.Type) string {
	if typ == nil {
		return ""
	}

	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}

	if typ.PkgPath() == "" || typ.Name() == "" {
		return typ.String()
	}

	return typ.PkgPath() + "." + typ.Name()
}

// isNilProvider 判断 interface 内部是否包着 nil 指针。
func isNilProvider(provider Provider) bool {
	value := reflect.ValueOf(provider)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
