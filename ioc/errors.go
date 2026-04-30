package ioc

import (
	"errors"
	"fmt"
)

var (
	// ErrAppAlreadyStarted 表示应用生命周期已经开始，不能再注册 provider。
	ErrAppAlreadyStarted = errors.New("ioc: app already started")

	// ErrAppFailed 表示应用生命周期已经失败，不能继续后续阶段。
	ErrAppFailed = errors.New("ioc: app failed")

	// ErrAppClosed 表示应用生命周期已经关闭，不能继续后续阶段。
	ErrAppClosed = errors.New("ioc: app closed")

	// ErrBindingAlreadyExists 表示容器中已经存在相同类型和名称的绑定。
	ErrBindingAlreadyExists = errors.New("ioc: binding already exists")

	// ErrBindingNotFound 表示容器中不存在请求的绑定。
	ErrBindingNotFound = errors.New("ioc: binding not found")

	// ErrBindingTypeMismatch 表示绑定的工厂或实例类型与请求类型不匹配。
	ErrBindingTypeMismatch = errors.New("ioc: binding type mismatch")

	// ErrCircularDependency 表示解析过程中检测到循环依赖。
	ErrCircularDependency = errors.New("ioc: circular dependency detected")

	// ErrContainerNotFound 表示上下文中不存在容器。
	ErrContainerNotFound = errors.New("ioc: container not found")

	// ErrFactoryNil 表示注册绑定时传入了 nil 工厂。
	ErrFactoryNil = errors.New("ioc: factory is nil")

	// ErrProviderNameEmpty 表示 provider 名称为空。
	ErrProviderNameEmpty = errors.New("ioc: provider name is empty")

	// ErrProviderNameExists 表示已经注册了相同名称的 provider。
	ErrProviderNameExists = errors.New("ioc: provider name already exists")

	// ErrProviderNil 表示注册了 nil provider。
	ErrProviderNil = errors.New("ioc: provider is nil")

	// ErrProvidersNotRegistered 表示尚未执行 provider 注册阶段。
	ErrProvidersNotRegistered = errors.New("ioc: providers are not registered")

	// ErrProvidersAlreadyRegistered 表示 provider 注册阶段已经执行过。
	ErrProvidersAlreadyRegistered = errors.New("ioc: providers already registered")

	// ErrProvidersAlreadySetup 表示 provider setup 阶段已经执行过。
	ErrProvidersAlreadySetup = errors.New("ioc: providers already setup")

	// ErrSetupInProgress 表示 provider setup 阶段正在执行。
	ErrSetupInProgress = errors.New("ioc: setup in progress")
)

// wrapError 为哨兵错误追加上下文信息。
func wrapError(err error, detail string) error {
	if detail == "" {
		return err
	}

	return fmt.Errorf("%w: %s", err, detail)
}

// providerError 返回带 provider 名称和阶段的生命周期错误。
func providerError(stage string, name string, err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("ioc: provider %s failed: %s: %w", stage, name, err)
}
