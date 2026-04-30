package provider

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"

	"github.com/toolbelts/forge/ioc"
)

// AppName 表示当前应用名称，作为 {app}.yaml 配置文件名的依据。
type AppName string

// ConfigProvider 配置提供者，使用 viper 加载 common.yaml 与 {app}.yaml 并支持热重载。
type ConfigProvider struct {
	appName string   // 应用名称
	dirs    []string // 配置搜索目录
	names   []string // 配置文件名列表（不含扩展名）
	envs    []string // .env 文件列表，按顺序加载，后者覆盖前者
}

// NewConfigProvider 创建配置提供者，appName 决定 {app}.yaml 文件名。
func NewConfigProvider(appName string) *ConfigProvider {
	return &ConfigProvider{
		appName: appName,
		dirs:    []string{"./configs", "."},
		names:   []string{"common", "message", appName},
		envs:    []string{".env", ".env.local"},
	}
}

// WithConfigDirs 自定义配置搜索目录。
func (p *ConfigProvider) WithConfigDirs(dirs ...string) *ConfigProvider {
	p.dirs = dirs
	return p
}

// WithConfigNames 自定义配置文件名列表（不含扩展名）。
func (p *ConfigProvider) WithConfigNames(names ...string) *ConfigProvider {
	p.names = names
	return p
}

// WithEnvFiles 自定义 .env 文件列表，按顺序加载，后者覆盖前者。
func (p *ConfigProvider) WithEnvFiles(envs ...string) *ConfigProvider {
	p.envs = envs
	return p
}

// Register 加载所有 yaml 配置并将 viper 实例与应用名称绑定到容器。
func (p *ConfigProvider) Register(ctx context.Context) error {
	if len(p.names) == 0 {
		return fmt.Errorf("config names is empty")
	}

	v := viper.New()
	v.SetConfigType("yaml")
	for _, dir := range p.dirs {
		v.AddConfigPath(dir)
	}

	for i, name := range p.names {
		v.SetConfigName(name)
		load := v.MergeInConfig
		if i == 0 {
			load = v.ReadInConfig
		}
		if err := load(); err != nil {
			isLast := i == len(p.names)-1
			var notFound viper.ConfigFileNotFoundError
			if !isLast && errors.As(err, &notFound) {
				continue
			}
			return fmt.Errorf("load config[%d] %s: %w", i, name, err)
		}
	}

	for _, envFile := range p.envs {
		if err := godotenv.Overload(envFile); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("load env file %s: %w", envFile, err)
		}
	}

	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	ioc.MustInstance(ctx, v)
	ioc.MustInstance(ctx, AppName(p.appName))

	return nil
}

// Setup 无操作，配置加载已在 Register 阶段完成。
func (p *ConfigProvider) Setup(ctx context.Context) error {
	return nil
}

// MustGetViper 从容器获取 viper 实例，缺失时 panic。
func MustGetViper(ctx context.Context) *viper.Viper {
	return ioc.MustGet[*viper.Viper](ctx)
}

// MustGetAppName 从容器获取应用名称，缺失时 panic。
func MustGetAppName(ctx context.Context) AppName {
	return ioc.MustGet[AppName](ctx)
}
