package provider

import (
	"context"
	"os"
	"strconv"
	"strings"

	"github.com/fsnotify/fsnotify"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"

	"github.com/toolbelts/forge/ioc"
)

// LoggerProvider 日志提供者，初始化 zerolog 全局 logger 并绑定配置热重载。
type LoggerProvider struct{}

// Register 初始化 zerolog 全局 logger，并启用 viper 文件监听以热更新日志级别。
func (p *LoggerProvider) Register(ctx context.Context) error {
	v := ioc.MustGet[*viper.Viper](ctx)

	zerolog.CallerMarshalFunc = shortCallerMarshalFunc

	hostname, _ := os.Hostname()

	level := parseLogLevel(v.GetString("log.level"))
	zerolog.SetGlobalLevel(level)

	logger := zerolog.New(os.Stdout).
		With().
		Timestamp().
		Caller().
		Str("hostname", hostname).
		Logger()
	log.Logger = logger
	zerolog.DefaultContextLogger = &logger

	v.OnConfigChange(func(e fsnotify.Event) {
		newLevel := parseLogLevel(v.GetString("log.level"))
		if newLevel != zerolog.GlobalLevel() {
			log.Info().
				Str("old_level", zerolog.GlobalLevel().String()).
				Str("new_level", newLevel.String()).
				Msg("log level changed")
			zerolog.SetGlobalLevel(newLevel)
		}
		log.Info().Str("file", e.Name).Msg("config changed")
	})
	v.WatchConfig()

	log.Info().
		Str("level", level.String()).
		Str("hostname", hostname).
		Msg("logger initialized")

	return nil
}

// Setup 无操作。
func (p *LoggerProvider) Setup(ctx context.Context) error {
	return nil
}

// shortCallerMarshalFunc 自定义 zerolog caller 短路径，保留最后两段路径。
// 例：/usr/local/.../pkg/ioc/app.go:42 -> ioc/app.go:42
func shortCallerMarshalFunc(pc uintptr, file string, line int) string {
	short := file
	if idx1 := strings.LastIndexByte(file, '/'); idx1 > 0 {
		if idx2 := strings.LastIndexByte(file[:idx1], '/'); idx2 >= 0 {
			short = file[idx2+1:]
		}
	}
	return short + ":" + strconv.Itoa(line)
}

// parseLogLevel 解析日志级别，无效或为空时回退到 info。
func parseLogLevel(s string) zerolog.Level {
	s = strings.TrimSpace(s)
	if s == "" {
		return zerolog.InfoLevel
	}
	lvl, err := zerolog.ParseLevel(s)
	if err != nil {
		return zerolog.InfoLevel
	}
	return lvl
}
