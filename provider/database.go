package provider

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
	"github.com/uptrace/bun/extra/bunotel"

	"github.com/toolbelts/forge/ioc"
)

// DatabaseProvider 数据库提供者，支持从配置加载多个 PostgreSQL 实例。
type DatabaseProvider struct {
	clients     map[string]*bun.DB
	otelEnabled bool
}

// Register 扫描 database.* 配置创建多个 bun.DB 实例，连通性验证后绑定到容器。
func (p *DatabaseProvider) Register(ctx context.Context) error {
	v := ioc.MustGet[*viper.Viper](ctx)
	dbMap := v.GetStringMap("database")
	p.clients = make(map[string]*bun.DB, len(dbMap))
	p.otelEnabled = traceInstrumentationEnabled(v, observabilityComponentDatabase) ||
		metricsInstrumentationEnabled(v, observabilityComponentDatabase)

	for name := range dbMap {
		prefix := "database." + name
		dsn := v.GetString(prefix + ".dsn")
		if dsn == "" {
			return fmt.Errorf("database [%s] dsn is empty", name)
		}

		sqldb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsn)))
		if n := v.GetInt(prefix + ".max_open_conns"); n > 0 {
			sqldb.SetMaxOpenConns(n)
		}
		if n := v.GetInt(prefix + ".max_idle_conns"); n > 0 {
			sqldb.SetMaxIdleConns(n)
		}
		if d := v.GetDuration(prefix + ".conn_max_lifetime"); d > 0 {
			sqldb.SetConnMaxLifetime(d)
		}
		if d := v.GetDuration(prefix + ".conn_max_idle_time"); d > 0 {
			sqldb.SetConnMaxIdleTime(d)
		}

		db := bun.NewDB(sqldb, pgdialect.New())

		if p.otelEnabled {
			db.AddQueryHook(bunotel.NewQueryHook(bunotel.WithDBName(name)))
		}

		if v.GetBool(prefix + ".debug") {
			db.AddQueryHook(&debugQueryHook{name: name})
		}

		if slow := v.GetDuration(prefix + ".slow"); slow > 0 {
			db.AddQueryHook(&slowQueryHook{name: name, threshold: slow})
		}

		if err := db.PingContext(ctx); err != nil {
			return fmt.Errorf("database [%s] ping failed: %w", name, err)
		}

		ioc.MustInstanceNamed(ctx, name, db)
		p.clients[name] = db
		log.Ctx(ctx).Info().Str("db_name", name).Msg("database connected")
	}

	return nil
}

// Setup 无操作。
func (p *DatabaseProvider) Setup(ctx context.Context) error {
	return nil
}

// Shutdown 关闭所有数据库连接，错误用 errors.Join 聚合返回。
func (p *DatabaseProvider) Shutdown(ctx context.Context) error {
	var errs []error
	for name, db := range p.clients {
		if err := db.Close(); err != nil {
			log.Ctx(ctx).Error().Err(err).Str("db_name", name).Msg("database close failed")
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// GetDb 从容器获取指定名称的数据库句柄。
func GetDb(ctx context.Context, name string) (*bun.DB, error) {
	return ioc.GetNamed[*bun.DB](ctx, name)
}

// MustGetDb 从容器获取指定名称的数据库句柄，缺失时 panic。
func MustGetDb(ctx context.Context, name string) *bun.DB {
	return ioc.MustGetNamed[*bun.DB](ctx, name)
}

// debugQueryHook bun 全量查询钩子，以 zerolog debug 级别记录每条 SQL。
type debugQueryHook struct {
	name string
}

// BeforeQuery 钩子前置回调，仅透传 ctx。
func (h *debugQueryHook) BeforeQuery(ctx context.Context, e *bun.QueryEvent) context.Context {
	return ctx
}

// AfterQuery 钩子后置回调，以 debug 级别输出 SQL、耗时与错误。
func (h *debugQueryHook) AfterQuery(ctx context.Context, e *bun.QueryEvent) {
	ev := log.Ctx(ctx).Debug().
		Str("db_name", h.name).
		Dur("duration", time.Since(e.StartTime)).
		Str("query", e.Query)
	if e.Err != nil {
		ev = ev.Err(e.Err)
	}
	ev.Msg("sql query")
}

// slowQueryHook bun 慢查询钩子，超过阈值时用 zerolog warn 记录。
type slowQueryHook struct {
	name      string
	threshold time.Duration
}

// BeforeQuery 钩子前置回调，仅透传 ctx。
func (h *slowQueryHook) BeforeQuery(ctx context.Context, e *bun.QueryEvent) context.Context {
	return ctx
}

// AfterQuery 钩子后置回调，超过阈值时输出 warn 级别日志。
func (h *slowQueryHook) AfterQuery(ctx context.Context, e *bun.QueryEvent) {
	dur := time.Since(e.StartTime)
	if dur < h.threshold {
		return
	}
	ev := log.Ctx(ctx).Warn().
		Str("db_name", h.name).
		Dur("duration", dur).
		Str("query", e.Query)
	if e.Err != nil {
		ev = ev.Err(e.Err)
	}
	ev.Msg("slow query detected")
}
