// Package cron 是 github.com/robfig/cron/v3 的薄封装,统一日志格式与执行语义,
// 把 zerolog 接到调度器内部事件,并默认应用 SkipIfStillRunning + Recover 中间件:
//
//   - 上一次任务还没跑完时,本次触发被丢弃(打 Warn 日志而非排队/并发)
//   - 任务 panic 不影响调度器,Recover 中间件捕获并打 Error
//   - 表达式使用 6 字段(秒 分 时 日 月 周),同时兼容 @every / @hourly 等 descriptor
//
// 业务方拿到 *Cron 后调用 AddJob(name, spec, fn) 注册任务即可,不必关心 robfig/cron 的细节。
package cron
