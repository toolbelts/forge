// Package migration 提供基于 bun migrate 的多 db 迁移集合管理。
//
// 核心概念:Set 是一组按 db 名分组的迁移集合。调用方传入 fs.FS(通常是 embed.FS),
// Set.New 扫描其顶层每个目录,目录名作为 db 名,目录内的 *.sql 走 bun migrate.Discover
// 解析为版本化迁移。
//
// 设计取舍:本包不依赖 IOC 容器、不依赖具体 *bun.DB 实例,仅描述"有哪些迁移、
// 对一个 db 跑什么动作"。把 db 解析、配置读取留给上层 Provider,本包可被任何
// 用 bun 的项目直接 import。
package migration
