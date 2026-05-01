// Package errkit 定义 forge 与应用之间唯一稳定的错误契约。
//
// 设计目标:
//   - 公共库内部不依赖任何 proto / 业务码,可被任何应用引入
//   - 应用层 errorpb.BizError 通过实现 errkit.Error 与本包互通
//   - 中间件统一用 errkit.Error 抽取 code/message/metadata,序列化细节由应用通过 Encoder 注入
package errkit
