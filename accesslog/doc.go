// Package accesslog 提供 gRPC 一元/流式访问日志拦截器。
//
// 使用约定:
//   - 由 provider/accesslog.go 接入 InterceptorChain
//   - 注册位置:Recovery 内层、Error 外层。Recovery 已把 panic 转成 errkit.Error,
//     Error 已把裸 error 归一化,AccessLog 看到的 err 永远满足 errkit.Error 或 nil,
//     可以稳定通过 errkit.FromError 提取 error_code / error_name。
//   - 字段命名采用下划线风格(user_id、user_ip、error_code 等),与项目其它日志一致。
//   - 不做 panic recover、不做 error 归一化、不做限流/鉴权/校验,这些由对应 Provider 各自负责。
//
// payload 字段形态契约(payload 启用时):
//   - req / resp:要么缺席,要么必为嵌套 JSON 对象(protojson 输出经 RawJSON 直接嵌入,零二次转义)。
//   - req_text / resp_text:要么缺席,要么必为字符串,与同名对象字段互斥。落 text 的情况:
//     非 proto 值(fmt.Sprint)、超长截断前缀、wire size 超限占位符、marshal 失败占位符、
//     顶层非对象的 wrapper 类型(StringValue 等)。
//   - 两组字段类型恒定,避免下游索引(ES 等)同字段对象/字符串混型冲突。
package accesslog
