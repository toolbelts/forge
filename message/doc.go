// Package message 提供通用的邮件 + 短信发送组件。
//
// 邮件后端 V1 支持 SMTP(任意中继 - 自建 / AWS SES / Aliyun DM / SendGrid SMTP 等)
// 与 SendGrid HTTP API。短信后端 V1 支持 Twilio / BytePlus / Aliyun 国际版 / Aliyun 国内版。
//
// 多后端路由:Manager 各持一个有序的 sender 列表。SendEmail / SendSms 都走
// per-recipient 决策:每个收件人独立按优先级遍历兼容 sender(邮件按域名后缀 Include/Exclude
// 筛选,短信按区号前缀筛选 + mode 兼容);邮件再按所选 sender 把 To 分组,每组一封下发,
// Cc/Bcc 在每组邮件里都会复制一份。
//
// 邮件模板:程序启动期把 (subject, html, text) 解析为 *template.Template,
// 运行期按 id 渲染,html 部分用 html/template 自动转义防 XSS。
//
// # 自定义后端
//
// 内置类型之外的后端(WhatsApp、某家区域短信商……)由业务仓库自己实现
// SmsSender / EmailSender,两条路都可以:
//
//	message.New(..., message.WithSmsSender(mySender))        // 自己建 Manager
//	provider.RegisterSmsSender("whatsapp", myFactory)        // 走 yaml 的 providers[]
//
// 后者让自定义后端和内置的用同一套配置形状(顺序即 fallback 优先级、区号筛选、
// 超时),forge 不需要认识任何具体供应商。注册时机见 provider/message_registry.go
// 的包注释:必须早于 MessageProvider.Setup,也就是业务 Provider 的 Register 阶段。
package message
