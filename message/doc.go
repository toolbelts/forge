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
package message
