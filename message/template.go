package message

import (
	"bytes"
	"fmt"
	htmltemplate "html/template"
	texttemplate "text/template"
)

// emailTemplate 是一个已编译的邮件模板,subject / html / text 三部分各自独立可空。
//
// HTML body 用 html/template 自动转义防 XSS;subject 与 text 用 text/template。
// 任一字段为空表示该部分不参与渲染,渲染结果按相同规则保留为空 string。
type emailTemplate struct {
	id      string
	subject *texttemplate.Template
	html    *htmltemplate.Template
	text    *texttemplate.Template
}

// compileEmailTemplate 把三段模板字符串解析为 emailTemplate;空 string 跳过解析。
// 任一段语法错误立刻返回,New() 借此 fail-fast。
func compileEmailTemplate(id, subject, html, text string) (*emailTemplate, error) {
	tpl := &emailTemplate{id: id}

	if subject != "" {
		t, err := texttemplate.New(id + ":subject").Parse(subject)
		if err != nil {
			return nil, fmt.Errorf("message: parse template %q subject: %w", id, err)
		}
		tpl.subject = t
	}
	if html != "" {
		t, err := htmltemplate.New(id + ":html").Parse(html)
		if err != nil {
			return nil, fmt.Errorf("message: parse template %q html: %w", id, err)
		}
		tpl.html = t
	}
	if text != "" {
		t, err := texttemplate.New(id + ":text").Parse(text)
		if err != nil {
			return nil, fmt.Errorf("message: parse template %q text: %w", id, err)
		}
		tpl.text = t
	}
	return tpl, nil
}

// render 用 data 渲染三个部分,空模板返回空 string。
func (t *emailTemplate) render(data map[string]any) (subject, html, text string, err error) {
	if t.subject != nil {
		var buf bytes.Buffer
		if err = t.subject.Execute(&buf, data); err != nil {
			err = fmt.Errorf("message: render template %q subject: %w", t.id, err)
			return
		}
		subject = buf.String()
	}
	if t.html != nil {
		var buf bytes.Buffer
		if err = t.html.Execute(&buf, data); err != nil {
			err = fmt.Errorf("message: render template %q html: %w", t.id, err)
			return
		}
		html = buf.String()
	}
	if t.text != nil {
		var buf bytes.Buffer
		if err = t.text.Execute(&buf, data); err != nil {
			err = fmt.Errorf("message: render template %q text: %w", t.id, err)
			return
		}
		text = buf.String()
	}
	return
}
