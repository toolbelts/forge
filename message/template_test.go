package message

import (
	"strings"
	"testing"
)

func TestCompileEmailTemplate_AllParts(t *testing.T) {
	tpl, err := compileEmailTemplate("welcome",
		"Hi {{.Name}}",
		"<p>Hello {{.Name}}, code <b>{{.Code}}</b></p>",
		"Hello {{.Name}}, code {{.Code}}")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	subject, html, text, err := tpl.render(map[string]any{"Name": "Alice", "Code": "X"})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if subject != "Hi Alice" {
		t.Fatalf("subject mismatch: %q", subject)
	}
	if !strings.Contains(html, "<b>X</b>") {
		t.Fatalf("html missing rendered code: %q", html)
	}
	if text != "Hello Alice, code X" {
		t.Fatalf("text mismatch: %q", text)
	}
}

func TestCompileEmailTemplate_HtmlEscaping(t *testing.T) {
	// html/template 应自动转义 user-controlled 数据,防 XSS。
	tpl, err := compileEmailTemplate("safe", "", "<p>{{.Code}}</p>", "")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, html, _, err := tpl.render(map[string]any{"Code": "<script>alert(1)</script>"})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if strings.Contains(html, "<script>") {
		t.Fatalf("html template did not escape: %q", html)
	}
	if !strings.Contains(html, "&lt;script&gt;") {
		t.Fatalf("expected escaped script tag, got: %q", html)
	}
}

func TestCompileEmailTemplate_EmptyParts(t *testing.T) {
	tpl, err := compileEmailTemplate("plain", "Subject", "", "Plain body")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	subject, html, text, err := tpl.render(nil)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if subject != "Subject" || html != "" || text != "Plain body" {
		t.Fatalf("expected (Subject, '', 'Plain body'), got (%q, %q, %q)", subject, html, text)
	}
}

func TestCompileEmailTemplate_BadSyntaxReturnsErr(t *testing.T) {
	if _, err := compileEmailTemplate("bad", "{{ .Unclosed", "", ""); err == nil {
		t.Fatal("expected parse error for unclosed action")
	}
	if _, err := compileEmailTemplate("badhtml", "", "<p>{{ .Unclosed", ""); err == nil {
		t.Fatal("expected parse error in html")
	}
}
