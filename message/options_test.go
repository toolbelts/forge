package message

import (
	"testing"
	"time"
)

func TestWithSmtp_SkipsWhenIncomplete(t *testing.T) {
	cases := []SmtpConfig{
		{Name: "no-host", Port: 587, From: "a@b"},
		{Name: "no-port", Host: "h", From: "a@b"},
		{Name: "no-from", Host: "h", Port: 587},
	}
	for _, cfg := range cases {
		c := &config{}
		WithSmtp(cfg)(c)
		if len(c.emailSpecs) != 0 {
			t.Fatalf("WithSmtp should skip incomplete %q, got %d specs", cfg.Name, len(c.emailSpecs))
		}
	}
}

func TestWithSmtp_AccumulatesInOrder(t *testing.T) {
	c := &config{}
	WithSmtp(SmtpConfig{Name: "a", Host: "h1", Port: 587, From: "a@b"})(c)
	WithSmtp(SmtpConfig{Name: "b", Host: "h2", Port: 25, From: "c@d"})(c)
	if len(c.emailSpecs) != 2 {
		t.Fatalf("expected 2 email specs, got %d", len(c.emailSpecs))
	}
	if c.emailSpecs[0].(SmtpConfig).Name != "a" {
		t.Fatalf("expected first spec name=a, got %q", c.emailSpecs[0].(SmtpConfig).Name)
	}
	if c.emailSpecs[1].(SmtpConfig).Name != "b" {
		t.Fatalf("expected second spec name=b, got %q", c.emailSpecs[1].(SmtpConfig).Name)
	}
}

func TestWithSendGrid_SkipsWhenIncomplete(t *testing.T) {
	c := &config{}
	WithSendGrid(SendGridConfig{Name: "no-key", From: "a@b"})(c)
	WithSendGrid(SendGridConfig{Name: "no-from", ApiKey: "k"})(c)
	if len(c.emailSpecs) != 0 {
		t.Fatalf("WithSendGrid should skip incomplete configs, got %d specs", len(c.emailSpecs))
	}
}

func TestWithTwilioSms_SkipsWhenIncomplete(t *testing.T) {
	c := &config{}
	WithTwilioSms(TwilioConfig{Name: "no-token", AccountSid: "AC", From: "+1"})(c)
	WithTwilioSms(TwilioConfig{Name: "no-from", AccountSid: "AC", AuthToken: "t"})(c)
	if len(c.smsSpecs) != 0 {
		t.Fatalf("expected 0 sms specs from incomplete twilio configs, got %d", len(c.smsSpecs))
	}
}

func TestWithBytePlusSms_SkipsWhenIncomplete(t *testing.T) {
	c := &config{}
	WithBytePlusSms(BytePlusConfig{Name: "no-sign", AccessKey: "ak", SecretKey: "sk", Region: "r", SmsAccount: "a"})(c)
	if len(c.smsSpecs) != 0 {
		t.Fatalf("expected 0 sms specs without sign, got %d", len(c.smsSpecs))
	}
}

func TestWithAliyunSms_SkipsWhenIncomplete(t *testing.T) {
	c := &config{}
	WithAliyunSms(AliyunSmsConfig{Name: "no-endpoint", AccessKey: "ak", SecretKey: "sk"})(c)
	if len(c.smsSpecs) != 0 {
		t.Fatalf("expected 0 sms specs without endpoint, got %d", len(c.smsSpecs))
	}
}

func TestWithEmailTemplate_IgnoresEmptyId(t *testing.T) {
	c := &config{}
	WithEmailTemplate("", "Subject", "<p>x</p>", "x")(c)
	if len(c.templates) != 0 {
		t.Fatalf("empty id should be skipped, got %d templates", len(c.templates))
	}
}

func TestPickTimeout(t *testing.T) {
	if got := pickTimeout(0); got != defaultSendTimeout {
		t.Fatalf("0 should map to default, got %v", got)
	}
	if got := pickTimeout(-time.Second); got != defaultSendTimeout {
		t.Fatalf("negative should map to default, got %v", got)
	}
	if got := pickTimeout(2 * time.Second); got != 2*time.Second {
		t.Fatalf("positive should pass through, got %v", got)
	}
}
