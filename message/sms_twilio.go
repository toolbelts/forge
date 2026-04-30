package message

import (
	"context"
	"fmt"

	"github.com/go-resty/resty/v2"
)

// twilioBaseURL 是 Twilio Messages API 的默认根地址,测试可改成 httptest.Server URL。
const twilioBaseURL = "https://api.twilio.com"

// twilioSender 是 smsSender 的 Twilio 实现(raw mode)。
//
// 单条 SmsMessage 只承载一个 To,Twilio Messages API 也是一次一号,直接 POST 一次。
type twilioSender struct {
	regionFilter
	cfg     TwilioConfig
	baseURL string
	client  *resty.Client
}

// build 让 TwilioConfig 实现 smsSpec 接口。
func (c TwilioConfig) build() (smsSender, error) {
	return newTwilioSender(c)
}

func newTwilioSender(cfg TwilioConfig) (*twilioSender, error) {
	if cfg.AccountSid == "" || cfg.AuthToken == "" || cfg.From == "" {
		return nil, fmt.Errorf("message: twilio %q missing account_sid/auth_token/from", cfg.Name)
	}
	client := resty.New().SetTimeout(pickTimeout(cfg.Timeout))
	return &twilioSender{
		regionFilter: regionFilter{Include: cfg.IncludeRegions, Exclude: cfg.ExcludeRegions},
		cfg:          cfg,
		baseURL:      twilioBaseURL,
		client:       client,
	}, nil
}

func (t *twilioSender) Mode() smsMode { return smsModeRaw }

func (t *twilioSender) Name() string {
	if t.cfg.Name == "" {
		return "twilio"
	}
	return "twilio:" + t.cfg.Name
}

func (t *twilioSender) Send(ctx context.Context, msg SmsMessage) error {
	if msg.Content == "" {
		return fmt.Errorf("%w: twilio requires Content", ErrInvalidSmsMessage)
	}

	endpoint := fmt.Sprintf("%s/2010-04-01/Accounts/%s/Messages.json", t.baseURL, t.cfg.AccountSid)
	resp, err := t.client.R().
		SetContext(ctx).
		SetBasicAuth(t.cfg.AccountSid, t.cfg.AuthToken).
		SetFormData(map[string]string{
			"To":   msg.To,
			"From": t.cfg.From,
			"Body": msg.Content,
		}).
		Post(endpoint)
	if err != nil {
		return fmt.Errorf("message: twilio send to %s: %w", msg.To, err)
	}
	if !resp.IsSuccess() {
		return fmt.Errorf("message: twilio send to %s status=%d body=%s",
			msg.To, resp.StatusCode(), resp.String())
	}
	return nil
}
