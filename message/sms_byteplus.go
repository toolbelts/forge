package message

import (
	"context"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"resty.dev/v3"
)

// BytePlus SMS OpenAPI 走 Volcengine V4 签名,字段名沿用控制台 / 文档大写驼峰。
const (
	bytePlusBaseURL    = "https://sms.byteplusapi.com"
	bytePlusAction     = "SendSms"
	bytePlusVersion    = "2020-01-01"
	bytePlusService    = "volcSMS" // 签名 scope 中的 service 段
	bytePlusAlgorithm  = "HMAC-SHA256"
	bytePlusISOLayout  = "20060102T150405Z"
	bytePlusDateLayout = "20060102"
)

// bytePlusSender 是 smsSender 的 BytePlus 实现(template mode)。
type bytePlusSender struct {
	regionFilter
	cfg     BytePlusConfig
	baseURL string
	client  *resty.Client
}

// build 让 BytePlusConfig 实现 smsSpec 接口。
func (c BytePlusConfig) build() (smsSender, error) {
	return newBytePlusSender(c)
}

func newBytePlusSender(cfg BytePlusConfig) (*bytePlusSender, error) {
	if cfg.AccessKey == "" || cfg.SecretKey == "" || cfg.Region == "" ||
		cfg.SmsAccount == "" || cfg.Sign == "" {
		return nil, fmt.Errorf("message: byteplus %q missing required fields", cfg.Name)
	}
	client := resty.New().SetTimeout(pickTimeout(cfg.Timeout))
	return &bytePlusSender{
		regionFilter: regionFilter{Include: cfg.IncludeRegions, Exclude: cfg.ExcludeRegions},
		cfg:          cfg,
		baseURL:      bytePlusBaseURL,
		client:       client,
	}, nil
}

func (b *bytePlusSender) Mode() smsMode { return smsModeTemplate }

func (b *bytePlusSender) Name() string {
	if b.cfg.Name == "" {
		return "byteplus"
	}
	return "byteplus:" + b.cfg.Name
}

// bytePlusSendSmsBody 是 SendSms 请求体的字段定义,与 BytePlus 文档完全对应。
type bytePlusSendSmsBody struct {
	SmsAccount    string `json:"SmsAccount"`
	Sign          string `json:"Sign"`
	TemplateID    string `json:"TemplateID"`
	TemplateParam string `json:"TemplateParam"`
	PhoneNumbers  string `json:"PhoneNumbers"`
}

func (b *bytePlusSender) Send(ctx context.Context, msg SmsMessage) error {
	if msg.TemplateId == "" {
		return fmt.Errorf("%w: byteplus requires TemplateId", ErrInvalidSmsMessage)
	}

	paramJson := ""
	if len(msg.Params) > 0 {
		buf, err := json.Marshal(msg.Params)
		if err != nil {
			return fmt.Errorf("message: byteplus marshal code: %w", err)
		}
		paramJson = string(buf)
	}

	body := bytePlusSendSmsBody{
		SmsAccount:    b.cfg.SmsAccount,
		Sign:          b.cfg.Sign,
		TemplateID:    msg.TemplateId,
		TemplateParam: paramJson,
		PhoneNumbers:  msg.To,
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("message: byteplus marshal body: %w", err)
	}

	now := time.Now().UTC()
	xDate := now.Format(bytePlusISOLayout)
	dateShort := now.Format(bytePlusDateLayout)

	queryParams := url.Values{
		"Action":  []string{bytePlusAction},
		"Version": []string{bytePlusVersion},
	}
	canonicalQuery := canonicalQueryString(queryParams)

	parsed, err := url.Parse(b.baseURL)
	if err != nil {
		return fmt.Errorf("message: byteplus parse base url: %w", err)
	}
	host := parsed.Host

	bodyHash := hexSha256(bodyBytes)
	signedHeaders := "content-type;host;x-content-sha256;x-date"
	canonicalHeaders := "content-type:application/json\n" +
		"host:" + host + "\n" +
		"x-content-sha256:" + bodyHash + "\n" +
		"x-date:" + xDate + "\n"

	canonicalRequest := strings.Join([]string{
		"POST",
		"/",
		canonicalQuery,
		canonicalHeaders,
		signedHeaders,
		bodyHash,
	}, "\n")

	credentialScope := dateShort + "/" + b.cfg.Region + "/" + bytePlusService + "/request"
	stringToSign := strings.Join([]string{
		bytePlusAlgorithm,
		xDate,
		credentialScope,
		hexSha256([]byte(canonicalRequest)),
	}, "\n")

	signingKey := volcSigningKey(b.cfg.SecretKey, dateShort, b.cfg.Region, bytePlusService)
	signature := hexHmacSha256(signingKey, []byte(stringToSign))
	authorization := fmt.Sprintf("%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		bytePlusAlgorithm, b.cfg.AccessKey, credentialScope, signedHeaders, signature)

	resp, err := b.client.R().
		SetContext(ctx).
		SetHeader("Content-Type", "application/json").
		SetHeader("Host", host).
		SetHeader("X-Date", xDate).
		SetHeader("X-Content-Sha256", bodyHash).
		SetHeader("Authorization", authorization).
		SetQueryString(canonicalQuery).
		SetBody(bodyBytes).
		Post(b.baseURL + "/")
	if err != nil {
		return fmt.Errorf("message: byteplus send: %w", err)
	}
	if !resp.IsStatusSuccess() {
		return fmt.Errorf("message: byteplus send status=%d body=%s",
			resp.StatusCode(), resp.String())
	}
	return nil
}

// canonicalQueryString 返回按 key 排序、URL-encoded 后用 & 连接的 query string。
// 同 key 多值按出现顺序保留。
func canonicalQueryString(values url.Values) string {
	keys := slices.Sorted(maps.Keys(values))

	var sb strings.Builder
	first := true
	for _, k := range keys {
		for _, v := range values[k] {
			if !first {
				sb.WriteByte('&')
			}
			first = false
			sb.WriteString(url.QueryEscape(k))
			sb.WriteByte('=')
			sb.WriteString(url.QueryEscape(v))
		}
	}
	return sb.String()
}

// volcSigningKey 派生 Volcengine V4 签名密钥。
// 链:secretKey → kDate → kRegion → kService → kSigning。
func volcSigningKey(secret, date, region, service string) []byte {
	kDate := hmacSha256([]byte(secret), []byte(date))
	kRegion := hmacSha256(kDate, []byte(region))
	kService := hmacSha256(kRegion, []byte(service))
	return hmacSha256(kService, []byte("request"))
}
