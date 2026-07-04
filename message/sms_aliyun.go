package message

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strings"
	"time"

	"resty.dev/v3"
)

// Aliyun 国际版 SendMessageToGlobe(Dysmsapi 2018-05-01)走 V1 签名:
// HMAC-SHA1 + base64,公共参数与业务参数同一个 query string。
const (
	aliyunSmsAction     = "SendMessageToGlobe"
	aliyunSmsVersion    = "2018-05-01"
	aliyunSmsSigMethod  = "HMAC-SHA1"
	aliyunSmsSigVersion = "1.0"
	aliyunSmsFormat     = "JSON"
	aliyunSmsTsLayout   = "2006-01-02T15:04:05Z"
	aliyunSmsHttpMethod = "GET"
	aliyunSmsNonceBytes = 16
)

// aliyunSmsSender 是 smsSender 的 Aliyun 国际版实现(raw mode)。
type aliyunSmsSender struct {
	regionFilter
	cfg     AliyunSmsConfig
	baseURL string
	client  *resty.Client
}

// build 让 AliyunSmsConfig 实现 smsSpec 接口。
func (c AliyunSmsConfig) build() (smsSender, error) {
	return newAliyunSmsSender(c)
}

func newAliyunSmsSender(cfg AliyunSmsConfig) (*aliyunSmsSender, error) {
	if cfg.AccessKey == "" || cfg.SecretKey == "" || cfg.Endpoint == "" {
		return nil, fmt.Errorf("message: aliyun-sms %q missing access_key/secret_key/endpoint", cfg.Name)
	}
	client := resty.New().SetTimeout(pickTimeout(cfg.Timeout))
	return &aliyunSmsSender{
		regionFilter: regionFilter{Include: cfg.IncludeRegions, Exclude: cfg.ExcludeRegions},
		cfg:          cfg,
		baseURL:      strings.TrimRight(cfg.Endpoint, "/"),
		client:       client,
	}, nil
}

func (a *aliyunSmsSender) Mode() smsMode { return smsModeRaw }

func (a *aliyunSmsSender) Name() string {
	if a.cfg.Name == "" {
		return "aliyun"
	}
	return "aliyun:" + a.cfg.Name
}

func (a *aliyunSmsSender) Send(ctx context.Context, msg SmsMessage) error {
	if msg.Content == "" {
		return fmt.Errorf("%w: aliyun requires Content", ErrInvalidSmsMessage)
	}

	nonce, err := randomHex(aliyunSmsNonceBytes)
	if err != nil {
		return fmt.Errorf("message: aliyun-sms nonce: %w", err)
	}

	params := url.Values{
		// 公共参数
		"Format":           []string{aliyunSmsFormat},
		"Version":          []string{aliyunSmsVersion},
		"AccessKeyId":      []string{a.cfg.AccessKey},
		"SignatureMethod":  []string{aliyunSmsSigMethod},
		"Timestamp":        []string{time.Now().UTC().Format(aliyunSmsTsLayout)},
		"SignatureVersion": []string{aliyunSmsSigVersion},
		"SignatureNonce":   []string{nonce},
		"Action":           []string{aliyunSmsAction},
		// 业务参数
		"To":      []string{msg.To},
		"Message": []string{msg.Content},
	}
	if msg.SignName != "" {
		params.Set("From", msg.SignName)
	}

	signature := aliyunSignV1(aliyunSmsHttpMethod, params, a.cfg.SecretKey)
	params.Set("Signature", signature)

	resp, err := a.client.R().
		SetContext(ctx).
		SetQueryString(params.Encode()).
		Get(a.baseURL + "/")
	if err != nil {
		return fmt.Errorf("message: aliyun-sms send to %s: %w", msg.To, err)
	}
	if !resp.IsStatusSuccess() {
		return fmt.Errorf("message: aliyun-sms send to %s status=%d body=%s",
			msg.To, resp.StatusCode(), resp.String())
	}
	return nil
}

// aliyunSignV1 计算 Aliyun RPC API V1 签名:
//
//	StringToSign = HTTPMethod + "&" + pe("/") + "&" + pe(canonicalQuery)
//	Signature    = base64(HMAC-SHA1(secret + "&", StringToSign))
//
// 其中 canonicalQuery 是按 key 排序的 pe(k)=pe(v) 串,pe 为 Aliyun 严格百分号编码
// (RFC 3986 + "+"→"%20" + "*"→"%2A" + "%7E"→"~")。
func aliyunSignV1(method string, params url.Values, secretKey string) string {
	keys := slices.Sorted(maps.Keys(params))

	var canonical strings.Builder
	for i, k := range keys {
		if i > 0 {
			canonical.WriteByte('&')
		}
		canonical.WriteString(aliyunPercentEncode(k))
		canonical.WriteByte('=')
		canonical.WriteString(aliyunPercentEncode(params.Get(k)))
	}

	stringToSign := method + "&" + aliyunPercentEncode("/") + "&" + aliyunPercentEncode(canonical.String())

	mac := hmac.New(sha1.New, []byte(secretKey+"&"))
	mac.Write([]byte(stringToSign))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// aliyunPercentEncode 对单个值做 Aliyun 严格百分号编码,与 URLEncoder.encode 一致。
func aliyunPercentEncode(s string) string {
	encoded := url.QueryEscape(s)
	encoded = strings.ReplaceAll(encoded, "+", "%20")
	encoded = strings.ReplaceAll(encoded, "*", "%2A")
	encoded = strings.ReplaceAll(encoded, "%7E", "~")
	return encoded
}

// randomHex 返回 n 字节的随机 hex,用作签名 nonce。
func randomHex(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
