package message

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/goccy/go-json"
)

// Aliyun 国内版短信(Dysmsapi 2017-05-25 SendSms)走 V1 签名,template mode。
//
// 与国际版(SendMessageToGlobe)的差异:
//   - Action / Version 不同
//   - Endpoint 默认 dysmsapi.aliyuncs.com
//   - 字段:PhoneNumbers + SignName + TemplateCode + TemplateParam
//   - SignName 即「签名」,通常是 "【公司/品牌】" 形式的前缀
//   - 200 也可能业务失败,需要看响应 body 里的 Code 字段
//
// 上游 PhoneNumbers 支持逗号分隔多个号码,但本组件单条 SmsMessage 只承载一个 To,
// 因此这里直接把 msg.To 作为 PhoneNumbers 下发,不再 join。
const (
	aliyunCnSmsAction         = "SendSms"
	aliyunCnSmsVersion        = "2017-05-25"
	aliyunCnSmsDefaultBaseURL = "https://dysmsapi.aliyuncs.com"
	aliyunCnSmsBizCodeOk      = "OK"
)

// aliyunCnSmsSender 是 smsSender 的 Aliyun 国内实现(template mode)。
type aliyunCnSmsSender struct {
	regionFilter
	cfg     AliyunCnSmsConfig
	baseURL string
	client  *resty.Client
}

// build 让 AliyunCnSmsConfig 实现 smsSpec 接口。
func (c AliyunCnSmsConfig) build() (smsSender, error) {
	return newAliyunCnSmsSender(c)
}

func newAliyunCnSmsSender(cfg AliyunCnSmsConfig) (*aliyunCnSmsSender, error) {
	if cfg.AccessKey == "" || cfg.SecretKey == "" {
		return nil, fmt.Errorf("message: aliyun-cn %q missing access_key/secret_key", cfg.Name)
	}
	endpoint := cfg.Endpoint
	if endpoint == "" {
		endpoint = aliyunCnSmsDefaultBaseURL
	}
	include := cfg.IncludeRegions
	if len(include) == 0 {
		include = []string{"86"}
	}
	client := resty.New().SetTimeout(pickTimeout(cfg.Timeout))
	return &aliyunCnSmsSender{
		regionFilter: regionFilter{Include: include, Exclude: cfg.ExcludeRegions},
		cfg:          cfg,
		baseURL:      strings.TrimRight(endpoint, "/"),
		client:       client,
	}, nil
}

func (a *aliyunCnSmsSender) Mode() smsMode { return smsModeTemplate }

func (a *aliyunCnSmsSender) Name() string {
	if a.cfg.Name == "" {
		return "aliyun-cn"
	}
	return "aliyun-cn:" + a.cfg.Name
}

// aliyunCnSmsResponse 是 SendSms 200 OK 时业务层的关键字段。
// Code == "OK" 才是成功,其它都是业务错误(如 isv.SMS_SIGNATURE_ILLEGAL)。
type aliyunCnSmsResponse struct {
	Code      string `json:"Code"`
	Message   string `json:"Message"`
	RequestId string `json:"RequestId"`
	BizId     string `json:"BizId"`
}

func (a *aliyunCnSmsSender) Send(ctx context.Context, msg SmsMessage) error {
	if msg.TemplateId == "" {
		return fmt.Errorf("%w: aliyun-cn requires TemplateId", ErrInvalidSmsMessage)
	}

	signName := msg.SignName
	if signName == "" {
		signName = a.cfg.SignName
	}
	if signName == "" {
		return fmt.Errorf("%w: aliyun-cn requires SignName (msg or config)", ErrInvalidSmsMessage)
	}

	paramJson := ""
	if len(msg.Params) > 0 {
		buf, err := json.Marshal(msg.Params)
		if err != nil {
			return fmt.Errorf("message: aliyun-cn marshal params: %w", err)
		}
		paramJson = string(buf)
	}

	nonce, err := randomHex(aliyunSmsNonceBytes)
	if err != nil {
		return fmt.Errorf("message: aliyun-cn nonce: %w", err)
	}

	params := url.Values{
		// 公共参数
		"Format":           []string{aliyunSmsFormat},
		"Version":          []string{aliyunCnSmsVersion},
		"AccessKeyId":      []string{a.cfg.AccessKey},
		"SignatureMethod":  []string{aliyunSmsSigMethod},
		"Timestamp":        []string{time.Now().UTC().Format(aliyunSmsTsLayout)},
		"SignatureVersion": []string{aliyunSmsSigVersion},
		"SignatureNonce":   []string{nonce},
		"Action":           []string{aliyunCnSmsAction},
		// 业务参数
		"PhoneNumbers": []string{msg.To},
		"SignName":     []string{signName},
		"TemplateCode": []string{msg.TemplateId},
	}
	if paramJson != "" {
		params.Set("TemplateParam", paramJson)
	}

	signature := aliyunSignV1(aliyunSmsHttpMethod, params, a.cfg.SecretKey)
	params.Set("Signature", signature)

	resp, err := a.client.R().
		SetContext(ctx).
		SetQueryString(params.Encode()).
		Get(a.baseURL + "/")
	if err != nil {
		return fmt.Errorf("message: aliyun-cn send: %w", err)
	}
	if !resp.IsSuccess() {
		return fmt.Errorf("message: aliyun-cn status=%d body=%s",
			resp.StatusCode(), resp.String())
	}

	// 200 也可能业务失败 (Code != "OK"),不解析 body 会静默吞错。
	var body aliyunCnSmsResponse
	if err := json.Unmarshal(resp.Body(), &body); err != nil {
		return fmt.Errorf("message: aliyun-cn parse response: %w (body=%s)", err, resp.String())
	}
	if body.Code != aliyunCnSmsBizCodeOk {
		return fmt.Errorf("message: aliyun-cn biz error code=%s message=%s request_id=%s",
			body.Code, body.Message, body.RequestId)
	}
	return nil
}
