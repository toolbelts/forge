package message

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

// 本文件集中放各短信供应商签名可复用的 hash 原语。
// 各家签名算法本身差异大(BytePlus V4 用 SHA256-hex,Aliyun V1 用
// HMAC-SHA1-base64),不强行统一签名器,只共享底层摘要工具。

// hexSha256 对 data 求 SHA256 后返回小写 hex。
func hexSha256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// hmacSha256 返回 HMAC-SHA256(key, data) 的二进制摘要。
func hmacSha256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

// hexHmacSha256 返回 HMAC-SHA256(key, data) 的小写 hex。
func hexHmacSha256(key, data []byte) string {
	return hex.EncodeToString(hmacSha256(key, data))
}
