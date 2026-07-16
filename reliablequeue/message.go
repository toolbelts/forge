package reliablequeue

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
)

// Message 是可靠队列传输的稳定消息信封。
type Message struct {
	Id       string            `json:"id"`
	Payload  []byte            `json:"payload"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// Decode 把 Payload 解码到 target；空 target 或非法 JSON 返回错误。
func (message Message) Decode(target any) error {
	if target == nil {
		return fmt.Errorf("%w: nil decode target", ErrInvalidMessage)
	}
	if err := json.Unmarshal(message.Payload, target); err != nil {
		return fmt.Errorf("%w: decode payload: %v", ErrInvalidMessage, err)
	}
	return nil
}

// Clone 返回消息及 metadata 的独立副本，Payload 也不会与原切片共享底层数组。
func (message Message) Clone() Message {
	return Message{
		Id:       message.Id,
		Payload:  append([]byte(nil), message.Payload...),
		Metadata: maps.Clone(message.Metadata),
	}
}

// Publication 描述 Handler 成功后需要先可靠发布的一条输出消息。
type Publication struct {
	Topic   string
	Message Message
}

// Result 描述 Handler 成功后的输出；全部 Publications 发布成功后才确认输入消息。
type Result struct {
	Publications []Publication
}

// Handler 处理一条至少一次投递消息，并返回确认前需要发布的结果。
type Handler func(ctx context.Context, message Message) (Result, error)
