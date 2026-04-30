package jobqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/rs/zerolog/log"
)

// handler 是 Subscribe 注册后的内部表示。
type handler struct {
	topic       string
	fn          reflect.Value
	paramTypes  []reflect.Type // 不含 leading ctx 的剩余入参类型
	concurrency int            // 默认 1
	batch       int            // 0 = BRPOP;>1 = BLMPOP COUNT batch
}

var (
	ctxType = reflect.TypeFor[context.Context]()
	errType = reflect.TypeFor[error]()
)

// newHandler 用反射校验 fn 形态,生成 handler。要求:
//   - 至少一个入参,且第一个是 context.Context
//   - 出参恰好 1 个 error
func newHandler(topic string, fn any) (*handler, error) {
	if fn == nil {
		return nil, fmt.Errorf("%w: fn is nil", ErrInvalidFunc)
	}
	v := reflect.ValueOf(fn)
	t := v.Type()
	if t.Kind() != reflect.Func {
		return nil, fmt.Errorf("%w: fn must be a function, got %s", ErrInvalidFunc, t.Kind())
	}
	if t.NumIn() < 1 || t.In(0) != ctxType {
		return nil, fmt.Errorf("%w: first param must be context.Context", ErrInvalidFunc)
	}
	if t.NumOut() != 1 || t.Out(0) != errType {
		return nil, fmt.Errorf("%w: must return exactly one error", ErrInvalidFunc)
	}

	paramTypes := make([]reflect.Type, 0, t.NumIn()-1)
	for i := 1; i < t.NumIn(); i++ {
		paramTypes = append(paramTypes, t.In(i))
	}

	return &handler{
		topic:       topic,
		fn:          v,
		paramTypes:  paramTypes,
		concurrency: 1,
	}, nil
}

// dispatch 解码 payload 并调用 handler。所有错误(panic / 解码失败 / 参数个数不匹配 / fn 返回 err)
// 均仅打日志后返回,不会让 worker 退出 —— at-most-once 语义下消息直接丢弃。
func (h *handler) dispatch(ctx context.Context, payload []byte) {
	defer func() {
		if r := recover(); r != nil {
			log.Ctx(ctx).Error().
				Str("topic", h.topic).
				Interface("panic", r).
				Msg("jobqueue: handler panicked")
		}
	}()

	args, err := h.decode(payload)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).
			Str("topic", h.topic).
			Bytes("payload", truncate(payload, 256)).
			Msg("jobqueue: decode failed, drop message")
		return
	}

	in := make([]reflect.Value, 0, len(args)+1)
	in = append(in, reflect.ValueOf(ctx))
	in = append(in, args...)

	start := time.Now()
	out := h.fn.Call(in)
	dur := time.Since(start)

	errVal := out[0]
	if !errVal.IsNil() {
		log.Ctx(ctx).Error().
			Err(errVal.Interface().(error)).
			Str("topic", h.topic).
			Dur("duration", dur).
			Msg("jobqueue: handler returned error, drop message")
		return
	}
	log.Ctx(ctx).Debug().
		Str("topic", h.topic).
		Dur("duration", dur).
		Msg("jobqueue: handler done")
}

// decode 将 wire payload (JSON 数组) 解为 fn 期望的 reflect.Value 序列。
// 统一走 reflect.New(t).Interface() 解码,避免指针形参出现 **T 问题。
func (h *handler) decode(payload []byte) ([]reflect.Value, error) {
	if len(h.paramTypes) == 0 {
		// fn 只接受 ctx;允许 payload 为 "[]" 或 "null"。其它视为参数个数不匹配。
		if len(payload) == 0 {
			return nil, nil
		}
		var raws []json.RawMessage
		if err := json.Unmarshal(payload, &raws); err != nil {
			return nil, fmt.Errorf("payload not a JSON array: %w", err)
		}
		if len(raws) != 0 {
			return nil, fmt.Errorf("expected 0 args, got %d", len(raws))
		}
		return nil, nil
	}

	var raws []json.RawMessage
	if err := json.Unmarshal(payload, &raws); err != nil {
		return nil, fmt.Errorf("payload not a JSON array: %w", err)
	}
	if len(raws) != len(h.paramTypes) {
		return nil, fmt.Errorf("expected %d args, got %d", len(h.paramTypes), len(raws))
	}

	values := make([]reflect.Value, len(h.paramTypes))
	for i, t := range h.paramTypes {
		ptr := reflect.New(t)
		if err := json.Unmarshal(raws[i], ptr.Interface()); err != nil {
			return nil, fmt.Errorf("decode arg #%d (%s): %w", i, t, err)
		}
		values[i] = ptr.Elem()
	}
	return values, nil
}

// truncate 截断字节串用于日志,避免大 payload 占满日志行。
func truncate(b []byte, n int) []byte {
	if len(b) <= n {
		return b
	}
	return b[:n]
}
