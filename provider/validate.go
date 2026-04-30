package provider

import (
	"context"

	"buf.build/go/protovalidate"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/toolbelts/forge/errkit"
	"github.com/toolbelts/forge/ioc"
)

// ValidateProvider 集成 buf 的 protovalidate，在 handler 之前自动校验请求消息。
// 业务方在 .proto 字段上写 [(buf.validate.field).xxx] 注解即可触发，无需手写校验逻辑。
//
// 流式 RPC 由于校验语义不同（每条 RecvMsg 校验），本期只做一元拦截器。
type ValidateProvider struct{}

// Register 无前置依赖。
func (p *ValidateProvider) Register(ctx context.Context) error { return nil }

// Setup 创建 validator 并加进 chain。
func (p *ValidateProvider) Setup(ctx context.Context) error {
	v, err := protovalidate.New()
	if err != nil {
		return err
	}
	chain := ioc.MustGet[*InterceptorChain](ctx)
	chain.Use(validateUnaryInterceptor(v))
	log.Ctx(ctx).Info().Str("provider", "validate").Msg("validate interceptor registered")
	return nil
}

// validateUnaryInterceptor 若请求是 proto.Message 则跑 protovalidate,失败转 errkit.CodeInvalidArgument。
func validateUnaryInterceptor(v protovalidate.Validator) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if msg, ok := req.(proto.Message); ok {
			if err := v.Validate(msg); err != nil {
				return nil, errkit.New(errkit.CodeInvalidArgument, err.Error()).
					WithCause(err)
			}
		}
		return handler(ctx, req)
	}
}
