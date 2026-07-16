package provider

import "github.com/spf13/viper"

const (
	observabilityComponentRedis         = "redis"
	observabilityComponentDatabase      = "database"
	observabilityComponentGrpc          = "grpc"
	observabilityComponentGateway       = "gateway"
	observabilityComponentJobqueue      = "jobqueue"
	observabilityComponentReliableQueue = "reliablequeue"
)

// traceInstrumentationEnabled 返回指定组件是否启用 trace 自动插桩。
func traceInstrumentationEnabled(v *viper.Viper, component string) bool {
	return instrumentationEnabled(v, "trace", component)
}

// metricsInstrumentationEnabled 返回指定组件是否启用 metrics 自动插桩。
func metricsInstrumentationEnabled(v *viper.Viper, component string) bool {
	return instrumentationEnabled(v, "metrics", component)
}

func instrumentationEnabled(v *viper.Viper, signal, component string) bool {
	componentKey := signal + ".instrumentation." + component
	if v.IsSet(componentKey) {
		return v.GetBool(componentKey)
	}

	globalKey := signal + ".instrumentation.enabled"
	if v.IsSet(globalKey) {
		return v.GetBool(globalKey)
	}

	return v.GetBool(signal + ".enabled")
}
