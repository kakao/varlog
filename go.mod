module github.com/kakao/varlog

go 1.15

require (
	github.com/cockroachdb/pebble v0.0.0-20210222213630-560af6e14c3c
	github.com/gogo/protobuf v1.3.2
	github.com/gogo/status v1.1.0
	github.com/golang/mock v1.4.4
	github.com/google/gofuzz v1.2.0
	github.com/hashicorp/go-multierror v1.0.0
	github.com/hashicorp/vault/api v1.0.4
	github.com/pkg/errors v0.9.1
	github.com/smartystreets/assertions v1.2.0
	github.com/smartystreets/goconvey v1.6.4
	github.com/soheilhy/cmux v0.1.4
	github.com/spf13/cobra v1.1.1
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.7.1
	github.com/stretchr/testify v1.7.0
	github.com/urfave/cli/v2 v2.3.0
	go.etcd.io/etcd v0.0.0-20201125193152-8a03d2e9614b
	go.opentelemetry.io/contrib/instrumentation/host v0.17.0
	go.opentelemetry.io/contrib/instrumentation/runtime v0.17.0
	go.opentelemetry.io/otel v0.17.0
	go.opentelemetry.io/otel/exporters/otlp v0.17.0
	go.opentelemetry.io/otel/exporters/stdout v0.17.0
	go.opentelemetry.io/otel/metric v0.17.0
	go.opentelemetry.io/otel/sdk v0.17.0
	go.opentelemetry.io/otel/sdk/export/metric v0.17.0
	go.opentelemetry.io/otel/sdk/metric v0.17.0
	go.opentelemetry.io/otel/trace v0.17.0
	go.uber.org/goleak v1.1.10
	go.uber.org/multierr v1.6.0
	go.uber.org/zap v1.16.0
	golang.org/x/sync v0.0.0-20201207232520-09787c993a3a
	golang.org/x/sys v0.0.0-20201018230417-eeed37f84f13
	google.golang.org/grpc v1.35.0
	google.golang.org/grpc/examples v0.0.0-20210226164526-c949703b4b98 // indirect
	google.golang.org/protobuf v1.25.0
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	k8s.io/api v0.19.0
	k8s.io/apimachinery v0.19.0
	k8s.io/client-go v0.19.0
)
