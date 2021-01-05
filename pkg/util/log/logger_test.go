package log

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"

	"go.opentelemetry.io/otel/exporters/stdout"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
)

type loggerBenchmark struct {
	loggerOptions Options
	telemetry     bool
	cleanup       func()
	name          string
}

func initLoggerBenchmark(tb testing.TB) []loggerBenchmark {
	var benchmarks []loggerBenchmark
	trueOrFalse := []bool{true, false}
	for _, debugBool := range trueOrFalse {
		for _, disableStderrBool := range trueOrFalse {
			for _, fileBool := range trueOrFalse {
				for _, telemetryBool := range trueOrFalse {
					if disableStderrBool && !fileBool {
						continue
					}
					benchmark := loggerBenchmark{
						loggerOptions: Options{
							DisableLogToStderr: disableStderrBool,
							Debug:              debugBool,
						},
						telemetry: telemetryBool,
						cleanup:   func() {},
					}
					if fileBool {
						tmpdir, err := ioutil.TempDir("", "")
						if err != nil {
							tb.Fatal(err)
						}
						benchmark.loggerOptions.Path = tmpdir
						benchmark.cleanup = func() {
							if err := os.RemoveAll(tmpdir); err != nil {
								tb.Fatal(err)
							}
						}
					}
					benchmark.name = fmt.Sprintf("stderr=%v,debug=%v,file=%v,telemetry=%v", !disableStderrBool, debugBool, fileBool, telemetryBool)
					benchmarks = append(benchmarks, benchmark)
				}
			}
		}
	}
	return benchmarks
}

func BenchmarkZapLogger(b *testing.B) {
	logger, err := zap.NewProduction()
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		_ = logger.Sync()
	}()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		logger.Info("logger benchmark", zap.Int("i", i))
	}
}

func BenchmarkLogger(b *testing.B) {
	benchmarks := initLoggerBenchmark(b)
	for i := range benchmarks {
		benchmark := benchmarks[i]
		b.Run(benchmark.name, func(b *testing.B) {
			benchmarkLogger(b, benchmark)
		})
	}
}

func benchmarkLoggerWithTelemetry(ctx context.Context, logger *Logger, b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.InfoWithTrace(ctx, "logger benchmark", zap.Int("i", i))
	}
}

func benchmarkLoggerWithoutTelemetry(logger *Logger, b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Info("logger benchmark", zap.Int("i", i))
	}
}

func benchmarkLogger(b *testing.B, benchmark loggerBenchmark) {
	logger, err := New(benchmark.loggerOptions)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		_ = logger.Sync()
		benchmark.cleanup()
	}()

	var (
		ctx  = context.Background()
		span trace.Span
	)
	if benchmark.telemetry {
		exporter, err := stdout.NewExporter(
			stdout.WithQuantiles([]float64{0.5, 0.9, 0.99}),
			stdout.WithPrettyPrint(),
		)
		if err != nil {
			b.Fatalf("failed to initialize stdout exporter pipeline: %v", err)
		}

		bsp := sdktrace.NewBatchSpanProcessor(exporter)
		tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(bsp))
		defer func() { _ = tp.Shutdown(context.Background()) }()

		tracer := tp.Tracer("test-tracer")
		ctx, span = tracer.Start(context.TODO(), "test")
		defer span.End()
	}

	if benchmark.telemetry {
		benchmarkLoggerWithTelemetry(ctx, logger, b)
	} else {
		benchmarkLoggerWithoutTelemetry(logger, b)
	}
}

func TestNewLogger(t *testing.T) {
	logger, err := New(Options{Debug: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = logger.Sync()
	}()

	exporter, err := stdout.NewExporter(
		stdout.WithQuantiles([]float64{0.5, 0.9, 0.99}),
		stdout.WithPrettyPrint(),
	)
	if err != nil {
		t.Fatalf("failed to initialize stdout exporter pipeline: %v", err)
	}

	bsp := sdktrace.NewBatchSpanProcessor(exporter)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(bsp))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	tracer := tp.Tracer("test-tracer")
	ctx, span := tracer.Start(context.TODO(), "test")
	defer span.End()

	logger = logger.Named("test").With(zap.String("k1", "v1"))

	logger.InfoWithTrace(ctx, "debug message",
		zap.Bool("bool", true),
		zap.Uint32("uint32", math.MaxUint32),
		zap.Uint64("uint64", math.MaxUint64),
		zap.Float32("float32", math.MaxFloat32),
		zap.Float64("float64", math.MaxFloat64),
		zap.Duration("duration", 10*time.Millisecond),
		zap.Any("any-array", []string{"a", "b", "c"}),
		zap.Int32s("uint32s", []int32{1, 2, 3}),
		zap.Binary("binary", []byte{1, 2, 3}),
		zap.ByteString("bytestring", []byte("123")),
		zap.Any("reflect", struct {
			a int
			b string
		}{a: 1, b: "foo"}),
		zap.Error(os.ErrInvalid),
	)
}

type CtxKey int

func TestContext(t *testing.T) {
	var key CtxKey
	ctxBackground := context.Background()
	ctxParent := context.WithValue(ctxBackground, key, map[string]int{"a": 1, "b": 100})
	ctxChild, cancel := context.WithCancel(ctxParent)
	defer cancel()

	t.Log(ctxBackground.Value(key))
	t.Log(ctxParent.Value(key))
	t.Log(ctxChild.Value(key))

	ctxParent.Value(key).(map[string]int)["c"] = 200

	t.Log(ctxBackground.Value(key))
	t.Log(ctxParent.Value(key))
	t.Log(ctxChild.Value(key))
}
