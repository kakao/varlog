package main

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"time"

	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/unit"
	"go.uber.org/multierr"
	"google.golang.org/grpc"

	"github.com/kakao/varlog/pkg/util/telemetry"
	"github.com/kakao/varlog/proto/rpcbenchpb"
)

type client struct {
	conn      *grpc.ClientConn
	rpcClient rpcbenchpb.RPCBenchClient
	tm        telemetry.Telemetry

	rng           *rand.Rand
	dataSizeBytes int

	benchCount    int
	benchDuration time.Duration

	callResponseTime metric.Float64Histogram
}

func newClient(addr, telemetryEndpoint string, dataSizeBytes, benchCount int, benchDuration time.Duration) (*client, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, errors.WithStack(err)
	}

	c := &client{
		conn:          conn,
		rpcClient:     rpcbenchpb.NewRPCBenchClient(conn),
		rng:           rand.New(rand.NewSource(time.Now().UnixNano())),
		dataSizeBytes: dataSizeBytes,
		benchCount:    benchCount,
		benchDuration: benchDuration,
	}

	exporterType := "otel"
	if len(telemetryEndpoint) == 0 {
		exporterType = "nop"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	tm, err := telemetry.New(
		ctx,
		"rpcbench",
		"rpcbench",
		telemetry.WithExporterType(exporterType),
		telemetry.WithEndpoint(telemetryEndpoint),
	)
	if err != nil {
		log.Fatalf("telemetry initialization: %+v", err)
	}
	c.tm = tm

	c.callResponseTime, err = telemetry.Meter("rpcbench").NewFloat64Histogram(
		"rpcbench.call.response_time",
		metric.WithUnit(unit.Milliseconds),
	)
	if err != nil {
		log.Fatalf("metric initialization: %+v", err)
	}

	log.Printf("client is initialized: server_addr=%s, telemetry=type=%s,endpoint=%s, bench=count=%d,duration=%v, payload=size=%d", addr, exporterType, telemetryEndpoint, c.benchCount, c.benchDuration, c.dataSizeBytes)

	return c, nil
}

func (c *client) Run() error {
	data := make([]byte, c.dataSizeBytes)
	_, _ = c.rng.Read(data)

	ctx, cancel := context.WithTimeout(context.Background(), c.benchDuration)
	defer cancel()

	for i := 0; i < c.benchCount; i++ {

		if err := c.Call(ctx, data); err != nil {
			return err
		}

	}
	return nil
}

func (c *client) Call(ctx context.Context, data []byte) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	start := time.Now()

	if _, err := c.rpcClient.Call(ctx, &rpcbenchpb.Request{}); err != nil {
		return errors.WithStack(err)
	}

	c.callResponseTime.Record(
		ctx,
		float64(time.Since(start).Microseconds())/1000.0,
	)
	return nil
}

func (c *client) Close() error {
	err := multierr.Append(
		c.conn.Close(),
		c.tm.Close(context.Background()),
	)
	return errors.WithStack(err)
}

func main() {
	var (
		cl  *client
		err error
	)
	cl, err = newClient(serverAddr, telemetryEndpoint, dataSizeBytes, benchCount, benchDuration)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	defer func() {
		err = multierr.Append(err, cl.Close())
		if err != nil {
			log.Fatalf("%+v", err)
		}
	}()

	err = cl.Run()
}

const (
	defaultTelemetryEndpoint = "localhost:4317"
	defaultDataSizeBytes     = 4
	defaultBenchCount        = 1e4
	defaultBenchDuration     = 5 * time.Minute
)

var (
	serverAddr        string
	dataSizeBytes     int
	telemetryEndpoint string
	benchCount        int
	benchDuration     time.Duration
)

func init() {
	flag.StringVar(&serverAddr, "a", "", "server address")
	flag.StringVar(&telemetryEndpoint, "t", defaultTelemetryEndpoint, "telemetry endpoint")
	flag.IntVar(&dataSizeBytes, "s", defaultDataSizeBytes, "data size in bytes")
	flag.IntVar(&benchCount, "c", defaultBenchCount, "bench count")
	flag.DurationVar(&benchDuration, "d", defaultBenchDuration, "bench duration")
	flag.Parse()

}
