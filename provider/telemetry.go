package telemetry

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"google.golang.org/grpc"
)

type Config struct {
	Service      string // REQUIRED
	OTLPEndpoint string // default "otel-collector:4317"
	InsecureOTLP bool   // default true

	MetricsAddr string // default ":9464"  ← NEW
}

func (c *Config) WithDefaults() *Config {
	if c.Service == "" {
		panic("telemetry.Config.Service is required")
	}
	if c.OTLPEndpoint == "" {
		c.OTLPEndpoint = "otel-collector:4317"
	}
	if c.MetricsAddr == "" {
		c.MetricsAddr = ":9464"
	}
	return c
}

type Telemetry struct {
	Metrics  http.Handler                    // expose at /metrics
	Shutdown func(ctx context.Context) error // defer this in main()
}

func Init(cfg Config) (*Telemetry, error) {
	cfg = *cfg.WithDefaults() // panics if Service == ""

	// ----- common resource -----
	res, _ := resource.Merge(resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL,
			semconv.ServiceNameKey.String(cfg.Service)),
	)

	// ----- traces → OTLP -----
	exp, err := otlptracegrpc.New(context.Background(),
		otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint),
		otlptracegrpc.WithInsecure(), // collector inside docker‑net; TLS off
	)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	promExp, err := prometheus.New()
	if err != nil {
		return nil, err
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(promExp),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)
	otel.SetMeterProvider(mp)

	// Set up proper propagation for HTTP calls
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return &Telemetry{
		Metrics:  promhttp.Handler(),
		Shutdown: tp.Shutdown,
	}, nil
}
func AttachGin(r *gin.Engine, service string, t *Telemetry) {
	r.Use(otelgin.Middleware(service))
}
func ServerOption() grpc.ServerOption {
	return grpc.StatsHandler(otelgrpc.NewServerHandler())
}

func DialOption() grpc.DialOption {
	return grpc.WithStatsHandler(otelgrpc.NewClientHandler())
}

// HTTPClient returns an HTTP client instrumented with OpenTelemetry
func HTTPClient() *http.Client {
	return &http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}
}
