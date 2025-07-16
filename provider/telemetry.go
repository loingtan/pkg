package telemetry

import (
	"context"
	"fmt"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/prometheus"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type TelemetryManager struct {
	service_name        string
	tracer              oteltrace.Tracer
	meter               otelmetric.Meter
	prometheusExporter  *prometheus.Exporter
	shutdownTracer      func(context.Context) error
	httpRequestsTotal   otelmetric.Int64Counter
	httpRequestDuration otelmetric.Float64Histogram
	grpcRequestsTotal   otelmetric.Int64Counter
	grpcRequestDuration otelmetric.Float64Histogram
}

func NewTelemetryManager(serviceName string) (*TelemetryManager, error) {

	shutdownTracer, err := InitTracerProvider(serviceName)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize tracer: %w", err)
	}

	prometheusExporter, err := InitMeterProvider(serviceName)
	if err != nil {
		shutdownTracer(context.Background())
		return nil, fmt.Errorf("failed to initialize metrics: %w", err)
	}

	tracer := otel.Tracer(serviceName)
	meter := otel.Meter(serviceName)

	tm := &TelemetryManager{
		service_name:       serviceName,
		tracer:             tracer,
		meter:              meter,
		prometheusExporter: prometheusExporter,
		shutdownTracer:     shutdownTracer,
	}

	if err := tm.initMetrics(); err != nil {
		shutdownTracer(context.Background())
		return nil, fmt.Errorf("failed to initialize custom metrics: %w", err)
	}

	return tm, nil
}

func (tm *TelemetryManager) initMetrics() error {
	var err error

	tm.httpRequestsTotal, err = tm.meter.Int64Counter(
		"http_requests_total",
		otelmetric.WithDescription("Total number of HTTP requests"),
	)
	if err != nil {
		return err
	}

	tm.httpRequestDuration, err = tm.meter.Float64Histogram(
		"http_request_duration_seconds",
		otelmetric.WithDescription("HTTP request duration in seconds"),
		otelmetric.WithUnit("s"),
	)
	if err != nil {
		return err
	}

	tm.grpcRequestsTotal, err = tm.meter.Int64Counter(
		"grpc_requests_total",
		otelmetric.WithDescription("Total number of gRPC requests"),
	)
	if err != nil {
		return err
	}

	tm.grpcRequestDuration, err = tm.meter.Float64Histogram(
		"grpc_request_duration_seconds",
		otelmetric.WithDescription("gRPC request duration in seconds"),
		otelmetric.WithUnit("s"),
	)
	if err != nil {
		return err
	}

	return nil
}

func (tm *TelemetryManager) Shutdown(ctx context.Context) error {
	return tm.shutdownTracer(ctx)
}

func (tm *TelemetryManager) GetPrometheusExporter() *prometheus.Exporter {
	return tm.prometheusExporter
}

func (tm *TelemetryManager) SetupGinServer() *gin.Engine {
	r := gin.New()

	r.Use(otelgin.Middleware(tm.service_name))
	r.Use(tm.ginMetricsMiddleware())
	r.Use(gin.Recovery())

	return r
}

func (tm *TelemetryManager) ginMetricsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		c.Next()

		duration := time.Since(start).Seconds()

		labels := []attribute.KeyValue{
			attribute.String("method", c.Request.Method),
			attribute.String("route", c.FullPath()),
			attribute.Int("status_code", c.Writer.Status()),
		}

		tm.httpRequestsTotal.Add(c.Request.Context(), 1, otelmetric.WithAttributes(labels...))
		tm.httpRequestDuration.Record(c.Request.Context(), duration, otelmetric.WithAttributes(labels...))
	}
}

func (tm *TelemetryManager) GrpcClientMetricsInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		start := time.Now()

		err := invoker(ctx, method, req, reply, cc, opts...)

		duration := time.Since(start).Seconds()
		status := "OK"
		if err != nil {
			status = "ERROR"
		}

		labels := []attribute.KeyValue{
			attribute.String("method", method),
			attribute.String("status", status),
			attribute.String("type", "client"),
		}

		tm.grpcRequestsTotal.Add(ctx, 1, otelmetric.WithAttributes(labels...))
		tm.grpcRequestDuration.Record(ctx, duration, otelmetric.WithAttributes(labels...))

		return err
	}
}

func (tm *TelemetryManager) GrpcServerMetricsInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()

		resp, err := handler(ctx, req)

		duration := time.Since(start).Seconds()
		status := "OK"
		if err != nil {
			status = "ERROR"
		}

		labels := []attribute.KeyValue{
			attribute.String("method", info.FullMethod),
			attribute.String("status", status),
			attribute.String("type", "server"),
		}

		tm.grpcRequestsTotal.Add(ctx, 1, otelmetric.WithAttributes(labels...))
		tm.grpcRequestDuration.Record(ctx, duration, otelmetric.WithAttributes(labels...))

		return resp, err
	}
}

func (tm *TelemetryManager) AddSpanEvent(ctx context.Context, name string, attributes ...attribute.KeyValue) {
	span := oteltrace.SpanFromContext(ctx)
	span.AddEvent(name, oteltrace.WithAttributes(attributes...))
}

func (tm *TelemetryManager) SetSpanStatus(ctx context.Context, code codes.Code, description string) {
	span := oteltrace.SpanFromContext(ctx)
	span.SetStatus(code, description)
}

func (tm *TelemetryManager) StartSpan(ctx context.Context, name string, opts ...oteltrace.SpanStartOption) (context.Context, oteltrace.Span) {
	return tm.tracer.Start(ctx, name, opts...)
}

func (tm *TelemetryManager) InjectTraceContext(ctx context.Context) context.Context {
	md := metadata.MD{}
	otel.GetTextMapPropagator().Inject(ctx, &metadataSupplier{metadata: &md})
	return metadata.NewOutgoingContext(ctx, md)
}

func (tm *TelemetryManager) ExtractTraceContext(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, &metadataSupplier{metadata: &md})
}

type metadataSupplier struct {
	metadata *metadata.MD
}

func (s *metadataSupplier) Get(key string) string {
	values := s.metadata.Get(key)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func (s *metadataSupplier) Set(key, value string) {
	s.metadata.Set(key, value)
}

func (s *metadataSupplier) Keys() []string {
	keys := make([]string, 0, len(*s.metadata))
	for k := range *s.metadata {
		keys = append(keys, k)
	}
	return keys
}
func InitTracerProvider(serviceName string) (func(context.Context) error, error) {
	exporter, err := otlptracehttp.New(context.Background())
	if err != nil {
		return nil, err
	}

	tp := trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceName),
		)),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
}

func InitMeterProvider(serviceName string) (*prometheus.Exporter, error) {
	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String(serviceName),
	)

	exporter, err := prometheus.New()
	if err != nil {
		return nil, err
	}

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(exporter),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(provider)

	return exporter, nil
}
