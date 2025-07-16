package telemetry

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
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
	"google.golang.org/grpc/status"
)

// TelemetryManager manages all telemetry components
type TelemetryManager struct {
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

	// gRPC metrics
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

// Shutdown gracefully shuts down telemetry
func (tm *TelemetryManager) Shutdown(ctx context.Context) error {
	return tm.shutdownTracer(ctx)
}

// GetPrometheusExporter returns the Prometheus exporter for metrics endpoint
func (tm *TelemetryManager) GetPrometheusExporter() *prometheus.Exporter {
	return tm.prometheusExporter
}

// ginMetricsMiddleware creates a middleware for custom HTTP metrics

// grpcClientMetricsInterceptor creates a unary interceptor for gRPC client metrics
func (tm *TelemetryManager) grpcClientMetricsInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		start := time.Now()

		err := invoker(ctx, method, req, reply, cc, opts...)

		duration := time.Since(start).Seconds()
		status := "OK"
		if err != nil {
			status = "ERROR"
		}

		// Record metrics
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
func (tm *TelemetryManager) grpcServerMetricsInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()

		resp, err := handler(ctx, req)

		duration := time.Since(start).Seconds()
		status := "OK"
		if err != nil {
			status = "ERROR"
		}

		// Record metrics
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

// AddSpanEvent adds an event to the current span
func (tm *TelemetryManager) AddSpanEvent(ctx context.Context, name string, attributes ...attribute.KeyValue) {
	span := oteltrace.SpanFromContext(ctx)
	span.AddEvent(name, oteltrace.WithAttributes(attributes...))
}

// SetSpanStatus sets the status of the current span
func (tm *TelemetryManager) SetSpanStatus(ctx context.Context, code codes.Code, description string) {
	span := oteltrace.SpanFromContext(ctx)
	span.SetStatus(code, description)
}

// StartSpan starts a new span
func (tm *TelemetryManager) StartSpan(ctx context.Context, name string, opts ...oteltrace.SpanStartOption) (context.Context, oteltrace.Span) {
	return tm.tracer.Start(ctx, name, opts...)
}

// InjectTraceContext injects trace context into gRPC metadata
func (tm *TelemetryManager) InjectTraceContext(ctx context.Context) context.Context {
	md := metadata.MD{}
	otel.GetTextMapPropagator().Inject(ctx, &metadataSupplier{metadata: &md})
	return metadata.NewOutgoingContext(ctx, md)
}

// ExtractTraceContext extracts trace context from gRPC metadata
func (tm *TelemetryManager) ExtractTraceContext(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, &metadataSupplier{metadata: &md})
}

// metadataSupplier implements the TextMapCarrier interface for gRPC metadata
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

// }
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

// InitMeterProvider initializes a Prometheus meter provider.
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
func (tm *TelemetryManager) GinTracingMiddleware(serviceName string) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Create span name from method and path
		spanName := fmt.Sprintf("%s %s", c.Request.Method, c.FullPath())
		if c.FullPath() == "" {
			spanName = fmt.Sprintf("%s %s", c.Request.Method, c.Request.URL.Path)
		}

		ctx, span := tm.StartSpan(c.Request.Context(), spanName)
		defer span.End()

		// Set request context with span
		c.Request = c.Request.WithContext(ctx)

		// Add span attributes
		span.SetAttributes(
			attribute.String("service.name", serviceName),
			attribute.String("http.method", c.Request.Method),
			attribute.String("http.url", c.Request.URL.String()),
			attribute.String("http.scheme", c.Request.URL.Scheme),
			attribute.String("http.host", c.Request.Host),
			attribute.String("http.target", c.Request.URL.Path),
			attribute.String("http.route", c.FullPath()),
			attribute.String("http.user_agent", c.Request.UserAgent()),
			attribute.String("http.client_ip", c.ClientIP()),
			attribute.String("http.request_id", c.GetHeader("X-Request-ID")),
		)

		// Add event for request start
		tm.AddSpanEvent(ctx, "http.request.start")

		// Process request
		c.Next()

		// Add response attributes
		span.SetAttributes(
			attribute.Int("http.status_code", c.Writer.Status()),
			attribute.Int("http.response_size", c.Writer.Size()),
		)

		// Set span status based on HTTP status code
		if c.Writer.Status() >= 400 {
			span.SetStatus(codes.Error, fmt.Sprintf("HTTP %d", c.Writer.Status()))
		} else {
			span.SetStatus(codes.Ok, "")
		}

		// Add event for request completion
		tm.AddSpanEvent(ctx, "http.request.complete",
			attribute.Int("http.status_code", c.Writer.Status()),
		)
	}
}

// GinMetricsMiddleware creates a Gin middleware for custom metrics
func (tm *TelemetryManager) GinMetricsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		// Process request
		c.Next()

		duration := time.Since(start).Seconds()

		// Record metrics with detailed labels
		labels := []attribute.KeyValue{
			attribute.String("method", c.Request.Method),
			attribute.String("route", c.FullPath()),
			attribute.String("status_code", fmt.Sprintf("%d", c.Writer.Status())),
			attribute.String("status_class", getStatusClass(c.Writer.Status())),
		}

		tm.httpRequestsTotal.Add(c.Request.Context(), 1, otelmetric.WithAttributes(labels...))
		tm.httpRequestDuration.Record(c.Request.Context(), duration, otelmetric.WithAttributes(labels...))
	}
}

// GinLoggingMiddleware creates a Gin middleware for structured logging with tracing
func (tm *TelemetryManager) GinLoggingMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		raw := c.Request.URL.RawQuery

		// Process request
		c.Next()

		// Get span from context for trace correlation
		span := oteltrace.SpanFromContext(c.Request.Context())
		spanContext := span.SpanContext()

		// Create log entry with trace information
		duration := time.Since(start)

		// Add span event with detailed request information
		tm.AddSpanEvent(c.Request.Context(), "http.request.logged",
			attribute.String("path", path),
			attribute.String("query", raw),
			attribute.String("method", c.Request.Method),
			attribute.Int("status", c.Writer.Status()),
			attribute.String("duration", duration.String()),
			attribute.String("trace_id", spanContext.TraceID().String()),
			attribute.String("span_id", spanContext.SpanID().String()),
		)
	}
}

// GinErrorHandlingMiddleware creates a Gin middleware for error handling with tracing
func (tm *TelemetryManager) GinErrorHandlingMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Process request
		c.Next()

		// Handle errors if any
		if len(c.Errors) > 0 {
			ctx := c.Request.Context()

			// Get the last error
			err := c.Errors.Last()

			// Add error information to span
			span := oteltrace.SpanFromContext(ctx)
			span.SetStatus(codes.Error, err.Error())
			span.SetAttributes(
				attribute.String("error.type", err.Type.String()),
				attribute.String("error.message", err.Error()),
			)

			// Add error event
			tm.AddSpanEvent(ctx, "http.error.occurred",
				attribute.String("error.message", err.Error()),
				attribute.String("error.type", err.Type.String()),
			)
		}
	}
}

func (tm *TelemetryManager) GRPCTracingUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Extract trace context from metadata
		ctx = tm.ExtractTraceContext(ctx)

		// Create span for the gRPC method
		ctx, span := tm.StartSpan(ctx, info.FullMethod)
		defer span.End()

		// Add span attributes
		span.SetAttributes(
			attribute.String("rpc.system", "grpc"),
			attribute.String("rpc.service", getServiceName(info.FullMethod)),
			attribute.String("rpc.method", getMethodName(info.FullMethod)),
			attribute.String("rpc.grpc.status_code", "OK"), // Will be updated if error occurs
		)

		// Add event for request start
		tm.AddSpanEvent(ctx, "rpc.request.start")

		// Call the handler
		resp, err := handler(ctx, req)

		// Update span based on response
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.SetAttributes(
				attribute.String("rpc.grpc.status_code", status.Code(err).String()),
				attribute.String("error.message", err.Error()),
			)
			tm.AddSpanEvent(ctx, "rpc.error.occurred",
				attribute.String("error.message", err.Error()),
			)
		} else {
			span.SetStatus(codes.Ok, "")
			tm.AddSpanEvent(ctx, "rpc.request.complete")
		}

		return resp, err
	}
}

// GRPCTracingStreamServerInterceptor creates a gRPC stream server interceptor for tracing
func (tm *TelemetryManager) GRPCTracingStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// Extract trace context from metadata
		ctx := tm.ExtractTraceContext(stream.Context())

		// Create span for the gRPC method
		ctx, span := tm.StartSpan(ctx, info.FullMethod)
		defer span.End()

		// Add span attributes
		span.SetAttributes(
			attribute.String("rpc.system", "grpc"),
			attribute.String("rpc.service", getServiceName(info.FullMethod)),
			attribute.String("rpc.method", getMethodName(info.FullMethod)),
			attribute.Bool("rpc.grpc.stream", true),
		)

		// Create wrapped stream with new context
		wrappedStream := &wrappedServerStream{
			ServerStream: stream,
			ctx:          ctx,
		}

		// Add event for stream start
		tm.AddSpanEvent(ctx, "rpc.stream.start")

		// Call the handler
		err := handler(srv, wrappedStream)

		// Update span based on response
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.SetAttributes(
				attribute.String("rpc.grpc.status_code", status.Code(err).String()),
				attribute.String("error.message", err.Error()),
			)
		} else {
			span.SetStatus(codes.Ok, "")
		}

		tm.AddSpanEvent(ctx, "rpc.stream.complete")

		return err
	}
}

// GRPCMetricsUnaryServerInterceptor creates a gRPC unary server interceptor for metrics
func (tm *TelemetryManager) GRPCMetricsUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()

		// Call the handler
		resp, err := handler(ctx, req)

		duration := time.Since(start).Seconds()

		// Record metrics
		statusCode := "OK"
		if err != nil {
			statusCode = status.Code(err).String()
		}

		labels := []attribute.KeyValue{
			attribute.String("rpc.service", getServiceName(info.FullMethod)),
			attribute.String("rpc.method", getMethodName(info.FullMethod)),
			attribute.String("rpc.grpc.status_code", statusCode),
			attribute.String("rpc.type", "unary"),
		}

		tm.grpcRequestsTotal.Add(ctx, 1, otelmetric.WithAttributes(labels...))
		tm.grpcRequestDuration.Record(ctx, duration, otelmetric.WithAttributes(labels...))

		return resp, err
	}
}

// GRPCTracingUnaryClientInterceptor creates a gRPC unary client interceptor for tracing
func (tm *TelemetryManager) GRPCTracingUnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		// Create span for the gRPC call
		ctx, span := tm.StartSpan(ctx, method)
		defer span.End()

		// Add span attributes
		span.SetAttributes(
			attribute.String("rpc.system", "grpc"),
			attribute.String("rpc.service", getServiceName(method)),
			attribute.String("rpc.method", getMethodName(method)),
			attribute.String("rpc.grpc.status_code", "OK"), // Will be updated if error occurs
		)

		// Inject trace context into metadata
		ctx = tm.InjectTraceContext(ctx)

		// Add event for request start
		tm.AddSpanEvent(ctx, "rpc.client.request.start")

		// Make the call
		err := invoker(ctx, method, req, reply, cc, opts...)

		// Update span based on response
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.SetAttributes(
				attribute.String("rpc.grpc.status_code", status.Code(err).String()),
				attribute.String("error.message", err.Error()),
			)
		} else {
			span.SetStatus(codes.Ok, "")
		}

		tm.AddSpanEvent(ctx, "rpc.client.request.complete")

		return err
	}
}

// GRPCMetricsUnaryClientInterceptor creates a gRPC unary client interceptor for metrics
func (tm *TelemetryManager) GRPCMetricsUnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		start := time.Now()

		// Make the call
		err := invoker(ctx, method, req, reply, cc, opts...)

		duration := time.Since(start).Seconds()

		// Record metrics
		statusCode := "OK"
		if err != nil {
			statusCode = status.Code(err).String()
		}

		labels := []attribute.KeyValue{
			attribute.String("rpc.service", getServiceName(method)),
			attribute.String("rpc.method", getMethodName(method)),
			attribute.String("rpc.grpc.status_code", statusCode),
			attribute.String("rpc.type", "unary"),
		}

		tm.grpcRequestsTotal.Add(ctx, 1, otelmetric.WithAttributes(labels...))
		tm.grpcRequestDuration.Record(ctx, duration, otelmetric.WithAttributes(labels...))

		return err
	}
}
func getStatusClass(statusCode int) string {
	switch {
	case statusCode >= 100 && statusCode < 200:
		return "1xx"
	case statusCode >= 200 && statusCode < 300:
		return "2xx"
	case statusCode >= 300 && statusCode < 400:
		return "3xx"
	case statusCode >= 400 && statusCode < 500:
		return "4xx"
	case statusCode >= 500:
		return "5xx"
	default:
		return "unknown"
	}
}

// getServiceName extracts service name from gRPC method
func getServiceName(method string) string {
	parts := strings.Split(method, "/")
	if len(parts) >= 2 {
		return parts[1]
	}
	return "unknown"
}

// getMethodName extracts method name from gRPC method
func getMethodName(method string) string {
	parts := strings.Split(method, "/")
	if len(parts) >= 3 {
		return parts[2]
	}
	return "unknown"
}

// wrappedServerStream wraps grpc.ServerStream to provide a custom context
type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedServerStream) Context() context.Context {
	return w.ctx
}
func (tm *TelemetryManager) SetupGinServerWithAllMiddleware(serviceName string) *gin.Engine {
	r := gin.New()
	r.Use(tm.GinTracingMiddleware(serviceName))
	r.Use(tm.GinMetricsMiddleware())
	r.Use(tm.GinLoggingMiddleware())
	r.Use(tm.GinErrorHandlingMiddleware())
	r.Use(gin.Recovery())
	return r
}
