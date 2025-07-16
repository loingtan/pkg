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

// TelemetryManager manages all telemetry components
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

// NewTelemetryManager creates a new telemetry manager
func NewTelemetryManager(serviceName string) (*TelemetryManager, error) {
	// Initialize tracing
	shutdownTracer, err := InitTracerProvider(serviceName)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize tracer: %w", err)
	}

	// Initialize metrics
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

	// Initialize metrics
	if err := tm.initMetrics(); err != nil {
		shutdownTracer(context.Background())
		return nil, fmt.Errorf("failed to initialize custom metrics: %w", err)
	}

	return tm, nil
}

// initMetrics initializes custom metrics
func (tm *TelemetryManager) initMetrics() error {
	var err error

	// HTTP metrics
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

// SetupGinServer sets up a Gin server with OpenTelemetry instrumentation
func (tm *TelemetryManager) SetupGinServer() *gin.Engine {
	r := gin.New()

	// Add OpenTelemetry middleware
	r.Use(otelgin.Middleware(tm.service_name))
	// Add custom metrics middleware
	r.Use(tm.ginMetricsMiddleware())

	// Add other middleware (recovery, logger, etc.)
	r.Use(gin.Recovery())

	return r
}

// ginMetricsMiddleware creates a middleware for custom HTTP metrics
func (tm *TelemetryManager) ginMetricsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		c.Next()

		duration := time.Since(start).Seconds()

		// Record metrics
		labels := []attribute.KeyValue{
			attribute.String("method", c.Request.Method),
			attribute.String("route", c.FullPath()),
			attribute.Int("status_code", c.Writer.Status()),
		}

		tm.httpRequestsTotal.Add(c.Request.Context(), 1, otelmetric.WithAttributes(labels...))
		tm.httpRequestDuration.Record(c.Request.Context(), duration, otelmetric.WithAttributes(labels...))
	}
}

// CreateGRPCClient creates a gRPC client with OpenTelemetry instrumentation
// func (tm *TelemetryManager) CreateGRPCClient(target string) (*grpc.ClientConn, error) {
// 	conn, err := grpc.Dial(target,
// 		grpc.WithTransportCredentials(insecure.NewCredentials()),
// 		grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
// 		grpc.WithStreamInterceptor(otelgrpc.StreamClientInterceptor()),
// 		grpc.WithUnaryInterceptor(tm.grpcClientMetricsInterceptor()),
// 	)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to create gRPC client: %w", err)
// 	}

// 	return conn, nil
// }

// grpcClientMetricsInterceptor creates a unary interceptor for gRPC client metrics
func (tm *TelemetryManager) grpcClientMetricsInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
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

// // CreateGRPCServer creates a gRPC server with OpenTelemetry instrumentation
// func (tm *TelemetryManager) CreateGRPCServer() *grpc.Server {
// 	s := grpc.NewServer(
// 		grpc.UnaryInterceptor(otelgrpc.UnaryServerInterceptor()),
// 		grpc.StreamInterceptor(otelgrpc.StreamServerInterceptor()),
// 		grpc.ChainUnaryInterceptors(tm.grpcServerMetricsInterceptor()),
// 	)

// 	return s
// }

// grpcServerMetricsInterceptor creates a unary interceptor for gRPC server metrics
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

// Example usage functions

// ExampleHTTPHandler shows how to use telemetry in HTTP handlers
// func (tm *TelemetryManager) ExampleHTTPHandler() gin.HandlerFunc {
// 	return func(c *gin.Context) {
// 		// The span is automatically created by otelgin middleware
// 		ctx := c.Request.Context()

// 		// Add custom span attributes
// 		span := oteltrace.SpanFromContext(ctx)
// 		span.SetAttributes(
// 			attribute.String("user.id", c.GetHeader("X-User-ID")),
// 			attribute.String("custom.attribute", "example-value"),
// 		)

// 		// Add span event
// 		tm.AddSpanEvent(ctx, "processing.started",
// 			attribute.String("step", "validation"))

// 		// Simulate some work
// 		time.Sleep(100 * time.Millisecond)

// 		// Call gRPC service
// 		if err := tm.exampleGRPCCall(ctx); err != nil {
// 			tm.SetSpanStatus(ctx, codes.Error, "gRPC call failed")
// 			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
// 			return
// 		}

// 		tm.AddSpanEvent(ctx, "processing.completed")
// 		c.JSON(http.StatusOK, gin.H{"message": "success"})
// 	}
// }

// exampleGRPCCall demonstrates calling a gRPC service with proper context propagation
// func (tm *TelemetryManager) exampleGRPCCall(ctx context.Context) error {
// 	// Create a child span for the gRPC call
// 	ctx, span := tm.StartSpan(ctx, "grpc.call.user_service")
// 	defer span.End()

// 	// Inject trace context into gRPC metadata
// 	ctx = tm.InjectTraceContext(ctx)

// 	// Create gRPC client (in real code, this would be cached)
// 	conn, err := tm.CreateGRPCClient("localhost:50051")
// 	if err != nil {
// 		return err
// 	}
// 	defer conn.Close()

// 	// Make gRPC call (example - replace with your actual service)
// 	// client := userpb.NewUserServiceClient(conn)
// 	// resp, err := client.GetUser(ctx, &userpb.GetUserRequest{Id: "123"})

// 	// Simulate gRPC call
// 	time.Sleep(50 * time.Millisecond)

// 	return nil
// }

// ExampleGRPCServiceMethod shows how to implement a gRPC service method with telemetry
// func (tm *TelemetryManager) ExampleGRPCServiceMethod(ctx context.Context, req interface{}) (interface{}, error) {
// 	// Extract trace context from incoming metadata
// 	ctx = tm.ExtractTraceContext(ctx)

// 	// Create a span for this method
// 	ctx, span := tm.StartSpan(ctx, "service.method.get_user")
// 	defer span.End()

// 	// Add method-specific attributes
// 	span.SetAttributes(
// 		attribute.String("service.method", "GetUser"),
// 		attribute.String("service.version", "v1.0.0"),
// 	)

// 	// Add span event
// 	tm.AddSpanEvent(ctx, "method.started")

// 	// Simulate some processing
// 	time.Sleep(30 * time.Millisecond)

// 	// Call another gRPC service (service-to-service)
// 	if err := tm.exampleServiceToServiceCall(ctx); err != nil {
// 		tm.SetSpanStatus(ctx, codes.Error, "downstream service call failed")
// 		return nil, err
// 	}

// 	tm.AddSpanEvent(ctx, "method.completed")
// 	return "response", nil
// }

// exampleServiceToServiceCall demonstrates gRPC service-to-service communication
// func (tm *TelemetryManager) exampleServiceToServiceCall(ctx context.Context) error {
// 	// Create a child span for the downstream service call
// 	ctx, span := tm.StartSpan(ctx, "grpc.call.notification_service")
// 	defer span.End()

// 	// Inject trace context for downstream service
// 	ctx = tm.InjectTraceContext(ctx)

// 	// Create gRPC client for downstream service
// 	conn, err := tm.CreateGRPCClient("localhost:50052")
// 	if err != nil {
// 		return err
// 	}
// 	defer conn.Close()

// 	// Make gRPC call to downstream service
// 	// client := notificationpb.NewNotificationServiceClient(conn)
// 	// resp, err := client.SendNotification(ctx, &notificationpb.SendNotificationRequest{...})

// 	// Simulate downstream service call
// 	time.Sleep(25 * time.Millisecond)

// 	return nil
// }

// InitTracerProvider initializes an OTLP trace provider and sets it as the global tracer.
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
