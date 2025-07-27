package middleware

import (
	"context"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type ClientAuthInterceptor struct {
	clientID  string
	clientKey string
}

func NewClientAuthInterceptor(clientID, clientKey string) *ClientAuthInterceptor {
	return &ClientAuthInterceptor{
		clientID:  clientID,
		clientKey: clientKey,
	}
}

// extractTraceIDFromContext extracts trace ID from various sources in the context
func extractTraceIDFromContext(ctx context.Context) string {
	// Try to get trace ID from OpenTelemetry span context first
	if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
		return span.SpanContext().TraceID().String()
	}

	// Try to get from context values directly (set by TraceMiddleware)
	if traceID := ctx.Value("trace_id"); traceID != nil {
		if id, ok := traceID.(string); ok {
			return id
		}
	}
	if traceID := ctx.Value("request_id"); traceID != nil {
		if id, ok := traceID.(string); ok {
			return id
		}
	}

	// Try to get trace ID from Gin context (if available)
	if ginCtx, ok := ctx.(*gin.Context); ok {
		// Try to get from context values
		if traceID, exists := ginCtx.Get("trace_id"); exists {
			if id, ok := traceID.(string); ok {
				return id
			}
		}
		if traceID, exists := ginCtx.Get("request_id"); exists {
			if id, ok := traceID.(string); ok {
				return id
			}
		}

		// Try various header names for trace ID
		if traceID := ginCtx.GetHeader("X-Trace-ID"); traceID != "" {
			return traceID
		}
		if traceID := ginCtx.GetHeader("X-Request-ID"); traceID != "" {
			return traceID
		}
		if traceID := ginCtx.GetHeader("Trace-ID"); traceID != "" {
			return traceID
		}
	}

	return ""
}

func (c *ClientAuthInterceptor) UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {

		// Start with client auth metadata
		md := metadata.Pairs(
			"client-id", c.clientID,
			"client-key", c.clientKey,
		)

		// Extract and add trace ID if available
		if traceID := extractTraceIDFromContext(ctx); traceID != "" {
			traceMD := metadata.Pairs(
				"trace-id", traceID,
				"x-trace-id", traceID,
				"x-request-id", traceID,
			)
			md = metadata.Join(md, traceMD)
		}

		// Join with any existing metadata
		if existingMD, ok := metadata.FromOutgoingContext(ctx); ok {
			md = metadata.Join(md, existingMD)
		}

		ctx = metadata.NewOutgoingContext(ctx, md)

		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func (c *ClientAuthInterceptor) StreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {

		// Start with client auth metadata
		md := metadata.Pairs(
			"client-id", c.clientID,
			"client-key", c.clientKey,
		)

		// Extract and add trace ID if available
		if traceID := extractTraceIDFromContext(ctx); traceID != "" {
			traceMD := metadata.Pairs(
				"trace-id", traceID,
				"x-trace-id", traceID,
				"x-request-id", traceID,
			)
			md = metadata.Join(md, traceMD)
		}

		// Join with any existing metadata
		if existingMD, ok := metadata.FromOutgoingContext(ctx); ok {
			md = metadata.Join(md, existingMD)
		}

		ctx = metadata.NewOutgoingContext(ctx, md)

		return streamer(ctx, desc, cc, method, opts...)
	}
}
