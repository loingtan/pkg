package validation

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// contextKey is a custom type for context keys to avoid collisions
type contextKey string

const (
	// traceIDKey is the context key for trace ID
	traceIDKey contextKey = "trace_id"
)

// GRPCValidationInterceptor provides comprehensive gRPC validation interceptors
type GRPCValidationInterceptor struct {
	validator *Validator
	logger    *zap.Logger
}

// NewGRPCValidationInterceptor creates a new gRPC validation interceptor
func NewGRPCValidationInterceptor(logger *zap.Logger) *GRPCValidationInterceptor {
	if logger == nil {
		logger, _ = zap.NewProduction()
	}

	return &GRPCValidationInterceptor{
		validator: NewValidator(),
		logger:    logger,
	}
}

// UnaryServerInterceptor returns a gRPC unary server interceptor for validation
func (i *GRPCValidationInterceptor) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()

		// Extract trace ID from metadata
		traceID := extractTraceIDFromMetadata(ctx)

		// Add trace ID to context
		ctx = context.WithValue(ctx, traceIDKey, traceID)

		// Log request
		i.logger.Info("gRPC request received",
			zap.String("method", info.FullMethod),
			zap.String("trace_id", traceID),
		)

		// Validate the request
		if validationErrors := i.validator.Validate(req); len(validationErrors) > 0 {
			i.logger.Warn("gRPC request validation failed",
				zap.String("method", info.FullMethod),
				zap.String("trace_id", traceID),
				zap.Any("validation_errors", validationErrors),
				zap.Duration("duration", time.Since(start)),
			)

			return nil, i.createValidationError(validationErrors, traceID)
		}

		// Call the handler
		resp, err := handler(ctx, req)

		// Log response
		duration := time.Since(start)
		if err != nil {
			i.logger.Error("gRPC request failed",
				zap.String("method", info.FullMethod),
				zap.String("trace_id", traceID),
				zap.Error(err),
				zap.Duration("duration", duration),
			)
		} else {
			i.logger.Info("gRPC request completed",
				zap.String("method", info.FullMethod),
				zap.String("trace_id", traceID),
				zap.Duration("duration", duration),
			)
		}

		return resp, err
	}
}

// StreamServerInterceptor returns a gRPC stream server interceptor for validation
func (i *GRPCValidationInterceptor) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()

		// Extract trace ID from metadata
		traceID := extractTraceIDFromMetadata(ss.Context())

		// Create wrapped stream with validation
		wrappedStream := &validationServerStream{
			ServerStream: ss,
			validator:    i.validator,
			logger:       i.logger,
			traceID:      traceID,
			method:       info.FullMethod,
		}

		i.logger.Info("gRPC stream request received",
			zap.String("method", info.FullMethod),
			zap.String("trace_id", traceID),
		)

		err := handler(srv, wrappedStream)

		duration := time.Since(start)
		if err != nil {
			i.logger.Error("gRPC stream request failed",
				zap.String("method", info.FullMethod),
				zap.String("trace_id", traceID),
				zap.Error(err),
				zap.Duration("duration", duration),
			)
		} else {
			i.logger.Info("gRPC stream request completed",
				zap.String("method", info.FullMethod),
				zap.String("trace_id", traceID),
				zap.Duration("duration", duration),
			)
		}

		return err
	}
}

// createValidationError creates a gRPC error from validation errors
func (i *GRPCValidationInterceptor) createValidationError(errors ValidationErrors, traceID string) error {
	// Create detailed error message
	errorMsg := fmt.Sprintf("Validation failed: %s", errors.Error())

	// Create gRPC status with details
	st := status.New(codes.InvalidArgument, errorMsg)

	// Log the validation error with trace ID for debugging
	i.logger.Warn("Validation error occurred",
		zap.String("trace_id", traceID),
		zap.String("error", errorMsg),
		zap.Any("validation_errors", errors))

	return st.Err()
}

// validationServerStream wraps grpc.ServerStream with validation
type validationServerStream struct {
	grpc.ServerStream
	validator *Validator
	logger    *zap.Logger
	traceID   string
	method    string
}

// RecvMsg validates incoming stream messages
func (s *validationServerStream) RecvMsg(m interface{}) error {
	err := s.ServerStream.RecvMsg(m)
	if err != nil {
		return err
	}

	// Validate the received message
	if validationErrors := s.validator.Validate(m); len(validationErrors) > 0 {
		s.logger.Warn("gRPC stream message validation failed",
			zap.String("method", s.method),
			zap.String("trace_id", s.traceID),
			zap.Any("validation_errors", validationErrors),
		)

		return status.Error(codes.InvalidArgument, fmt.Sprintf("Message validation failed: %s", validationErrors.Error()))
	}

	return nil
}

// extractTraceIDFromMetadata extracts trace ID from gRPC metadata
func extractTraceIDFromMetadata(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return generateTraceID()
	}

	traceIDs := md.Get("trace-id")
	if len(traceIDs) > 0 {
		return traceIDs[0]
	}

	// Try alternative header names
	traceIDs = md.Get("x-trace-id")
	if len(traceIDs) > 0 {
		return traceIDs[0]
	}

	traceIDs = md.Get("x-request-id")
	if len(traceIDs) > 0 {
		return traceIDs[0]
	}

	return generateTraceID()
}

// generateTraceID generates a new trace ID
func generateTraceID() string {
	return fmt.Sprintf("trace-%d", time.Now().UnixNano())
}

// UnaryClientInterceptor returns a gRPC unary client interceptor for validation
func (i *GRPCValidationInterceptor) UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		start := time.Now()

		// Extract or generate trace ID
		traceID := extractTraceIDFromContext(ctx)

		// Add trace ID to outgoing metadata
		ctx = metadata.AppendToOutgoingContext(ctx, "trace-id", traceID)

		// Validate the request
		if validationErrors := i.validator.Validate(req); len(validationErrors) > 0 {
			i.logger.Warn("gRPC client request validation failed",
				zap.String("method", method),
				zap.String("trace_id", traceID),
				zap.Any("validation_errors", validationErrors),
			)

			return status.Error(codes.InvalidArgument, fmt.Sprintf("Request validation failed: %s", validationErrors.Error()))
		}

		// Make the call
		err := invoker(ctx, method, req, reply, cc, opts...)

		duration := time.Since(start)
		if err != nil {
			i.logger.Error("gRPC client request failed",
				zap.String("method", method),
				zap.String("trace_id", traceID),
				zap.Error(err),
				zap.Duration("duration", duration),
			)
		} else {
			i.logger.Debug("gRPC client request completed",
				zap.String("method", method),
				zap.String("trace_id", traceID),
				zap.Duration("duration", duration),
			)
		}

		return err
	}
}

// StreamClientInterceptor returns a gRPC stream client interceptor for validation
func (i *GRPCValidationInterceptor) StreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		// Extract or generate trace ID
		traceID := extractTraceIDFromContext(ctx)

		// Add trace ID to outgoing metadata
		ctx = metadata.AppendToOutgoingContext(ctx, "trace-id", traceID)

		// Create the stream
		stream, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, err
		}

		// Wrap the stream with validation
		return &validationClientStream{
			ClientStream: stream,
			validator:    i.validator,
			logger:       i.logger,
			traceID:      traceID,
			method:       method,
		}, nil
	}
}

// validationClientStream wraps grpc.ClientStream with validation
type validationClientStream struct {
	grpc.ClientStream
	validator *Validator
	logger    *zap.Logger
	traceID   string
	method    string
}

// SendMsg validates outgoing stream messages
func (s *validationClientStream) SendMsg(m interface{}) error {
	// Validate the message before sending
	if validationErrors := s.validator.Validate(m); len(validationErrors) > 0 {
		s.logger.Warn("gRPC client stream message validation failed",
			zap.String("method", s.method),
			zap.String("trace_id", s.traceID),
			zap.Any("validation_errors", validationErrors),
		)

		return status.Error(codes.InvalidArgument, fmt.Sprintf("Message validation failed: %s", validationErrors.Error()))
	}

	return s.ClientStream.SendMsg(m)
}

// extractTraceIDFromContext extracts trace ID from context
func extractTraceIDFromContext(ctx context.Context) string {
	if traceID, ok := ctx.Value(traceIDKey).(string); ok {
		return traceID
	}

	return generateTraceID()
}

// ValidateRequest validates a gRPC request and returns appropriate error
func (i *GRPCValidationInterceptor) ValidateRequest(req interface{}) error {
	if validationErrors := i.validator.Validate(req); len(validationErrors) > 0 {
		return status.Error(codes.InvalidArgument, fmt.Sprintf("Validation failed: %s", validationErrors.Error()))
	}
	return nil
}
