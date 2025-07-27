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

type contextKey string

const (
	traceIDKey contextKey = "trace_id"
)

type GRPCValidationInterceptor struct {
	validator *Validator
	logger    *zap.Logger
}

func NewGRPCValidationInterceptor(logger *zap.Logger) *GRPCValidationInterceptor {
	if logger == nil {
		logger, _ = zap.NewProduction()
	}

	return &GRPCValidationInterceptor{
		validator: NewValidator(),
		logger:    logger,
	}
}

func (i *GRPCValidationInterceptor) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()

		traceID := extractTraceIDFromMetadata(ctx)

		ctx = context.WithValue(ctx, traceIDKey, traceID)

		i.logger.Info("gRPC request received",
			zap.String("method", info.FullMethod),
			zap.String("trace_id", traceID),
		)

		if validationErrors := i.validator.Validate(req); len(validationErrors) > 0 {
			i.logger.Warn("gRPC request validation failed",
				zap.String("method", info.FullMethod),
				zap.String("trace_id", traceID),
				zap.Any("validation_errors", validationErrors),
				zap.Duration("duration", time.Since(start)),
			)

			return nil, i.createValidationError(validationErrors, traceID)
		}

		resp, err := handler(ctx, req)

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

func (i *GRPCValidationInterceptor) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()

		traceID := extractTraceIDFromMetadata(ss.Context())

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

func (i *GRPCValidationInterceptor) createValidationError(errors ValidationErrors, traceID string) error {

	errorMsg := fmt.Sprintf("Validation failed: %s", errors.Error())

	st := status.New(codes.InvalidArgument, errorMsg)

	i.logger.Warn("Validation error occurred",
		zap.String("trace_id", traceID),
		zap.String("error", errorMsg),
		zap.Any("validation_errors", errors))

	return st.Err()
}

type validationServerStream struct {
	grpc.ServerStream
	validator *Validator
	logger    *zap.Logger
	traceID   string
	method    string
}

func (s *validationServerStream) RecvMsg(m interface{}) error {
	err := s.ServerStream.RecvMsg(m)
	if err != nil {
		return err
	}

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

func extractTraceIDFromMetadata(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return generateTraceID()
	}

	traceIDs := md.Get("trace-id")
	if len(traceIDs) > 0 {
		return traceIDs[0]
	}

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

func generateTraceID() string {
	return fmt.Sprintf("trace-%d", time.Now().UnixNano())
}

func (i *GRPCValidationInterceptor) UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		start := time.Now()

		traceID := extractTraceIDFromContext(ctx)

		ctx = metadata.AppendToOutgoingContext(ctx, "trace-id", traceID)

		if validationErrors := i.validator.Validate(req); len(validationErrors) > 0 {
			i.logger.Warn("gRPC client request validation failed",
				zap.String("method", method),
				zap.String("trace_id", traceID),
				zap.Any("validation_errors", validationErrors),
			)

			return status.Error(codes.InvalidArgument, fmt.Sprintf("Request validation failed: %s", validationErrors.Error()))
		}

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

func (i *GRPCValidationInterceptor) StreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {

		traceID := extractTraceIDFromContext(ctx)

		ctx = metadata.AppendToOutgoingContext(ctx, "trace-id", traceID)

		stream, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, err
		}

		return &validationClientStream{
			ClientStream: stream,
			validator:    i.validator,
			logger:       i.logger,
			traceID:      traceID,
			method:       method,
		}, nil
	}
}

type validationClientStream struct {
	grpc.ClientStream
	validator *Validator
	logger    *zap.Logger
	traceID   string
	method    string
}

func (s *validationClientStream) SendMsg(m interface{}) error {

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

func extractTraceIDFromContext(ctx context.Context) string {
	if traceID, ok := ctx.Value(traceIDKey).(string); ok {
		return traceID
	}

	return generateTraceID()
}

func (i *GRPCValidationInterceptor) ValidateRequest(req interface{}) error {
	if validationErrors := i.validator.Validate(req); len(validationErrors) > 0 {
		return status.Error(codes.InvalidArgument, fmt.Sprintf("Validation failed: %s", validationErrors.Error()))
	}
	return nil
}
