package middleware

import (
	"context"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func ServiceAuthInterceptor(
	verifier ServiceClientVerifier,
	skipMethods map[string]bool,
) grpc.UnaryServerInterceptor {
	tracer := otel.Tracer("middleware/service_auth")
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		// ─── Create a child span so the check shows up in the trace ────────────────
		ctx, span := tracer.Start(ctx, "auth.validate",
			trace.WithAttributes(attribute.String("rpc.method", info.FullMethod)))
		defer span.End()

		if skipMethods[info.FullMethod] {
			return handler(ctx, req) // still pass the traced ctx downstream
		}

		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			err := status.Errorf(grpcCodes.Unauthenticated, "missing metadata")
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error()) // use otel codes for tracing status
			return nil, err
		}

		// ─── Bearer‑token path ────────────────────────────────────────────────────
		if authHeader, ok := md["authorization"]; ok && len(authHeader) > 0 {
			token := strings.TrimPrefix(authHeader[0], "Bearer ")
			if token != authHeader[0] {
				if verified, err := verifier.VerifyServiceToken(ctx, token); err == nil && verified {
					return handler(ctx, req)
				}
				// verification failed
			}
		}

		// ─── client‑id / client‑key path ──────────────────────────────────────────
		clientID, hasClientID := md["client-id"]
		clientKey, hasClientKey := md["client-key"]
		if !hasClientID || !hasClientKey ||
			len(clientID) == 0 || len(clientKey) == 0 {

			err := status.Errorf(grpcCodes.Unauthenticated, "missing client key or client ID")
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error()) // use otel codes for tracing status
			return nil, err
		}

		// Propagate the SAME ctx so VerifyServiceClient can do outbound calls
		verified, err := verifier.VerifyServiceClient(ctx, clientID[0], clientKey[0])
		if err != nil || !verified {
			if err == nil { // fabricate one for tracing clarity
				err = status.Errorf(grpcCodes.Unauthenticated, "invalid client ID or key")
			}
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error()) // use otel codes for tracing status
			return nil, err
		}

		span.SetStatus(codes.Ok, "") // use otel codes for tracing status
		return handler(ctx, req)     // keep the trace flowing to business logic
	}
}

func LoggingInterceptor(logger *zap.Logger) grpc.UnaryServerInterceptor {
	if logger == nil {
		logger = zap.NewNop()
	}

	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		logger.Info("gRPC call started",
			zap.String("method", info.FullMethod),
		)

		resp, err := handler(ctx, req)
		duration := time.Since(start)

		if err != nil {
			logger.Error("gRPC call failed",
				zap.String("method", info.FullMethod),
				zap.Duration("duration", duration),
				zap.Error(err),
			)
		} else {
			logger.Info("gRPC call completed",
				zap.String("method", info.FullMethod),
				zap.Duration("duration", duration),
			)
		}

		return resp, err
	}
}

func RecoveryInterceptor(logger *zap.Logger) grpc.UnaryServerInterceptor {
	if logger == nil {
		logger = zap.NewNop()
	}

	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("panic recovered",
					zap.String("method", info.FullMethod),
					zap.Any("recover", r),
				)
				err = status.Errorf(grpcCodes.Internal, "internal server error")
			}
		}()

		return handler(ctx, req)
	}
}
