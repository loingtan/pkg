package middleware

import (
	"context"
	"log"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func ServiceAuthInterceptor(verifier ServiceClientVerifier, skipMethods map[string]bool) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if skipMethods[info.FullMethod] {
			return handler(ctx, req)
		}

		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Errorf(codes.Unauthenticated, "missing metadata")
		}
		if authHeader, ok := md["authorization"]; ok && len(authHeader) > 0 {
			token := strings.TrimPrefix(authHeader[0], "Bearer ")
			if token != authHeader[0] {
				if verified, err := verifier.VerifyServiceToken(ctx, token); err == nil && verified {
					return handler(ctx, req)
				}
			}
		}

		clientID, hasClientID := md["client-id"]
		clientKey, hasClientKey := md["client-key"]
		log.Printf("Verifying service client with ID: %v", clientID)
		log.Printf("Using client key: %v", clientKey)
		if !hasClientID || !hasClientKey || len(clientID) == 0 || len(clientKey) == 0 {
			return nil, status.Errorf(codes.Unauthenticated, "missing client key or client ID")
		}
		log.Printf("Verifying service client with ID: %v", clientID)
		log.Printf("Using client key: %v", clientKey)
		verified, err := verifier.VerifyServiceClient(ctx, clientID[0], clientKey[0])
		if err != nil || !verified {
			return nil, status.Errorf(codes.Unauthenticated, "invalid client ID or key")
		}

		return handler(ctx, req)
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
				err = status.Errorf(codes.Internal, "internal server error")
			}
		}()

		return handler(ctx, req)
	}
}
