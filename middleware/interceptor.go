package middleware

import (
	"context"

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
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {

		if skipMethods[info.FullMethod] {
			return handler(ctx, req)
		}

		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			err := status.Errorf(grpcCodes.Unauthenticated, "missing metadata")

			return nil, err
		}
		clientID, hasClientID := md["client-id"]
		clientKey, hasClientKey := md["client-key"]
		if !hasClientID || !hasClientKey ||
			len(clientID) == 0 || len(clientKey) == 0 {

			err := status.Errorf(grpcCodes.Unauthenticated, "missing client key or client ID")
			return nil, err
		}

		verified, err := verifier.VerifyServiceClient(ctx, clientID[0], clientKey[0])
		if err != nil || !verified {
			if err == nil {
				err = status.Errorf(grpcCodes.Unauthenticated, "invalid client ID or key")
			}
			return nil, err
		}

		return handler(ctx, req)
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
