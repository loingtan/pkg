package middleware

import (
	"context"

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

func (c *ClientAuthInterceptor) UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {

		md := metadata.Pairs(
			"client-id", c.clientID,
			"client-key", c.clientKey,
		)

		if existingMD, ok := metadata.FromOutgoingContext(ctx); ok {
			md = metadata.Join(md, existingMD)
		}

		ctx = metadata.NewOutgoingContext(ctx, md)

		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func (c *ClientAuthInterceptor) StreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {

		md := metadata.Pairs(
			"client-id", c.clientID,
			"client-key", c.clientKey,
		)

		if existingMD, ok := metadata.FromOutgoingContext(ctx); ok {
			md = metadata.Join(md, existingMD)
		}

		ctx = metadata.NewOutgoingContext(ctx, md)

		return streamer(ctx, desc, cc, method, opts...)
	}
}
