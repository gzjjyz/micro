package grpcsuit

import (
	"context"
	"fmt"
	"github.com/gzjjyz/micro/discovery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/resolver"
)

var RoundRobinDialOpts = []grpc.DialOption{
	grpc.WithInsecure(),
	grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingConfig": [{"%s":{}}]}`, roundrobin.Name)),
}

var NotRoundRobinDialOpts = []grpc.DialOption{
	grpc.WithInsecure(),
	grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingConfig": [{"%s":{}}]}`, roundrobin.Name)),
}

var customDoOnDiscoverSrvUpdated discovery.OnSrvUpdatedFunc = func(ctx context.Context, evt discovery.Evt, srv *discovery.Service) {}

func InitGrpcResolver(ctx context.Context) error {
	builder, err := NewBuilder(ctx)
	if err != nil {
		return err
	}
	resolver.Register(builder)
	return nil
}

func OnDiscoverSrvUpdated(fn discovery.OnSrvUpdatedFunc) {
	customDoOnDiscoverSrvUpdated = fn
}
