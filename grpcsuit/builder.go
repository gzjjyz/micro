package grpcsuit

import (
	"context"
	"github.com/gzjjyz/micro/discovery"
	"github.com/gzjjyz/micro/factory"
	"github.com/gzjjyz/srvlib/alg/doublelinked"
	"google.golang.org/grpc/resolver"
	"sync"
)

const (
	ResolveSchema = "jjyz"
)

func NewBuilder(ctx context.Context) (resolver.Builder, error) {
	discover, err := factory.GetOrMakeDiscovery()
	if err != nil {
		return nil, err
	}

	builder := &Builder{
		srvNameToResolversMap: map[string]*doublelinked.LinkedList{},
	}

	discover.OnSrvUpdated(func(ctx context.Context, evt discovery.Evt, srv *discovery.Service) {
		builder.mu.RLock()
		defer builder.mu.RUnlock()

		resolvers, ok := builder.srvNameToResolversMap[srv.SrvName]
		if !ok {
			return
		}

		_ = resolvers.Walk(func(node *doublelinked.LinkedNode) (bool, error) {
			node.Payload.(*Resolver).UpdateSrvCfg(srv)
			return true, nil
		})

		customDoOnDiscoverSrvUpdated(ctx, evt, srv)
	})

	_, err = discover.LoadAll(ctx)
	if err != nil {
		return nil, err
	}

	builder.discover = discover

	return builder, nil
}

type Builder struct {
	srvNameToResolversMap map[string]*doublelinked.LinkedList
	discover              discovery.Discovery
	mu                    sync.RWMutex
}

func (b *Builder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	srvName := target.Endpoint

	srv, err := b.discover.Discover(context.Background(), srvName)
	if err != nil {
		return nil, err
	}

	resolve := NewResolver(srvName, cc, b)
	resolve.UpdateSrvCfg(srv)

	b.mu.Lock()
	defer b.mu.Unlock()

	resolvers, ok := b.srvNameToResolversMap[srvName]
	if !ok {
		resolvers = &doublelinked.LinkedList{}
		b.srvNameToResolversMap[srvName] = resolvers
	}
	resolvers.Append(resolve)

	return resolve, nil
}

func (b *Builder) Scheme() string {
	return ResolveSchema
}

func (b *Builder) OnResolverClosed(resolve *Resolver) {
	b.mu.Lock()
	defer b.mu.Unlock()

	resolvers := b.srvNameToResolversMap[resolve.srvName]
	resolvers.Delete(resolve)
	if resolvers.Len() == 0 {
		delete(b.srvNameToResolversMap, resolve.srvName)
	}

	return
}

var _ resolver.Builder = (*Builder)(nil)
