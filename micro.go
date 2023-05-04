package micro

import (
	"context"
	"github.com/gzjjyz/micro/env"
	"github.com/gzjjyz/micro/grpcsuit"
)

func InitSuitWithGrpc(ctx context.Context, metaFilePath string) error {
	if err := env.InitMeta(metaFilePath); err != nil {
		return err
	}

	if err := grpcsuit.InitGrpcResolver(ctx); err != nil {
		return err
	}

	return nil
}
