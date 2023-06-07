package service

import (
	"context"
	"time"

	"github.com/streamingfast/dmetering"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	pbssinternal "github.com/streamingfast/substreams/pb/sf/substreams/intern/v2"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
)

func tier1SendMetering(ctx context.Context, logger *zap.Logger, endpoint string, resp *pbsubstreamsrpc.Response) {
	meter := dmetering.GetBytesMeter(ctx)
	bytesRead := meter.BytesReadDelta()
	bytesWritten := meter.BytesWrittenDelta()

	event := dmetering.Event{
		Endpoint: endpoint,
		Metrics: map[string]float64{
			"egress_bytes":  float64(proto.Size(resp)),
			"written_bytes": float64(bytesWritten),
			"read_bytes":    float64(bytesRead),
		},
		Timestamp: time.Now(),
	}

	err := dmetering.Emit(ctx, event)
	if err != nil {
		logger.Warn("unable to emit metrics event", zap.Error(err), zap.Object("event", event))
	}
}

func tier2SendMetering(ctx context.Context, logger *zap.Logger, endpoint string, resp *pbssinternal.ProcessRangeResponse) {
	meter := dmetering.GetBytesMeter(ctx)
	bytesRead := meter.BytesReadDelta()
	bytesWritten := meter.BytesWrittenDelta()

	event := dmetering.Event{
		Endpoint: endpoint,
		Metrics: map[string]float64{
			"egress_bytes":  float64(proto.Size(resp)),
			"written_bytes": float64(bytesWritten),
			"read_bytes":    float64(bytesRead),
		},
		Timestamp: time.Now(),
	}

	err := dmetering.Emit(ctx, event)
	if err != nil {
		logger.Warn("unable to emit metrics event", zap.Error(err), zap.Object("event", event))
	}
}
