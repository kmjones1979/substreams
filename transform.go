package substreams

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/transform"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/eth-go/rpc"
	"github.com/streamingfast/firehose"
	pbfirehose "github.com/streamingfast/pbgo/sf/firehose/v1"
	"github.com/streamingfast/substreams/manifest"
	pbtransform "github.com/streamingfast/substreams/pb/sf/substreams/transform/v1"
	"github.com/streamingfast/substreams/pipeline"
	ssrpc "github.com/streamingfast/substreams/rpc"
	"github.com/streamingfast/substreams/state"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

var MessageName = proto.MessageName(&pbtransform.Transform{})

func GetRPCClient(endpoint string, cachePath string) (*rpc.Client, *ssrpc.Cache, error) {
	var cache *ssrpc.Cache

	if cachePath != "" {
		rpcCacheStore, err := dstore.NewStore(cachePath, "", "", false)
		if err != nil {
			return nil, nil, fmt.Errorf("setting up store for rpc-cache: %w", err)
		}
		cache = ssrpc.NewCache(rpcCacheStore, rpcCacheStore, 0, 999) // FIXME: kind of a hack here...
		cache.Load(context.Background())                             // FIXME: dont load this every request
	}

	httpClient := &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true, // don't reuse connections
		},
		Timeout: 3 * time.Second,
	}

	return rpc.NewClient(endpoint, rpc.WithHttpClient(httpClient)), cache, nil
}

func TransformFactory(rpcEndpoint, rpcCachePath, stateStorePath, protobufBlockType string, firehoseServer *firehose.Server) *transform.Factory {

	return &transform.Factory{
		Obj: &pbtransform.Transform{},
		NewFunc: func(message *anypb.Any) (transform.Transform, error) {
			mname := message.MessageName()
			if mname != MessageName {
				return nil, fmt.Errorf("expected type url %q, recevied %q ", MessageName, message.TypeUrl)
			}

			req := &pbtransform.Transform{}
			err := proto.Unmarshal(message.Value, req)
			if err != nil {
				return nil, fmt.Errorf("unexpected unmarshall error: %w", err)
			}

			if req.Manifest == nil {
				return nil, fmt.Errorf("missing manifest in request")
			}

			rpcClient, rpcCache, err := GetRPCClient(rpcEndpoint, rpcCachePath)
			if err != nil {
				return nil, fmt.Errorf("setting up rpc client: %w", err)
			}

			stateStore, err := dstore.NewStore(stateStorePath, "", "", false)
			if err != nil {
				return nil, fmt.Errorf("setting up store for data: %w", err)
			}

			ioFactory := state.NewStoreFactory(stateStore)

			graph, err := manifest.NewModuleGraph(req.Manifest.Modules)
			if err != nil {
				return nil, fmt.Errorf("create module graph %w", err)
			}

			t := &ssTransform{
				pipeline: pipeline.New(
					rpcClient,
					rpcCache,
					req.Manifest,
					graph,
					req.OutputModule,
					protobufBlockType,
					ioFactory,
				),
				description:    req.Manifest.Description,
				firehoseServer: firehoseServer,
			}

			return t, nil
		},
	}
}

type ssTransform struct {
	pipeline       *pipeline.Pipeline
	description    string
	firehoseServer *firehose.Server
}

func (t *ssTransform) Run(ctx context.Context, req *pbfirehose.Request, output func(*bstream.Cursor, *anypb.Any) error) error {
	fmt.Println("inside Run")

	newReq := &pbfirehose.Request{
		StartBlockNum: req.StartBlockNum,
		StopBlockNum:  req.StopBlockNum,
		StartCursor:   req.StartCursor,
		ForkSteps:     []pbfirehose.ForkStep{pbfirehose.ForkStep_STEP_IRREVERSIBLE}, //FIXME ?

		// ...FIXME ?
	}

	returnHandler := func(any *anypb.Any) error {
		return output(nil, any)
	}

	if req.StartBlockNum < 0 {
		return fmt.Errorf("invalid negative startblock (not handled in substreams): %d", req.StartBlockNum)
		// FIXME we want logger too
		// FIXME start block resolving is an art, it should be handled here
	}

	handler, err := t.pipeline.HandlerFactory(ctx, uint64(req.StartBlockNum), req.StopBlockNum, returnHandler)
	if err != nil {
		return fmt.Errorf("error building substreams pipeline handler: %w", err)
	}

	client := t.firehoseServer.BlocksFromLocal(ctx, newReq)
	for {
		resp, err := client.Recv()
		if err != nil {
			return fmt.Errorf("error receiving block from firehose client")
		}

		if resp.Block.TypeUrl != "dfuse.bstream.v1.block" {
			panic(resp.Block.TypeUrl)
		}
		_ = handler

	}
}

func (t *ssTransform) String() string {
	return t.description
}