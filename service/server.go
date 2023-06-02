package service

import (
	"context"
	"net/http"
	"net/url"
	"strings"

	"github.com/streamingfast/dauth/authenticator"
	dgrpcserver "github.com/streamingfast/dgrpc/server"
	connectweb "github.com/streamingfast/dgrpc/server/connect-web"
	"github.com/streamingfast/dgrpc/server/factory"
	pbssinternal "github.com/streamingfast/substreams/pb/sf/substreams/intern/v2"
	ssconnect "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2/pbsubstreamsrpcconnect"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func GetCommonServerOptions(listenAddr string, logger *zap.Logger, healthcheck dgrpcserver.HealthCheck) []dgrpcserver.Option {
	tracerProvider := otel.GetTracerProvider()
	options := []dgrpcserver.Option{
		dgrpcserver.WithLogger(logger),
		dgrpcserver.WithHealthCheck(dgrpcserver.HealthCheckOverGRPC|dgrpcserver.HealthCheckOverHTTP, healthcheck),
		dgrpcserver.WithPostUnaryInterceptor(otelgrpc.UnaryServerInterceptor(otelgrpc.WithTracerProvider(tracerProvider))),
		dgrpcserver.WithPostStreamInterceptor(otelgrpc.StreamServerInterceptor(otelgrpc.WithTracerProvider(tracerProvider))),
		dgrpcserver.WithGRPCServerOptions(grpc.MaxRecvMsgSize(25 * 1024 * 1024)),
	}
	if strings.Contains(listenAddr, "*") {
		options = append(options, dgrpcserver.WithInsecureServer())
	} else {
		options = append(options, dgrpcserver.WithPlainTextServer())
	}
	return options
}

func ListenTier1(
	addr string,
	svc *Tier1Service,
	auth authenticator.Authenticator,
	logger *zap.Logger,
	healthcheck dgrpcserver.HealthCheck,
) error {

	options := GetCommonServerOptions(addr, logger, healthcheck)
	options = append(options, dgrpcserver.WithAuthChecker(auth.Check, auth.GetAuthTokenRequirement() == authenticator.AuthTokenRequired))

	location, handler := ssconnect.NewStreamHandler(svc)
	mappings := map[string]http.Handler{
		location: handler,
	}

	servOpts := []dgrpcserver.Option{
		dgrpcserver.WithReflection(location),
		//		grpcservers.WithLogger(logger),
		dgrpcserver.WithPermissiveCORS(),
		dgrpcserver.WithHealthCheck(dgrpcserver.HealthCheckOverHTTP, func(_ context.Context) (isReady bool, out interface{}, err error) { return true, nil, nil }),
	}
	servOpts = append(servOpts, dgrpcserver.WithPlainTextServer())

	srv := connectweb.New(mappings, servOpts...)
	srv.Launch(addr)
	<-srv.Terminated()
	return srv.Err()
}

func ListenTier2(
	addr string,
	serviceDiscoveryURL *url.URL,
	svc *Tier2Service,
	logger *zap.Logger,
	healthcheck dgrpcserver.HealthCheck,
) (err error) {
	options := GetCommonServerOptions(addr, logger, healthcheck)
	if serviceDiscoveryURL != nil {
		options = append(options, dgrpcserver.WithServiceDiscoveryURL(serviceDiscoveryURL))
	}
	grpcServer := factory.ServerFromOptions(options...)
	pbssinternal.RegisterSubstreamsServer(grpcServer.ServiceRegistrar(), svc)

	done := make(chan struct{})
	grpcServer.OnTerminated(func(e error) {
		err = e
		close(done)
	})
	grpcServer.Launch(addr)
	<-done

	return

}
