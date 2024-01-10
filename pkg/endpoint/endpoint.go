package endpoint

import (
	"context"
	"net"
	"os"
	"strings"

	"github.com/estebanpavansarquis/kine/pkg/drivers/generic"
	"github.com/estebanpavansarquis/kine/pkg/drivers/msobjectstore"
	"github.com/estebanpavansarquis/kine/pkg/server"
	"github.com/estebanpavansarquis/kine/pkg/tls"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/soheilhy/cmux"
	"go.etcd.io/etcd/server/v3/embed"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const (
	KineSocket           = "unix://kine.sock"
	SQLiteBackend        = "sqlite"
	DQLiteBackend        = "dqlite"
	ETCDBackend          = "etcd3"
	JetStreamBackend     = "jetstream"
	NATSBackend          = "nats"
	MySQLBackend         = "mysql"
	PostgresBackend      = "postgres"
	MSObjectStoreBackend = "ms-object-store"
	ListenAddress        = "127.0.0.1:" + ListenPort
	ListenPort           = "2379"
	ListenNetwork        = "tcp"
)

type Config struct {
	GRPCServer           *grpc.Server
	Listener             string
	Endpoint             string
	ConnectionPoolConfig generic.ConnectionPoolConfig
	ServerTLSConfig      tls.Config
	BackendTLSConfig     tls.Config
	MetricsRegisterer    prometheus.Registerer
}

type ETCDConfig struct {
	Endpoints   []string
	TLSConfig   tls.Config
	LeaderElect bool
}

func Listen(ctx context.Context) error {

	backend, err := msobjectstore.New()
	if err := backend.Start(ctx); err != nil {
		return errors.Wrap(err, "starting kine backend")
	}

	// set up GRPC server and register services
	b := server.New(backend, "http")
	grpcServer := grpcServer()
	if err != nil {
		return errors.Wrap(err, "creating GRPC server")
	}
	b.Register(grpcServer)

	// set up HTTP server with basic mux
	//httpServer := httpServer()

	// Create raw listener and wrap in cmux for protocol switching
	listener, err := net.Listen(ListenNetwork, ListenAddress)
	if err != nil {
		return errors.Wrap(err, "creating listener")
	}
	m := cmux.New(listener)

	//if config.ServerTLSConfig.CertFile != "" && config.ServerTLSConfig.KeyFile != "" {
	//	// If using TLS, wrap handler in GRPC/HTTP switching handler and serve TLS
	//	httpServer.Handler = grpcHandlerFunc(grpcServer, httpServer.Handler)
	//	anyl := m.Match(cmux.Any())
	//	go func() {
	//		if err := httpServer.ServeTLS(anyl, config.ServerTLSConfig.CertFile, config.ServerTLSConfig.KeyFile); err != nil {
	//			logrus.Errorf("Kine TLS server shutdown: %v", err)
	//		}
	//	}()
	//} else {
	// If using plaintext, use cmux matching for GRPC/HTTP switching
	grpcl := m.Match(cmux.HTTP2())
	go func() {
		if err := grpcServer.Serve(grpcl); err != nil {
			logrus.Errorf("Kine GRPC server shutdown: %v", err)
		}
	}()
	//httpl := m.Match(cmux.HTTP1())
	//go func() {
	//	if err := httpServer.Serve(httpl); err != nil {
	//		logrus.Errorf("Kine HTTP server shutdown: %v", err)
	//	}
	//}()
	//}

	go func() {
		if err := m.Serve(); err != nil {
			logrus.Errorf("Kine listener shutdown: %v", err)
			grpcServer.Stop()
		}
	}()

	endpoint := "http://127.0.0.1:" + ListenPort
	logrus.Infof("Kine available at %s", endpoint)

	return nil
}

// endpointURL returns a URI string suitable for use as a local etcd endpoint.
// For TCP sockets, it is assumed that the port can be reached via the loopback address.
func endpointURL(config Config, listener net.Listener) string {
	scheme := endpointScheme(config)
	address := listener.Addr().String()
	if !strings.HasPrefix(scheme, "unix") {
		_, port, err := net.SplitHostPort(address)
		if err != nil {
			logrus.Warnf("failed to get listener port: %v", err)
			port = "2379"
		}
		address = "127.0.0.1:" + port
	}

	return scheme + "://" + address
}

// endpointScheme returns the URI scheme for the listener specified by the configuration.
func endpointScheme(config Config) string {
	if config.Listener == "" {
		config.Listener = KineSocket
	}

	network, _ := networkAndAddress(config.Listener)
	if network != "unix" {
		network = "http"
	}

	if config.ServerTLSConfig.CertFile != "" && config.ServerTLSConfig.KeyFile != "" {
		// yes, etcd supports the "unixs" scheme for TLS over unix sockets
		network += "s"
	}

	return network
}

// createListener returns a listener bound to the requested protocol and address.
func createListener(config Config) (ret net.Listener, rerr error) {
	if config.Listener == "" {
		config.Listener = KineSocket
	}
	network, address := networkAndAddress(config.Listener)

	if network == "unix" {
		if err := os.Remove(address); err != nil && !os.IsNotExist(err) {
			logrus.Warnf("failed to remove socket %s: %v", address, err)
		}
		defer func() {
			if err := os.Chmod(address, 0600); err != nil {
				rerr = err
			}
		}()
	} else {
		network = "tcp"
	}

	return net.Listen(network, address)
}

// grpcServer returns either a preconfigured GRPC server, or builds a new GRPC
// server using upstream keepalive defaults plus the local Server TLS configuration.
func grpcServer() *grpc.Server {
	opts := []grpc.ServerOption{
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             embed.DefaultGRPCKeepAliveMinTime,
			PermitWithoutStream: false,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    embed.DefaultGRPCKeepAliveInterval,
			Timeout: embed.DefaultGRPCKeepAliveTimeout,
		}),
	}

	return grpc.NewServer(opts...)
}

// getKineStorageBackend parses the driver string, and returns a bool
// indicating whether the backend requires leader election, and a suitable
// backend datastore connection.
//func getKineStorageBackend(ctx context.Context, driver, dsn string, cfg Config) (bool, server.Backend, error) {
//	backend, err := msobjectstore.New(ctx, dsn, cfg.BackendTLSConfig)
//
//	return true, backend, err
//}

// ParseStorageEndpoint returns the driver name and endpoint string from a datastore endpoint URL.
func ParseStorageEndpoint(storageEndpoint string) (string, string) {
	network, address := networkAndAddress(storageEndpoint)
	switch network {
	case "":
		return SQLiteBackend, ""
	case "nats":
		return NATSBackend, storageEndpoint
	case "http":
		fallthrough
	case "https":
		return ETCDBackend, address
	}
	return network, address
}

// networkAndAddress crudely splits a URL string into network (scheme) and address,
// where the address includes everything after the scheme/authority separator.
func networkAndAddress(str string) (string, string) {
	parts := strings.SplitN(str, "://", 2)
	if len(parts) > 1 {
		return parts[0], parts[1]
	}
	return "", parts[0]
}
