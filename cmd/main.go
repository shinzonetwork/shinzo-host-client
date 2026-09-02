package main

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/shinzonetwork/shinzo-host-client/config"
	"github.com/shinzonetwork/shinzo-host-client/pkg/host"
	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
)

const (
	// shutdownTimeout bounds Close. It has to stay under the container's stop grace
	// period, or the process is killed part-way through the shutdown.
	shutdownTimeout = 30 * time.Second
	// blockProfileRate samples one blocking event per this many nanoseconds spent blocked.
	blockProfileRate = 10000
	// debugReadHeaderTimeout bounds how long a client may take to send its request
	// headers, so an idle connection cannot hold the listener open.
	debugReadHeaderTimeout = 5 * time.Second
)

// newDebugMux returns the profiling endpoints on a mux of their own, so this listener is
// the only way to reach them.
//
// They cannot be taken from the default mux: a dependency registers them there for its
// own use, which is also why no listener here should ever be given a nil handler.
func newDebugMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	// Symbolisation takes its address list in the request body, so it also accepts POST.
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("POST /debug/pprof/symbol", pprof.Symbol)
	mux.Handle("GET /debug/vars", expvar.Handler())
	return mux
}

// serveDebug starts the debug listener on addr. The listener is what makes the endpoints
// reachable, so leaving the address unset turns them off without a rebuild.
//
// Binds synchronously, so an address that cannot be served is returned as an error
// instead of failing later in the background.
func serveDebug(addr string) error {
	if os.Getenv("PPROF_BLOCK_MUTEX") != "" {
		runtime.SetBlockProfileRate(blockProfileRate)
		runtime.SetMutexProfileFraction(1)
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	srv := &http.Server{
		Handler:           newDebugMux(),
		ReadHeaderTimeout: debugReadHeaderTimeout,
	}

	fmt.Fprintf(os.Stderr, "debug endpoints listening on %s\n", listener.Addr())

	go func() {
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintf(os.Stderr, "debug listener stopped: %v\n", err)
		}
	}()

	return nil
}

func findConfigFile() string {
	possiblePaths := []string{
		"./config/config.yaml", // From project root
		"./config.yaml",        // Docker / mounted path
		"../config.yaml",       // From bin/ directory
	}

	for _, path := range possiblePaths {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}

	return "config.yaml"
}

func main() {
	if addr := os.Getenv("PPROF_ADDR"); addr != "" {
		if err := serveDebug(addr); err != nil {
			fmt.Fprintf(os.Stderr, "debug listener not started: %v\n", err)
		}
	}

	// Registered before startup so a signal arriving during it is not left unhandled.
	// The buffer holds it until the shutdown below.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	configPath := findConfigFile()
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		panic(fmt.Errorf("unable to load config: %w", err))
	}

	myHost, err := host.StartHosting(cfg)
	if err != nil {
		panic(fmt.Errorf("failed to start hosting: %w", err))
	}

	// Shutdown is signal-driven because a deferred Close does not run when the process
	// is terminated by a signal.
	sig := <-stop
	logger.Sugar.Infof("Received %s, shutting down", sig)

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := myHost.Close(ctx); err != nil {
		logger.Sugar.Errorf("Shutdown error: %v", err)
	}
	// Close returning at all is what separates a clean exit from one the container killed.
	logger.Sugar.Info("Shutdown complete")
}
