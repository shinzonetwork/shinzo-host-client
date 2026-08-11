package main

import (
	"context"
	"errors"
	"fmt"
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
	// The level is read once at corelog init, so without this the only way to change it is
	// a container recreate, which drops the in-memory prune queue.
	mux.HandleFunc(logger.LevelPath, logger.LevelHandler)
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

// serveDebug starts the debug listener on addr. The listener is what makes the endpoints
// reachable, so leaving the address unset turns them off without a rebuild.
func serveDebug(addr string) {
	if os.Getenv("PPROF_BLOCK_MUTEX") != "" {
		runtime.SetBlockProfileRate(blockProfileRate)
		runtime.SetMutexProfileFraction(1)
	}

	srv := &http.Server{
		Addr:              addr,
		Handler:           newDebugMux(),
		ReadHeaderTimeout: debugReadHeaderTimeout,
	}

	go func() {
		fmt.Fprintf(os.Stderr, "debug endpoints listening on %s\n", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintf(os.Stderr, "debug listener stopped: %v\n", err)
		}
	}()
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
		serveDebug(addr)
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
	fmt.Fprintf(os.Stderr, "received %s, shutting down\n", sig)

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := myHost.Close(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "shutdown error: %v\n", err)
	}
}
