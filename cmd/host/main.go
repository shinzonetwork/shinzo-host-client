package main

import (
	"errors"
	"expvar"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"time"

	"github.com/shinzonetwork/shinzo-host-client/cmd/host/internal/cli"
)

const (
	blockProfileRate       = 10000
	debugReadHeaderTimeout = 5 * time.Second
)

func newDebugMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("POST /debug/pprof/symbol", pprof.Symbol)
	mux.Handle("GET /debug/vars", expvar.Handler())
	return mux
}

func serveDebug(addr string) error {
	if os.Getenv("SHINZO_PPROF_BLOCK_MUTEX") != "" {
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

func main() {
	if addr := os.Getenv("SHINZO_PPROF_ADDR"); addr != "" {
		if err := serveDebug(addr); err != nil {
			fmt.Fprintf(os.Stderr, "debug listener not started: %v\n", err)
		}
	}

	if err := cli.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
