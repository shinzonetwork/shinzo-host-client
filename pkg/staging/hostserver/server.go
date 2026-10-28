package hostserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"slices"
	"time"

	"go.uber.org/zap"
)

const (
	shutdownTimeout   = 15 * time.Second
	readHeaderTimeout = 10 * time.Second
)

// ErrNilLogger is returned by New when given a nil logger.
var ErrNilLogger = errors.New("hostserver: nil logger")

// Server is an HTTP server built around one shared mux.
type Server struct {
	addr string

	log *zap.SugaredLogger
	mux *http.ServeMux

	httpSrv *http.Server

	shutdownFns []func(context.Context) error
}

// New builds a Server that will listen on addr once Start is called.
func New(addr string, log *zap.Logger) (*Server, error) {
	if log == nil {
		return nil, ErrNilLogger
	}
	return &Server{
		addr: addr,
		log:  log.Sugar(),
		mux:  http.NewServeMux(),
	}, nil
}

// Mux is where callers register handlers.
func (s *Server) Mux() *http.ServeMux {
	return s.mux
}

// RegisterShutdown adds fn to run, in reverse registration order, when
// Close runs — for anything else that needs to stop alongside the HTTP server.
func (s *Server) RegisterShutdown(fn func(context.Context) error) {
	s.shutdownFns = append(s.shutdownFns, fn)
}

// Start binds addr and begins serving in the background.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("binding %s: %w", s.addr, err)
	}

	s.httpSrv = &http.Server{Handler: s.mux, ReadHeaderTimeout: readHeaderTimeout}

	go func() {
		if err := s.httpSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Errorw("http server stopped unexpectedly", "err", err)
		}
	}()

	return nil
}

// Close shuts the HTTP server down, giving in-flight requests up to
// shutdownTimeout to finish, then runs every registered shutdown
// function, reverse of the order they were added in.
func (s *Server) Close(ctx context.Context) error {
	shutdownCtx, cancel := context.WithTimeout(ctx, shutdownTimeout)
	defer cancel()

	var errs []error

	if s.httpSrv != nil {
		if err := s.httpSrv.Shutdown(shutdownCtx); err != nil {
			errs = append(errs, fmt.Errorf("http server shutdown: %w", err))
		}
	}

	for _, fn := range slices.Backward(s.shutdownFns) {
		if err := fn(shutdownCtx); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}
