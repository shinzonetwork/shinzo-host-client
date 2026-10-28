package hostserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"slices"
	"sync"
	"time"

	"go.uber.org/zap"
)

const (
	// shutdownFnsTimeout is the shutdown fns' own budget, separate from the HTTP drain.
	shutdownFnsTimeout = 15 * time.Second
	readHeaderTimeout  = 10 * time.Second
)

// ErrNilLogger is returned by New when given a nil logger.
var ErrNilLogger = errors.New("hostserver: nil logger")

// Server is an HTTP server built around one shared mux.
type Server struct {
	addr      string
	boundAddr string

	log *zap.SugaredLogger
	mux *http.ServeMux

	httpSrv   *http.Server
	onFailure func()
	crashErr  error

	shutdownFns []func(context.Context) error

	closeOnce sync.Once
	closeErr  error
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

// Addr returns the address Start actually bound.
func (s *Server) Addr() string {
	return s.boundAddr
}

// RegisterShutdown adds fn to run, in reverse registration order, when
// Close runs — for anything else that needs to stop alongside the HTTP server.
func (s *Server) RegisterShutdown(fn func(context.Context) error) {
	s.shutdownFns = append(s.shutdownFns, fn)
}

// OnFailure registers fn to run if Serve stops on its own, for any
// reason other than Close.
func (s *Server) OnFailure(fn func()) {
	s.onFailure = fn
}

// Err returns the error Serve stopped with, or nil if it hasn't
// failed (including if it was stopped cleanly by Close).
func (s *Server) Err() error {
	return s.crashErr
}

// Start binds addr and begins serving in the background.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("binding %s: %w", s.addr, err)
	}
	s.boundAddr = ln.Addr().String()

	s.httpSrv = &http.Server{Handler: s.mux, ReadHeaderTimeout: readHeaderTimeout}

	go func() {
		if err := s.httpSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Errorw("http server stopped unexpectedly", "err", err)
			s.crashErr = err
			if s.onFailure != nil {
				s.onFailure()
			}
		}
	}()

	return nil
}

// Close shuts the HTTP server down, then runs shutdown fns with their
// own budget. Safe to call more than once.
func (s *Server) Close(ctx context.Context) error {
	s.closeOnce.Do(func() {
		s.closeErr = s.close(ctx)
	})
	return s.closeErr
}

func (s *Server) close(ctx context.Context) error {
	var errs []error

	if s.httpSrv != nil {
		if err := s.httpSrv.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("http server shutdown: %w", err))
		}
	}

	fnsCtx, cancel := context.WithTimeout(ctx, shutdownFnsTimeout)
	defer cancel()
	for _, fn := range slices.Backward(s.shutdownFns) {
		if err := fn(fnsCtx); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}
