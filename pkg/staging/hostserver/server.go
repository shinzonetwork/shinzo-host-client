package hostserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

const (
	shutdownTimeout    = 15 * time.Second
	shutdownFnsTimeout = 15 * time.Second
	readHeaderTimeout  = 10 * time.Second
)

// ErrNilLogger is returned by New when given a nil logger.
var ErrNilLogger = errors.New("hostserver: nil logger")

// ErrAlreadyStarted is returned by Start if it's already been called.
var ErrAlreadyStarted = errors.New("hostserver: already started")

// Server is an HTTP server built around one shared mux.
type Server struct {
	addr      string
	boundAddr atomic.Pointer[string]
	started   atomic.Bool

	log *zap.SugaredLogger
	mux *http.ServeMux

	httpSrv   *http.Server
	onFailure atomic.Pointer[func()]
	crashErr  atomic.Pointer[error]

	shutdownMu  sync.Mutex
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

// Mux is where callers register handlers, before calling Start.
func (s *Server) Mux() *http.ServeMux {
	return s.mux
}

// Addr returns the address Start actually bound.
func (s *Server) Addr() string {
	if p := s.boundAddr.Load(); p != nil {
		return *p
	}
	return ""
}

// RegisterShutdown adds fn to run, in reverse registration order, when Close runs.
func (s *Server) RegisterShutdown(fn func(context.Context) error) {
	s.shutdownMu.Lock()
	defer s.shutdownMu.Unlock()
	s.shutdownFns = append(s.shutdownFns, fn)
}

// OnFailure registers fn to run if Serve stops on its own, for any
// reason other than Close. Safe to call at any time, including while
// already serving.
func (s *Server) OnFailure(fn func()) {
	s.onFailure.Store(&fn)
}

// Start binds addr and begins serving in the background. Returns
// ErrAlreadyStarted if called more than once.
func (s *Server) Start() error {
	if !s.started.CompareAndSwap(false, true) {
		return ErrAlreadyStarted
	}
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		s.started.Store(false) // allow retry
		return fmt.Errorf("binding %s: %w", s.addr, err)
	}
	return s.startOn(ln)
}

// startOn is Start's body, taking the listener so tests can force a real Serve failure.
func (s *Server) startOn(ln net.Listener) error {
	addr := ln.Addr().String()
	s.boundAddr.Store(&addr)

	s.httpSrv = &http.Server{Handler: s.mux, ReadHeaderTimeout: readHeaderTimeout}

	go func() {
		if err := s.httpSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Errorw("http server stopped unexpectedly", "err", err)
			s.crashErr.Store(&err)
			if p := s.onFailure.Load(); p != nil {
				(*p)()
			}
		}
	}()

	return nil
}

// Close reports any crash Serve hit, shuts the HTTP server down, then
// runs shutdown fns, each with its own budget. Safe to call more than once.
func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		s.closeErr = s.close()
	})
	return s.closeErr
}

func (s *Server) close() error {
	var errs []error

	if p := s.crashErr.Load(); p != nil {
		errs = append(errs, fmt.Errorf("http server stopped: %w", *p))
	}

	if s.httpSrv != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := s.httpSrv.Shutdown(shutdownCtx); err != nil {
			errs = append(errs, fmt.Errorf("http server shutdown: %w", err))
		}
	}

	s.shutdownMu.Lock()
	fns := slices.Clone(s.shutdownFns)
	s.shutdownMu.Unlock()

	fnsCtx, cancel := context.WithTimeout(context.Background(), shutdownFnsTimeout)
	defer cancel()
	for _, fn := range slices.Backward(fns) {
		if err := fn(fnsCtx); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}
