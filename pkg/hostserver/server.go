package hostserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/hostconfig"
)

const shutdownTimeout = 15 * time.Second

type Server struct {
	cfg *hostconfig.Config
	log *zap.SugaredLogger
	mux *http.ServeMux

	httpSrv *http.Server

	// reverse order on Close, keeps going even if one fails
	shutdownFns []func(context.Context) error
}

func New(cfg *hostconfig.Config, log *zap.Logger) (*Server, error) {
	if cfg == nil {
		return nil, errors.New("hostserver: nil config")
	}
	if log == nil {
		return nil, errors.New("hostserver: nil logger")
	}
	return &Server{
		cfg: cfg,
		log: log.Sugar(),
		mux: http.NewServeMux(),
	}, nil
}

func (s *Server) Mux() *http.ServeMux {
	return s.mux
}

func (s *Server) RegisterShutdown(fn func(context.Context) error) {
	s.shutdownFns = append(s.shutdownFns, fn)
}

func (s *Server) Start(context.Context) error {
	// bind first so a bad addr/taken port errors here, not in the goroutine
	ln, err := net.Listen("tcp", s.cfg.HTTP.Addr)
	if err != nil {
		return fmt.Errorf("binding %s: %w", s.cfg.HTTP.Addr, err)
	}

	s.httpSrv = &http.Server{Handler: s.mux}

	go func() {
		if err := s.httpSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Errorw("http server stopped unexpectedly", "err", err)
		}
	}()

	s.log.Infow("host http server listening", "addr", ln.Addr().String())
	return nil
}

func (s *Server) Close(ctx context.Context) error {
	shutdownCtx, cancel := context.WithTimeout(ctx, shutdownTimeout)
	defer cancel()

	var errs []error

	if s.httpSrv != nil {
		if err := s.httpSrv.Shutdown(shutdownCtx); err != nil {
			errs = append(errs, fmt.Errorf("http server shutdown: %w", err))
		}
	}

	for i := len(s.shutdownFns) - 1; i >= 0; i-- {
		if err := s.shutdownFns[i](shutdownCtx); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}
