// new host composition root, one mux, one port, everything mounted on it.
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
	deps    deps

	// reverse order on Close, teardown lives next to whatever starts it
	shutdownFns []func(context.Context) error
}

// no I/O yet, call Start to actually bring it up
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

// doesn't block, call Close when done
func (s *Server) Start(ctx context.Context) error {
	// still stubs, panic if uncommented, bring back one at a time
	// if err := s.startDefraNode(ctx); err != nil {
	// 	return fmt.Errorf("starting defra node: %w", err)
	// }
	// if err := s.mountGraphQL(ctx); err != nil {
	// 	return fmt.Errorf("mounting graphql: %w", err)
	// }
	// if err := s.mountPlayground(); err != nil {
	// 	return fmt.Errorf("mounting playground: %w", err)
	// }
	s.mountHealth()
	// if err := s.startEventSubscription(ctx); err != nil {
	// 	return fmt.Errorf("starting event subscription: %w", err)
	// }

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

// http server first, then everything else in reverse, keep going on errors
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
