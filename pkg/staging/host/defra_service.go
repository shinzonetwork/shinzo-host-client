package host

import (
	"context"
	"net/http"

	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostkey"
	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
)

// NewDefraService builds a DefraService. Not implemented yet.
func NewDefraService(cfg hostconfig.Config, log *zap.Logger, keys hostkey.NodeKeys) DefraService {
	_ = cfg
	_ = log
	_ = keys
	return &defraService{}
}

type defraService struct {
	viewManager *view.Manager
	handler     http.Handler
}

func (s *defraService) Start(_ context.Context) error { return nil }

func (s *defraService) Bootstrap(_ context.Context) {}

func (s *defraService) MaintainPeerConnections(_ context.Context) {}

func (s *defraService) AttestSignatures(_ context.Context) {}

func (s *defraService) TrackDocumentMetrics(_ context.Context) {}

func (s *defraService) PruneDocuments(_ context.Context) {}

func (s *defraService) Stop(_ context.Context) error { return nil }

func (s *defraService) Views() *view.Manager { return s.viewManager }

func (s *defraService) Handler() http.Handler { return s.handler }
