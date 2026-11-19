package host

import (
	"context"
	"net/http"

	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostkey"
	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
)

// NewDefra builds a Defra. Not implemented yet.
func NewDefra(cfg hostconfig.Config, log *zap.Logger, keys hostkey.NodeKeys) *Defra {
	_ = cfg
	_ = log
	_ = keys
	return &Defra{}
}

var _ DefraService = (*Defra)(nil)

// Defra is the DefraService implementation backed by a DefraDB node.
type Defra struct {
	viewManager *view.Manager
	handler     http.Handler
}

// Start brings the node up and gets it ready to serve. Not implemented yet.
func (s *Defra) Start(_ context.Context) error { return nil }

// Run connects to the network and starts background upkeep. Not
// implemented yet.
func (s *Defra) Run(_ context.Context) error { return nil }

// Stop shuts down the network, the pruners and the node. Not implemented
// yet.
func (s *Defra) Stop(_ context.Context) error { return nil }

// Views returns the view manager. It's nil until Start has run: Start
// builds it last, after bootstrap, because building it queries the node.
func (s *Defra) Views() *view.Manager { return s.viewManager }

// Handler returns the API handler. It's nil until Start has run: Start
// builds it last, alongside the view manager.
func (s *Defra) Handler() http.Handler { return s.handler }
