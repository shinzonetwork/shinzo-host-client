package host

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"sync"

	"github.com/sourcenetwork/defradb/client/options"
	"github.com/sourcenetwork/defradb/node"
	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostkey"
	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
)

// NewDefra builds a Defra. Nothing starts until Start.
func NewDefra(cfg hostconfig.Config, log *zap.Logger, keys hostkey.NodeKeys) *Defra {
	return &Defra{
		cfg:  cfg,
		log:  log,
		keys: keys,
	}
}

var _ DefraService = (*Defra)(nil)

// Defra is the DefraService implementation backed by a DefraDB node.
type Defra struct {
	cfg  hostconfig.Config
	log  *zap.Logger
	keys hostkey.NodeKeys

	mu   sync.Mutex // guards node
	node *node.Node // nil when not running

	viewManager *view.Manager
	handler     http.Handler
}

const (
	storeDirName           = "defradb"
	badgerValueLogFileSize = 64 << 20

	// wazero returns wasm traps as Go errors; wasmtime panics across cgo.
	lensRuntimeWazero options.NodeLensRuntimeType = "wazero"
)

// nodeOptions builds the node's options, with the API and P2P off.
func (s *Defra) nodeOptions() (*options.NodeOptionsBuilder, error) {
	peerKey, err := s.keys.PeerPrivateKey()
	if err != nil {
		return nil, err
	}
	peerKeyBytes, err := peerKey.Raw()
	if err != nil {
		return nil, fmt.Errorf("encoding p2p key: %w", err)
	}

	opts := options.Node().
		SetDisableAPI(true).
		SetDisableP2P(true)
	opts.Store().
		SetPath(filepath.Join(s.cfg.DataDir, storeDirName)).
		SetBadgerFileSize(badgerValueLogFileSize)
	opts.DB().
		SetNodeIdentity(s.keys.IdentityKey).
		SetLensRuntime(lensRuntimeWazero)
	opts.P2P().SetPrivateKey(peerKeyBytes)

	return opts, nil
}

// Start brings the node up and gets it ready to serve.
func (s *Defra) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.node != nil {
		return ErrDefraAlreadyStarted
	}

	opts, err := s.nodeOptions()
	if err != nil {
		return err
	}

	// Node lives until Stop, not until ctx is cancelled.
	nodeCtx := context.WithoutCancel(ctx)

	n, err := node.New(nodeCtx, opts)
	if err != nil {
		return fmt.Errorf("creating defra node: %w", err)
	}

	// No n.Close on failure: it panics on a half-started node.
	if err := n.Start(nodeCtx); err != nil {
		return fmt.Errorf("starting defra node: %w", err)
	}

	s.node = n
	s.log.Info("defra node started", zap.String("did", s.keys.DID()))

	return nil
}

// Run connects to the network and starts background upkeep. Not
// implemented yet.
func (s *Defra) Run(_ context.Context) error { return nil }

// Stop closes the node. It's a no-op if the node isn't running.
func (s *Defra) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.node == nil {
		return nil
	}

	err := s.node.Close(ctx)
	s.node = nil
	if err != nil {
		return fmt.Errorf("closing defra node: %w", err)
	}

	s.log.Info("defra node stopped")

	return nil
}

// Views returns the view manager. It's nil until Start has run: Start
// builds it last, after bootstrap, because building it queries the node.
func (s *Defra) Views() *view.Manager { return s.viewManager }

// Handler returns the API handler. It's nil until Start has run: Start
// builds it last, alongside the view manager.
func (s *Defra) Handler() http.Handler { return s.handler }
