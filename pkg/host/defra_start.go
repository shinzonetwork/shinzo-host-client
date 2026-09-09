package host

import (
	"context"
	"errors"

	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/hostserver"
	"github.com/sourcenetwork/defradb/node"
)

// startDefra brings up the embedded DefraDB node. srv is only ever used
// through its generic public surface, Mux() and RegisterShutdown(), it
// still has no idea defra exists, this function just uses what any caller
// could use. Registers its own teardown here rather than handing a close
// func back for Start to wire up itself.
func startDefra(ctx context.Context, srv *hostserver.Server, cfg *hostconfig.Config, log *zap.Logger) (*node.Node, error) {
	return nil, errors.New("host: startDefra not implemented yet")
}
