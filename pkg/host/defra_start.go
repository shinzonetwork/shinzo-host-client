package host

import (
	"context"
	"errors"

	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/hostserver"
	"github.com/sourcenetwork/defradb/acp/identity"
	"github.com/sourcenetwork/defradb/node"
)

func startDefra(
	ctx context.Context,
	srv *hostserver.Server,
	cfg *hostconfig.Config,
	log *zap.Logger,
	identityKey identity.FullIdentity,
	peerKeySeed []byte,
) (*node.Node, error) {
	return nil, errors.New("host: startDefra not implemented yet")
}
