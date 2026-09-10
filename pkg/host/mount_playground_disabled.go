//go:build !hostplayground

package host

import (
	"github.com/shinzonetwork/shinzo-host-client/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/hostserver"
)

func mountPlayground(_ *hostserver.Server, _ *hostconfig.Config) error {
	return nil
}
