package host

import (
	"context"
	"fmt"

	"github.com/sourcenetwork/defradb/client/options"
	defradbHttp "github.com/sourcenetwork/defradb/http"
	"github.com/sourcenetwork/defradb/node"

	"github.com/shinzonetwork/shinzo-host-client/pkg/hostserver"
)

func mountGraphQL(srv *hostserver.Server, db node.DB, opts *options.NodeOptions) error {
	handler, err := defradbHttp.NewHandler(db, opts)
	if err != nil {
		return fmt.Errorf("building graphql handler: %w", err)
	}

	srv.Mux().Handle("/api/", handler)
	srv.RegisterShutdown(func(context.Context) error {
		handler.Close()
		return nil
	})

	return nil
}
