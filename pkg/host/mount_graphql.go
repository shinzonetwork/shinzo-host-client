package host

import (
	"context"
	"fmt"

	defradbHttp "github.com/sourcenetwork/defradb/http"
	"github.com/sourcenetwork/defradb/node"

	"github.com/shinzonetwork/shinzo-host-client/pkg/hostserver"
)

func mountGraphQL(srv *hostserver.Server, defraNode *node.Node) error {
	handler, err := defradbHttp.NewHandler(defraNode.DB, defraNode.Options())
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
