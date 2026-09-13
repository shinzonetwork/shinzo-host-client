package host

import "github.com/shinzonetwork/shinzo-host-client/pkg/hostserver"

func mountMetrics(srv *hostserver.Server, defra DefraService) {
	srv.Mux().Handle("/metrics", defra.Metrics())
}
