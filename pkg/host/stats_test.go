package host

import (
	"context"
	"testing"
	"time"

	"github.com/shinzonetwork/shinzo-host-client/pkg/server"
)

// The reporter runs for the life of the host, so a cancelled context has to end it
// rather than leave it ticking. The wait is shorter than statsInterval, so a reporter
// that only stops on its own tick fails here.
func TestReportStatsStopsWithContext(t *testing.T) {
	h := &Host{metrics: server.NewHostMetrics()}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		h.reportStats(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reportStats did not return after its context was cancelled")
	}
}
