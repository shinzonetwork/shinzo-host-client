package host

import (
	"context"

	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/shinzonetwork/shinzo-host-client/pkg/defradb"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// hostMetrics holds the host's OpenTelemetry instruments.
type hostMetrics struct {
	documents          metric.Int64Counter
	blockSignatures    metric.Int64Counter
	blockSigsDropped   metric.Int64Counter
	attestations       metric.Int64Counter
	attestationTime    metric.Float64Histogram
	success, failure   metric.MeasurementOption
	collections        map[string]metric.MeasurementOption
	gaugesRegistration metric.Registration
}

const meterName = "github.com/shinzonetwork/shinzo-host-client/pkg/host"

func newHostMetrics() hostMetrics {
	meter := otel.Meter(meterName)
	m := hostMetrics{
		success:     metric.WithAttributeSet(attribute.NewSet(attribute.String("result", "success"))),
		failure:     metric.WithAttributeSet(attribute.NewSet(attribute.String("result", "error"))),
		collections: make(map[string]metric.MeasurementOption),
	}
	for _, name := range []string{
		constants.CollectionBlock,
		constants.CollectionTransaction,
		constants.CollectionLog,
		constants.CollectionAccessListEntry,
		constants.CollectionBlockSignature,
		constants.CollectionAttestationRecord,
	} {
		m.collections[name] = metric.WithAttributeSet(attribute.NewSet(attribute.String("collection", name)))
	}

	var err error
	m.documents, err = meter.Int64Counter("shinzo.host.documents",
		metric.WithDescription("Documents replicated from peers, by collection."),
		metric.WithUnit("{document}"),
	)
	if err != nil {
		otel.Handle(err)
	}
	m.blockSignatures, err = meter.Int64Counter("shinzo.host.block_signatures",
		metric.WithDescription("Block signatures from peers, by verification result."),
		metric.WithUnit("{signature}"),
	)
	if err != nil {
		otel.Handle(err)
	}
	m.blockSigsDropped, err = meter.Int64Counter("shinzo.host.block_signatures.dropped",
		metric.WithDescription("Block signatures dropped because the queue was full."),
		metric.WithUnit("{signature}"),
	)
	if err != nil {
		otel.Handle(err)
	}
	m.attestations, err = meter.Int64Counter("shinzo.host.attestations",
		metric.WithDescription("Attestation record writes, by result."),
		metric.WithUnit("{attestation}"),
	)
	if err != nil {
		otel.Handle(err)
	}
	m.attestationTime, err = meter.Float64Histogram("shinzo.host.attestation.duration",
		metric.WithDescription("Attestation record write duration, by result."),
		metric.WithUnit("ms"),
	)
	if err != nil {
		otel.Handle(err)
	}

	return m
}

func registerHostGauges(h *Host) metric.Registration {
	meter := otel.Meter(meterName)
	height, err := meter.Int64ObservableGauge("shinzo.host.block.height",
		metric.WithDescription("Highest block with an attestation on this host."),
		metric.WithUnit("{block}"),
	)
	if err != nil {
		otel.Handle(err)
	}
	received, err := meter.Int64ObservableGauge("shinzo.host.block.received",
		metric.WithDescription("Highest block number in a verified block signature from peers."),
		metric.WithUnit("{block}"),
	)
	if err != nil {
		otel.Handle(err)
	}
	queued, err := meter.Int64ObservableGauge("shinzo.host.queue.documents",
		metric.WithDescription("Documents waiting in the queue."),
		metric.WithUnit("{document}"),
	)
	if err != nil {
		otel.Handle(err)
	}
	views, err := meter.Int64ObservableGauge("shinzo.host.views",
		metric.WithDescription("Views registered on this host."),
		metric.WithUnit("{view}"),
	)
	if err != nil {
		otel.Handle(err)
	}
	peers, err := meter.Int64ObservableGauge("shinzo.p2p.peers",
		metric.WithDescription("Unique peers connected to the embedded DefraDB node."),
		metric.WithUnit("{peer}"),
	)
	if err != nil {
		otel.Handle(err)
	}

	registration, err := meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		if b := h.GetCurrentBlock(); b > 0 {
			o.ObserveInt64(height, b)
		}
		if b := h.highestReceivedBlock.Load(); b > 0 {
			o.ObserveInt64(received, b)
		}
		if q := h.observedDocQueue.Load(); q != nil {
			o.ObserveInt64(queued, int64(len(*q)))
		}
		if h.viewManager != nil {
			o.ObserveInt64(views, int64(h.viewManager.GetViewCount()))
		}
		if h.DefraNode == nil || h.NetworkHandler == nil || !h.NetworkHandler.IsNetworkActive() {
			return nil
		}
		addrs, err := h.DefraNode.DB.ActivePeers(ctx)
		if err != nil {
			return err
		}
		active, _ := defradb.BootstrapIntoPeers(addrs)
		unique := make(map[string]struct{}, len(active))
		for _, p := range active {
			unique[p.ID] = struct{}{}
		}
		o.ObserveInt64(peers, int64(len(unique)))
		return nil
	}, height, received, queued, views, peers)
	if err != nil {
		otel.Handle(err)
	}

	return registration
}

func (m *hostMetrics) unregister() {
	if m.gaugesRegistration == nil {
		return
	}
	if err := m.gaugesRegistration.Unregister(); err != nil {
		otel.Handle(err)
	}
	m.gaugesRegistration = nil
}
