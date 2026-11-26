package host

import (
	"context"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
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
	processingTime     metric.Float64Histogram
	chains            map[string]*chainInstruments // By chain prefix; not changed after the host is built
	gaugesRegistration metric.Registration
}

// chainInstruments holds one enabled chain's attribute options and the state its gauges read.
type chainInstruments struct {
	chain            metric.MeasurementOption            // shinzo.chain.name and shinzo.chain.network
	success, failure metric.MeasurementOption            // The chain's attributes and result
	tables           map[string]metric.MeasurementOption // By collection name: the chain's attributes and collection
	received         atomic.Int64                        // Highest block in a verified block signature from peers
	queue            atomic.Pointer[chan string]         // The chain's attestation queue, once the listener made it
}

const meterName = "github.com/shinzonetwork/shinzo-host-client/pkg/host"

func newHostMetrics(chains []chain.Collections) hostMetrics {
	meter := otel.Meter(meterName)
	m := hostMetrics{chains: make(map[string]*chainInstruments, len(chains))}
	for _, c := range chains {
		m.chains[c.Prefix] = newChainInstruments(c)
	}

	var err error
	m.documents, err = meter.Int64Counter("shinzo.host.documents",
		metric.WithDescription("Documents replicated from peers, by chain and collection."),
		metric.WithUnit("{document}"),
	)
	if err != nil {
		otel.Handle(err)
	}
	m.blockSignatures, err = meter.Int64Counter("shinzo.host.block_signatures",
		metric.WithDescription("Block signatures from peers, by chain and verification result."),
		metric.WithUnit("{signature}"),
	)
	if err != nil {
		otel.Handle(err)
	}
	m.blockSigsDropped, err = meter.Int64Counter("shinzo.host.block_signatures.dropped",
		metric.WithDescription("Block signatures dropped because the chain's attestation queue was full."),
		metric.WithUnit("{signature}"),
	)
	if err != nil {
		otel.Handle(err)
	}
	m.attestations, err = meter.Int64Counter("shinzo.host.attestations",
		metric.WithDescription("Attestation record writes, by chain and result."),
		metric.WithUnit("{attestation}"),
	)
	if err != nil {
		otel.Handle(err)
	}
	m.attestationTime, err = meter.Float64Histogram("shinzo.host.attestation.duration",
		metric.WithDescription("Attestation record write duration, by chain and result."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10), //nolint:mnd // OTel semconv HTTP duration buckets
	)
	if err != nil {
		otel.Handle(err)
	}
	m.processingTime, err = meter.Float64Histogram("shinzo.host.block_signatures.duration",
		metric.WithDescription("Time to verify a block signature from peers and write its attestation records, by chain."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10), //nolint:mnd // OTel semconv HTTP duration buckets
	)
	if err != nil {
		otel.Handle(err)
	}

	return m
}

// chainAttributes returns the chain name and network attributes of the chain with prefix
// "<Name>__<Network>".
func chainAttributes(prefix string) []attribute.KeyValue {
	name, network, _ := strings.Cut(prefix, "__")
	return []attribute.KeyValue{
		attribute.String("shinzo.chain.name", name),
		attribute.String("shinzo.chain.network", network),
	}
}

// boolGauge returns 1 for true and 0 for false.
func boolGauge(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// newChainInstruments precomputes chain c's attribute options. Its prefix is "<Name>__<Network>"
// (chain.Validate checks it), the same name and network the generator reports.
func newChainInstruments(c chain.Collections) *chainInstruments {
	attrs := chainAttributes(c.Prefix)
	with := func(kv attribute.KeyValue) metric.MeasurementOption {
		return metric.WithAttributeSet(attribute.NewSet(append(slices.Clone(attrs), kv)...))
	}

	ci := &chainInstruments{
		chain:   metric.WithAttributeSet(attribute.NewSet(attrs...)),
		success: with(attribute.String("result", "success")),
		failure: with(attribute.String("result", "error")),
		tables:  make(map[string]metric.MeasurementOption),
	}
	// Named without the chain prefix, so every chain reports its tables under the same names.
	for _, table := range c.Subscribed() {
		ci.tables[table] = with(attribute.String("collection", strings.TrimPrefix(table, c.Prefix+"__")))
	}
	return ci
}

// chain returns the instruments of the enabled chain with prefix, or nil when there are none.
func (m *hostMetrics) chain(prefix string) *chainInstruments {
	return m.chains[prefix]
}

func registerHostGauges(h *Host) metric.Registration {
	meter := otel.Meter(meterName)
	height, err := meter.Int64ObservableGauge("shinzo.host.block.height",
		metric.WithDescription("Newest block this host attested on the chain since it started."),
		metric.WithUnit("{block}"),
	)
	if err != nil {
		otel.Handle(err)
	}
	received, err := meter.Int64ObservableGauge("shinzo.host.block.received",
		metric.WithDescription("Highest block number in a verified block signature from peers, by chain."),
		metric.WithUnit("{block}"),
	)
	if err != nil {
		otel.Handle(err)
	}
	queued, err := meter.Int64ObservableGauge("shinzo.host.queue.documents",
		metric.WithDescription("Block signatures waiting in the chain's attestation queue."),
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

	enabled, err := meter.Int64ObservableGauge("shinzo.host.chain.enabled",
		metric.WithDescription("Whether the host serves the chain at this start (1) or not (0), with the reason when it does not. Includes configured chains that are not enabled and stored chains the config no longer lists."),
		metric.WithUnit("1"),
	)
	if err != nil {
		otel.Handle(err)
	}
	ready, err := meter.Int64ObservableGauge("shinzo.host.chain.ready",
		metric.WithDescription("Whether the chain is ready (1) or not (0): enabled, with its last document, or the host's start before its first, within the ready window."),
		metric.WithUnit("1"),
	)
	if err != nil {
		otel.Handle(err)
	}
	lastDocument, err := meter.Int64ObservableGauge("shinzo.host.document.last_received.timestamp",
		metric.WithDescription("Unix time of the last document a peer sent the chain, reported after the first."),
		metric.WithUnit("s"),
	)
	if err != nil {
		otel.Handle(err)
	}
	cutoff, err := meter.Int64ObservableGauge("shinzo.host.prune.cutoff",
		metric.WithDescription("The chain's pruner cutoff: blocks at or below it are pruned. Reported once the pruner sets one."),
		metric.WithUnit("{block}"),
	)
	if err != nil {
		otel.Handle(err)
	}

	registration, err := meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		if h.metrics != nil {
			// Every chain the endpoints report, not only the enabled ones the other instruments cover.
			for _, s := range h.metrics.ChainStatuses() {
				attrs := chainAttributes(s.Chain)
				chainOpt := metric.WithAttributes(attrs...)
				enabledOpt := chainOpt
				if s.Reason != "" {
					enabledOpt = metric.WithAttributes(append(attrs, attribute.String("reason", s.Reason))...)
				}
				o.ObserveInt64(enabled, boolGauge(s.Enabled), enabledOpt)
				o.ObserveInt64(ready, boolGauge(s.Ready), chainOpt)
				if !s.LastDocumentTime.IsZero() {
					o.ObserveInt64(lastDocument, s.LastDocumentTime.Unix(), chainOpt)
				}
				if s.Cutoff > 0 {
					o.ObserveInt64(cutoff, s.Cutoff, chainOpt)
				}
			}
		}
		for _, c := range h.chains {
			ci := h.instruments.chain(c.Prefix)
			if ci == nil {
				continue
			}
			if m := h.chainMetrics(c.Prefix); m != nil {
				if b := m.CurrentBlock(); b > 0 {
					o.ObserveInt64(height, b, ci.chain)
				}
			}
			if b := ci.received.Load(); b > 0 {
				o.ObserveInt64(received, b, ci.chain)
			}
			if q := ci.queue.Load(); q != nil {
				o.ObserveInt64(queued, int64(len(*q)), ci.chain)
			}
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
	}, height, received, queued, views, peers, enabled, ready, lastDocument, cutoff)
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
