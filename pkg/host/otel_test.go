package host

import (
	"testing"

	attestationService "github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// Sets the global meter provider, so it cannot run in parallel.
func TestHostInstruments_RecordPerChain(t *testing.T) {
	orig := otel.GetMeterProvider()
	t.Cleanup(func() { otel.SetMeterProvider(orig) })
	reader := sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))

	other := chain.EVM("Arbitrum__Sepolia")
	chains := []chain.Collections{testCollections, other}
	h := &Host{
		chains: chains,
		metrics: server.NewHostMetrics(
			&server.ChainMetrics{Prefix: testCollections.Prefix, Enabled: true},
			&server.ChainMetrics{Prefix: other.Prefix, Enabled: true},
		),
		blockSignatureVerifier: attestationService.NewBlockSignatureVerifier(10),
		instruments:            newHostMetrics(chains),
	}
	h.instruments.gaugesRegistration = registerHostGauges(h)
	t.Cleanup(h.instruments.unregister)

	assert.Nil(t, h.instruments.chain("Unserved__Chain"))

	// An invalid signature on the second chain counts as an error on that chain only.
	require.False(t, h.verifyBlockSignature(t.Context(), chain.Config{Prefix: other.Prefix, ChainID: 421614}, &attestationService.BlockSignature{
		BlockNumber: 7, MerkleRoot: "zz", SignatureValue: "zz",
	}))
	// Only the first chain has attested a block, so only it reports a height.
	h.chainMetrics(testCollections.Prefix).UpdateCurrentBlock(42)
	h.instruments.chain(other.Prefix).received.Store(9)
	// The collection attribute names the table without the chain prefix.
	require.Len(t, h.instruments.chain(other.Prefix).tables, len(other.Subscribed()))
	h.instruments.documents.Add(t.Context(), 1, h.instruments.chain(other.Prefix).tables[other.Block.Name])

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	sigs := int64Points(t, rm, "shinzo.host.block_signatures")
	require.Len(t, sigs, 1)
	assert.Equal(t, attribute.NewSet(
		attribute.String("shinzo.chain.name", "Arbitrum"),
		attribute.String("shinzo.chain.network", "Sepolia"),
		attribute.String("result", "error"),
	), sigs[0].Attributes)
	assert.Equal(t, int64(1), sigs[0].Value)

	heights := int64Points(t, rm, "shinzo.host.block.height")
	require.Len(t, heights, 1)
	name, _ := heights[0].Attributes.Value("shinzo.chain.name")
	network, _ := heights[0].Attributes.Value("shinzo.chain.network")
	assert.Equal(t, "Ethereum", name.AsString())
	assert.Equal(t, "Mainnet", network.AsString())
	assert.Equal(t, int64(42), heights[0].Value)

	docs := int64Points(t, rm, "shinzo.host.documents")
	require.Len(t, docs, 1)
	assert.Equal(t, attribute.NewSet(
		attribute.String("collection", "Block"),
		attribute.String("shinzo.chain.name", "Arbitrum"),
		attribute.String("shinzo.chain.network", "Sepolia"),
	), docs[0].Attributes)

	received := int64Points(t, rm, "shinzo.host.block.received")
	require.Len(t, received, 1)
	name, _ = received[0].Attributes.Value("shinzo.chain.name")
	assert.Equal(t, "Arbitrum", name.AsString())
	assert.Equal(t, int64(9), received[0].Value)
}

// int64Points returns the data points of the int64 sum or gauge named name in rm.
func int64Points(t *testing.T, rm metricdata.ResourceMetrics, name string) []metricdata.DataPoint[int64] {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				return data.DataPoints
			case metricdata.Gauge[int64]:
				return data.DataPoints
			default:
				t.Fatalf("%s has unexpected data type %T", name, m.Data)
			}
		}
	}
	return nil
}
