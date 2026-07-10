package accounting

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	ethcrypto "github.com/ethereum/go-ethereum/crypto"

	"github.com/shinzonetwork/shinzo-host-client/pkg/pool"
	"github.com/shinzonetwork/shinzo-querysig/billing"
)

// RecordInput is the billing context for one served query.
type RecordInput struct {
	// Extensions carries the user's signed request: RequestSignature, Nonce,
	// QueryHash, and RequestTimestamp.
	Extensions billing.Extensions
	// ViewAddress is the on-chain address of the served view; pool_id derives from it.
	ViewAddress common.Address
	// RowsQueried is the number of rows the view served.
	RowsQueried uint64
	// AttestedIndexers is the set of indexers the host saw attesting around the
	// serve time (network-wide, not scoped to the query's served blocks).
	AttestedIndexers []string
}

// Recorder signs a QueryResponse over a served query and submits the service
// record to the accounting service.
type Recorder struct {
	client      *Client
	signer      *ecdsa.PrivateKey
	hostAddress common.Address
	chainID     uint64
	now         func() time.Time
}

// NewRecorder returns a Recorder that signs responses with signer (the host's
// node key) under chainID and submits records via client. The host address is
// the Ethereum address signer recovers to.
func NewRecorder(client *Client, signer *ecdsa.PrivateKey, chainID uint64) *Recorder {
	return &Recorder{
		client:      client,
		signer:      signer,
		hostAddress: ethcrypto.PubkeyToAddress(signer.PublicKey),
		chainID:     chainID,
		now:         time.Now,
	}
}

// Record builds and submits the service record for a served query: it signs an
// EIP-712 QueryResponse over the served rows and POSTs a record carrying both the
// user's request signature and the host's response signature. ResponseCIDs are
// empty for v1.
func (r *Recorder) Record(ctx context.Context, in RecordInput) error {
	req, _, err := in.Extensions.Request()
	if err != nil {
		return fmt.Errorf("decode extensions: %w", err)
	}

	poolID, err := pool.PoolID(in.ViewAddress, pool.DefaultWindowSize)
	if err != nil {
		return fmt.Errorf("derive pool id: %w", err)
	}
	respondedAt := uint64(r.now().Unix()) //nolint:gosec // Unix seconds for the current time is always within uint64 range
	cids := []string{}

	signature, err := billing.SignQueryResponse(r.chainID, r.signer, billing.QueryResponse{
		QueryHash:        req.QueryHash,
		Host:             r.hostAddress,
		Pool:             poolID,
		RowsQueried:      in.RowsQueried,
		RespondedAt:      respondedAt,
		ResponseCidsHash: billing.ResponseCidsHash(cids),
	})
	if err != nil {
		return fmt.Errorf("sign query response: %w", err)
	}

	return r.client.Submit(ctx, ServiceRecord{
		Nonce:             in.Extensions.Nonce,
		PoolID:            poolID.Hex(),
		QueryHash:         in.Extensions.QueryHash,
		HostAddress:       r.hostAddress.Hex(),
		RequestTimestamp:  in.Extensions.RequestTimestamp,
		RequestSignature:  in.Extensions.RequestSignature,
		RowsQueried:       in.RowsQueried,
		RespondedAt:       respondedAt,
		ResponseCIDs:      cids,
		ResponseSignature: hexutil.Encode(signature),
		AttestedIndexers:  in.AttestedIndexers,
	})
}
