package acp

import (
	"context"
	"errors"
	"fmt"

	coreTypes "github.com/sourcenetwork/acp_core/pkg/types"
	sourcehub "github.com/sourcenetwork/sourcehub/sdk"
	sourcehubTypes "github.com/sourcenetwork/sourcehub/x/acp/types"
)

// Sentinel errors reported by NewSourceHubAuthorizer when a required
// configuration value is missing. Callers branch on these with errors.Is
// rather than parsing error strings.
var (
	ErrGRPCAddrRequired  = errors.New("sourcehub grpc address required")
	ErrCometAddrRequired = errors.New("sourcehub comet rpc address required")
	ErrPolicyIDRequired  = errors.New("sourcehub policy id required")
)

// Authorizer answers: may this caller read this view? The middleware
// holds an Authorizer rather than calling SourceHub directly so the
// test surface can substitute a stub and stay free of any gRPC
// dependency.
type Authorizer interface {
	AuthorizeView(ctx context.Context, callerDID, contractAddr string) (bool, error)
}

// Resource and permission names match the SourceHub policy expression
// `view.read = (subscriber) - banned` registered for Shinzo. Hard-coded
// because the middleware only gates the view resource on read; mutations
// continue through whatever path the policy defines.
const (
	viewResourceName = "view"
	readPermission   = "read"
)

// SourceHubAuthorizer evaluates view-read access by calling SourceHub's
// VerifyAccessRequest. The call is a Cosmos read query against the ACP
// keeper: no gas, no transaction, latency bounded by the network hop to
// the configured gRPC endpoint.
type SourceHubAuthorizer struct {
	client   *sourcehub.Client
	policyID string
}

// NewSourceHubAuthorizer dials SourceHub over gRPC and CometBFT RPC and
// returns a ready Authorizer. The CometBFT RPC endpoint is required by
// the SDK even when the caller only uses read queries.
func NewSourceHubAuthorizer(grpcAddr, cometRPCAddr, policyID string) (*SourceHubAuthorizer, error) {
	if grpcAddr == "" {
		return nil, ErrGRPCAddrRequired
	}
	if cometRPCAddr == "" {
		return nil, ErrCometAddrRequired
	}
	if policyID == "" {
		return nil, ErrPolicyIDRequired
	}
	client, err := sourcehub.NewClient(
		sourcehub.WithGRPCAddr(grpcAddr),
		sourcehub.WithCometRPCAddr(cometRPCAddr),
	)
	if err != nil {
		return nil, fmt.Errorf("sourcehub client: %w", err)
	}
	return &SourceHubAuthorizer{
		client:   client,
		policyID: policyID,
	}, nil
}

// AuthorizeView returns true if SourceHub's policy evaluation finds a
// subscriber tuple for (contractAddr, callerDID) under the configured
// policy. A non-nil error from the gRPC call is treated by the caller as
// an indeterminate state, distinct from a clean "deny".
func (a *SourceHubAuthorizer) AuthorizeView(ctx context.Context, callerDID, contractAddr string) (bool, error) {
	resp, err := a.client.ACPQueryClient().VerifyAccessRequest(ctx,
		&sourcehubTypes.QueryVerifyAccessRequestRequest{
			PolicyId: a.policyID,
			AccessRequest: &coreTypes.AccessRequest{
				Operations: []*coreTypes.Operation{{
					Object:     coreTypes.NewObject(viewResourceName, contractAddr),
					Permission: readPermission,
				}},
				Actor: &coreTypes.Actor{Id: callerDID},
			},
		})
	if err != nil {
		return false, fmt.Errorf("verify access request: %w", err)
	}
	return resp.Valid, nil
}
