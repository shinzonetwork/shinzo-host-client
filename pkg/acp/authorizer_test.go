package acp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testGRPCAddr  = "localhost:9090"
	testCometAddr = "tcp://localhost:26657"
	testPolicyID  = "policy-1"
)

// NewSourceHubAuthorizer must reject incomplete configuration up front; a
// missing grpc address, comet rpc address, or policy id all leave the
// SDK in a state where the first AuthorizeView call would silently fail
// far from its cause.
func TestNewSourceHubAuthorizer_RejectsMissingParams(t *testing.T) {
	tests := []struct {
		name      string
		grpc      string
		comet     string
		policy    string
		expectErr error
	}{
		{name: "missing grpc", grpc: "", comet: testCometAddr, policy: testPolicyID, expectErr: ErrGRPCAddrRequired},
		{name: "missing comet", grpc: testGRPCAddr, comet: "", policy: testPolicyID, expectErr: ErrCometAddrRequired},
		{name: "missing policy", grpc: testGRPCAddr, comet: testCometAddr, policy: "", expectErr: ErrPolicyIDRequired},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, err := NewSourceHubAuthorizer(tt.grpc, tt.comet, tt.policy)
			require.ErrorIs(t, err, tt.expectErr)
			require.Nil(t, a)
		})
	}
}

// The positive-path constructor (NewSourceHubAuthorizer with valid grpc +
// comet + policy) reaches out to CometBFT during NewClient, so it requires
// a live SourceHub to exercise. That path is covered end-to-end by the
// integration stages against a local or develop-devnet SourceHub; keeping
// it out of the unit suite avoids a flaky dependency on external services.
