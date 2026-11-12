package host

import (
	"testing"
	"time"

	"github.com/shinzonetwork/shinzo-evidence-codec/rule"
	"github.com/stretchr/testify/require"
)

// A verified block's agreeing signers are observed on its chain's attesters set, and a chain without
// a set, as when recording is off, observes nothing.
func TestVerifierHooksObserveSigners(t *testing.T) {
	const tokensChain, other = "Bsc__Mainnet", "Testchain__Devnet"
	cases := []struct {
		name      string
		attesters map[string]*observedAttesters
		want      []string
	}{
		{"recording on", map[string]*observedAttesters{tokensChain: newObservedAttesters(time.Minute), other: newObservedAttesters(time.Minute)}, []string{"02aa", "03bb"}},
		{"recording off", nil, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := &Host{attesters: tc.attesters}

			h.verifierHooks(tokensChain).Verified(rule.Block{Number: 100}, []string{"03bb", "02aa"})

			if tc.attesters == nil {
				return
			}
			require.Equal(t, tc.want, tc.attesters[tokensChain].Snapshot())
			require.Empty(t, tc.attesters[other].Snapshot())
		})
	}
}
