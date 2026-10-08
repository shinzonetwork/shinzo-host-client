package chain

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEVM(t *testing.T) {
	cases := []struct {
		prefix         string
		want           Collections
		wantSubscribed []string
	}{
		{
			// Generators write these exact names.
			prefix: EthereumMainnet,
			want: Collections{
				Block:             Collection{Name: "Ethereum__Mainnet__Block", HeightField: "number"},
				Transaction:       Collection{Name: "Ethereum__Mainnet__Transaction", HeightField: "blockNumber"},
				Log:               Collection{Name: "Ethereum__Mainnet__Log", HeightField: "blockNumber"},
				AccessListEntry:   Collection{Name: "Ethereum__Mainnet__AccessListEntry", HeightField: "blockNumber"},
				BlockSignature:    Collection{Name: "Ethereum__Mainnet__BlockSignature", HeightField: "blockNumber"},
				SnapshotSignature: Collection{Name: "Ethereum__Mainnet__SnapshotSignature", HeightField: "endBlock"},
				AttestationRecord: Collection{Name: "Ethereum__Mainnet__AttestationRecord", HeightField: "blockNumber"},
			},
			wantSubscribed: []string{
				"Ethereum__Mainnet__Block",
				"Ethereum__Mainnet__Transaction",
				"Ethereum__Mainnet__AccessListEntry",
				"Ethereum__Mainnet__Log",
				"Ethereum__Mainnet__BlockSignature",
			},
		},
		{
			prefix: "Testchain__Devnet",
			want: Collections{
				Block:             Collection{Name: "Testchain__Devnet__Block", HeightField: "number"},
				Transaction:       Collection{Name: "Testchain__Devnet__Transaction", HeightField: "blockNumber"},
				Log:               Collection{Name: "Testchain__Devnet__Log", HeightField: "blockNumber"},
				AccessListEntry:   Collection{Name: "Testchain__Devnet__AccessListEntry", HeightField: "blockNumber"},
				BlockSignature:    Collection{Name: "Testchain__Devnet__BlockSignature", HeightField: "blockNumber"},
				SnapshotSignature: Collection{Name: "Testchain__Devnet__SnapshotSignature", HeightField: "endBlock"},
				AttestationRecord: Collection{Name: "Testchain__Devnet__AttestationRecord", HeightField: "blockNumber"},
			},
			wantSubscribed: []string{
				"Testchain__Devnet__Block",
				"Testchain__Devnet__Transaction",
				"Testchain__Devnet__AccessListEntry",
				"Testchain__Devnet__Log",
				"Testchain__Devnet__BlockSignature",
			},
		},
	}

	for _, c := range cases {
		t.Run(c.prefix, func(t *testing.T) {
			got := EVM(c.prefix)
			require.Equal(t, c.want, got)
			require.Equal(t, c.wantSubscribed, got.Subscribed())
		})
	}
}
