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
				Prefix:            "Ethereum__Mainnet",
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
				Prefix:            "Testchain__Devnet",
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

// A tokens-mode chain's contract tables are named by address in lower-case hex without 0x, as
// generators name them, and listed in address order.
func TestTokens(t *testing.T) {
	c := Config{
		Prefix:    "Bsc__Mainnet",
		Mode:      ModeTokens,
		Contracts: []string{"0x8D0D000EE44948FC98C9B98A4FA4921476F08B0D", "55d398326f99059ff775485246999027b3197955"},
	}
	header := Collection{Name: "Bsc__Mainnet__Header", HeightField: "number"}
	bscUSD := Collection{Name: "Bsc__Mainnet__ContractLog_55d398326f99059ff775485246999027b3197955", HeightField: "blockNumber"}
	usd1 := Collection{Name: "Bsc__Mainnet__ContractLog_8d0d000ee44948fc98c9b98a4fa4921476f08b0d", HeightField: "blockNumber"}
	signature := Collection{Name: "Bsc__Mainnet__BlockSignature", HeightField: "blockNumber"}

	got := c.Collections()
	require.Equal(t, Collections{
		Prefix:            "Bsc__Mainnet",
		BlockSignature:    signature,
		AttestationRecord: Collection{Name: "Bsc__Mainnet__AttestationRecord", HeightField: "blockNumber"},
		Header:            header,
		ContractLogs:      []Collection{bscUSD, usd1},
	}, got)
	require.Equal(t, []Collection{header, bscUSD, usd1, signature}, got.Generated())
	require.Equal(t, []string{header.Name, bscUSD.Name, usd1.Name, signature.Name}, got.Subscribed())

	require.Equal(t, EVM("Bsc__Mainnet"), Config{Prefix: "Bsc__Mainnet"}.Collections())
}
