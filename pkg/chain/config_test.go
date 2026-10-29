package chain

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidate(t *testing.T) {
	cases := []struct {
		desc    string
		chains  []Config
		wantErr error
	}{
		{"no chains", nil, nil},
		{"one chain", []Config{{Prefix: EthereumMainnet, ChainID: 1}}, nil},
		{"two chains", []Config{{Prefix: EthereumMainnet, ChainID: 1}, {Prefix: "Testchain__Devnet", ChainID: 2}}, nil},
		{"digits after the first letter", []Config{{Prefix: "Base2__Sepolia1", ChainID: 1}}, nil},
		{"empty prefix", []Config{{Prefix: ""}}, ErrInvalidPrefix},
		{"no network", []Config{{Prefix: "Ethereum"}}, ErrInvalidPrefix},
		{"single underscore", []Config{{Prefix: "Ethereum_Mainnet"}}, ErrInvalidPrefix},
		{"three parts", []Config{{Prefix: "Ethereum__Mainnet__Block"}}, ErrInvalidPrefix},
		{"part starts with a digit", []Config{{Prefix: "Ethereum__1Mainnet"}}, ErrInvalidPrefix},
		{"part starts with a lower-case letter", []Config{{Prefix: "Ethereum__mainnet"}}, ErrInvalidPrefix},
		{"underscore inside a part", []Config{{Prefix: "Arbitrum_One__Mainnet"}}, ErrInvalidPrefix},
		{"same prefix twice", []Config{{Prefix: EthereumMainnet, ChainID: 1}, {Prefix: EthereumMainnet, ChainID: 1}}, ErrDuplicatePrefix},
		{"prefixes differ only in case", []Config{{Prefix: EthereumMainnet, ChainID: 1}, {Prefix: "ETHEREUM__Mainnet", ChainID: 1}}, ErrDuplicatePrefix},
		{
			"http and https generators",
			[]Config{{Prefix: EthereumMainnet, ChainID: 1, Generators: []Generator{{URL: "http://10.0.0.5:8080"}, {URL: "https://gen.example.com"}}}},
			nil,
		},
		{"generator without scheme", []Config{{Prefix: EthereumMainnet, ChainID: 1, Generators: []Generator{{URL: "10.0.0.5:8080"}}}}, ErrInvalidGeneratorURL},
		{"generator without host", []Config{{Prefix: EthereumMainnet, ChainID: 1, Generators: []Generator{{URL: "http://"}}}}, ErrInvalidGeneratorURL},
		{"negative max_blocks", []Config{{Prefix: EthereumMainnet, MaxBlocks: -1}}, ErrInvalidMaxBlocks},
		{"no chain_id", []Config{{Prefix: EthereumMainnet}}, ErrMissingChainID},
		{"generator with another scheme", []Config{{Prefix: EthereumMainnet, ChainID: 1, Generators: []Generator{{URL: "ws://10.0.0.5:8080"}}}}, ErrInvalidGeneratorURL},
		{
			"generator peers in each accepted form",
			[]Config{{Prefix: EthereumMainnet, ChainID: 1, Generators: []Generator{
				{URL: "http://10.0.0.5:8080", Peer: "/ip4/10.0.0.5/tcp/9171"},
				{URL: "http://10.0.0.6:8080", Peer: "10.0.0.6:9171"},
				{URL: "http://10.0.0.7:8080", Peer: "10.0.0.7"},
				{URL: "http://10.0.0.8:8080", Peer: "  "},
			}}},
			nil,
		},
		{"generator peer with a host name", []Config{{Prefix: EthereumMainnet, ChainID: 1, Generators: []Generator{{URL: "http://10.0.0.5:8080", Peer: "gen.example.com:9171"}}}}, ErrInvalidGeneratorPeer},
		{"generator peer with a bad multiaddr", []Config{{Prefix: EthereumMainnet, ChainID: 1, Generators: []Generator{{URL: "http://10.0.0.5:8080", Peer: "/ip4/10.0.0.5/tcpp/9171"}}}}, ErrInvalidGeneratorPeer},
	}

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			require.ErrorIs(t, Validate(c.chains), c.wantErr)
		})
	}
}
