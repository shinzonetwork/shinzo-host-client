package chain

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidate(t *testing.T) {
	signers := []string{"02aa"}
	const usd1 = "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d"
	cases := []struct {
		desc    string
		chains  []Config
		wantErr error
	}{
		{"no chains", nil, nil},
		{"one chain", []Config{{Prefix: EthereumMainnet, ChainID: 1, Signers: signers}}, nil},
		{"two chains", []Config{{Prefix: EthereumMainnet, ChainID: 1, Signers: signers}, {Prefix: "Testchain__Devnet", ChainID: 2, Signers: signers}}, nil},
		{"digits after the first letter", []Config{{Prefix: "Base2__Sepolia1", ChainID: 1, Signers: signers}}, nil},
		{"empty prefix", []Config{{Prefix: ""}}, ErrInvalidPrefix},
		{"no network", []Config{{Prefix: "Ethereum"}}, ErrInvalidPrefix},
		{"single underscore", []Config{{Prefix: "Ethereum_Mainnet"}}, ErrInvalidPrefix},
		{"three parts", []Config{{Prefix: "Ethereum__Mainnet__Block"}}, ErrInvalidPrefix},
		{"part starts with a digit", []Config{{Prefix: "Ethereum__1Mainnet"}}, ErrInvalidPrefix},
		{"part starts with a lower-case letter", []Config{{Prefix: "Ethereum__mainnet"}}, ErrInvalidPrefix},
		{"underscore inside a part", []Config{{Prefix: "Arbitrum_One__Mainnet"}}, ErrInvalidPrefix},
		{"same prefix twice", []Config{{Prefix: EthereumMainnet, ChainID: 1, Signers: signers}, {Prefix: EthereumMainnet, ChainID: 1, Signers: signers}}, ErrDuplicatePrefix},
		{"prefixes differ only in case", []Config{{Prefix: EthereumMainnet, ChainID: 1, Signers: signers}, {Prefix: "ETHEREUM__Mainnet", ChainID: 1, Signers: signers}}, ErrDuplicatePrefix},
		{
			"http and https generators",
			[]Config{{Prefix: EthereumMainnet, ChainID: 1, Signers: signers, Generators: []Generator{{URL: "http://10.0.0.5:8080"}, {URL: "https://gen.example.com"}}}},
			nil,
		},
		{"generator without scheme", []Config{{Prefix: EthereumMainnet, ChainID: 1, Signers: signers, Generators: []Generator{{URL: "10.0.0.5:8080"}}}}, ErrInvalidGeneratorURL},
		{"generator without host", []Config{{Prefix: EthereumMainnet, ChainID: 1, Signers: signers, Generators: []Generator{{URL: "http://"}}}}, ErrInvalidGeneratorURL},
		{"negative max_blocks", []Config{{Prefix: EthereumMainnet, MaxBlocks: -1}}, ErrInvalidMaxBlocks},
		{"no chain_id", []Config{{Prefix: EthereumMainnet}}, ErrMissingChainID},
		{"no signers", []Config{{Prefix: EthereumMainnet, ChainID: 1}}, ErrNoSigners},
		{"signer with a 0x prefix", []Config{{Prefix: EthereumMainnet, ChainID: 1, Signers: []string{"0x02aa"}}}, nil},
		{"signer not in hex", []Config{{Prefix: EthereumMainnet, ChainID: 1, Signers: []string{"02zz"}}}, ErrInvalidSigner},
		{"empty signer", []Config{{Prefix: EthereumMainnet, ChainID: 1, Signers: []string{"0x"}}}, ErrInvalidSigner},
		{"generator with another scheme", []Config{{Prefix: EthereumMainnet, ChainID: 1, Signers: signers, Generators: []Generator{{URL: "ws://10.0.0.5:8080"}}}}, ErrInvalidGeneratorURL},
		{
			"generator peers in each accepted form",
			[]Config{{Prefix: EthereumMainnet, ChainID: 1, Signers: signers, Generators: []Generator{
				{URL: "http://10.0.0.5:8080", Peer: "/ip4/10.0.0.5/tcp/9171"},
				{URL: "http://10.0.0.6:8080", Peer: "10.0.0.6:9171"},
				{URL: "http://10.0.0.7:8080", Peer: "10.0.0.7"},
				{URL: "http://10.0.0.8:8080", Peer: "  "},
			}}},
			nil,
		},
		{"generator peer with a host name", []Config{{Prefix: EthereumMainnet, ChainID: 1, Signers: signers, Generators: []Generator{{URL: "http://10.0.0.5:8080", Peer: "gen.example.com:9171"}}}}, ErrInvalidGeneratorPeer},
		{"generator peer with a bad multiaddr", []Config{{Prefix: EthereumMainnet, ChainID: 1, Signers: signers, Generators: []Generator{{URL: "http://10.0.0.5:8080", Peer: "/ip4/10.0.0.5/tcpp/9171"}}}}, ErrInvalidGeneratorPeer},
		{"tokens mode", []Config{{Prefix: EthereumMainnet, ChainID: 1, Signers: signers, Mode: ModeTokens, Contracts: []string{usd1, "8D0D000EE44948FC98C9B98A4FA4921476F08B0E"}}}, nil},
		{"unknown mode", []Config{{Prefix: EthereumMainnet, ChainID: 1, Signers: signers, Mode: "logs"}}, ErrInvalidMode},
		{"tokens mode without contracts", []Config{{Prefix: EthereumMainnet, ChainID: 1, Signers: signers, Mode: ModeTokens}}, ErrInvalidContracts},
		{"contracts in full mode", []Config{{Prefix: EthereumMainnet, ChainID: 1, Signers: signers, Mode: ModeFull, Contracts: []string{usd1}}}, ErrInvalidContracts},
		{"contract not 20 bytes", []Config{{Prefix: EthereumMainnet, ChainID: 1, Signers: signers, Mode: ModeTokens, Contracts: []string{"0x8d0d"}}}, ErrInvalidContract},
		{"contract not hex", []Config{{Prefix: EthereumMainnet, ChainID: 1, Signers: signers, Mode: ModeTokens, Contracts: []string{"0x8d0d000ee44948fc98c9b98a4fa4921476f08b0g"}}}, ErrInvalidContract},
		{"contract listed twice", []Config{{Prefix: EthereumMainnet, ChainID: 1, Signers: signers, Mode: ModeTokens, Contracts: []string{usd1, "8D0D000EE44948FC98C9B98A4FA4921476F08B0D"}}}, ErrInvalidContract},
	}

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			require.ErrorIs(t, Validate(c.chains), c.wantErr)
		})
	}
}

func TestConfigAllowsSigner(t *testing.T) {
	c := Config{Signers: []string{"0x02AB", "03cd"}}
	cases := []struct {
		identity string
		want     bool
	}{
		{"02ab", true},
		{"03cd", true},
		{"02AB", false},
		{"0x03cd", false},
		{"02ac", false},
		{"", false},
	}
	for _, tc := range cases {
		require.Equal(t, tc.want, c.AllowsSigner(tc.identity), tc.identity)
	}
}
