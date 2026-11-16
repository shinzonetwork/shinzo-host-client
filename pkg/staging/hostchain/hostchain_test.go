package hostchain

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidate(t *testing.T) {
	cases := []struct {
		desc string
		ids  []string
		err  error
	}{
		{"none", nil, nil},
		{"one", []string{"Ethereum__Mainnet"}, nil},
		{"several", []string{"Ethereum__Mainnet", "Base__Mainnet", "Ethereum__Sepolia"}, nil},
		{"digits after first letter", []string{"Arbitrum1__Mainnet2"}, nil},
		{"lower case", []string{"ethereum__mainnet"}, ErrInvalidID},
		{"single underscore", []string{"Ethereum_Mainnet"}, ErrInvalidID},
		{"no network", []string{"Ethereum"}, ErrInvalidID},
		{"three parts", []string{"Ethereum__Mainnet__Block"}, ErrInvalidID},
		{"leading digit", []string{"1Ethereum__Mainnet"}, ErrInvalidID},
		{"space", []string{" Ethereum__Mainnet"}, ErrInvalidID},
		{"empty", []string{""}, ErrInvalidID},
		{"differ only in case", []string{"Ethereum__Mainnet", "ETHEREUM__MAINNET"}, ErrDuplicateID},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			chains := map[string]Config{}
			for _, id := range tc.ids {
				chains[id] = Config{}
			}

			err := Validate(chains)
			if tc.err == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tc.err)
		})
	}
}

// Map order is random, so the pair must still be named the same way.
func TestValidateNamesDuplicatePairDeterministically(t *testing.T) {
	chains := map[string]Config{"Ethereum__Mainnet": {}, "ETHEREUM__MAINNET": {}}

	for range 20 {
		require.EqualError(t, Validate(chains),
			`chains "ETHEREUM__MAINNET" and "Ethereum__Mainnet": network IDs must differ ignoring case`)
	}
}

func TestRequire(t *testing.T) {
	require.ErrorIs(t, Require(nil), ErrNoChains)
	require.ErrorIs(t, Require(map[string]Config{}), ErrNoChains)
	require.NoError(t, Require(map[string]Config{"Ethereum__Mainnet": {}}))
}

func TestValidateMaxBlocks(t *testing.T) {
	cases := []struct {
		desc      string
		maxBlocks int64
		err       error
	}{
		{"unset uses the pruner default", 0, nil},
		{"positive", 100_000, nil},
		{"negative", -1, ErrInvalidMaxBlocks},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			err := Validate(map[string]Config{"Ethereum__Mainnet": {MaxBlocks: tc.maxBlocks}})
			if tc.err == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tc.err)
		})
	}
}

func TestValidateGeneratorURL(t *testing.T) {
	cases := []struct {
		desc string
		url  string
		err  error
	}{
		{"http", "http://10.0.0.5:8080", nil},
		{"https", "https://gen.example.com", nil},
		{"with path", "https://gen.example.com/api", nil},
		{"empty", "", ErrInvalidGeneratorURL},
		{"no scheme", "10.0.0.5:8080", ErrInvalidGeneratorURL},
		{"other scheme", "ftp://gen.example.com", ErrInvalidGeneratorURL},
		{"no host", "http://", ErrInvalidGeneratorURL},
		{"port only", "http://:8080", ErrInvalidGeneratorURL},
		{"unparsable", "http://%zz", ErrInvalidGeneratorURL},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			err := Validate(map[string]Config{"Ethereum__Mainnet": {Generators: []Generator{{URL: tc.url}}}})
			if tc.err == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tc.err)
		})
	}
}

// Every generator is checked, not just the first.
func TestValidateChecksEveryGenerator(t *testing.T) {
	err := Validate(map[string]Config{"Ethereum__Mainnet": {Generators: []Generator{
		{URL: "http://10.0.0.5:8080"},
		{URL: "not a url"},
	}}})
	require.ErrorIs(t, err, ErrInvalidGeneratorURL)
	require.ErrorContains(t, err, `chain "Ethereum__Mainnet" generator "not a url"`)
}
