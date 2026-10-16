package chain

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParsePeer(t *testing.T) {
	cases := []struct {
		desc string
		addr string
		want string
	}{
		{
			"full multiaddr",
			"/ip4/35.239.160.177/tcp/9171/p2p/12D3KooWNgSiQsYTdRon2r7439zSockGQxqwNSGFrwmdqTknhN6r",
			"/ip4/35.239.160.177/tcp/9171/p2p/12D3KooWNgSiQsYTdRon2r7439zSockGQxqwNSGFrwmdqTknhN6r",
		},
		{"multiaddr without peer ID", "/ip4/35.239.160.177/tcp/9171", "/ip4/35.239.160.177/tcp/9171"},
		{"IPv6 multiaddr", "/ip6/::1/tcp/9171", "/ip6/::1/tcp/9171"},
		{"IPv4 with port", "35.239.160.177:9171", "/ip4/35.239.160.177/tcp/9171"},
		{"IPv4 with another port", "10.0.0.1:4001", "/ip4/10.0.0.1/tcp/4001"},
		{"loopback IPv4 with port", "127.0.0.1:9171", "/ip4/127.0.0.1/tcp/9171"},
		{"IPv4 only", "35.239.160.177", "/ip4/35.239.160.177/tcp/9171"},
		{"IPv6 with port", "[::1]:9171", "/ip6/::1/tcp/9171"},
		{"full IPv6 with port", "[2001:db8::1]:4001", "/ip6/2001:db8::1/tcp/4001"},
		{"IPv6 only", "::1", "/ip6/::1/tcp/9171"},
		{"bracketed IPv6", "[::1]", "/ip6/::1/tcp/9171"},
		{"not an address", "not-a-valid-address", ""},
		{"host name with port", "not-an-ip:9171", ""},
		{"unknown multiaddr protocol", "/invalid/format", ""},
	}

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			maddr, err := ParsePeer(c.addr)
			if c.want == "" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, c.want, maddr.String())
		})
	}
}
