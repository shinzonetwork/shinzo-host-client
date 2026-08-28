package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetRegistrationData_UsesRegistrationPublicHostForLoopback(t *testing.T) {
	mock := &mockHealthChecker{
		defraReg: DefraPKRegistration{
			PublicKey:   testRegPublicKey,
			SignedPKMsg: testRegSignedPKMsg,
		},
		peerReg: PeerIDRegistration{PeerID: testRegPeerID, SignedPeerMsg: testRegSignedPeerID},
		peerInfo: &P2PInfo{
			Self: &PeerInfo{
				ID:        "12D3KooWTestPeer",
				Addresses: []string{"/ip4/127.0.0.1/tcp/9171", "/ip4/172.21.0.3/tcp/9171"},
			},
		},
	}
	hs := newHS(mock, "")
	hs.registrationPublicHost = "203.0.113.25:8080"

	req := httptest.NewRequest(http.MethodGet, "/registration", nil)
	req.Host = "127.0.0.1:8080"

	reg, err := hs.getRegistrationData(req)
	require.NoError(t, err)
	require.Equal(t, "http://203.0.113.25:8080/api/v0/graphql", reg.EndpointAddress)
	require.Equal(t, "/ip4/203.0.113.25/tcp/9171/p2p/12D3KooWTestPeer", reg.ConnectionString)
}

func TestGetRegistrationData_PrefersForwardedHostOverPublicHost(t *testing.T) {
	mock := &mockHealthChecker{
		defraReg: DefraPKRegistration{
			PublicKey:   testRegPublicKey,
			SignedPKMsg: testRegSignedPKMsg,
		},
		peerReg: PeerIDRegistration{PeerID: testRegPeerID, SignedPeerMsg: testRegSignedPeerID},
		peerInfo: &P2PInfo{
			Self: &PeerInfo{
				ID:        "12D3KooWTestPeer",
				Addresses: []string{"/ip4/172.21.0.3/tcp/9171"},
			},
		},
	}
	hs := newHS(mock, "")
	hs.registrationPublicHost = "203.0.113.25:8080"

	req := httptest.NewRequest(http.MethodGet, "/registration", nil)
	req.Host = "127.0.0.1:8080"
	req.Header.Set("X-Forwarded-Host", "host.example.com")
	req.Header.Set("X-Forwarded-Proto", "https")

	reg, err := hs.getRegistrationData(req)
	require.NoError(t, err)
	require.Equal(t, "https://host.example.com/api/v0/graphql", reg.EndpointAddress)
	require.Equal(t, "/ip4/203.0.113.25/tcp/9171/p2p/12D3KooWTestPeer", reg.ConnectionString)
}

func TestIsUnusableRegistrationHost(t *testing.T) {
	require.True(t, isUnusableRegistrationHost(""))
	require.True(t, isUnusableRegistrationHost("127.0.0.1:8080"))
	require.True(t, isUnusableRegistrationHost("localhost:8080"))
	require.True(t, isUnusableRegistrationHost("172.21.0.3:8080"))
	require.False(t, isUnusableRegistrationHost("203.0.113.25:8080"))
	require.False(t, isUnusableRegistrationHost("host.example.com"))
}
