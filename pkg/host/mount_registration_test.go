package host

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/sourcenetwork/defradb/client/options"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/pkg/hostserver"
	"github.com/shinzonetwork/shinzo-host-client/pkg/server"
	"github.com/shinzonetwork/shinzo-host-client/pkg/signer"
)

type registrationDB struct {
	node.DB
	addresses []string
}

func (d *registrationDB) PeerInfo(context.Context, ...options.Enumerable[options.PeerInfoOptions]) ([]string, error) {
	return d.addresses, nil
}

func (d *registrationDB) ActivePeers(context.Context, ...options.Enumerable[options.ActivePeersOptions]) ([]string, error) {
	return nil, nil
}

type registrationDefra struct {
	*fakeDefraService
	db node.DB
}

func (d *registrationDefra) DB() node.DB { return d.db }

func verifyRegistrationSignatures(t *testing.T, registration *server.DisplayRegistration, keys NodeKeys) {
	t.Helper()
	require.NotNil(t, registration)
	require.True(t, registration.Enabled)
	require.Equal(t, keys.DID(), registration.DID)
	message, err := hex.DecodeString(strings.TrimPrefix(registration.Message, "0x"))
	require.NoError(t, err)
	require.Equal(t, server.RegistrationMessage, string(message))
	defra := registration.DefraPKRegistration
	require.NoError(t, signer.VerifyDefraSignature(
		strings.TrimPrefix(defra.PublicKey, "0x"), string(message), strings.TrimPrefix(defra.SignedPKMsg, "0x"),
	))
	require.Error(t, signer.VerifyDefraSignature(
		strings.TrimPrefix(defra.PublicKey, "0x"), "changed message", strings.TrimPrefix(defra.SignedPKMsg, "0x"),
	))
	p2p := registration.PeerIDRegistration
	require.NoError(t, signer.VerifyP2PSignature(
		strings.TrimPrefix(p2p.PeerID, "0x"), string(message), strings.TrimPrefix(p2p.SignedPeerMsg, "0x"),
	))
	publicBytes, err := hex.DecodeString(strings.TrimPrefix(p2p.PeerID, "0x"))
	require.NoError(t, err)
	public, err := libp2pcrypto.UnmarshalEd25519PublicKey(publicBytes)
	require.NoError(t, err)
	publishedPeer, err := peer.IDFromPublicKey(public)
	require.NoError(t, err)
	activePeer, err := keys.PeerID()
	require.NoError(t, err)
	require.Equal(t, activePeer, publishedPeer)
}

func TestMountRegistration_TunnelResponseUsesActiveKeys(t *testing.T) {
	keys, err := deriveKeys(testMnemonic(t))
	require.NoError(t, err)
	peerID, err := keys.PeerID()
	require.NoError(t, err)
	fake := &registrationDefra{
		fakeDefraService: &fakeDefraService{},
		db: &registrationDB{addresses: []string{
			"/ip4/127.0.0.1/tcp/9171/p2p/" + peerID.String(),
			"/ip4/192.168.1.118/tcp/9171/p2p/" + peerID.String(),
		}},
	}
	fake.Metrics().UpdateMostRecentBlock(123)
	srv, err := hostserver.New(testHostConfig(), zap.NewNop())
	require.NoError(t, err)
	require.NoError(t, mountRegistration(srv, keys, fake))
	require.NoError(t, mountNodeInfo(srv, keys))

	req := httptest.NewRequest(http.MethodGet, "http://localhost:8080/registration", nil)
	req.Header.Set("X-Forwarded-Host", "test.trycloudflare.com")
	req.Header.Set("X-Forwarded-Proto", "https")
	rec := httptest.NewRecorder()
	srv.Mux().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	var response server.HealthResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	verifyRegistrationSignatures(t, response.Registration, keys)
	require.Equal(t, "ready", response.Status)
	require.EqualValues(t, 123, response.CurrentBlock)
	require.True(t, response.DefraDBConnected)
	require.Equal(t, peerID.String(), response.P2P.Self.ID)
	require.Equal(t, "https://test.trycloudflare.com/api/v0/graphql", response.Registration.EndpointAddress)
	require.NotContains(t, rec.Body.String(), "connection_string")

	nodeRec := httptest.NewRecorder()
	srv.Mux().ServeHTTP(nodeRec, httptest.NewRequest(http.MethodGet, "/api/node", nil))
	var info nodeInfo
	require.NoError(t, json.Unmarshal(nodeRec.Body.Bytes(), &info))
	require.Equal(t, info.DID, response.Registration.DID)
	require.Equal(t, info.PeerID, response.P2P.Self.ID)

	// Request-dependent addresses must not be cached from the first tunnel request.
	req = httptest.NewRequest(http.MethodGet, "http://65.21.94.184:8080/registration", nil)
	rec = httptest.NewRecorder()
	srv.Mux().ServeHTTP(rec, req)
	var direct server.HealthResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &direct))
	require.Equal(t, "http://65.21.94.184:8080/api/v0/graphql", direct.Registration.EndpointAddress)
	require.Equal(t, "/ip4/65.21.94.184/tcp/9171/p2p/"+peerID.String(), direct.Registration.ConnectionString)
}

func TestMountRegistration_UnreadyStillReturnsSignedDocument(t *testing.T) {
	keys, err := deriveKeys(testMnemonic(t))
	require.NoError(t, err)
	for _, test := range []struct {
		name string
		db   node.DB
		last time.Time
	}{
		{name: "offline"},
		{name: "stale", db: &registrationDB{}, last: time.Now().Add(-10 * time.Minute)},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := &registrationDefra{fakeDefraService: &fakeDefraService{}, db: test.db}
			fake.Metrics().LastDocumentTime = test.last
			srv, err := hostserver.New(testHostConfig(), zap.NewNop())
			require.NoError(t, err)
			require.NoError(t, mountRegistration(srv, keys, fake))
			rec := httptest.NewRecorder()
			srv.Mux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/registration", nil))
			require.Equal(t, http.StatusServiceUnavailable, rec.Code)
			require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
			var response server.HealthResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
			require.Equal(t, "not ready", response.Status)
			verifyRegistrationSignatures(t, response.Registration, keys)
		})
	}
}

func TestMountRegistration_RejectsOtherMethodsAndInvalidKeys(t *testing.T) {
	keys, err := deriveKeys(testMnemonic(t))
	require.NoError(t, err)
	srv, err := hostserver.New(testHostConfig(), zap.NewNop())
	require.NoError(t, err)
	require.ErrorIs(t, mountRegistration(srv, NodeKeys{}, &fakeDefraService{}), errRegistrationIdentity)
	invalidPeer := keys
	invalidPeer.PeerKeySeed = nil
	require.Error(t, mountRegistration(srv, invalidPeer, &fakeDefraService{}))
	require.NoError(t, mountRegistration(srv, keys, &fakeDefraService{}))
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		rec := httptest.NewRecorder()
		srv.Mux().ServeHTTP(rec, httptest.NewRequest(method, "/registration", nil))
		require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
		require.Equal(t, http.MethodGet, rec.Header().Get("Allow"))
	}
}
