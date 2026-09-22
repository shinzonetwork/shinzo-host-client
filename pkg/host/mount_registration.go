package host

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"

	"github.com/shinzonetwork/shinzo-host-client/pkg/hostserver"
	"github.com/shinzonetwork/shinzo-host-client/pkg/server"
)

var errRegistrationIdentity = errors.New("registration requires a full node identity")

const registrationReadyWindow = 5 * time.Minute

// mountRegistration signs with the keys already loaded into this host. No request
// reads the keyring, so changing files cannot change the advertised running identity.
func mountRegistration(srv *hostserver.Server, keys NodeKeys, defra DefraService) error {
	signed, err := signRegistration(keys)
	if err != nil {
		return err
	}
	startedAt := time.Now()
	srv.Mux().HandleFunc("/registration", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		resp := readHostHealth(r.Context(), defra, startedAt)
		registration := signed
		registration.EndpointAddress = server.DeriveEndpointAddress(r)
		registration.ConnectionString = server.DeriveConnectionString(r, resp.P2P)
		resp.Registration = &registration

		ready := resp.DefraDBConnected &&
			(resp.LastProcessed.IsZero() || time.Since(resp.LastProcessed) <= registrationReadyWindow)
		resp.Status = "ready"
		w.Header().Set("Content-Type", "application/json")
		if !ready {
			resp.Status = "not ready"
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	return nil
}

func signRegistration(keys NodeKeys) (server.DisplayRegistration, error) {
	if keys.IdentityKey == nil || keys.IdentityKey.PrivateKey() == nil || keys.IdentityKey.PublicKey() == nil {
		return server.DisplayRegistration{}, errRegistrationIdentity
	}
	message := []byte(server.RegistrationMessage)
	signature, err := keys.IdentityKey.PrivateKey().Sign(message)
	if err != nil {
		return server.DisplayRegistration{}, fmt.Errorf("signing registration identity: %w", err)
	}

	// This branch derives the peer seed separately from the node identity key.
	peerKey, _, err := libp2pcrypto.GenerateEd25519Key(bytes.NewReader(keys.PeerKeySeed))
	if err != nil {
		return server.DisplayRegistration{}, fmt.Errorf("loading registration peer key: %w", err)
	}
	peerSignature, err := peerKey.Sign(message)
	if err != nil {
		return server.DisplayRegistration{}, fmt.Errorf("signing registration peer: %w", err)
	}
	peerPublic, err := peerKey.GetPublic().Raw()
	if err != nil {
		return server.DisplayRegistration{}, fmt.Errorf("encoding registration peer key: %w", err)
	}

	return server.DisplayRegistration{
		Enabled: true,
		Message: "0x" + hex.EncodeToString(message),
		DID:     keys.DID(),
		DefraPKRegistration: server.DefraPKRegistration{
			PublicKey:   "0x" + hex.EncodeToString(keys.IdentityKey.PublicKey().Raw()),
			SignedPKMsg: "0x" + hex.EncodeToString(signature),
		},
		PeerIDRegistration: server.PeerIDRegistration{
			PeerID:        "0x" + hex.EncodeToString(peerPublic),
			SignedPeerMsg: "0x" + hex.EncodeToString(peerSignature),
		},
	}, nil
}
