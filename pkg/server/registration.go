package server

import (
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"

	sdkcrypto "github.com/TBD54566975/ssi-sdk/crypto"
	didkey "github.com/TBD54566975/ssi-sdk/did/key"
	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
)

const (
	registrationMessage = "Shinzo Network host registration"
	registrationAppHost = "registration-staging.shinzo.network"
)

// DisplayRegistration represents the registration status and signed messages for display in the health endpoint.
type DisplayRegistration struct {
	Enabled             bool                `json:"enabled"`
	Message             string              `json:"message"`
	DID                 string              `json:"did,omitempty"`
	ConnectionString    string              `json:"connection_string,omitempty"`
	EndpointAddress     string              `json:"endpoint_address,omitempty"`
	DefraPKRegistration DefraPKRegistration `json:"defra_pk_registration,omitzero"`
	PeerIDRegistration  PeerIDRegistration  `json:"peer_id_registration,omitzero"`
}

// DefraPKRegistration represents the registration information for the host's DefraDB public key, including the signed message.
type DefraPKRegistration struct {
	PublicKey   string `json:"public_key,omitempty"`
	SignedPKMsg string `json:"signed_pk_message,omitempty"`
}

// PeerIDRegistration represents the registration information for the host's P2P peer ID, including the signed message.
type PeerIDRegistration struct {
	PeerID        string `json:"peer_id,omitempty"`
	SignedPeerMsg string `json:"signed_peer_message,omitempty"`
}

// getRegistrationData returns the signed registration data for the host.
func (hs *HealthServer) getRegistrationData(r *http.Request) (*DisplayRegistration, error) {
	if hs.host == nil {
		return nil, ErrHostNotAvailable
	}

	defraReg, peerReg, signErr := hs.host.SignMessages(registrationMessage)
	registration := &DisplayRegistration{
		Enabled: signErr == nil,
		Message: normalizeHex(hex.EncodeToString([]byte(registrationMessage))),
	}
	if signErr != nil {
		return registration, signErr
	}

	registration.DefraPKRegistration = DefraPKRegistration{
		PublicKey:   normalizeHex(defraReg.PublicKey),
		SignedPKMsg: normalizeHex(defraReg.SignedPKMsg),
	}
	registration.PeerIDRegistration = PeerIDRegistration{
		PeerID:        normalizeHex(peerReg.PeerID),
		SignedPeerMsg: normalizeHex(peerReg.SignedPeerMsg),
	}
	if did, err := deriveDID(registration.DefraPKRegistration.PublicKey); err == nil {
		registration.DID = did
	} else {
		logger.Sugar.Debugf("failed to derive registration DID: %v", err)
	}
	registration.EndpointAddress = deriveEndpointAddress(r)
	if p2p, err := hs.host.GetPeerInfo(); err == nil {
		registration.ConnectionString = deriveConnectionString(r, p2p)
	}

	return registration, nil
}

// registrationAppHandler redirects to the registration app with registration data as query params.
func (hs *HealthServer) registrationAppHandler(w http.ResponseWriter, r *http.Request) {
	registration, err := hs.getRegistrationData(r)
	if err != nil || registration == nil || !registration.Enabled {
		http.Error(w, "Registration data not available", http.StatusServiceUnavailable)
		return
	}

	http.Redirect(w, r, buildRegistrationAppURL(registration), http.StatusTemporaryRedirect)
}

func buildRegistrationAppURL(registration *DisplayRegistration) string {
	redirectURL := url.URL{
		Scheme: "https",
		Host:   registrationAppHost,
		Path:   "/host-registration",
	}
	query := redirectURL.Query()
	query.Set("role", "host")
	query.Set("signedMessage", registration.Message)
	query.Set("defraPublicKey", registration.DefraPKRegistration.PublicKey)
	query.Set("defraPublicKeySignedMessage", registration.DefraPKRegistration.SignedPKMsg)
	query.Set("connectionString", registration.ConnectionString)
	query.Set("endpointAddress", registration.EndpointAddress)
	redirectURL.RawQuery = query.Encode()

	return redirectURL.String()
}

// normalizeHex ensures a string is represented as a 0x-prefixed hex string.
// If the string is empty, it is returned unchanged.
func normalizeHex(s string) string {
	if s == "" {
		return s
	}
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		return "0x" + s[2:]
	}
	return "0x" + s
}

func deriveDID(publicKeyHex string) (string, error) {
	publicKeyBytes, err := hex.DecodeString(strings.TrimPrefix(normalizeHex(publicKeyHex), "0x"))
	if err != nil {
		return "", fmt.Errorf("decode public key: %w", err)
	}
	didDoc, err := didkey.CreateDIDKey(sdkcrypto.SECP256k1, publicKeyBytes)
	if err != nil {
		return "", fmt.Errorf("create did key: %w", err)
	}
	return didDoc.String(), nil
}

func deriveEndpointAddress(r *http.Request) string {
	if r == nil {
		return ""
	}

	proto := firstForwardedValue(r.Header.Get("X-Forwarded-Proto"))
	if proto == "" {
		if r.TLS != nil {
			proto = "https"
		} else {
			proto = "http"
		}
	}

	host := firstForwardedValue(r.Header.Get("X-Forwarded-Host"))
	if host == "" {
		host = r.Host
	}
	if host == "" && r.URL != nil {
		host = r.URL.Host
	}
	if host == "" {
		return ""
	}

	return (&url.URL{
		Scheme: proto,
		Host:   host,
		Path:   "/api/v0/graphql",
	}).String()
}

func deriveConnectionString(r *http.Request, p2p *P2PInfo) string {
	if p2p == nil || p2p.Self == nil || p2p.Self.ID == "" {
		return ""
	}

	port := p2pPort(p2p.Self.Addresses)
	if ip := requestHostIP(r); ip != "" {
		return fmt.Sprintf("/ip4/%s/tcp/%s/p2p/%s", ip, port, p2p.Self.ID)
	}

	if addr := firstUsableP2PAddress(p2p.Self.Addresses); addr != "" {
		return fmt.Sprintf("%s/p2p/%s", addr, p2p.Self.ID)
	}
	if len(p2p.Self.Addresses) > 0 && p2p.Self.Addresses[0] != "" {
		return fmt.Sprintf("%s/p2p/%s", p2p.Self.Addresses[0], p2p.Self.ID)
	}
	return ""
}

func firstForwardedValue(value string) string {
	if value == "" {
		return ""
	}
	return strings.TrimSpace(strings.Split(value, ",")[0])
}

func requestHostIP(r *http.Request) string {
	if r == nil {
		return ""
	}
	hosts := []string{
		firstForwardedValue(r.Header.Get("X-Forwarded-Host")),
		r.Host,
	}
	for _, host := range hosts {
		if ip := hostIP(host); ip != "" {
			return ip
		}
	}
	return ""
}

func hostIP(host string) string {
	if splitHost, _, err := net.SplitHostPort(host); err == nil {
		host = splitHost
	}
	host = strings.Trim(host, "[]")
	ip := net.ParseIP(host)
	if ip == nil || ip.To4() == nil {
		return ""
	}
	return ip.String()
}

func p2pPort(addresses []string) string {
	for _, addr := range addresses {
		parts := strings.Split(addr, "/")
		for i := 0; i < len(parts)-1; i++ {
			if parts[i] == "tcp" && parts[i+1] != "" {
				return parts[i+1]
			}
		}
	}
	return "9171"
}

func firstUsableP2PAddress(addresses []string) string {
	for _, addr := range addresses {
		if isUsableIP4Multiaddr(addr) {
			return addr
		}
	}
	return ""
}

func isUsableIP4Multiaddr(addr string) bool {
	parts := strings.Split(addr, "/")
	for i := 0; i < len(parts)-1; i++ {
		if parts[i] != "ip4" {
			continue
		}
		ip := net.ParseIP(parts[i+1])
		return ip != nil && ip.To4() != nil && !ip.IsLoopback() && !ip.IsUnspecified()
	}
	return false
}
