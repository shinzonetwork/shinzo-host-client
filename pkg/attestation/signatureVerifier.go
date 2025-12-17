package attestation

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/attestation"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
	"github.com/sourcenetwork/defradb/node"
)

type SignatureVerifier interface {
	Verify(ctx context.Context, cid string, signature attestation.Signature) error
}

type DefraSignatureVerifier struct { // Implements SignatureVerifier interface
	defraNode  *node.Node
	httpClient *http.Client
}

func NewDefraSignatureVerifier(defraNode *node.Node) *DefraSignatureVerifier {
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100, // Total idle connections
			MaxIdleConnsPerHost: 10,  // Idle connections per host
			MaxConnsPerHost:     50,  // Max connections per host
			IdleConnTimeout:     90 * time.Second,
			DialContext: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
		},
	}

	return &DefraSignatureVerifier{
		defraNode:  defraNode,
		httpClient: httpClient,
	}
}

// Verify verifies that the signature is valid for the given CID using DefraDB's HTTP API
func (v *DefraSignatureVerifier) Verify(ctx context.Context, cid string, signature attestation.Signature) error {
	// Validate required fields
	if signature.Identity == "" {
		return fmt.Errorf("empty identity in signature for CID %s", cid)
	}
	if cid == "" {
		return fmt.Errorf("empty CID provided for signature verification")
	}
	if v.defraNode == nil || v.defraNode.APIURL == "" {
		return fmt.Errorf("defradb node or API URL is not available for signature verification")
	}

	// Validate signature type - only ES256K is expected from DefraDB
	if strings.ToUpper(signature.Type) != "ES256K" {
		return fmt.Errorf("invalid signature type %s, expected ES256K", signature.Type)
	}
	keyType := "secp256k1"

	// Validate and use hex identity directly (DefraDB always provides hex format)
	if keyBytes, hexErr := hex.DecodeString(signature.Identity); hexErr != nil || len(keyBytes) == 0 {
		return fmt.Errorf("identity must be valid hex format, got: %s", signature.Identity)
	}
	publicKeyStr := signature.Identity

	baseURL, err := url.Parse(v.defraNode.APIURL)
	if err != nil {
		return fmt.Errorf("failed to parse defradb API URL %s: %w", v.defraNode.APIURL, err)
	}
	apiURL := baseURL.JoinPath("/api/v0/block/verify-signature")

	params := url.Values{}
	params.Set("cid", cid)
	params.Set("public-key", publicKeyStr)
	params.Set("type", keyType)
	apiURL.RawQuery = params.Encode()

	fullURL := apiURL.String()

	// Assemble and log the complete curl equivalent request
	curlCmd := fmt.Sprintf("curl -X GET '%s'", fullURL)
	if logger.Sugar != nil {
		logger.Sugar.Infof("Signature verification request for CID %s: %s", cid, curlCmd)
	}

	// Make HTTP GET request to the verify-signature endpoint
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create HTTP request for signature verification: %w", err)
	}

	resp, err := v.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute signature verification request for CID %s: %w", cid, err)
	}
	defer resp.Body.Close()

	// Check response status - body is expected to be nil, we are just looking for a 200 status
	if resp.StatusCode == http.StatusOK {
		return nil
	}

	// Read response body for error message
	bodyBytes, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("signature verification failed for CID %s: HTTP %d, Response: %s", cid, resp.StatusCode, string(bodyBytes))
}

type MockSignatureVerifier struct { // Implements SignatureVerifier interface
	verifyFunc func(ctx context.Context, cid string, signature attestation.Signature) error
}

func (m *MockSignatureVerifier) Verify(ctx context.Context, cid string, signature attestation.Signature) error {
	if m.verifyFunc != nil {
		return m.verifyFunc(ctx, cid, signature)
	}
	return nil
}
