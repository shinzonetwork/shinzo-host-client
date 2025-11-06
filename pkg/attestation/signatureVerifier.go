package attestation

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/shinzonetwork/app-sdk/pkg/attestation"
	"github.com/shinzonetwork/app-sdk/pkg/logger"
	"github.com/sourcenetwork/defradb/node"
)

type SignatureVerifier interface {
	Verify(ctx context.Context, cid string, signature attestation.Signature) error
}

type DefraSignatureVerifier struct { // Implements SignatureVerifier interface
	defraNode *node.Node
}

func NewDefraSignatureVerifier(defraNode *node.Node) *DefraSignatureVerifier {
	return &DefraSignatureVerifier{defraNode: defraNode}
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

	// Determine key type from signature type
	keyType := "secp256k1"
	sigType := strings.ToLower(signature.Type)
	switch sigType {
	case "", "ed25519", "eddsa":
		keyType = "ed25519"
	case "es256k", "ecdsa-secp256k1", "secp256k1":
		keyType = "secp256k1"
	default:
		return fmt.Errorf("encountered unexpected signature type %s", signature.Type)
	}

	var publicKeyStr string

	base64Decoded, base64Err := base64.StdEncoding.DecodeString(signature.Identity)
	if base64Err != nil {
		base64Decoded, base64Err = base64.URLEncoding.DecodeString(signature.Identity)
	}
	if base64Err == nil && len(base64Decoded) > 0 {
		decodedStr := string(base64Decoded)

		if keyBytes, keyErr := hex.DecodeString(decodedStr); keyErr == nil && len(keyBytes) > 0 {
			publicKeyStr = hex.EncodeToString(keyBytes)
		} else {
			return fmt.Errorf("identity in expected format, expected base64 encoded hex-represented ASCII")
		}
	} else {
		return fmt.Errorf("identity in unexpected format, expected base64 encoded")
	}

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

	client := &http.Client{}
	resp, err := client.Do(req)
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
