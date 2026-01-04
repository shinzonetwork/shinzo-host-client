package attestation

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/sourcenetwork/defradb/node"
)

type SignatureVerifier interface {
	Verify(ctx context.Context, cid string, signature Signature) error
}

// In signatureVerifier.go - revert to original
type CachedSignatureVerifier struct {
	defraNode  *node.Node
	httpClient *http.Client
	cache      *sync.Map
}

func NewSignatureVerifier(defraNode *node.Node) *CachedSignatureVerifier {
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			MaxConnsPerHost:     50,
			IdleConnTimeout:     90 * time.Second,
			DialContext: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
		},
	}

	return &CachedSignatureVerifier{
		defraNode:  defraNode,
		httpClient: httpClient,
		cache:      &sync.Map{},
	}
}

func (v *CachedSignatureVerifier) Verify(ctx context.Context, cid string, signature Signature) error {
	// Remove metrics calls
	// Check cache first
	if cached, exists := v.cache.Load(cid); exists {
		if result, ok := cached.(error); ok {
			return result
		}
		return nil
	}

	// Validate inputs
	if signature.Identity == "" || cid == "" {
		return fmt.Errorf("empty identity or CID")
	}
	if v.defraNode == nil || v.defraNode.APIURL == "" {
		return fmt.Errorf("defradb node or API URL not available")
	}
	if strings.ToUpper(signature.Type) != "ES256K" {
		return fmt.Errorf("invalid signature type %s", signature.Type)
	}

	// Perform HTTP verification
	err := v.verifyHTTP(ctx, cid, signature)

	// Cache result
	v.cache.Store(cid, err)

	return err
}

func (v *CachedSignatureVerifier) verifyHTTP(ctx context.Context, cid string, signature Signature) error {
	baseURL, err := url.Parse(v.defraNode.APIURL)
	if err != nil {
		return fmt.Errorf("failed to parse API URL: %w", err)
	}

	apiURL := baseURL.JoinPath("/api/v0/block/verify-signature")
	params := url.Values{}
	params.Set("cid", cid)
	params.Set("public-key", signature.Identity)
	params.Set("type", "secp256k1")
	apiURL.RawQuery = params.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}

	resp, err := v.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return nil
	}

	bodyBytes, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("verification failed: HTTP %d, %s", resp.StatusCode, string(bodyBytes))
}

// Legacy for backward compatibility
type DefraSignatureVerifier struct {
	defraNode  *node.Node
	httpClient *http.Client
}

func NewDefraSignatureVerifier(defraNode *node.Node) *DefraSignatureVerifier {
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			MaxConnsPerHost:     50,
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

func (v *DefraSignatureVerifier) Verify(ctx context.Context, cid string, signature Signature) error {
	if signature.Identity == "" || cid == "" {
		return fmt.Errorf("empty identity or CID")
	}
	if v.defraNode == nil || v.defraNode.APIURL == "" {
		return fmt.Errorf("defradb node or API URL not available")
	}
	if strings.ToUpper(signature.Type) != "ES256K" {
		return fmt.Errorf("invalid signature type %s", signature.Type)
	}

	baseURL, err := url.Parse(v.defraNode.APIURL)
	if err != nil {
		return fmt.Errorf("failed to parse API URL: %w", err)
	}

	apiURL := baseURL.JoinPath("/api/v0/block/verify-signature")
	params := url.Values{}
	params.Set("cid", cid)
	params.Set("public-key", signature.Identity)
	params.Set("type", "secp256k1")
	apiURL.RawQuery = params.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}

	resp, err := v.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return nil
	}

	bodyBytes, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("verification failed: HTTP %d, %s", resp.StatusCode, string(bodyBytes))
}

type MockSignatureVerifier struct {
	verifyFunc func(ctx context.Context, cid string, signature Signature) error
}

func (m *MockSignatureVerifier) Verify(ctx context.Context, cid string, signature Signature) error {
	if m.verifyFunc != nil {
		return m.verifyFunc(ctx, cid, signature)
	}
	return nil
}
