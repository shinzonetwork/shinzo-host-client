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
	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/app-sdk/pkg/logger"
	"github.com/sourcenetwork/defradb/node"
)

type AttestationRecord attestation.AttestationRecord

func verifySignature(ctx context.Context, defraNode *node.Node, cid string, signature attestation.Signature) error {
	// Validate required fields
	if signature.Identity == "" {
		return fmt.Errorf("empty identity in signature for CID %s", cid)
	}
	if cid == "" {
		return fmt.Errorf("empty CID provided for signature verification")
	}
	if defraNode == nil || defraNode.APIURL == "" {
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
		return fmt.Errorf("Encountered unexpected signature type %s", signature.Type)
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
			return fmt.Errorf("Identity in expected format, expected base64 encoded hex-represented ASCII")
		}
	} else {
		return fmt.Errorf("Identity in unexpected format, expected base64 encoded")
	}

	baseURL, err := url.Parse(defraNode.APIURL)
	if err != nil {
		return fmt.Errorf("failed to parse defradb API URL %s: %w", defraNode.APIURL, err)
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
	logger.Sugar.Infof("Signature verification request for CID %s: %s", cid, curlCmd)

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

func CreateAttestationRecord(ctx context.Context, defraNode *node.Node, docId string, sourceDocId string, versions []attestation.Version) (*AttestationRecord, error) {
	attestationRecord := &AttestationRecord{
		AttestedDocId: docId,
		SourceDocId:   sourceDocId,
		CIDs:          []string{},
	}
	for _, version := range versions {
		// Validate the signature against the CID using DefraDB's API
		if err := verifySignature(ctx, defraNode, version.CID, version.Signature); err != nil {
			// Todo here we might want to send a message to ShinzoHub (or similar) indicating that we received an invalid signature
			logger.Sugar.Errorf("Invalid signature for CID %s from identity %s: %w", version.CID, version.Signature.Identity, err)
			continue
		}
		attestationRecord.CIDs = append(attestationRecord.CIDs, version.CID)
	}

	return attestationRecord, nil
}

func (record *AttestationRecord) PostAttestationRecord(ctx context.Context, defraNode *node.Node, viewName string) error {
	cidsArray := make([]string, len(record.CIDs))
	for i, cid := range record.CIDs {
		cidsArray[i] = fmt.Sprintf(`"%s"`, cid)
	}
	cidsString := fmt.Sprintf("[%s]", strings.Join(cidsArray, ", "))

	attestationCollectionName := fmt.Sprintf("AttestationRecord_%s", viewName)
	mutation := fmt.Sprintf(`
		mutation {
			create_%s(input: {
				attested_doc: "%s",
				source_doc: "%s",
				CIDs: %s
			}) {
				_docID
				attested_doc
				source_doc
				CIDs
			}
		}
	`, attestationCollectionName, record.AttestedDocId, record.SourceDocId, cidsString)

	_, err := defra.PostMutation[AttestationRecord](ctx, defraNode, mutation)
	if err != nil {
		return fmt.Errorf("error posting attestation record mutation: %v", err)
	}

	return nil
}
