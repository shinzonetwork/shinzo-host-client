package attestation

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/node"
)

// SignFunc signs a canonical payload and returns a hex-encoded signature. The host
// supplies one backed by its cached DefraDB identity so the write path can sign
// without reopening the keyring on every attestation.
type SignFunc func(payload string) (string, error)

// BlockAttestationSigningPayload returns the canonical bytes the host signs for a
// block attestation. A verifier (e.g. the accounting service) must reconstruct the
// identical string to check hostSignature. Signer identities are sorted so the
// payload does not depend on the order indexers were observed.
func BlockAttestationSigningPayload(r *constants.BlockAttestation) string {
	signers := append([]string(nil), r.SignerIdentities...)
	sort.Strings(signers)
	return fmt.Sprintf("%s|%d|%s|%s|%s",
		r.HostID, r.BlockNumber, r.BlockHash, r.MerkleRoot, strings.Join(signers, ","))
}

// PostBlockAttestation writes or updates the host's attestation for a block. It is
// a read-modify-write keyed by (hostId, blockNumber, merkleRoot): a second indexer attesting
// the same block+root is unioned into the existing record's signer set; a different
// merkleRoot for the same block lands on its own record (divergence). The host is the
// sole writer of its records, so the list fields are safe under last-writer-wins.
func PostBlockAttestation(ctx context.Context, defraNode *node.Node, record *constants.BlockAttestation, sign SignFunc) error {
	col, err := defraNode.DB.GetCollectionByName(ctx, constants.CollectionBlockAttestation)
	if err != nil {
		return fmt.Errorf("failed to get block attestation collection: %w", err)
	}

	existing, err := lookupExistingBlockAttestation(ctx, defraNode, col, record.HostID, record.BlockNumber, record.MerkleRoot)
	if err != nil {
		return fmt.Errorf("failed to lookup existing block attestation: %w", err)
	}

	if existing != nil {
		return updateBlockAttestation(ctx, col, existing, record, sign)
	}
	return createBlockAttestation(ctx, col, record, sign)
}

// createBlockAttestation writes a new record, setting voteCount to the distinct
// signer count and hostSignature over the canonical payload.
func createBlockAttestation(ctx context.Context, col client.Collection, record *constants.BlockAttestation, sign SignFunc) error {
	record.VoteCount = len(record.SignerIdentities)
	sig, err := sign(BlockAttestationSigningPayload(record))
	if err != nil {
		return fmt.Errorf("failed to sign block attestation: %w", err)
	}
	record.HostSignature = sig

	data := map[string]any{
		"hostId":           record.HostID,
		"blockNumber":      record.BlockNumber,
		"blockHash":        record.BlockHash,
		"merkleRoot":       record.MerkleRoot,
		"signerIdentities": toAnySlice(record.SignerIdentities),
		"cids":             toAnySlice(record.CIDs),
		"voteCount":        record.VoteCount,
		"hostSignature":    record.HostSignature,
		"shinzoHeight":     record.ShinzoHeight,
		"createdAt":        record.CreatedAt,
	}

	doc, err := client.NewDocFromMap(ctx, data, col.Version())
	if err != nil {
		return fmt.Errorf("failed to create block attestation document: %w", err)
	}
	return col.Save(ctx, doc)
}

// updateBlockAttestation unions the new signer and CIDs into an existing record,
// recomputes voteCount as the distinct signer count, and re-signs so hostSignature
// always matches the identities currently stored. mergeStringListField and
// extractDocIDFromResult are defined in attestationRecordService.go (same package).
func updateBlockAttestation(ctx context.Context, col client.Collection, doc *client.Document, record *constants.BlockAttestation, sign SignFunc) error {
	mergedSigners := mergeStringListField(doc, "signerIdentities", record.SignerIdentities)
	if err := doc.Set(ctx, "signerIdentities", toAnySlice(mergedSigners)); err != nil {
		return fmt.Errorf("failed to set signerIdentities: %w", err)
	}

	mergedCIDs := mergeStringListField(doc, "cids", record.CIDs)
	if err := doc.Set(ctx, "cids", toAnySlice(mergedCIDs)); err != nil {
		return fmt.Errorf("failed to set cids: %w", err)
	}

	if err := doc.Set(ctx, "voteCount", len(mergedSigners)); err != nil {
		return fmt.Errorf("failed to set voteCount: %w", err)
	}

	resigned := &constants.BlockAttestation{
		HostID:           record.HostID,
		BlockNumber:      record.BlockNumber,
		BlockHash:        record.BlockHash,
		MerkleRoot:       record.MerkleRoot,
		SignerIdentities: mergedSigners,
	}
	sig, err := sign(BlockAttestationSigningPayload(resigned))
	if err != nil {
		return fmt.Errorf("failed to sign block attestation: %w", err)
	}
	if err := doc.Set(ctx, "hostSignature", sig); err != nil {
		return fmt.Errorf("failed to set hostSignature: %w", err)
	}

	return col.Save(ctx, doc)
}

// lookupExistingBlockAttestation finds this host's record for a given block and
// merkleRoot. The filter is scoped by hostId so the read-modify-write only ever
// matches this host's own record, even if another host's record is present
// locally. Returns (nil, nil) when none exists.
func lookupExistingBlockAttestation(ctx context.Context, defraNode *node.Node, col client.Collection, hostID string, blockNumber int64, merkleRoot string) (*client.Document, error) {
	query := fmt.Sprintf(`query {
		%s(filter: {hostId: {_eq: "%s"}, blockNumber: {_eq: %d}, merkleRoot: {_eq: "%s"}}) {
			_docID
		}
	}`, constants.CollectionBlockAttestation, hostID, blockNumber, merkleRoot)

	result := defraNode.DB.ExecRequest(ctx, query)

	docIDStr := extractDocIDFromResult(result.GQL.Data, constants.CollectionBlockAttestation)
	if docIDStr == "" {
		return nil, nil //nolint:nilnil
	}

	docID, err := client.NewDocIDFromString(docIDStr)
	if err != nil {
		return nil, err
	}

	return col.Get(ctx, docID)
}

// toAnySlice converts a string slice to []any for a DefraDB document map.
func toAnySlice(ss []string) []any {
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}
