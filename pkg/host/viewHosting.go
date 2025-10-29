package host

import (
	"context"
	"fmt"
	"strings"

	"github.com/shinzonetwork/app-sdk/pkg/attestation"
	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/app-sdk/pkg/logger"
	hostAttestation "github.com/shinzonetwork/host/pkg/attestation"
	"github.com/shinzonetwork/host/pkg/graphql"
	"github.com/shinzonetwork/host/pkg/view"
)

// extractVersionFromDocument extracts the _version field from a document map and converts it to []attestation.Version
func extractVersionFromDocument(doc map[string]any) ([]attestation.Version, error) {
	versionData, exists := doc["_version"]
	if !exists {
		return nil, fmt.Errorf("_version field not found in document")
	}

	versionArray, ok := versionData.([]any)
	if !ok {
		return nil, fmt.Errorf("_version field is not an array, got type: %T", versionData)
	}

	versions := make([]attestation.Version, 0, len(versionArray))
	for i, v := range versionArray {
		versionMap, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("_version[%d] is not a map, got type: %T", i, v)
		}

		version := attestation.Version{}

		// Extract CID
		if cid, ok := versionMap["cid"].(string); ok {
			version.CID = cid
		}

		// Extract signature
		sigMap, ok := versionMap["signature"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("_version[%d].signature is not a map, got type: %T", i, versionMap["signature"])
		}

		signature := attestation.Signature{}
		if sigType, ok := sigMap["type"].(string); ok {
			signature.Type = sigType
		}
		if identity, ok := sigMap["identity"].(string); ok {
			signature.Identity = identity
		}
		if value, ok := sigMap["value"].(string); ok {
			signature.Value = value
		}
		if typename, ok := sigMap["__typename"].(string); ok {
			signature.Typename = typename
		}

		version.Signature = signature
		versions = append(versions, version)
	}

	return versions, nil
}

func (h *Host) PrepareView(ctx context.Context, v view.View) error {
	err := v.SubscribeTo(ctx, h.DefraNode)
	if err != nil {
		if strings.Contains(err.Error(), "collection already exists") {
			logger.Sugar.Warnf("Error subscribing to view %+v: %w", v, err)
		} else {
			return fmt.Errorf("Error subscribing to view %+v: %w", v, err)
		}
	}

	err = attestation.AddAttestationRecordCollection(ctx, h.DefraNode, v.Name)
	if err != nil {
		if strings.Contains(err.Error(), "collection already exists") {
			logger.Sugar.Warnf("Error subscribing to view %+v: %w", v, err)
		} else {
			return fmt.Errorf("Error subscribing to view %+v: %w", v, err)
		}
	}

	if v.HasLenses() {
		err = v.PostWasmToFile(ctx, h.LensRegistryPath)
		if err != nil {
			return fmt.Errorf("Error downloading lenses to local env: %w", err)
		}

		err = v.ConfigureLens(ctx, h.DefraNode)
		if err != nil {
			return fmt.Errorf("Error configuring lenses: %w", err)
		}
	}

	return nil
}

func (h *Host) ApplyView(ctx context.Context, v view.View, startingBlockNumber uint64, endingBlockNumber uint64) error {
	query, err := graphql.WithBlockNumberFilter(*v.Query, startingBlockNumber, endingBlockNumber)
	if err != nil {
		return fmt.Errorf("Error assembling query: %w", err)
	}

	query, err = graphql.WithReturnDocIdAndVersion(query)
	if err != nil {
		return fmt.Errorf("Error assembling query: %w", err)
	}

	sourceDocuments, err := defra.QueryArray[map[string]any](ctx, h.DefraNode, query)
	if err != nil {
		return fmt.Errorf("Error fetching source data with query %s: %w", query, err)
	}
	if len(sourceDocuments) == 0 {
		return fmt.Errorf("No source data found using query %s", query)
	}

	type attestationInfo struct {
		SourceDocumentId string
		Version          []attestation.Version
	}

	transformedDocuments := map[*attestationInfo][]map[string]any{} // mapping source doc attestation info to transformed docs ([]map[string]any)
	for _, sourceDocument := range sourceDocuments {
		sourceDocumentId, ok := sourceDocument["_docID"].(string)
		if !ok {
			return fmt.Errorf("Error retrieving _docID from source document: %+v", sourceDocument)
		}
		sourceVersion, err := extractVersionFromDocument(sourceDocument)
		if err != nil {
			return fmt.Errorf("Error retrieving _version from source document: %w", err)
		}
		sourceAttestationInfo := &attestationInfo{
			SourceDocumentId: sourceDocumentId,
			Version:          sourceVersion,
		}

		if v.HasLenses() {
			transformed, err := v.ApplyLensTransform(ctx, h.DefraNode, []map[string]any{sourceDocument})
			if err != nil {
				return fmt.Errorf("Error applying lens transforms from view %s: %w", v.Name, err)
			}
			if len(transformed) == 0 {
				continue
			}
			transformedDocuments[sourceAttestationInfo] = transformed
		} else {
			transformedDocuments[sourceAttestationInfo] = []map[string]any{sourceDocument}
		}
	}

	for sourceDocumentAttestationInfo, transformedDocs := range transformedDocuments {
		transformedDocIds, err := v.WriteTransformedToCollection(ctx, h.DefraNode, transformedDocs)
		if err != nil {
			return fmt.Errorf("Error writing transformed data to collection %s: %w", v.Name, err)
		}

		for _, transformedDocId := range transformedDocIds {
			attestationRecord, err := hostAttestation.CreateAttestationRecord(transformedDocId, sourceDocumentAttestationInfo.SourceDocumentId, sourceDocumentAttestationInfo.Version)
			if err != nil {
				return fmt.Errorf("Error creating attestation record: %w", err)
			}

			err = attestationRecord.PostAttestationRecord(ctx, h.DefraNode, v.Name)
			if err != nil {
				return fmt.Errorf("Error posting attestation record %+v: %w", attestationRecord, err)
			}
		}
	}

	return nil
}
