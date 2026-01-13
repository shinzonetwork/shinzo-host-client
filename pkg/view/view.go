package view

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
	"github.com/shinzonetwork/view-creator/core/models"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/node"
	"github.com/sourcenetwork/immutable"
	"github.com/sourcenetwork/lens/host-go/config/model"
)

// Type aliases for view-creator models
type Transform = models.Transform
type Metadata = models.Metadata
type Lens = models.Lens

type View struct {
	Name         string    `json:"name"`
	Query        *string   `json:"query"`
	Sdl          *string   `json:"sdl"`
	Materialized bool      `json:"materialized"`
	Transform    Transform `json:"transform"`
	Metadata     Metadata  `json:"metadata"`
}

func (view *View) SubscribeTo(ctx context.Context, defraNode *node.Node) error {
	// Check if collection exists before subscribing
	_, err := defraNode.DB.GetCollectionByName(ctx, view.Name)
	if err != nil {
		return fmt.Errorf("cannot subscribe to view %s: collection does not exist", view.Name)
	}

	err = defraNode.DB.AddP2PCollections(ctx, view.Name)
	if err != nil {
		return fmt.Errorf("error subscribing to collection %s: %v", view.Name, err)
	}

	return nil
}

// parseSDLFields extracts field names from a GraphQL SDL schema
func parseSDLFields(sdl string) (map[string]bool, error) {
	if sdl == "" {
		return nil, fmt.Errorf("SDL is empty")
	}

	// Find the type definition for the collection
	// Look for pattern: type CollectionName { ... }
	re := regexp.MustCompile(`type\s+\w+\s*\{([^}]+)\}`)
	matches := re.FindStringSubmatch(sdl)
	if len(matches) < 2 {
		return nil, fmt.Errorf("no type definition found in SDL")
	}

	fields := make(map[string]bool)
	fieldSection := matches[1]

	// Extract field names from the type definition
	// Look for pattern: fieldName: Type
	fieldRe := regexp.MustCompile(`(\w+)\s*:`)
	fieldMatches := fieldRe.FindAllStringSubmatch(fieldSection, -1)

	for _, match := range fieldMatches {
		if len(match) > 1 {
			fieldName := strings.TrimSpace(match[1])
			// Skip GraphQL built-in fields like __typename
			if !strings.HasPrefix(fieldName, "__") {
				fields[fieldName] = true
			}
		}
	}

	return fields, nil
}

// getUniqueViewName generates a unique view name by appending "_view" to the base view name
// This ensures that views don't conflict with collections that have the same name
// However, DefraDB creates collections with just the view name, so we return the base name
func (v *View) getUniqueViewName() string {
	return v.Name // DefraDB creates collections with the view name directly
}

// filterDocumentFields filters a document to only include fields defined in the SDL schema
func (v *View) filterDocumentFields(document map[string]any) (map[string]any, error) {
	if v.Sdl == nil || *v.Sdl == "" {
		// If no SDL is provided, return the document as-is
		return document, nil
	}

	schemaFields, err := parseSDLFields(*v.Sdl)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SDL schema: %w", err)
	}

	// If no fields were found in the schema, return the document as-is
	if len(schemaFields) == 0 {
		return document, nil
	}

	// Filter the document to only include fields defined in the schema
	filteredDocument := make(map[string]any)
	// Create a case-insensitive map of schema fields for matching
	schemaFieldsLower := make(map[string]string)
	for field := range schemaFields {
		schemaFieldsLower[strings.ToLower(field)] = field
	}

	// Filter the document, matching keys case-insensitively
	for key, value := range document {
		if originalKey, ok := schemaFieldsLower[strings.ToLower(key)]; ok {
			filteredDocument[originalKey] = value
		}
	}

	return filteredDocument, nil
}

// Modifies the view such that the path, which is originally parsed as the actual wasm, is replaced with a path to the wasm lens (in a newly created file)
func (v *View) PostWasmToFile(ctx context.Context, lensRegistryPath string) error {
	if len(v.Transform.Lenses) < 1 {
		return fmt.Errorf("no lenses provided in view %+v", v)
	}

	// Find or create the lens registry directory
	registryDir, err := v.findOrCreateLensRegistryDir(lensRegistryPath)
	if err != nil {
		return fmt.Errorf("failed to find or create lens registry directory: %w", err)
	}

	for i, lense := range v.Transform.Lenses {
		// Decode the base64-encoded WASM data
		wasmDecoded, err := base64.StdEncoding.DecodeString(lense.Path)
		if err != nil {
			return fmt.Errorf("failed to decode base64 WASM data for lens %s: %w", lense.Label, err)
		}

		// Create the WASM file path using the lens label
		wasmFileName := fmt.Sprintf("%s.wasm", lense.Label)
		wasmFilePath := filepath.Join(registryDir, wasmFileName)

		// Write the decoded WASM data to the file
		err = os.WriteFile(wasmFilePath, wasmDecoded, 0644)
		if err != nil {
			return fmt.Errorf("failed to write WASM file %s: %w", wasmFilePath, err)
		}

		// Get the absolute path and format it as a file URL for DefraDB
		absPath, err := filepath.Abs(wasmFilePath)
		if err != nil {
			return fmt.Errorf("failed to get absolute path for %s: %w", wasmFilePath, err)
		}

		// Replace the lens path with the absolute file URL path
		v.Transform.Lenses[i].Path = "file://" + absPath
	}

	return nil
}

// findOrCreateLensRegistryDir finds the lens registry directory or creates it if it doesn't exist
func (v *View) findOrCreateLensRegistryDir(registryPath string) (string, error) {
	// Try to find the directory using the same pattern as file.FindFile
	possiblePaths := []string{
		registryPath,                          // From project root
		fmt.Sprintf("../%s", registryPath),    // From bin/ directory
		fmt.Sprintf("../../%s", registryPath), // From pkg/*/ directory - test context
	}

	// First, try to find an existing directory
	for _, path := range possiblePaths {
		if stat, err := os.Stat(path); err == nil && stat.IsDir() {
			return path, nil
		}
	}

	// If no existing directory found, create it at the first possible path
	dirPath := possiblePaths[0]
	err := os.MkdirAll(dirPath, 0755)
	if err != nil {
		return "", fmt.Errorf("failed to create lens registry directory at %s: %w", dirPath, err)
	}

	return dirPath, nil
}

// ConfigureLens creates or updates the view with the lens transform using AddView.
// According to the DefraDB guide, views are created with db.AddView providing:
// - A GQL query string (source query)
// - A GQL SDL (output structure, should have @materialized(if: false) for non-materialized)
// - Optionally, a lens configuration (transform)
func (v *View) ConfigureLens(ctx context.Context, defraNode *node.Node, schemaService *SchemaService) error {
	if v.Query == nil || *v.Query == "" {
		return fmt.Errorf("view query is required")
	}
	if v.Sdl == nil || *v.Sdl == "" {
		return fmt.Errorf("view SDL is required")
	}

	// Transform query to use proper collection names (e.g., "Log" -> "Ethereum__Mainnet__Log")
	transformedQuery := transformQueryCollectionNames(*v.Query)
	logger.Sugar.Debugf("🔧 View %s: transformed query: %s", v.Name, transformedQuery)

	// Normalize SDL using service
	sdl := schemaService.NormalizeSDL(*v.Sdl, v.getUniqueViewName(), SDLOptions{
		Materialized:   false,
		RequiredFields: []FieldDef{{Name: "blockNumber", Type: "Int"}},
	})
	logger.Sugar.Debugf("🔧 View %s: normalized SDL: %s", v.Name, sdl)

	// Build lens modules
	lens := v.buildLensModules()
	logger.Sugar.Debugf("🔧 View %s: lens modules: %+v", v.Name, lens)

	// Register lens and create view
	lensCID, err := defraNode.DB.AddLens(ctx, lens)
	if err != nil {
		return fmt.Errorf("failed to register lens: %w", err)
	}
	logger.Sugar.Debugf("🔧 View %s: lens CID: %s", v.Name, lensCID)

	_, err = defraNode.DB.AddView(ctx, transformedQuery, sdl, immutable.Some(lensCID))
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return fmt.Errorf("failed to create view: %w", err)
	}
	if err != nil {
		logger.Sugar.Debugf("🔧 View %s: view already exists", v.Name)
	} else {
		logger.Sugar.Infof("🔧 View %s: created successfully", v.Name)
	}

	return nil
}

// transformQueryCollectionNames transforms short collection names to full names
// e.g., "Log { ... }" -> "Ethereum__Mainnet__Log { ... }"
func transformQueryCollectionNames(query string) string {
	// Map of short names to full collection names
	collectionMap := map[string]string{
		"Log":             "Ethereum__Mainnet__Log",
		"Block":           "Ethereum__Mainnet__Block",
		"Transaction":     "Ethereum__Mainnet__Transaction",
		"AccessListEntry": "Ethereum__Mainnet__AccessListEntry",
	}

	if strings.Contains(query, "Ethereum__Mainnet__") {
		return query
	}

	result := query
	for shortName, fullName := range collectionMap {
		// Match the short name at the start of the query or after whitespace
		// but not if it's already prefixed with Ethereum__Mainnet__
		pattern := regexp.MustCompile(`(?:^|\s)(` + shortName + `)(\s*\{)`)
		result = pattern.ReplaceAllString(result, " "+fullName+"$2")
	}

	return strings.TrimSpace(result)
}

// Add to view.go
func (v *View) buildLensModules() model.Lens {
	lenses := make([]model.LensModule, 0, len(v.Transform.Lenses))
	for _, lense := range v.Transform.Lenses {
		lenses = append(lenses, model.LensModule{
			Path:      lense.Path,
			Inverse:   false,
			Arguments: lense.Arguments,
		})
	}
	return model.Lens{
		Lenses: lenses,
	}
}

// ApplyLensTransform queries the non-materialized view collection to get transformed results.
// According to the DefraDB guide, non-materialized views automatically apply lens transformations
// when queried. We simply query the view with the same filters as the source query.
//
// The sourceQuery parameter is the filtered source query (e.g., "Log(filter: {...}) { ... }").
// We replace the source collection name with the view name and query it directly.
func (v *View) ApplyLensTransform(ctx context.Context, defraNode *node.Node, sourceQuery string) ([]map[string]any, error) {
	// Get fields from SDL to build the selection set for the view query
	var fields string
	if v.Sdl != nil && *v.Sdl != "" {
		schemaFields, err := parseSDLFields(*v.Sdl)
		if err == nil && len(schemaFields) > 0 {
			fieldList := make([]string, 0, len(schemaFields))
			for field := range schemaFields {
				fieldList = append(fieldList, field)
			}
			fields = strings.Join(fieldList, " ")
		}
	}

	// If we couldn't get fields from SDL, throw an error as requested
	if fields == "" {
		return nil, fmt.Errorf("unable to extract fields from SDL - no fields available for query")
	}

	// Extract the filter part from the source query and apply it to the view query
	// Pattern: CollectionName(filter: {...}) { ... } -> ViewName(filter: {...}) { fields }
	// Use unique view name to match the one created in ConfigureLens
	uniqueViewName := v.getUniqueViewName()
	re := regexp.MustCompile(`^\w+\s*(\([^)]*\))?\s*\{`)
	matches := re.FindStringSubmatch(sourceQuery)

	var viewQuery string
	if len(matches) > 1 && matches[1] != "" {
		// Source query has filters, apply them to view query
		filterPart := matches[1]
		viewQuery = fmt.Sprintf("%s%s { %s }", uniqueViewName, filterPart, fields)
	} else {
		return nil, fmt.Errorf("Must have at least one query param")
	}

	transformedDocs, err := defra.QueryArray[map[string]any](ctx, defraNode, viewQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query view collection %s: %w", uniqueViewName, err)
	}

	return transformedDocs, nil
}

func (v *View) WriteTransformedToCollection(ctx context.Context, defraNode *node.Node, transformedDocument []map[string]any) ([]string, error) {
	// Use unique view name to match the one created in ConfigureLens
	uniqueViewName := v.Name
	collection, err := defraNode.DB.GetCollectionByName(ctx, uniqueViewName)
	if err != nil {
		return nil, fmt.Errorf("error getting collection %s: %v", uniqueViewName, err)
	}

	createdDocumentIds := []string{}

	// Use Save instead of CreateMany to handle duplicate document IDs gracefully
	for _, documentAsMap := range transformedDocument {
		// Filter the document to only include fields defined in the SDL schema
		filteredDocument, err := v.filterDocumentFields(documentAsMap)
		if err != nil {
			return nil, fmt.Errorf("failed to filter document fields: %w", err)
		}

		document, err := client.NewDocFromMap(ctx, filteredDocument, collection.Version())
		if err != nil {
			return nil, fmt.Errorf("failed to create document from map: %w", err)
		}

		// Save will update if document exists, create if it doesn't
		err = collection.Save(ctx, document)
		if err != nil {
			return nil, fmt.Errorf("failed to save document in collection: %w", err)
		}

		createdDocumentIds = append(createdDocumentIds, document.ID().String())
	}

	return createdDocumentIds, nil
}

func (v *View) HasLenses() bool {
	return len(v.Transform.Lenses) > 0
}

// needsWasmConversion returns true if any lens path contains base64 data instead of a file path
func (v *View) needsWasmConversion() bool {
	for _, lens := range v.Transform.Lenses {
		// If path doesn't start with file:// or http(s)://, it's likely base64 data
		if !strings.HasPrefix(lens.Path, "file://") &&
			!strings.HasPrefix(lens.Path, "http://") &&
			!strings.HasPrefix(lens.Path, "https://") {
			return true
		}
	}
	return false
}
