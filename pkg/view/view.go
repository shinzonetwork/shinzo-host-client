package view

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/shinzonetwork/app-sdk/pkg/views"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/node"
	"github.com/sourcenetwork/immutable/enumerable"
	"github.com/sourcenetwork/lens/host-go/config/model"
)

type View views.View

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
	for key, value := range document {
		if schemaFields[key] {
			filteredDocument[key] = value
		}
	}

	return filteredDocument, nil
}

func (v *View) SubscribeTo(ctx context.Context, defraNode *node.Node) error {
	subscribeableView := views.View(*v)
	return subscribeableView.SubscribeTo(ctx, defraNode)
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

func (v *View) ConfigureLens(ctx context.Context, defraNode *node.Node) error {
	if len(v.Transform.Lenses) < 1 {
		return fmt.Errorf("no lenses provided in view %+v", v)
	}

	lenses := []model.LensModule{}
	for _, lense := range v.Transform.Lenses {
		lenses = append(lenses, model.LensModule{
			Path:      lense.Path,
			Inverse:   false, // Assume lenses can't be inversed as view creator does not specify
			Arguments: lense.Arguments,
		})
	}
	lensConfig := model.Lens{
		Lenses: lenses,
	}

	err := defraNode.DB.LensRegistry().SetMigration(ctx, v.Name, lensConfig)
	if err != nil {
		return fmt.Errorf("error setting lens config: %v", err)
	}

	return nil
}

func (v *View) ApplyLensTransform(ctx context.Context, defraNode *node.Node, sourceDocuments []map[string]any) ([]map[string]any, error) {
	src := enumerable.New(sourceDocuments)

	transformed, err := defraNode.DB.LensRegistry().MigrateUp(ctx, src, v.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to apply lens transformation: %w", err)
	}

	// Convert back to slice
	var result []map[string]any
	err = enumerable.ForEach(transformed, func(item map[string]any) {
		if item != nil {
			result = append(result, item)
		}
	})

	if err != nil {
		return nil, fmt.Errorf("failed to iterate through transformed results: %w", err)
	}

	return result, nil
}

func (v *View) WriteTransformedToCollection(ctx context.Context, defraNode *node.Node, transformedDocument []map[string]any) error {
	collection, err := defraNode.DB.GetCollectionByName(ctx, v.Name)
	if err != nil {
		return fmt.Errorf("error getting collection %s: %v", v.Name, err)
	}

	// Use Save instead of CreateMany to handle duplicate document IDs gracefully
	for _, documentAsMap := range transformedDocument {
		// Filter the document to only include fields defined in the SDL schema
		filteredDocument, err := v.filterDocumentFields(documentAsMap)
		if err != nil {
			return fmt.Errorf("failed to filter document fields: %w", err)
		}

		document, err := client.NewDocFromMap(filteredDocument, collection.Version())
		if err != nil {
			return fmt.Errorf("failed to create document from map: %w", err)
		}

		// Save will update if document exists, create if it doesn't
		err = collection.Save(ctx, document)
		if err != nil {
			return fmt.Errorf("failed to save document in collection: %w", err)
		}
	}

	return nil
}

func (v *View) HasLenses() bool {
	return len(v.Transform.Lenses) > 0
}
