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
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/views"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/node"
	"github.com/sourcenetwork/immutable"
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

// ConfigureLens creates or updates the view with the lens transform using AddView.
// According to the DefraDB guide, views are created with db.AddView providing:
// - A GQL query string (source query)
// - A GQL SDL (output structure, should have @materialized(if: false) for non-materialized)
// - Optionally, a lens configuration (transform)
func (v *View) ConfigureLens(ctx context.Context, defraNode *node.Node) error {
	if len(v.Transform.Lenses) < 1 {
		return fmt.Errorf("no lenses provided in view %+v", v)
	}

	if v.Query == nil || *v.Query == "" {
		return fmt.Errorf("view query is required")
	}

	if v.Sdl == nil || *v.Sdl == "" {
		return fmt.Errorf("view SDL is required")
	}

	// Ensure SDL has @materialized(if: false) for non-materialized views
	// We always want non-materialized views so lens transforms are applied on query
	// Also replace the type name with the unique view name to avoid conflicts
	// Add blockNumber field to the SDL so we can query/filter by it
	sdl := *v.Sdl
	uniqueViewName := v.getUniqueViewName()

	// Replace the type name in the SDL with the unique view name
	// Pattern: type ViewName { ... } -> type ViewName_<address> { ... }
	re := regexp.MustCompile(`type\s+(\w+)\s*`)
	sdl = re.ReplaceAllString(sdl, fmt.Sprintf("type %s ", uniqueViewName))

	if strings.Contains(sdl, "@materialized") {
		// Replace any existing @materialized directive with @materialized(if: false)
		// Handle various formats: @materialized, @materialized(if: true), @materialized(if: false), etc.
		re := regexp.MustCompile(`@materialized\s*(\([^)]*\))?`)
		sdl = re.ReplaceAllString(sdl, "@materialized(if: false)")
	} else {
		// Add @materialized(if: false) directive if not present
		// Find the type definition and add the directive before the opening brace
		re := regexp.MustCompile(`(type\s+\w+\s*)(\{)`)
		if re.MatchString(sdl) {
			sdl = re.ReplaceAllString(sdl, `${1}@materialized(if: false) $2`)
		}
	}

	// Add blockNumber field to the SDL so we can query/filter by it in the view
	// Insert it before the closing brace of the type definition
	// Check if blockNumber already exists to avoid duplicates
	if !strings.Contains(sdl, "blockNumber") {
		// Find the closing brace of the type definition and insert blockNumber before it
		// We'll find the last closing brace (which should be the type definition's closing brace)
		// and insert blockNumber with proper formatting
		idx := strings.LastIndex(sdl, "}")
		if idx != -1 {
			// Find the newline before the closing brace to maintain formatting
			newlineIdx := strings.LastIndex(sdl[:idx], "\n")
			if newlineIdx != -1 {
				// Extract indentation from the line before the closing brace
				// Look backwards from newlineIdx to find the start of indentation
				lineStart := newlineIdx + 1
				indent := ""
				if lineStart < idx {
					// Get the whitespace before the closing brace
					beforeBrace := sdl[lineStart:idx]
					// Extract leading whitespace as indent
					for i, r := range beforeBrace {
						if r != ' ' && r != '\t' {
							indent = beforeBrace[:i]
							break
						}
					}
					if indent == "" {
						indent = beforeBrace
					}
				}
				// If we couldn't determine indent, use 2 spaces as default
				if indent == "" {
					indent = "  "
				}
				// Insert blockNumber before the closing brace with proper indentation
				sdl = sdl[:idx] + indent + "blockNumber: Int\n" + sdl[idx:]
			} else {
				// No newline found, just add with space
				sdl = sdl[:idx] + " blockNumber: Int" + sdl[idx:]
			}
		}
	}

	// Convert view's Transform to model.Lens
	lenses := []model.LensModule{}
	for _, lense := range v.Transform.Lenses {
		lenses = append(lenses, model.LensModule{
			Path:      lense.Path,
			Inverse:   false, // Assume lenses can't be inversed as view creator does not specify
			Arguments: lense.Arguments,
		})
	}
	transform := immutable.Some(model.Lens{
		Lenses: lenses,
	})

	// Create the view with AddView (includes the lens transform)
	// If the view already exists from SubscribeTo, AddView will return an error
	// We handle that gracefully since the view might have been created without the transform
	_, err := defraNode.DB.AddView(ctx, *v.Query, sdl, transform)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") || strings.Contains(err.Error(), "collection already exists") {
			return nil
		}
		return fmt.Errorf("failed to create view with lens transform: %w", err)
	}

	return nil
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

	// If we couldn't get fields from SDL, use a minimal query
	if fields == "" {
		fields = "_docID"
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
