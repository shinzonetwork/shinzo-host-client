package view

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/shinzonetwork/viewbundle-go"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/client/options"
	"github.com/sourcenetwork/defradb/node"
	"github.com/sourcenetwork/lens/host-go/config/model"
)

// Transform is a type alias for viewbundle.Transform.
type Transform = viewbundle.Transform

// Lens is a type alias for viewbundle.Lens.
type Lens = viewbundle.Lens

// View represents a Shinzo view with viewbundle integration.
type View struct {
	Name string          `json:"name"`
	Data viewbundle.View `json:"data"`
}

// NewViewFromWire creates a new View from a viewbundle wire format.
func NewViewFromWire(wire []byte) (*View, error) {
	bundler := viewbundle.NewBundler()
	bundledView, err := bundler.UnbundleView(wire)
	if err != nil {
		return nil, fmt.Errorf("failed to unbundle view: %w", err)
	}

	view := &View{
		Name: "", // Will be extracted from SDL
		Data: bundledView,
	}

	// Extract name from SDL
	view.ExtractNameFromSDL()

	return view, nil
}

// NewViewFromBundle creates a new View from a viewbundle.View.
func NewViewFromBundle(bundledView viewbundle.View) (*View, error) {
	view := &View{
		Name: "", // Will be extracted from SDL
		Data: bundledView,
	}

	// Extract name from SDL
	view.ExtractNameFromSDL()

	return view, nil
}

// ExtractNameFromSDL extracts the type name from the SDL string.
func (v *View) ExtractNameFromSDL() {
	if v.Data.Sdl == "" {
		v.Name = ""
		return
	}

	// Look for pattern: type <Name> @...
	re := regexp.MustCompile(`type\s+(\w+)\s+@`)
	matches := re.FindStringSubmatch(v.Data.Sdl)
	if len(matches) > 1 {
		v.Name = matches[1]
	} else {
		// Fallback: look for type <Name> { pattern
		re = regexp.MustCompile(`type\s+(\w+)\s+{`)
		matches = re.FindStringSubmatch(v.Data.Sdl)
		if len(matches) > 1 {
			v.Name = matches[1]
		}
	}
}

// Validate validates the view configuration.
func (v *View) Validate() error {
	if v.Name == "" {
		return ErrViewNameRequired
	}
	if v.Data.Query == "" {
		return ErrViewQueryRequired
	}
	if v.Data.Sdl == "" {
		return ErrViewSDLRequired
	}

	// Validate lenses
	for i, lens := range v.Data.Transform.Lenses {
		if lens.Path == "" {
			return fmt.Errorf("lens %d: %w", i, ErrLensEmptyPath)
		}

		// Additional validation for base64 WASM
		if !strings.HasPrefix(lens.Path, "file://") && !strings.HasPrefix(lens.Path, "http") {
			if !isValidBase64(lens.Path) {
				return fmt.Errorf("lens %d: %w", i, ErrLensInvalidBase64)
			}
		}
	}

	return nil
}

// SubscribeTo subscribes to the view collection for real-time updates.
func (v *View) SubscribeTo(ctx context.Context, defraNode *node.Node) error {
	_, err := defraNode.DB.GetCollectionByName(ctx, v.Name)
	if err != nil {
		return fmt.Errorf("view %s: %w", v.Name, ErrCollectionNotFound)
	}

	err = defraNode.DB.CreateP2PCollections(ctx, []string{v.Name})
	if err != nil {
		return fmt.Errorf("error subscribing to collection %s: %w", v.Name, err)
	}

	return nil
}

// PostWasmToFile writes base64 WASM data to files and updates lens paths.
func (v *View) PostWasmToFile(lensRegistryPath string) error {
	registryDir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("failed to get working directory: %w", err)
	}
	registryDir = filepath.Join(registryDir, lensRegistryPath)

	// Create registry directory if it doesn't exist
	if err := os.MkdirAll(registryDir, 0o750); err != nil { // nolint:mnd
		return fmt.Errorf("failed to create registry directory: %w", err)
	}

	for i, lense := range v.Data.Transform.Lenses {
		// Skip if already a file path
		if strings.HasPrefix(lense.Path, "file://") || strings.HasPrefix(lense.Path, "http") {
			continue
		}

		// Validate base64 WASM data before attempting to decode
		if !isValidBase64(lense.Path) {
			return fmt.Errorf("lens %d: %w", i, ErrLensInvalidBase64)
		}

		// Decode base64 WASM
		wasmDecoded, err := base64.StdEncoding.DecodeString(lense.Path)
		if err != nil {
			return fmt.Errorf("failed to decode base64 WASM for lens %d: %w", i, err)
		}

		// Validate WASM file integrity (check for WASM magic number)
		if len(wasmDecoded) < 8 || !isValidWASM(wasmDecoded) {
			return fmt.Errorf("lens %d: %w", i, ErrInvalidWASMFormat)
		}

		// Generate short filename
		hash := sha256.Sum256([]byte(lense.Path))
		shortHash := hex.EncodeToString(hash[:8])
		wasmFileName := fmt.Sprintf("%s_%d.wasm", shortHash, i)
		wasmFilePath := filepath.Join(registryDir, wasmFileName)

		// Write WASM file
		err = os.WriteFile(wasmFilePath, wasmDecoded, 0o600) //nolint:mnd
		if err != nil {
			return fmt.Errorf("failed to write WASM file: %w", err)
		}

		// Update lens path
		absPath, err := filepath.Abs(wasmFilePath)
		if err != nil {
			return fmt.Errorf("failed to get absolute path: %w", err)
		}
		v.Data.Transform.Lenses[i].Path = "file://" + absPath
	}

	return nil
}

// ConfigureLens creates the view in DefraDB with the lens transform CID.
func (v *View) ConfigureLens(ctx context.Context, defraNode *node.Node, lensCID string) error {
	if v.Data.Query == "" || v.Data.Sdl == "" {
		return ErrViewQuerySDLRequired
	}

	var err error
	t0 := time.Now()
	fmt.Printf("[LENS-PROBE] %s AddView ENTER name=%s lensCID=%s queryLen=%d sdlLen=%d\n",
		t0.UTC().Format(time.RFC3339Nano), v.Name, lensCID, len(v.Data.Query), len(v.Data.Sdl))
	if lensCID != "" {
		// Create view with the lens transform CID so DefraDB applies the WASM lens
		viewOpts := options.AddView().SetTransformCID(lensCID)
		_, err = defraNode.DB.AddView(ctx, v.Data.Query, v.Data.Sdl, viewOpts)
	} else {
		_, err = defraNode.DB.AddView(ctx, v.Data.Query, v.Data.Sdl)
	}
	fmt.Printf("[LENS-PROBE] %s AddView EXIT  name=%s elapsed=%s err=%v\n",
		time.Now().UTC().Format(time.RFC3339Nano), v.Name, time.Since(t0), err)

	if err != nil && !contains(err.Error(), "already exists") {
		// Try to auto-fix common field name issues
		if strings.Contains(err.Error(), "Cannot query field") && strings.Contains(err.Error(), "Did you mean") {
			fmt.Printf("🔧 Auto-correcting query for view %s: %s\n", v.Name, err.Error())
			correctedQuery := v.attemptQueryCorrection(err.Error())
			if correctedQuery != v.Data.Query {
				fmt.Printf("🔧 Corrected query: %s -> %s\n", v.Data.Query, correctedQuery)
				v.Data.Query = correctedQuery
				// Retry with corrected query
				if lensCID != "" {
					viewOpts := options.AddView().SetTransformCID(lensCID)
					_, err = defraNode.DB.AddView(ctx, v.Data.Query, v.Data.Sdl, viewOpts)
				} else {
					_, err = defraNode.DB.AddView(ctx, v.Data.Query, v.Data.Sdl)
				}

				if err != nil && !contains(err.Error(), "already exists") {
					fmt.Printf("❌ Auto-correction retry failed for view %s: %v\n", v.Name, err)
					return fmt.Errorf("failed to create view after correction: %w", err)
				}
				fmt.Printf("✅ Query auto-corrected successfully for view %s\n", v.Name)
				return nil
			}
		}
		fmt.Printf("⚠️ Auto-correction produced same query for view %s\n", v.Name)
		return fmt.Errorf("failed to create view: %w", err)
	}

	return nil
}

// attemptQueryCorrection tries to fix common field name issues based on error message.
func (v *View) attemptQueryCorrection(errMsg string) string {
	// Error format: Cannot query field "inputData" on type "X". Did you mean "input"?
	re := regexp.MustCompile(`Cannot query field "([^"]+)".*Did you mean "([^"]+)"`)
	matches := re.FindStringSubmatch(errMsg)

	if len(matches) == minRegexMatchGroups {
		incorrectField := matches[1]
		suggestedField := matches[2]
		// Whole-word match so substrings inside other identifiers are not touched.
		tokenRe := regexp.MustCompile(`\b` + regexp.QuoteMeta(incorrectField) + `\b`)
		return tokenRe.ReplaceAllLiteralString(v.Data.Query, suggestedField)
	}

	return v.Data.Query
}

// SetupLensInDefraDB stores the lens WASM in DefraDB and returns the lens CID
// Note: SetMigration is NOT called here - it's only needed when using PatchCollection
// to transform data between collection versions, which we don't do for view lenses.
func SetupLensInDefraDB(ctx context.Context, defraNode *node.Node, v *View) (string, error) {
	if !v.HasLenses() {
		return "", nil
	}

	// Build lens config
	lensConfig, err := v.BuildLensConfig()
	if err != nil {
		return "", fmt.Errorf("failed to build lens config: %w", err)
	}

	// Add lens to DefraDB to store the WASM
	lensCID, err := defraNode.DB.AddLens(ctx, lensConfig.Lens)
	if err != nil {
		return "", fmt.Errorf("failed to add lens: %w", err)
	}

	return lensCID, nil
}

// BuildLensConfig creates the lens configuration for DefraDB SetMigration.
func (v *View) BuildLensConfig() (client.LensConfig, error) {
	lensModules := make([]model.LensModule, 0, len(v.Data.Transform.Lenses))

	for _, lens := range v.Data.Transform.Lenses {
		args := map[string]any{}
		if lens.Arguments != "" {
			if err := json.Unmarshal([]byte(lens.Arguments), &args); err != nil {
				return client.LensConfig{}, fmt.Errorf("failed to parse lens arguments: %w", err)
			}
		}
		lensModules = append(lensModules, model.LensModule{
			Path:      lens.Path,
			Inverse:   false,
			Arguments: args,
		})
	}

	return client.LensConfig{
		SourceCollectionVersionID:      v.Data.Query,
		DestinationCollectionVersionID: v.Data.Sdl,
		Lens: model.Lens{
			Lenses: lensModules,
		},
	}, nil
}

// HasLenses returns true if the view has lens transformations.
func (v *View) HasLenses() bool {
	return len(v.Data.Transform.Lenses) > 0
}

// needsWasmConversion returns true if any lens path contains base64 data instead of a file path.
func (v *View) needsWasmConversion() bool {
	for _, lens := range v.Data.Transform.Lenses {
		// If path doesn't start with file:// or http(s)://, it's likely base64 data
		if !strings.HasPrefix(lens.Path, "file://") && !strings.HasPrefix(lens.Path, "http") {
			return true
		}
	}
	return false
}

// Helper function to check if error contains substring.
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr ||
		(len(s) > len(substr) &&
			(s[:len(substr)] == substr ||
				s[len(s)-len(substr):] == substr ||
				indexOf(s, substr) >= 0)))
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// isValidBase64 checks if a string is valid base64.
func isValidBase64(s string) bool {
	_, err := base64.StdEncoding.DecodeString(s)
	return err == nil
}

// defaultWASMLength is the minimum length for a valid WASM file.
const defaultWASMLength = 8

// isValidWASM checks if the byte slice has a valid WASM magic number.
func isValidWASM(data []byte) bool {
	if len(data) < defaultWASMLength {
		return false
	}
	// WASM magic number: 0x00 0x61 0x73 0x6D (WebAssembly)
	return data[0] == 0x00 && data[1] == 0x61 && data[2] == 0x73 && data[3] == 0x6D
}
