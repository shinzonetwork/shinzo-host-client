package view

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

// WASMRegistry manages downloading and caching of WASM lens files.
type WASMRegistry struct {
	registryPath string
	httpClient   *http.Client
	logger       *zap.SugaredLogger
	mu           sync.RWMutex
	cached       map[string]string // url/hash -> local file path
}

// TODO: -- view lifecycle
// 1. Function which downloads wasm from event route
// 2. Function which saves wasm as a view
// 3. Function which executes setMigration within defra

// NewWASMRegistry creates a new WASM registry with the specified storage path.
func NewWASMRegistry(registryPath string, logger *zap.SugaredLogger) (*WASMRegistry, error) {
	// Create registry directory if it doesn't exist
	if err := os.MkdirAll(registryPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WASM registry directory: %w", err)
	}

	return &WASMRegistry{
		registryPath: registryPath,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		logger: logger,
		cached: make(map[string]string),
	}, nil
}

// AddViewsFromLensRegistry loads persisted view definitions from views.json in the registry
func AddViewsFromLensRegistry(registryPath string) ([]View, error) {
	viewsFilePath := filepath.Join(registryPath, "views.json")

	// Check if views.json exists
	data, err := os.ReadFile(viewsFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			// No persisted views - return empty slice (not an error)
			return []View{}, nil
		}
		return nil, fmt.Errorf("failed to read views.json: %w", err)
	}

	// Parse views
	var views []View
	if err := json.Unmarshal(data, &views); err != nil {
		return nil, fmt.Errorf("failed to parse views.json: %w", err)
	}

	return views, nil
}

// SaveViewToRegistry persists a view definition to views.json
func SaveViewToRegistry(registryPath string, v View) error {
	viewsFilePath := filepath.Join(registryPath, "views.json")

	// Load existing views
	views, err := AddViewsFromLensRegistry(registryPath)
	if err != nil {
		return err
	}

	// Check for duplicate and update or append
	found := false
	for i, existing := range views {
		if existing.Name == v.Name {
			views[i] = v
			found = true
			break
		}
	}
	if !found {
		views = append(views, v)
	}

	// Write back
	data, err := json.MarshalIndent(views, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal views: %w", err)
	}

	return os.WriteFile(viewsFilePath, data, 0644)
}

// this function needs to download or ensure it already exists for every register transaction
// EnsureWASM checks if a WASM file exists locally, downloads it if not.
// Returns the local file path (file:// URL format for DefraDB).
func (r *WASMRegistry) EnsureWASM(ctx context.Context, wasmURL string) (string, error) {
	r.mu.RLock()
	if localPath, exists := r.cached[wasmURL]; exists {
		r.mu.RUnlock()
		return localPath, nil
	}
	r.mu.RUnlock()

	// Determine local filename from URL
	filename := r.urlToFilename(wasmURL)
	localPath := filepath.Join(r.registryPath, filename)

	// Check if file already exists on disk
	if _, err := os.Stat(localPath); err == nil {
		absPath, _ := filepath.Abs(localPath)
		fileURL := "file://" + absPath
		r.cacheEntry(wasmURL, fileURL)
		r.logger.Infof("✅ WASM already cached: %s", filename)
		return fileURL, nil
	}

	// Download the WASM file
	r.logger.Infof("📥 Downloading WASM: %s", wasmURL)
	if err := r.downloadWASM(ctx, wasmURL, localPath); err != nil {
		return "", fmt.Errorf("failed to download WASM from %s: %w", wasmURL, err)
	}

	absPath, _ := filepath.Abs(localPath)
	fileURL := "file://" + absPath
	r.cacheEntry(wasmURL, fileURL)
	r.logger.Infof("✅ WASM downloaded and cached: %s", filename)

	return fileURL, nil
}

// EnsureAllWASM checks and downloads all WASM files for a list of views.
// Returns a map of original URL -> local file path.
func (r *WASMRegistry) EnsureAllWASM(ctx context.Context, wasmURLs []string) (map[string]string, error) {
	results := make(map[string]string)
	var errors []error

	for _, url := range wasmURLs {
		localPath, err := r.EnsureWASM(ctx, url)
		if err != nil {
			errors = append(errors, err)
			r.logger.Errorf("❌ Failed to ensure WASM %s: %v", url, err)
			continue
		}
		results[url] = localPath
	}

	if len(errors) > 0 {
		return results, fmt.Errorf("failed to download %d WASM files", len(errors))
	}

	return results, nil
}

// LensInfo represents a lens with a path that can be synced.
type LensInfo struct {
	Label string
	Path  string
}

// SyncLensWASM ensures all WASM files for a list of lenses are available locally.
// Returns updated paths map (original path -> local file path).
func (r *WASMRegistry) SyncLensWASM(ctx context.Context, lenses []LensInfo) (map[string]string, error) {
	pathMap := make(map[string]string)

	for _, lens := range lenses {
		wasmSource := lens.Path

		// Skip if already a local file path
		if strings.HasPrefix(wasmSource, "file://") {
			pathMap[wasmSource] = wasmSource
			continue
		}

		// Handle HTTP/HTTPS URLs
		if strings.HasPrefix(wasmSource, "http://") || strings.HasPrefix(wasmSource, "https://") {
			localPath, err := r.EnsureWASM(ctx, wasmSource)
			if err != nil {
				return nil, fmt.Errorf("failed to sync WASM for lens %s: %w", lens.Label, err)
			}
			pathMap[wasmSource] = localPath
			continue
		}

		// Base64 or other formats - return as-is (handled elsewhere)
		pathMap[wasmSource] = wasmSource
	}

	return pathMap, nil
}

// ListCachedWASM returns all cached WASM files.
func (r *WASMRegistry) ListCachedWASM() []string {
	files, err := os.ReadDir(r.registryPath)
	if err != nil {
		return nil
	}

	var wasmFiles []string
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".wasm") {
			wasmFiles = append(wasmFiles, f.Name())
		}
	}
	return wasmFiles
}

// CleanupUnused removes WASM files not in the provided list of active URLs.
func (r *WASMRegistry) CleanupUnused(activeURLs []string) (int, error) {
	activeFilenames := make(map[string]bool)
	for _, url := range activeURLs {
		activeFilenames[r.urlToFilename(url)] = true
	}

	files, err := os.ReadDir(r.registryPath)
	if err != nil {
		return 0, err
	}

	removed := 0
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".wasm") && !activeFilenames[f.Name()] {
			path := filepath.Join(r.registryPath, f.Name())
			if err := os.Remove(path); err == nil {
				removed++
				r.logger.Infof("🗑️ Removed unused WASM: %s", f.Name())
			}
		}
	}

	return removed, nil
}

// downloadWASM fetches a WASM file from a URL and saves it locally.
func (r *WASMRegistry) downloadWASM(ctx context.Context, url, destPath string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}

	// Create temp file first, then rename for atomicity
	tmpPath := destPath + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	_, err = io.Copy(f, resp.Body)
	f.Close()
	if err != nil {
		os.Remove(tmpPath)
		return err
	}

	return os.Rename(tmpPath, destPath)
}

// urlToFilename converts a URL to a safe filename using content hash.
func (r *WASMRegistry) urlToFilename(url string) string {
	// Extract filename from URL if possible
	parts := strings.Split(url, "/")
	if len(parts) > 0 {
		lastPart := parts[len(parts)-1]
		if strings.HasSuffix(lastPart, ".wasm") {
			// Use hash prefix + original name for uniqueness
			hash := sha256.Sum256([]byte(url))
			prefix := hex.EncodeToString(hash[:4])
			return prefix + "_" + lastPart
		}
	}

	// Fallback to full hash
	hash := sha256.Sum256([]byte(url))
	return hex.EncodeToString(hash[:]) + ".wasm"
}

func (r *WASMRegistry) cacheEntry(url, localPath string) {
	r.mu.Lock()
	r.cached[url] = localPath
	r.mu.Unlock()
}

// StartupSync checks all provided WASM URLs and downloads any missing files.
// This should be called on host startup with all known view lens URLs.
// Returns the count of files downloaded and any errors.
func (r *WASMRegistry) StartupSync(ctx context.Context, wasmURLs []string) (downloaded int, cached int, err error) {
	r.logger.Infof("🔄 Starting WASM sync check for %d files", len(wasmURLs))

	for _, url := range wasmURLs {
		// Skip non-URL paths (base64, local files)
		if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
			continue
		}

		filename := r.urlToFilename(url)
		localPath := filepath.Join(r.registryPath, filename)

		// Check if already exists
		if _, statErr := os.Stat(localPath); statErr == nil {
			absPath, _ := filepath.Abs(localPath)
			r.cacheEntry(url, "file://"+absPath)
			cached++
			continue
		}

		// Download missing file
		if downloadErr := r.downloadWASM(ctx, url, localPath); downloadErr != nil {
			r.logger.Errorf("❌ Failed to download WASM %s: %v", url, downloadErr)
			if err == nil {
				err = fmt.Errorf("failed to download %s: %w", url, downloadErr)
			}
			continue
		}

		absPath, _ := filepath.Abs(localPath)
		r.cacheEntry(url, "file://"+absPath)
		downloaded++
		r.logger.Infof("📥 Downloaded WASM: %s", filename)
	}

	r.logger.Infof("✅ WASM sync complete: %d downloaded, %d cached", downloaded, cached)
	return downloaded, cached, err
}

// GetLocalPath returns the local file path for a WASM URL if cached.
func (r *WASMRegistry) GetLocalPath(wasmURL string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	path, exists := r.cached[wasmURL]
	return path, exists
}

// RegistryPath returns the directory where WASM files are stored.
func (r *WASMRegistry) RegistryPath() string {
	return r.registryPath
}
