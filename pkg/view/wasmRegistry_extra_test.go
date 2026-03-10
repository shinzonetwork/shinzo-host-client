package view

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// downloadWASM - create file error path
// ---------------------------------------------------------------------------

func TestWASMRegistry_downloadWASM_CannotCreateFile(t *testing.T) {
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("wasm data"))
	}))
	defer svr.Close()

	reg, err := NewWASMRegistry(t.TempDir(), testLogger())
	require.NoError(t, err)

	// Use an invalid destination path that cannot be created
	dest := "/dev/null/impossible/file.wasm"
	err = reg.downloadWASM(context.Background(), svr.URL+"/test.wasm", dest)
	require.Error(t, err)
}

func TestWASMRegistry_downloadWASM_InvalidURL(t *testing.T) {
	reg, err := NewWASMRegistry(t.TempDir(), testLogger())
	require.NoError(t, err)

	dest := filepath.Join(t.TempDir(), "test.wasm")
	err = reg.downloadWASM(context.Background(), "://invalid-url", dest)
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// SaveViewToRegistry - edge cases
// ---------------------------------------------------------------------------

func TestWASMRegistry_SaveViewToRegistry_CorruptedFile(t *testing.T) {
	dir := t.TempDir()

	// Write corrupted JSON to views.json
	require.NoError(t, os.WriteFile(filepath.Join(dir, "views.json"), []byte("corrupted"), 0644))

	// Saving should fail because it tries to load existing views first
	err := SaveViewToRegistry(dir, View{Name: "new"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to parse views.json")
}

// ---------------------------------------------------------------------------
// EnsureWASM - download error wrapping
// ---------------------------------------------------------------------------

func TestWASMRegistry_EnsureWASM_DownloadError_WrapsMessage(t *testing.T) {
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer svr.Close()

	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	wasmURL := svr.URL + "/unavailable.wasm"
	_, err = reg.EnsureWASM(context.Background(), wasmURL)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to download WASM from")
}

// ---------------------------------------------------------------------------
// EnsureAllWASM - empty list
// ---------------------------------------------------------------------------

func TestWASMRegistry_EnsureAllWASM_EmptyList(t *testing.T) {
	reg, err := NewWASMRegistry(t.TempDir(), testLogger())
	require.NoError(t, err)

	results, err := reg.EnsureAllWASM(context.Background(), []string{})
	require.NoError(t, err)
	require.Empty(t, results)
}

// ---------------------------------------------------------------------------
// SyncLensWASM - empty list
// ---------------------------------------------------------------------------

func TestWASMRegistry_SyncLensWASM_EmptyList(t *testing.T) {
	reg, err := NewWASMRegistry(t.TempDir(), testLogger())
	require.NoError(t, err)

	results, err := reg.SyncLensWASM(context.Background(), []LensInfo{})
	require.NoError(t, err)
	require.Empty(t, results)
}

// ---------------------------------------------------------------------------
// StartupSync - empty list
// ---------------------------------------------------------------------------

func TestWASMRegistry_StartupSync_EmptyList(t *testing.T) {
	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	downloaded, cached, err := reg.StartupSync(context.Background(), []string{})
	require.NoError(t, err)
	require.Equal(t, 0, downloaded)
	require.Equal(t, 0, cached)
}

// ---------------------------------------------------------------------------
// CleanupUnused - empty active list removes all WASM files
// ---------------------------------------------------------------------------

func TestWASMRegistry_CleanupUnused_EmptyActiveRemovesAll(t *testing.T) {
	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(filepath.Join(dir, "orphan1.wasm"), []byte("o1"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "orphan2.wasm"), []byte("o2"), 0644))

	removed, err := reg.CleanupUnused([]string{})
	require.NoError(t, err)
	require.Equal(t, 2, removed)
}

// ---------------------------------------------------------------------------
// urlToFilename - edge cases
// ---------------------------------------------------------------------------

func TestWASMRegistry_urlToFilename_EmptyURL(t *testing.T) {
	reg, err := NewWASMRegistry(t.TempDir(), testLogger())
	require.NoError(t, err)

	name := reg.urlToFilename("")
	require.Contains(t, name, ".wasm")
}

func TestWASMRegistry_urlToFilename_SlashOnly(t *testing.T) {
	reg, err := NewWASMRegistry(t.TempDir(), testLogger())
	require.NoError(t, err)

	name := reg.urlToFilename("/")
	// Last part is empty string, not ending in .wasm, so full hash fallback
	require.Contains(t, name, ".wasm")
}

// ---------------------------------------------------------------------------
// SaveViewToRegistry - success paths
// ---------------------------------------------------------------------------

func TestSaveViewToRegistry_NewViewCreatesFile(t *testing.T) {
	dir := t.TempDir()

	query := "User { name }"
	sdl := "type SaveNewView { name: String }"
	v := View{Name: "SaveNewView", Query: &query, Sdl: &sdl}

	err := SaveViewToRegistry(dir, v)
	require.NoError(t, err)

	// Verify file was created
	data, err := os.ReadFile(filepath.Join(dir, "views.json"))
	require.NoError(t, err)
	require.Contains(t, string(data), "SaveNewView")
}

func TestSaveViewToRegistry_UpdateExistingView(t *testing.T) {
	dir := t.TempDir()

	query1 := "User { name }"
	sdl1 := "type SaveUpdateView { name: String }"
	v1 := View{Name: "SaveUpdateView", Query: &query1, Sdl: &sdl1}

	err := SaveViewToRegistry(dir, v1)
	require.NoError(t, err)

	// Save again with updated query
	query2 := "User { name age }"
	v2 := View{Name: "SaveUpdateView", Query: &query2, Sdl: &sdl1}

	err = SaveViewToRegistry(dir, v2)
	require.NoError(t, err)

	// Load and verify only one view exists with the updated query
	views, err := AddViewsFromLensRegistry(dir)
	require.NoError(t, err)
	require.Len(t, views, 1)
	require.Equal(t, "SaveUpdateView", views[0].Name)
	require.Equal(t, query2, *views[0].Query)
}

func TestSaveViewToRegistry_AppendsToExistingViews(t *testing.T) {
	dir := t.TempDir()

	query1 := "User { name }"
	sdl1 := "type ViewA { name: String }"
	err := SaveViewToRegistry(dir, View{Name: "ViewA", Query: &query1, Sdl: &sdl1})
	require.NoError(t, err)

	query2 := "User { age }"
	sdl2 := "type ViewB { age: Int }"
	err = SaveViewToRegistry(dir, View{Name: "ViewB", Query: &query2, Sdl: &sdl2})
	require.NoError(t, err)

	views, err := AddViewsFromLensRegistry(dir)
	require.NoError(t, err)
	require.Len(t, views, 2)
}

// ---------------------------------------------------------------------------
// AddViewsFromLensRegistry - additional paths
// ---------------------------------------------------------------------------

func TestAddViewsFromLensRegistry_NonExistentDirectory(t *testing.T) {
	views, err := AddViewsFromLensRegistry("/nonexistent/path/that/does/not/exist")
	require.NoError(t, err)
	require.Empty(t, views)
}

// ---------------------------------------------------------------------------
// downloadWASM - non-200 status code
// ---------------------------------------------------------------------------

func TestWASMRegistry_downloadWASM_Non200StatusCode(t *testing.T) {
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer svr.Close()

	reg, err := NewWASMRegistry(t.TempDir(), testLogger())
	require.NoError(t, err)

	dest := filepath.Join(t.TempDir(), "test.wasm")
	err = reg.downloadWASM(context.Background(), svr.URL+"/missing.wasm", dest)
	require.Error(t, err)
	require.Contains(t, err.Error(), "HTTP 404")
}

func TestWASMRegistry_downloadWASM_SuccessVerifyContent(t *testing.T) {
	expectedContent := "new fake wasm binary content"
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(expectedContent))
	}))
	defer svr.Close()

	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	dest := filepath.Join(dir, "downloaded_verify.wasm")
	err = reg.downloadWASM(context.Background(), svr.URL+"/test.wasm", dest)
	require.NoError(t, err)

	// Verify file was created with expected content
	data, err := os.ReadFile(dest)
	require.NoError(t, err)
	require.Equal(t, expectedContent, string(data))
}

// ---------------------------------------------------------------------------
// EnsureWASM - file already on disk (not in cache)
// ---------------------------------------------------------------------------

func TestWASMRegistry_EnsureWASM_FileOnDiskNotInCache(t *testing.T) {
	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	wasmURL := "https://example.com/test_ondisk_extra.wasm"
	filename := reg.urlToFilename(wasmURL)
	localPath := filepath.Join(dir, filename)

	// Pre-create the file to simulate it already being on disk but not in memory cache
	require.NoError(t, os.WriteFile(localPath, []byte("cached wasm data"), 0644))

	result, err := reg.EnsureWASM(context.Background(), wasmURL)
	require.NoError(t, err)
	require.Contains(t, result, "file://")

	// Verify it's now in cache
	cached, exists := reg.GetLocalPath(wasmURL)
	require.True(t, exists)
	require.Contains(t, cached, "file://")
}

// ---------------------------------------------------------------------------
// EnsureWASM - already cached
// ---------------------------------------------------------------------------

func TestWASMRegistry_EnsureWASM_AlreadyCached(t *testing.T) {
	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	wasmURL := "https://example.com/cached.wasm"
	expectedPath := "file:///cached/path.wasm"

	// Pre-populate the cache
	reg.cacheEntry(wasmURL, expectedPath)

	result, err := reg.EnsureWASM(context.Background(), wasmURL)
	require.NoError(t, err)
	require.Equal(t, expectedPath, result)
}

// ---------------------------------------------------------------------------
// StartupSync - with cached and downloaded files
// ---------------------------------------------------------------------------

func TestWASMRegistry_StartupSync_WithCachedFile(t *testing.T) {
	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	wasmURL := "https://example.com/startupsync.wasm"
	filename := reg.urlToFilename(wasmURL)
	localPath := filepath.Join(dir, filename)

	// Pre-create the file
	require.NoError(t, os.WriteFile(localPath, []byte("sync wasm"), 0644))

	downloaded, cached, err := reg.StartupSync(context.Background(), []string{wasmURL})
	require.NoError(t, err)
	require.Equal(t, 0, downloaded)
	require.Equal(t, 1, cached)
}

func TestWASMRegistry_StartupSync_SkipsNonHTTPURLs(t *testing.T) {
	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	downloaded, cached, err := reg.StartupSync(context.Background(), []string{
		"file:///local/path.wasm",
		"AGFzbQEAAAA=", // base64 data
	})
	require.NoError(t, err)
	require.Equal(t, 0, downloaded)
	require.Equal(t, 0, cached)
}

// ---------------------------------------------------------------------------
// SyncLensWASM - various path types
// ---------------------------------------------------------------------------

func TestWASMRegistry_SyncLensWASM_Base64Path(t *testing.T) {
	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	lenses := []LensInfo{
		{Label: "base64lens", Path: "AGFzbQEAAAA="},
	}

	results, err := reg.SyncLensWASM(context.Background(), lenses)
	require.NoError(t, err)
	require.Equal(t, "AGFzbQEAAAA=", results["AGFzbQEAAAA="])
}

func TestWASMRegistry_SyncLensWASM_FilePrefix(t *testing.T) {
	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	lenses := []LensInfo{
		{Label: "localfile", Path: "file:///some/path.wasm"},
	}

	results, err := reg.SyncLensWASM(context.Background(), lenses)
	require.NoError(t, err)
	require.Equal(t, "file:///some/path.wasm", results["file:///some/path.wasm"])
}

// ---------------------------------------------------------------------------
// ListCachedWASM - with wasm and non-wasm files
// ---------------------------------------------------------------------------

func TestWASMRegistry_ListCachedWASM_WasmAndJsonFiles(t *testing.T) {
	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	// Create wasm files and a json file
	require.NoError(t, os.WriteFile(filepath.Join(dir, "x.wasm"), []byte("w1"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "y.wasm"), []byte("w2"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "z.wasm"), []byte("w3"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "views.json"), []byte("{}"), 0644))

	wasmFiles := reg.ListCachedWASM()
	require.Len(t, wasmFiles, 3)
}

// ---------------------------------------------------------------------------
// GetLocalPath / RegistryPath
// ---------------------------------------------------------------------------

func TestWASMRegistry_GetLocalPath_NotCached(t *testing.T) {
	reg, err := NewWASMRegistry(t.TempDir(), testLogger())
	require.NoError(t, err)

	_, exists := reg.GetLocalPath("https://example.com/notcached.wasm")
	require.False(t, exists)
}

func TestWASMRegistry_RegistryPath_ReturnsCorrectDir(t *testing.T) {
	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	require.Equal(t, dir, reg.RegistryPath())
	require.NotEmpty(t, reg.RegistryPath())
}
