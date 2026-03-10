package view

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// testLogger returns a no-op sugared logger suitable for tests.
func testLogger() *zap.SugaredLogger {
	l, _ := zap.NewDevelopment()
	return l.Sugar()
}

// ---------------------------------------------------------------------------
// NewWASMRegistry
// ---------------------------------------------------------------------------

func TestWASMRegistry_NewWASMRegistry_ValidPath(t *testing.T) {
	dir := t.TempDir()
	regPath := filepath.Join(dir, "wasm-cache")

	reg, err := NewWASMRegistry(regPath, testLogger())
	require.NoError(t, err)
	require.NotNil(t, reg)

	// Directory should have been created
	info, statErr := os.Stat(regPath)
	require.NoError(t, statErr)
	require.True(t, info.IsDir())
}

func TestWASMRegistry_NewWASMRegistry_InvalidPath(t *testing.T) {
	// /dev/null is a file, so creating a subdirectory under it must fail.
	reg, err := NewWASMRegistry("/dev/null/impossible", testLogger())
	require.Error(t, err)
	require.Nil(t, reg)
	require.Contains(t, err.Error(), "failed to create WASM registry directory")
}

// ---------------------------------------------------------------------------
// urlToFilename
// ---------------------------------------------------------------------------

func TestWASMRegistry_urlToFilename_WasmSuffix(t *testing.T) {
	reg, err := NewWASMRegistry(t.TempDir(), testLogger())
	require.NoError(t, err)

	url := "https://example.com/lenses/filter.wasm"
	name := reg.urlToFilename(url)

	hash := sha256.Sum256([]byte(url))
	prefix := hex.EncodeToString(hash[:4])
	require.Equal(t, prefix+"_filter.wasm", name)
}

func TestWASMRegistry_urlToFilename_NoWasmSuffix(t *testing.T) {
	reg, err := NewWASMRegistry(t.TempDir(), testLogger())
	require.NoError(t, err)

	url := "https://example.com/lenses/something"
	name := reg.urlToFilename(url)

	hash := sha256.Sum256([]byte(url))
	expected := hex.EncodeToString(hash[:]) + ".wasm"
	require.Equal(t, expected, name)
}

// ---------------------------------------------------------------------------
// cacheEntry / GetLocalPath
// ---------------------------------------------------------------------------

func TestWASMRegistry_cacheEntry_and_GetLocalPath_CacheHit(t *testing.T) {
	reg, err := NewWASMRegistry(t.TempDir(), testLogger())
	require.NoError(t, err)

	reg.cacheEntry("http://example.com/lens.wasm", "file:///tmp/cached.wasm")
	path, ok := reg.GetLocalPath("http://example.com/lens.wasm")
	require.True(t, ok)
	require.Equal(t, "file:///tmp/cached.wasm", path)
}

func TestWASMRegistry_GetLocalPath_CacheMiss(t *testing.T) {
	reg, err := NewWASMRegistry(t.TempDir(), testLogger())
	require.NoError(t, err)

	path, ok := reg.GetLocalPath("http://example.com/unknown.wasm")
	require.False(t, ok)
	require.Empty(t, path)
}

// ---------------------------------------------------------------------------
// RegistryPath
// ---------------------------------------------------------------------------

func TestWASMRegistry_RegistryPath(t *testing.T) {
	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)
	require.Equal(t, dir, reg.RegistryPath())
}

// ---------------------------------------------------------------------------
// EnsureWASM
// ---------------------------------------------------------------------------

func TestWASMRegistry_EnsureWASM_AlreadyInCache(t *testing.T) {
	reg, err := NewWASMRegistry(t.TempDir(), testLogger())
	require.NoError(t, err)

	reg.cacheEntry("http://example.com/lens.wasm", "file:///cached/lens.wasm")

	result, err := reg.EnsureWASM(context.Background(), "http://example.com/lens.wasm")
	require.NoError(t, err)
	require.Equal(t, "file:///cached/lens.wasm", result)
}

func TestWASMRegistry_EnsureWASM_FileOnDisk(t *testing.T) {
	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	wasmURL := "http://example.com/existing.wasm"
	filename := reg.urlToFilename(wasmURL)
	localFile := filepath.Join(dir, filename)

	// Pre-create the file on disk
	require.NoError(t, os.WriteFile(localFile, []byte("fake wasm"), 0644))

	result, err := reg.EnsureWASM(context.Background(), wasmURL)
	require.NoError(t, err)
	require.Contains(t, result, "file://")
	require.Contains(t, result, filename)

	// Should now be cached
	cached, ok := reg.GetLocalPath(wasmURL)
	require.True(t, ok)
	require.Equal(t, result, cached)
}

func TestWASMRegistry_EnsureWASM_NeedsDownload(t *testing.T) {
	wasmContent := []byte("fake wasm binary content")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(wasmContent)
	}))
	defer server.Close()

	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	wasmURL := server.URL + "/lens.wasm"
	result, err := reg.EnsureWASM(context.Background(), wasmURL)
	require.NoError(t, err)
	require.Contains(t, result, "file://")

	// Verify the file was actually written
	filename := reg.urlToFilename(wasmURL)
	data, readErr := os.ReadFile(filepath.Join(dir, filename))
	require.NoError(t, readErr)
	require.Equal(t, wasmContent, data)
}

func TestWASMRegistry_EnsureWASM_DownloadFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	_, err = reg.EnsureWASM(context.Background(), server.URL+"/fail.wasm")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to download WASM")
}

// ---------------------------------------------------------------------------
// EnsureAllWASM
// ---------------------------------------------------------------------------

func TestWASMRegistry_EnsureAllWASM_AllSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("wasm"))
	}))
	defer server.Close()

	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	urls := []string{
		server.URL + "/a.wasm",
		server.URL + "/b.wasm",
	}

	results, err := reg.EnsureAllWASM(context.Background(), urls)
	require.NoError(t, err)
	require.Len(t, results, 2)
	for _, u := range urls {
		require.Contains(t, results, u)
	}
}

func TestWASMRegistry_EnsureAllWASM_MixedSuccessAndFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/good.wasm" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("wasm"))
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	urls := []string{
		server.URL + "/good.wasm",
		server.URL + "/bad.wasm",
	}

	results, err := reg.EnsureAllWASM(context.Background(), urls)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to download 1 WASM files")
	// The successful one should still be in results
	require.Contains(t, results, server.URL+"/good.wasm")
	require.NotContains(t, results, server.URL+"/bad.wasm")
}

// ---------------------------------------------------------------------------
// SyncLensWASM
// ---------------------------------------------------------------------------

func TestWASMRegistry_SyncLensWASM_FileScheme(t *testing.T) {
	reg, err := NewWASMRegistry(t.TempDir(), testLogger())
	require.NoError(t, err)

	lenses := []LensInfo{
		{Label: "local", Path: "file:///path/to/local.wasm"},
	}

	result, err := reg.SyncLensWASM(context.Background(), lenses)
	require.NoError(t, err)
	require.Equal(t, "file:///path/to/local.wasm", result["file:///path/to/local.wasm"])
}

func TestWASMRegistry_SyncLensWASM_HttpScheme(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("wasm content"))
	}))
	defer server.Close()

	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	wasmURL := server.URL + "/remote.wasm"
	lenses := []LensInfo{
		{Label: "remote", Path: wasmURL},
	}

	result, err := reg.SyncLensWASM(context.Background(), lenses)
	require.NoError(t, err)
	require.Contains(t, result[wasmURL], "file://")
}

func TestWASMRegistry_SyncLensWASM_OtherPath(t *testing.T) {
	reg, err := NewWASMRegistry(t.TempDir(), testLogger())
	require.NoError(t, err)

	base64Data := "SGVsbG8gV29ybGQ="
	lenses := []LensInfo{
		{Label: "base64", Path: base64Data},
	}

	result, err := reg.SyncLensWASM(context.Background(), lenses)
	require.NoError(t, err)
	require.Equal(t, base64Data, result[base64Data])
}

func TestWASMRegistry_SyncLensWASM_HttpDownloadFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	reg, err := NewWASMRegistry(t.TempDir(), testLogger())
	require.NoError(t, err)

	lenses := []LensInfo{
		{Label: "bad-lens", Path: server.URL + "/fail.wasm"},
	}

	_, err = reg.SyncLensWASM(context.Background(), lenses)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to sync WASM for lens bad-lens")
}

// ---------------------------------------------------------------------------
// ListCachedWASM
// ---------------------------------------------------------------------------

func TestWASMRegistry_ListCachedWASM_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	files := reg.ListCachedWASM()
	require.Empty(t, files)
}

func TestWASMRegistry_ListCachedWASM_WithWasmFiles(t *testing.T) {
	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.wasm"), []byte("a"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "b.wasm"), []byte("b"), 0644))

	files := reg.ListCachedWASM()
	require.Len(t, files, 2)
	require.Contains(t, files, "a.wasm")
	require.Contains(t, files, "b.wasm")
}

func TestWASMRegistry_ListCachedWASM_MixedFiles(t *testing.T) {
	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(filepath.Join(dir, "lens.wasm"), []byte("w"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "readme.txt"), []byte("t"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data.json"), []byte("j"), 0644))

	files := reg.ListCachedWASM()
	require.Len(t, files, 1)
	require.Equal(t, "lens.wasm", files[0])
}

func TestWASMRegistry_ListCachedWASM_InvalidDir(t *testing.T) {
	reg := &WASMRegistry{
		registryPath: "/nonexistent/path/that/does/not/exist",
		logger:       testLogger(),
		cached:       make(map[string]string),
	}
	files := reg.ListCachedWASM()
	require.Nil(t, files)
}

// ---------------------------------------------------------------------------
// CleanupUnused
// ---------------------------------------------------------------------------

func TestWASMRegistry_CleanupUnused_RemovesUnused(t *testing.T) {
	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	// Create two WASM files on disk
	activeURL := "http://example.com/active.wasm"
	unusedURL := "http://example.com/unused.wasm"

	activeName := reg.urlToFilename(activeURL)
	unusedName := reg.urlToFilename(unusedURL)

	require.NoError(t, os.WriteFile(filepath.Join(dir, activeName), []byte("a"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, unusedName), []byte("u"), 0644))

	removed, err := reg.CleanupUnused([]string{activeURL})
	require.NoError(t, err)
	require.Equal(t, 1, removed)

	// Active file should still exist
	_, statErr := os.Stat(filepath.Join(dir, activeName))
	require.NoError(t, statErr)

	// Unused file should be gone
	_, statErr = os.Stat(filepath.Join(dir, unusedName))
	require.True(t, os.IsNotExist(statErr))
}

func TestWASMRegistry_CleanupUnused_KeepsAllActive(t *testing.T) {
	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	url1 := "http://example.com/a.wasm"
	url2 := "http://example.com/b.wasm"

	require.NoError(t, os.WriteFile(filepath.Join(dir, reg.urlToFilename(url1)), []byte("a"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, reg.urlToFilename(url2)), []byte("b"), 0644))

	removed, err := reg.CleanupUnused([]string{url1, url2})
	require.NoError(t, err)
	require.Equal(t, 0, removed)
}

func TestWASMRegistry_CleanupUnused_SkipsNonWasmFiles(t *testing.T) {
	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	// Place a non-wasm file -- should not be touched
	require.NoError(t, os.WriteFile(filepath.Join(dir, "config.json"), []byte("{}"), 0644))

	removed, err := reg.CleanupUnused([]string{})
	require.NoError(t, err)
	require.Equal(t, 0, removed)

	// Non-wasm file should still exist
	_, statErr := os.Stat(filepath.Join(dir, "config.json"))
	require.NoError(t, statErr)
}

func TestWASMRegistry_CleanupUnused_InvalidDir(t *testing.T) {
	reg := &WASMRegistry{
		registryPath: "/nonexistent/dir",
		logger:       testLogger(),
		cached:       make(map[string]string),
	}
	_, err := reg.CleanupUnused([]string{})
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// downloadWASM
// ---------------------------------------------------------------------------

func TestWASMRegistry_downloadWASM_Success(t *testing.T) {
	expected := []byte("wasm binary data here")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(expected)
	}))
	defer server.Close()

	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	dest := filepath.Join(dir, "downloaded.wasm")
	err = reg.downloadWASM(context.Background(), server.URL+"/test.wasm", dest)
	require.NoError(t, err)

	data, readErr := os.ReadFile(dest)
	require.NoError(t, readErr)
	require.Equal(t, expected, data)

	// Temp file should not linger
	_, statErr := os.Stat(dest + ".tmp")
	require.True(t, os.IsNotExist(statErr))
}

func TestWASMRegistry_downloadWASM_HTTPError(t *testing.T) {
	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	// Use an invalid URL that the HTTP client cannot reach
	dest := filepath.Join(dir, "error.wasm")
	err = reg.downloadWASM(context.Background(), "http://127.0.0.1:0/unreachable.wasm", dest)
	require.Error(t, err)
}

func TestWASMRegistry_downloadWASM_Non200Status(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()

	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	dest := filepath.Join(dir, "forbidden.wasm")
	err = reg.downloadWASM(context.Background(), server.URL+"/test.wasm", dest)
	require.Error(t, err)
	require.Contains(t, err.Error(), "HTTP 403")
}

func TestWASMRegistry_downloadWASM_CancelledContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("data"))
	}))
	defer server.Close()

	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	dest := filepath.Join(dir, "cancelled.wasm")
	err = reg.downloadWASM(ctx, server.URL+"/test.wasm", dest)
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// StartupSync
// ---------------------------------------------------------------------------

func TestWASMRegistry_StartupSync_CachedFiles(t *testing.T) {
	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	wasmURL := "https://example.com/cached.wasm"
	filename := reg.urlToFilename(wasmURL)
	// Pre-create the file on disk so it counts as cached
	require.NoError(t, os.WriteFile(filepath.Join(dir, filename), []byte("cached"), 0644))

	downloaded, cached, err := reg.StartupSync(context.Background(), []string{wasmURL})
	require.NoError(t, err)
	require.Equal(t, 0, downloaded)
	require.Equal(t, 1, cached)

	// Should now be in the in-memory cache
	path, ok := reg.GetLocalPath(wasmURL)
	require.True(t, ok)
	require.Contains(t, path, "file://")
}

func TestWASMRegistry_StartupSync_NeedsDownload(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("fresh wasm"))
	}))
	defer server.Close()

	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	wasmURL := server.URL + "/new.wasm"
	downloaded, cached, err := reg.StartupSync(context.Background(), []string{wasmURL})
	require.NoError(t, err)
	require.Equal(t, 1, downloaded)
	require.Equal(t, 0, cached)

	path, ok := reg.GetLocalPath(wasmURL)
	require.True(t, ok)
	require.Contains(t, path, "file://")
}

func TestWASMRegistry_StartupSync_NonURLPathsSkipped(t *testing.T) {
	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	urls := []string{
		"file:///local/lens.wasm",
		"base64encodeddata",
		"/absolute/path/lens.wasm",
	}

	downloaded, cached, err := reg.StartupSync(context.Background(), urls)
	require.NoError(t, err)
	require.Equal(t, 0, downloaded)
	require.Equal(t, 0, cached)
}

func TestWASMRegistry_StartupSync_DownloadFailureReportsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	downloaded, cached, err := reg.StartupSync(context.Background(), []string{server.URL + "/fail.wasm"})
	require.Error(t, err)
	require.Equal(t, 0, downloaded)
	require.Equal(t, 0, cached)
	require.Contains(t, err.Error(), "failed to download")
}

func TestWASMRegistry_StartupSync_MixedCachedAndDownload(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("wasm"))
	}))
	defer server.Close()

	dir := t.TempDir()
	reg, err := NewWASMRegistry(dir, testLogger())
	require.NoError(t, err)

	cachedURL := "https://example.com/already.wasm"
	cachedFilename := reg.urlToFilename(cachedURL)
	require.NoError(t, os.WriteFile(filepath.Join(dir, cachedFilename), []byte("c"), 0644))

	newURL := server.URL + "/fresh.wasm"

	downloaded, cached, err := reg.StartupSync(context.Background(), []string{cachedURL, newURL})
	require.NoError(t, err)
	require.Equal(t, 1, downloaded)
	require.Equal(t, 1, cached)
}

// ---------------------------------------------------------------------------
// AddViewsFromLensRegistry
// ---------------------------------------------------------------------------

func TestWASMRegistry_AddViewsFromLensRegistry_NoFile(t *testing.T) {
	dir := t.TempDir()
	views, err := AddViewsFromLensRegistry(dir)
	require.NoError(t, err)
	require.Empty(t, views)
}

func TestWASMRegistry_AddViewsFromLensRegistry_ValidFile(t *testing.T) {
	dir := t.TempDir()

	query := "SELECT * FROM logs"
	sdl := "type Log { msg: String }"
	testViews := []View{
		{Name: "view1", Query: &query, Sdl: &sdl},
		{Name: "view2"},
	}
	data, err := json.Marshal(testViews)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "views.json"), data, 0644))

	views, err := AddViewsFromLensRegistry(dir)
	require.NoError(t, err)
	require.Len(t, views, 2)
	require.Equal(t, "view1", views[0].Name)
	require.Equal(t, "view2", views[1].Name)
	require.Equal(t, &query, views[0].Query)
}

func TestWASMRegistry_AddViewsFromLensRegistry_InvalidJSON(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "views.json"), []byte("not json"), 0644))

	views, err := AddViewsFromLensRegistry(dir)
	require.Error(t, err)
	require.Nil(t, views)
	require.Contains(t, err.Error(), "failed to parse views.json")
}

func TestWASMRegistry_AddViewsFromLensRegistry_ReadError(t *testing.T) {
	// Point to a path where views.json exists but is unreadable.
	// Use a directory as the file -- reading it will fail.
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "views.json"), 0755))

	views, err := AddViewsFromLensRegistry(dir)
	require.Error(t, err)
	require.Nil(t, views)
}

// ---------------------------------------------------------------------------
// SaveViewToRegistry
// ---------------------------------------------------------------------------

func TestWASMRegistry_SaveViewToRegistry_NewViewAppended(t *testing.T) {
	dir := t.TempDir()

	v := View{Name: "new-view"}
	err := SaveViewToRegistry(dir, v)
	require.NoError(t, err)

	// Read back
	views, err := AddViewsFromLensRegistry(dir)
	require.NoError(t, err)
	require.Len(t, views, 1)
	require.Equal(t, "new-view", views[0].Name)
}

func TestWASMRegistry_SaveViewToRegistry_ExistingViewUpdated(t *testing.T) {
	dir := t.TempDir()

	q1 := "query1"
	q2 := "query2"
	v1 := View{Name: "my-view", Query: &q1}
	v2 := View{Name: "my-view", Query: &q2}

	require.NoError(t, SaveViewToRegistry(dir, v1))
	require.NoError(t, SaveViewToRegistry(dir, v2))

	views, err := AddViewsFromLensRegistry(dir)
	require.NoError(t, err)
	require.Len(t, views, 1)
	require.Equal(t, "my-view", views[0].Name)
	require.Equal(t, &q2, views[0].Query)
}

func TestWASMRegistry_SaveViewToRegistry_MultipleViews(t *testing.T) {
	dir := t.TempDir()

	require.NoError(t, SaveViewToRegistry(dir, View{Name: "a"}))
	require.NoError(t, SaveViewToRegistry(dir, View{Name: "b"}))
	require.NoError(t, SaveViewToRegistry(dir, View{Name: "c"}))

	views, err := AddViewsFromLensRegistry(dir)
	require.NoError(t, err)
	require.Len(t, views, 3)
}

func TestWASMRegistry_SaveViewToRegistry_InvalidDir(t *testing.T) {
	// Cannot write to a path under /dev/null
	err := SaveViewToRegistry("/dev/null/impossible", View{Name: "x"})
	require.Error(t, err)
}
