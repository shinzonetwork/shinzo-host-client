package logger

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sourcenetwork/corelog"
	"github.com/stretchr/testify/require"
)

// restoreConfig puts corelog's process-wide config back after a test mutates it.
func restoreConfig(t *testing.T) {
	t.Helper()
	saved := corelog.GetConfig("")
	t.Cleanup(func() { corelog.SetConfig(saved) })
}

// call runs the handler and returns the response recorder and the decoded level, which
// is empty for a response that carries no level body.
func call(t *testing.T, method, target string) (*httptest.ResponseRecorder, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	LevelHandler(rec, httptest.NewRequest(method, target, nil))

	var body struct {
		Level string `json:"level"`
	}
	if strings.HasPrefix(rec.Header().Get("Content-Type"), "application/json") {
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	}
	return rec, body.Level
}

func TestLevelHandler_GetReportsCurrentLevel(t *testing.T) {
	restoreConfig(t)
	corelog.SetConfig(corelog.Config{Level: corelog.LevelError})

	rec, level := call(t, http.MethodGet, LevelPath)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, corelog.LevelError, level)
}

func TestLevelHandler_PostSetsLevel(t *testing.T) {
	restoreConfig(t)
	corelog.SetConfig(corelog.Config{Level: corelog.LevelError})

	rec, level := call(t, http.MethodPost, LevelPath+"?level=info")
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, corelog.LevelInfo, level)
	require.Equal(t, corelog.LevelInfo, corelog.GetConfig("").Level)
}

func TestLevelHandler_PutSetsLevel(t *testing.T) {
	restoreConfig(t)
	corelog.SetConfig(corelog.Config{Level: corelog.LevelInfo})

	rec, level := call(t, http.MethodPut, LevelPath+"?level=error")
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, corelog.LevelError, level)
	require.Equal(t, corelog.LevelError, corelog.GetConfig("").Level)
}

// corelog silently treats an unrecognised level as info, so the handler has to reject
// one rather than report a level the loggers are not using.
func TestLevelHandler_RejectsUnknownLevel(t *testing.T) {
	restoreConfig(t)
	corelog.SetConfig(corelog.Config{Level: corelog.LevelError})

	for _, level := range []string{"debug", "warn", "INFO", ""} {
		rec, _ := call(t, http.MethodPost, LevelPath+"?level="+level)
		require.Equal(t, http.StatusBadRequest, rec.Code, level)
		require.Equal(t, corelog.LevelError, corelog.GetConfig("").Level, level)
	}
}

func TestLevelHandler_RejectsUnsupportedMethod(t *testing.T) {
	restoreConfig(t)

	rec, _ := call(t, http.MethodDelete, LevelPath)
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	require.Equal(t, "GET, POST, PUT", rec.Header().Get("Allow"))
}

// A level change must not reset the output settings, which are configured from the
// environment at startup and never sent by the caller.
func TestLevelHandler_PreservesOtherConfigFields(t *testing.T) {
	restoreConfig(t)
	corelog.SetConfig(corelog.Config{
		Level:            corelog.LevelError,
		Format:           corelog.FormatJSON,
		Output:           corelog.OutputStderr,
		EnableSource:     true,
		EnableStackTrace: true,
		DisableColor:     true,
	})

	rec, _ := call(t, http.MethodPost, LevelPath+"?level=info")
	require.Equal(t, http.StatusOK, rec.Code)

	cfg := corelog.GetConfig("")
	require.Equal(t, corelog.LevelInfo, cfg.Level)
	require.Equal(t, corelog.FormatJSON, cfg.Format)
	require.Equal(t, corelog.OutputStderr, cfg.Output)
	require.True(t, cfg.EnableSource)
	require.True(t, cfg.EnableStackTrace)
	require.True(t, cfg.DisableColor)
}

// The named override the host installs for the "http" logger must keep winning, or
// flipping the global level would un-quiet it.
func TestLevelHandler_LeavesNamedOverrideIntact(t *testing.T) {
	restoreConfig(t)
	corelog.SetConfig(corelog.Config{Level: corelog.LevelError})
	corelog.SetConfigOverride("http", corelog.Config{Level: corelog.LevelError})

	rec, _ := call(t, http.MethodPost, LevelPath+"?level=info")
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, corelog.LevelInfo, corelog.GetConfig("").Level)
	require.Equal(t, corelog.LevelError, corelog.GetConfig("http").Level)
}
