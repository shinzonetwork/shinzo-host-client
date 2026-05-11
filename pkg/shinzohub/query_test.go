package shinzohub

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/shinzonetwork/viewbundle-go"
	"github.com/stretchr/testify/require"
)

// makeTestBundle returns a valid base64-encoded view bundle for a minimal view.
// The wasm bytes are nonsense (just enough to round-trip through the bundler);
// downstream tests don't actually run the lens.
func makeTestBundle(t *testing.T, name string) string {
	t.Helper()
	wasm := []byte{0x00, 0x61, 0x73, 0x6D, 0x01, 0x00, 0x00, 0x00}
	v := viewbundle.View{
		Query: `Ethereum__Mainnet__Log { address }`,
		Sdl:   fmt.Sprintf("type %s @materialized(if: false) { address: String }", name),
		Transform: viewbundle.Transform{
			Lenses: []viewbundle.Lens{
				{Path: base64.StdEncoding.EncodeToString(wasm)},
			},
		},
	}
	wire, err := viewbundle.NewBundler().BundleView(v)
	require.NoError(t, err)
	return base64.StdEncoding.EncodeToString(wire)
}

func TestGetViewBundle_Success(t *testing.T) {
	wantBundle := makeTestBundle(t, testViewName)
	wantContract := "0x6814589574569e6c25e72EB52f62aFaDDf5eF14D"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "/shinzonetwork/view/v1/views/"+wantContract, r.URL.Path)
		require.Equal(t, "true", r.URL.Query().Get("include_data"))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			lcdFieldView: LCDView{
				Name:            testViewName,
				Creator:         "shinzo1abc",
				ContractAddress: wantContract,
				Data:            wantBundle,
				Height:          "12345",
			},
		})
	}))
	defer srv.Close()

	c := NewRPCClient(srv.URL, nil)
	got, err := c.GetViewBundle(context.Background(), wantContract)
	require.NoError(t, err)
	require.Equal(t, wantBundle, got)
}

func TestGetViewBundle_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"code":5,"message":"view not found","details":[]}`))
	}))
	defer srv.Close()

	c := NewRPCClient(srv.URL, nil)
	_, err := c.GetViewBundle(context.Background(), "0xdeadbeef")
	require.Error(t, err)
	require.Contains(t, err.Error(), "HTTP 404")
}

func TestGetViewBundle_LCDNotConfigured(t *testing.T) {
	c := NewRPCClient("", nil)
	_, err := c.GetViewBundle(context.Background(), "0xabc")
	require.Error(t, err)
	require.Contains(t, err.Error(), "LCD URL not configured")
}

func TestGetViewBundle_EmptyData(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			lcdFieldView: LCDView{
				Name:            testViewName,
				ContractAddress: "0xabc",
				// Data deliberately empty
				Height: "1",
			},
		})
	}))
	defer srv.Close()

	c := NewRPCClient(srv.URL, nil)
	_, err := c.GetViewBundle(context.Background(), "0xabc")
	require.ErrorIs(t, err, ErrLCDEmptyData)
}

func TestFetchAllRegisteredViews_SinglePage(t *testing.T) {
	bundleA := makeTestBundle(t, testViewNameA)
	bundleB := makeTestBundle(t, testViewNameB)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/shinzonetwork/view/v1/views", r.URL.Path)
		require.Equal(t, "true", r.URL.Query().Get("include_data"))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			lcdFieldViews: []LCDView{
				{Name: testViewNameA, Creator: "shinzo1a", ContractAddress: "0xA", Data: bundleA, Height: "1"},
				{Name: testViewNameB, Creator: "shinzo1b", ContractAddress: "0xB", Data: bundleB, Height: "2"},
			},
			lcdFieldPagination: map[string]any{lcdFieldNextKey: nil, "total": "0"},
		})
	}))
	defer srv.Close()

	c := NewRPCClient(srv.URL, nil)
	views, count, err := c.FetchAllRegisteredViews(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, count)
	require.Len(t, views, 2)
	require.Equal(t, testViewNameA, views[0].Name)
	require.Equal(t, testViewNameB, views[1].Name)
}

func TestFetchAllRegisteredViews_Paginated(t *testing.T) {
	bundleA := makeTestBundle(t, testViewNameA)
	bundleB := makeTestBundle(t, testViewNameB)
	bundleC := makeTestBundle(t, "ViewC")
	const pageOneNextKey = "cursor-AAA"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Query().Get("pagination.key") {
		case "":
			_ = json.NewEncoder(w).Encode(map[string]any{
				lcdFieldViews:      []LCDView{{Name: testViewNameA, ContractAddress: "0xA", Data: bundleA}},
				lcdFieldPagination: map[string]any{lcdFieldNextKey: pageOneNextKey},
			})
		case pageOneNextKey:
			_ = json.NewEncoder(w).Encode(map[string]any{
				lcdFieldViews: []LCDView{
					{Name: testViewNameB, ContractAddress: "0xB", Data: bundleB},
					{Name: "ViewC", ContractAddress: "0xC", Data: bundleC},
				},
				lcdFieldPagination: map[string]any{lcdFieldNextKey: nil},
			})
		default:
			t.Errorf("unexpected pagination.key: %s", r.URL.Query().Get("pagination.key"))
		}
	}))
	defer srv.Close()

	c := NewRPCClient(srv.URL, nil)
	views, count, err := c.FetchAllRegisteredViews(context.Background())
	require.NoError(t, err)
	require.Equal(t, 3, count)
	require.Len(t, views, 3)
	require.Equal(t, []string{testViewNameA, testViewNameB, "ViewC"}, []string{views[0].Name, views[1].Name, views[2].Name})
}

func TestFetchAllRegisteredViews_SkipsMalformedEntries(t *testing.T) {
	good := makeTestBundle(t, "GoodView")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			lcdFieldViews: []LCDView{
				{Name: "EmptyData", ContractAddress: "0x1", Data: ""},
				{Name: "GoodView", ContractAddress: "0x2", Data: good},
				{Name: "BadBase64", ContractAddress: "0x3", Data: "not valid base64!!!"},
			},
			lcdFieldPagination: map[string]any{lcdFieldNextKey: nil},
		})
	}))
	defer srv.Close()

	c := NewRPCClient(srv.URL, nil)
	views, count, err := c.FetchAllRegisteredViews(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, count, "only the well-formed view should be returned")
	require.Len(t, views, 1)
	require.Equal(t, "GoodView", views[0].Name)
}

func TestFetchAllRegisteredViews_LCDNotConfigured(t *testing.T) {
	c := NewRPCClient("", nil)
	_, _, err := c.FetchAllRegisteredViews(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "LCD URL not configured")
}

func TestFetchAllRegisteredViews_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("upstream busted"))
	}))
	defer srv.Close()

	c := NewRPCClient(srv.URL, nil)
	_, _, err := c.FetchAllRegisteredViews(context.Background())
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "HTTP 500"), "expected HTTP 500 in error, got: %v", err)
}
