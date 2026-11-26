package host

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/verifier"
)

// The documents of every generator that answers are merged, each ID once, and a generator that does
// not answer, no longer stores the heights or answers over the cap is skipped. The listing fails
// only when no generator answers, wrapping verifier.ErrNotStored when every generator answers that
// it no longer stores the heights.
func TestGeneratorDocuments(t *testing.T) {
	serve := func(listed map[string][]string) string {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != documentsEndpoint || r.URL.Query().Get("from") != "10" || r.URL.Query().Get("to") != "12" {
				http.Error(w, "unexpected request", http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(listed)
		}))
		t.Cleanup(srv.Close)
		return srv.URL
	}
	a := serve(map[string][]string{"Header": {"h1"}, "BlockSignature": {"s1"}})
	b := serve(map[string][]string{"Header": {"h1"}, "BlockSignature": {"s2"}})
	answer := func(status int, body string) string {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
		}))
		t.Cleanup(srv.Close)
		return srv.URL
	}
	down := answer(http.StatusServiceUnavailable, "")
	gone := answer(http.StatusGone, `{"error":"gone","message":"heights below 500 are not stored","lowestHeight":500}`)
	tooLarge := answer(http.StatusOK, `{"Header":["`+strings.Repeat("a", maxDocumentsAnswerBytes)+`"]}`)

	cases := []struct {
		name       string
		generators []string
		want       map[string][]string
		wantErr    error
		notStored  bool
	}{
		{name: "merged across generators", generators: []string{a, b}, want: map[string][]string{"Header": {"h1"}, "BlockSignature": {"s1", "s2"}}},
		{name: "a generator that does not answer is skipped", generators: []string{down, a}, want: map[string][]string{"Header": {"h1"}, "BlockSignature": {"s1"}}},
		{name: "a generator that no longer stores the heights is skipped", generators: []string{gone, a}, want: map[string][]string{"Header": {"h1"}, "BlockSignature": {"s1"}}},
		{name: "an answer over the cap is skipped", generators: []string{tooLarge, a}, want: map[string][]string{"Header": {"h1"}, "BlockSignature": {"s1"}}},
		{name: "no generator answers", generators: []string{down}, wantErr: errDocumentsRefused},
		{name: "the only answer is over the cap", generators: []string{tooLarge}, wantErr: errDocumentsTooLarge},
		{name: "no generator stores the heights", generators: []string{gone, gone}, wantErr: errDocumentsNotStored, notStored: true},
		{name: "one generator no longer stores the heights and the other does not answer", generators: []string{gone, down}, wantErr: errDocumentsRefused},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var generators []chain.Generator
			for _, url := range tc.generators {
				generators = append(generators, chain.Generator{URL: url})
			}

			got, err := generatorDocuments(generators, http.DefaultClient)(context.Background(), 10, 12)

			if tc.want == nil {
				require.ErrorIs(t, err, tc.wantErr)
				require.Equal(t, tc.notStored, errors.Is(err, verifier.ErrNotStored))
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
