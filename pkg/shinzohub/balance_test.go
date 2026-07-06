package shinzohub

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGetQueryBalance(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		status  int
		want    string
		wantErr bool
	}{
		{"funded", `{"amount":"1500"}`, http.StatusOK, "1500", false},
		{"zero", `{"amount":"0"}`, http.StatusOK, "0", false},
		{"omitted amount is zero", `{}`, http.StatusOK, "0", false},
		{"not found", `not found`, http.StatusNotFound, "", true},
		{"non-numeric amount", `{"amount":"abc"}`, http.StatusOK, "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if want := "/shinzonetwork/querybalance/v1/balance/shinzo1abc"; r.URL.Path != want {
					t.Errorf("request path = %q, want %q", r.URL.Path, want)
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()

			got, err := NewRPCClient(srv.URL, nil).GetQueryBalance(context.Background(), "shinzo1abc")
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.String() != tc.want {
				t.Errorf("balance = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestGetQueryBalanceNoLCDConfigured(t *testing.T) {
	if _, err := NewRPCClient("", nil).GetQueryBalance(context.Background(), "shinzo1abc"); err == nil {
		t.Fatal("expected an error when no LCD URL is set, got nil")
	}
}
