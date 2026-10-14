package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/resource"
)

// Uses t.Setenv, so it cannot run in parallel.
func TestSetupOTel(t *testing.T) {
	tests := []struct {
		name     string
		exporter string
		attrs    string
		wantErr  bool
		errIs    error
	}{
		{name: "disabled ignores malformed attributes", exporter: "", attrs: "service.instance.id"},
		{name: "valid config", exporter: "none", attrs: "service.instance.id=node-1"},
		{name: "malformed attributes", exporter: "none", attrs: "service.instance.id", wantErr: true, errIs: resource.ErrPartialResource},
		{name: "unknown exporter", exporter: "bogus", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("OTEL_METRICS_EXPORTER", tt.exporter)
			t.Setenv("OTEL_RESOURCE_ATTRIBUTES", tt.attrs)

			shutdown, err := setupOTel(t.Context())
			if !tt.wantErr {
				require.NoError(t, err)
				require.NoError(t, shutdown(t.Context()))
				return
			}
			require.Error(t, err)
			if tt.errIs != nil {
				assert.ErrorIs(t, err, tt.errIs)
			}
		})
	}
}
