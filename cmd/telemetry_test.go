package main

import (
	"errors"
	"testing"

	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
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

	origLogger := logger.Sugar
	t.Cleanup(func() { logger.Sugar = origLogger })

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("OTEL_METRICS_EXPORTER", tt.exporter)
			t.Setenv("OTEL_RESOURCE_ATTRIBUTES", tt.attrs)
			core, logs := observer.New(zap.ErrorLevel)
			logger.Sugar = zap.New(core).Sugar()

			shutdown, err := setupOTel(t.Context())
			if !tt.wantErr {
				require.NoError(t, err)
				otel.Handle(errors.New("export failed")) //nolint:err113
				assert.Equal(t, 1, logs.FilterMessage("OpenTelemetry: export failed").Len(), "OTel errors must go to the logger")
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
