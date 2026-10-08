package hostconfig

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// validateLogger tested directly, independent of validate/Config, so a
// failure here points straight at the level check itself rather than
// leaving it ambiguous whether the problem is the check or the delegation
// from validate. TestValidateDelegatesToValidateLogger separately proves
// validate actually calls this, so there's no need to repeat this whole
// table again through validate(cfg).
func TestValidateLoggerDirectly(t *testing.T) {
	cases := []struct {
		desc  string
		level string
		pass  bool
	}{
		{"debug", "debug", true},
		{"info", "info", true},
		{"warn", "warn", true},
		{"error", "error", true},
		{"empty", "", false},
		{"unknown level", "bogus", false},
		{"uppercase not accepted", "DEBUG", false},
		{"trailing space not accepted", "debug ", false},
	}

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			err := validateLogger(LoggerConfig{Level: c.level})

			if c.pass {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, errInvalidLevel)
			}
		})
	}
}

// Confirms validate actually delegates to validateLogger, rather than
// just happening to return some other error for the same input.
func TestValidateDelegatesToValidateLogger(t *testing.T) {
	cfg := defaultConfig()
	cfg.Logger.Level = "bogus"

	err := validate(cfg)
	require.ErrorIs(t, err, errInvalidLevel)
	require.Equal(t, validateLogger(cfg.Logger), err)
}

func TestValidateHTTPAddr(t *testing.T) {
	cases := []struct {
		desc string
		addr string
		pass bool
	}{
		{"port only", ":8080", true},
		{"host and port", "0.0.0.0:8080", true},
		{"hostname and port", "localhost:3000", true},
		{"empty", "", false},
		{"missing port", "justahost", false},
		{"missing port, free text", "not an address", false},
		{"too many colons", "garbage:::", false},
		{"non-numeric port", ":notaport", false},
		{"port out of range", ":99999999", false},
	}

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			err := validateHTTP(HTTPConfig{Addr: c.addr})

			if c.pass {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, errInvalidAddr)
			}
		})
	}
}

func TestValidateDelegatesToValidateHTTP(t *testing.T) {
	cfg := defaultConfig()
	cfg.HTTP.Addr = "not an address"

	err := validate(cfg)
	require.ErrorIs(t, err, errInvalidAddr)
	require.Equal(t, validateHTTP(cfg.HTTP), err)
}
