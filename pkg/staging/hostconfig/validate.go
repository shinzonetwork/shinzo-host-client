package hostconfig

import (
	"net"
	"strconv"
)

func validate(cfg Config) error {
	if err := validateLogger(cfg.Logger); err != nil {
		return err
	}

	if err := validateHTTP(cfg.HTTP); err != nil {
		return err
	}

	return nil
}

func validateLogger(lc LoggerConfig) error {
	switch lc.Level {
	case logLevelDebug, logLevelInfo, logLevelWarn, logLevelError:
		return nil
	default:
		return errInvalidLevel
	}
}

func validateHTTP(hc HTTPConfig) error {
	_, port, err := net.SplitHostPort(hc.Addr)
	if err != nil {
		return errInvalidAddr
	}

	// SplitHostPort only checks the host:port shape, it doesn't check that
	// port is actually a number, e.g. ":notaport" passes it. ParseUint
	// with a 16-bit size catches that and rejects anything outside the
	// valid port range (0-65535) in one step.
	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		return errInvalidAddr
	}

	return nil
}
