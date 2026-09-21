package hostconfig

import "regexp"

// Validate checks cfg for problems that would stop the host from starting.
func Validate(cfg Config) error {
	if cfg.Name == "" {
		return errEmptyName
	}

	if !regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,62}$`).MatchString(cfg.Name) {
		return errInvalidName
	}

	return nil
}
