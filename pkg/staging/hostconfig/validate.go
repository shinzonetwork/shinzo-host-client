package hostconfig

import "regexp"

func validate(cfg Config) error {
	if cfg.Name == "" {
		return errEmptyName
	}

	// Name is capped at 63 characters (this leading character plus up to
	// 62 more), matching Kubernetes' DNS-1123 label limit. Borrowed as a
	// familiar, well established convention, since Name becomes part of
	// a directory path.
	if !regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,62}$`).MatchString(cfg.Name) {
		return errInvalidName
	}

	return nil
}
