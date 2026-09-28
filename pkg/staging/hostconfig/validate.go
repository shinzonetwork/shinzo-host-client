package hostconfig

import "regexp"

func validate(cfg Config) error {
	if cfg.Name == "" {
		return errEmptyName
	}

	// Name is capped at 63 characters (this leading character plus up to
	// 62 more), matching Kubernetes' DNS-1123 label length limit, borrowed
	// as a familiar, well established convention since Name becomes part
	// of a directory path. Lowercase only: macOS and Windows filesystems
	// are case-insensitive by default, so "Host1" and "host1" would
	// otherwise collide on one directory. Underscores and a trailing
	// hyphen are still allowed, unlike a real DNS-1123 label, since Name
	// only needs to be filesystem-safe, not a valid DNS label.
	if !regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`).MatchString(cfg.Name) {
		return errInvalidName
	}

	return nil
}
