package main

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// getOverrides reads back whichever flags the caller actually changed
// from their default. An override flag is any flag named with a dot
// (e.g. "logger.level"), the same shape hostconfig expects its override
// keys in; anything else (e.g. --help, --home) is skipped. Note this
// walks cmd.Flags(), which cobra merges with inherited persistent flags
// from parent commands at Execute time — fine today since root has no
// persistent flags, but worth remembering if one is ever added with a dot
// in its name. The result is ready to pass straight into
// hostconfig.Setup or hostconfig.Load.
func getOverrides(cmd *cobra.Command) (map[string]any, error) {
	overrides := map[string]any{}

	var visitErr error

	cmd.Flags().Visit(func(f *pflag.Flag) {
		if visitErr != nil || !strings.Contains(f.Name, ".") {
			return
		}

		switch f.Value.Type() {
		case "bool":
			v, err := cmd.Flags().GetBool(f.Name)
			if err != nil {
				visitErr = err
				return
			}
			overrides[f.Name] = v
		case "string":
			v, err := cmd.Flags().GetString(f.Name)
			if err != nil {
				visitErr = err
				return
			}
			overrides[f.Name] = v
		default:
			visitErr = fmt.Errorf("%w: --%s is a %s flag", errUnsupportedOverrideFlagType, f.Name, f.Value.Type())
		}
	})

	if visitErr != nil {
		return nil, visitErr
	}

	return overrides, nil
}
