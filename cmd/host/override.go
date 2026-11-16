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
// keys in. Anything else (e.g. --help, --home) is skipped. Note this
// walks cmd.Flags(), which cobra merges with inherited persistent flags
// from parent commands at Execute time. That's fine today since root has
// no persistent flags, but worth remembering if one is ever added with a
// dot in its name. --chain is the one exception: it has no dot and
// becomes a whole "chains" override (see getChains). The result is
// ready to pass straight into hostconfig.Prepare or hostconfig.Load.
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

	if cmd.Flags().Changed("chain") {
		chains, err := getChains(cmd)
		if err != nil {
			return nil, err
		}
		overrides["chains"] = chains
	}

	return overrides, nil
}

// getChains turns --chain into a chains override, each with default settings.
func getChains(cmd *cobra.Command) (map[string]any, error) {
	// StringArray, not StringSlice: StringSlice drops a repeated --chain "".
	values, err := cmd.Flags().GetStringArray("chain")
	if err != nil {
		return nil, err
	}

	chains := map[string]any{}
	for _, value := range values {
		for id := range strings.SplitSeq(value, ",") {
			id = strings.TrimSpace(id)
			if id == "" {
				return nil, errEmptyChainFlag
			}
			chains[id] = map[string]any{}
		}
	}

	return chains, nil
}
