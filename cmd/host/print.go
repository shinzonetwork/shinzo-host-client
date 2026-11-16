package main

import (
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"

	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostkey"
)

func printConfig(w io.Writer, cfg hostconfig.Config, nodeKeys hostkey.NodeKeys) error {
	if _, err := fmt.Fprintf(w, `Data dir:         %s
Config path:      %s
Key dir:          %s
Filter dir:       %s
Logger:           development=%t level=%s
HTTP:             addr=%s
Chains:           %s
`, cfg.DataDir, cfg.ConfigPath, cfg.KeyDir, cfg.FilterDir, cfg.Logger.Development, cfg.Logger.Level, cfg.HTTP.Addr, chainIDs(cfg)); err != nil {
		return err
	}

	shinzoAddr, err := nodeKeys.ShinzoAddress()
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(w, "Operator address: %s\n", shinzoAddr)
	return err
}

func chainIDs(cfg hostconfig.Config) string {
	if len(cfg.Chains) == 0 {
		return "none"
	}
	return strings.Join(slices.Sorted(maps.Keys(cfg.Chains)), ", ")
}
