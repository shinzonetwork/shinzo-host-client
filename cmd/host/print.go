package main

import (
	"fmt"
	"io"

	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostconfig"
)

func printConfig(w io.Writer, cfg hostconfig.Config) error {
	_, err := fmt.Fprintf(w, `Data dir:    %s
Config path: %s
Key dir:     %s
Filter dir:  %s
Logger:      development=%t level=%s
HTTP:        addr=%s
`, cfg.DataDir, cfg.ConfigPath, cfg.KeyDir, cfg.FilterDir, cfg.Logger.Development, cfg.Logger.Level, cfg.HTTP.Addr)

	return err
}
