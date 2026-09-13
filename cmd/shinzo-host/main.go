package main

import (
	"fmt"
	"os"

	"github.com/shinzonetwork/shinzo-host-client/cmd/shinzo-host/internal/cli"
)

func main() {
	if err := cli.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
