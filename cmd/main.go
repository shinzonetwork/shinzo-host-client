package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/shinzonetwork/shinzo-host-client/config"
	"github.com/shinzonetwork/shinzo-host-client/pkg/host"
)

func findConfigFile() string {
	possiblePaths := []string{
		"./config/config.yaml", // From project root
		"./config.yaml",        // Docker / mounted path
		"../config.yaml",       // From bin/ directory
	}

	for _, path := range possiblePaths {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}

	return "config.yaml"
}

func main() {
	configPath := findConfigFile()
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		panic(fmt.Errorf("unable to load config: %w", err))
	}

	myHost, err := host.StartHosting(cfg)
	if err != nil {
		panic(fmt.Errorf("failed to start hosting: %w", err))
	}

	defer func() { _ = myHost.Close(context.Background()) }()

	for {
		time.Sleep(1 * time.Second) // Run forever unless stopped
	}
}
