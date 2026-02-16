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
		"./config.yaml",     // From project root
		"../config.yaml",    // From bin/ directory
		"../../config.yaml", // From pkg/host/ directory - test context
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
		panic(fmt.Errorf("Unable to load config: %v", err))
	}

	myHost, err := host.StartHosting(cfg)
	if err != nil {
		panic(fmt.Errorf("Failed to start hosting: %v", err))
	}

	defer myHost.Close(context.Background())

	for {
		time.Sleep(1 * time.Second) // Run forever unless stopped
	}
}
