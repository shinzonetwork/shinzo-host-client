package main

import (
	"fmt"
	"os"

	"github.com/shinzonetwork/host/config"
	"github.com/shinzonetwork/host/pkg/host"
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

	err = host.StartHosting(cfg)
	if err != nil {
		panic(fmt.Errorf("Failed to start hosting: %v", err))
	}
}
