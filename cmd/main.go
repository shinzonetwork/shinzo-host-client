package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/shinzonetwork/shinzo-host-client/config"
	"github.com/shinzonetwork/shinzo-host-client/pkg/host"
)

// shutdownTimeout bounds Close. It has to stay under the container's stop grace period,
// or the process is killed part-way through the shutdown.
const shutdownTimeout = 30 * time.Second

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
	// Registered before startup so a signal arriving during it is not left unhandled.
	// The buffer holds it until the shutdown below.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	configPath := findConfigFile()
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		panic(fmt.Errorf("unable to load config: %w", err))
	}

	myHost, err := host.StartHosting(cfg)
	if err != nil {
		panic(fmt.Errorf("failed to start hosting: %w", err))
	}

	// Shutdown is signal-driven because a deferred Close does not run when the process
	// is terminated by a signal.
	sig := <-stop
	fmt.Fprintf(os.Stderr, "received %s, shutting down\n", sig)

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := myHost.Close(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "shutdown error: %v\n", err)
	}
}
