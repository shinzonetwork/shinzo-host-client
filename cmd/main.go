package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
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

	// Start pprof server
	pprofPort := "6060"

	go func() {
		log.Printf("🔍 Starting pprof server on :%s", pprofPort)
		log.Printf("📊 Pprof endpoints available at:")
		log.Printf("   - CPU Profile: http://localhost:%s/debug/pprof/profile", pprofPort)
		log.Printf("   - Heap Profile: http://localhost:%s/debug/pprof/heap", pprofPort)
		log.Printf("   - Goroutines: http://localhost:%s/debug/pprof/goroutine", pprofPort)
		log.Printf("   - All Profiles: http://localhost:%s/debug/pprof/", pprofPort)

		if err := http.ListenAndServe(":"+pprofPort, nil); err != nil {
			log.Printf("❌ Failed to start pprof server: %v", err)
		}
	}()

	myHost, err := host.StartHosting(cfg)
	if err != nil {
		panic(fmt.Errorf("Failed to start hosting: %v", err))
	}

	defer myHost.Close(context.Background())

	// Add metrics endpoint to the existing pprof server
	http.Handle("/metrics", myHost.GetMetricsHandler())
	log.Printf("📈 Metrics endpoint available at: http://localhost:%s/metrics", pprofPort)

	for true {
		time.Sleep(1 * time.Second) // Run forever unless stopped
	}
}
