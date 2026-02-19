package host

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	appConfig "github.com/shinzonetwork/shinzo-app-sdk/pkg/config"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/rustffi"
	"github.com/shinzonetwork/shinzo-host-client/config"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	localschema "github.com/shinzonetwork/shinzo-host-client/pkg/schema"
	"github.com/shinzonetwork/shinzo-host-client/pkg/server"
)

// initRustFFI creates and initializes the Rust FFI client for the host.
func initRustFFI(cfg *config.Config) (*rustffi.Client, error) {
	datastoreBackend := os.Getenv("STORE")
	if datastoreBackend == "" {
		datastoreBackend = "fjall"
	}

	rustCfg := &appConfig.RustFFIConfig{
		DBPath:           cfg.DefraDB.Store.Path,
		InMemory:         false,
		DatastoreBackend: datastoreBackend,
	}

	client, err := rustffi.NewClient(rustCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create Rust FFI client: %w", err)
	}

	schemaSDL := localschema.GetSchemaForBuild()
	if err := client.ApplySchema(schemaSDL); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to apply schema via FFI: %w", err)
	}

	logger.Sugar.Info("Rust FFI client initialized with schema")
	return client, nil
}

// waitForRustFFI polls the FFI client until the schema is ready.
func waitForRustFFI(client *rustffi.Client) error {
	logger.Sugar.Info("Waiting for Rust FFI node...")
	maxAttempts := 30
	query := fmt.Sprintf(`query { %s { __typename } }`, constants.CollectionBlock)

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		resp, err := client.Query(query)
		if err == nil && resp != "" {
			logger.Sugar.Info("Rust FFI node is responsive")
			return nil
		}

		if attempt < maxAttempts {
			logger.Sugar.Debugf("FFI attempt %d failed, retrying...", attempt)
			time.Sleep(1 * time.Second)
		}
	}

	return fmt.Errorf("Rust FFI node failed to become ready after %d attempts", maxAttempts)
}

// ffiQuerySingle executes a GraphQL query via FFI and returns a single result.
func ffiQuerySingle[T any](client *rustffi.Client, query string) (T, error) {
	var result T
	wrapped := wrapFFIQuery(query)

	resp, err := client.Query(wrapped)
	if err != nil {
		return result, fmt.Errorf("FFI query failed: %w", err)
	}

	data, err := extractFFIQueryData(resp)
	if err != nil {
		return result, err
	}

	// Marshal back to JSON and unmarshal into the target type
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return result, fmt.Errorf("failed to marshal FFI data: %w", err)
	}

	if err := json.Unmarshal(dataBytes, &result); err != nil {
		return result, fmt.Errorf("failed to unmarshal FFI data: %w", err)
	}
	return result, nil
}

// ffiQueryArray executes a GraphQL query via FFI and returns an array result.
func ffiQueryArray[T any](client *rustffi.Client, query string) ([]T, error) {
	var result []T
	wrapped := wrapFFIQuery(query)

	resp, err := client.Query(wrapped)
	if err != nil {
		return nil, fmt.Errorf("FFI query failed: %w", err)
	}

	data, err := extractFFIQueryData(resp)
	if err != nil {
		return nil, err
	}

	dataBytes, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal FFI data: %w", err)
	}

	if err := json.Unmarshal(dataBytes, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal FFI data: %w", err)
	}
	return result, nil
}

// wrapFFIQuery wraps a query string with "query { ... }" if needed.
func wrapFFIQuery(query string) string {
	trimmed := strings.TrimSpace(query)
	if strings.HasPrefix(trimmed, "query") || strings.HasPrefix(trimmed, "mutation") {
		return trimmed
	}
	return fmt.Sprintf("query { %s }", trimmed)
}

// extractFFIQueryData parses the FFI JSON response and extracts the first data field.
func extractFFIQueryData(resp string) (interface{}, error) {
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(resp), &parsed); err != nil {
		return nil, fmt.Errorf("failed to parse FFI response: %w", err)
	}

	if errs, ok := parsed["errors"]; ok {
		return nil, fmt.Errorf("GraphQL error: %v", errs)
	}

	data, ok := parsed["data"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("no data field in FFI response")
	}

	// Return the first value in the data map (the collection result)
	for _, v := range data {
		return v, nil
	}
	return nil, fmt.Errorf("empty data in FFI response")
}

// ffiExecRequest executes a raw GraphQL request via FFI and returns parsed data.
func ffiExecRequest(client *rustffi.Client, query string) (interface{}, error) {
	resp, err := client.Query(query)
	if err != nil {
		return nil, fmt.Errorf("FFI exec failed: %w", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(resp), &parsed); err != nil {
		return nil, fmt.Errorf("failed to parse FFI response: %w", err)
	}

	if errs, ok := parsed["errors"]; ok {
		return nil, fmt.Errorf("GraphQL error: %v", errs)
	}

	return parsed["data"], nil
}

// startFFIHost creates a Host running in Rust FFI mode.
func startFFIHost(cfg *config.Config) (*Host, error) {
	logger.Init(cfg.Logger.Development, "./logs")
	logger.Sugar.Info("Starting host in Rust FFI mode...")

	rustClient, err := initRustFFI(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Rust FFI: %w", err)
	}

	if err := waitForRustFFI(rustClient); err != nil {
		rustClient.Close()
		return nil, fmt.Errorf("Rust FFI node failed readiness check: %w", err)
	}

	logger.Sugar.Info("Rust FFI node ready, schema applied")

	return &Host{
		DefraNode:              nil, // No Go node in FFI mode
		NetworkHandler:         nil, // P2P handled by Rust node
		rustClient:             rustClient,
		webhookCleanupFunction: func() {},
		LensRegistryPath:       cfg.HostConfig.LensRegistryPath,
		processingCancel:       func() {},
		config:                 cfg,
		metrics:                nil, // Will be initialized by caller
	}, nil
}

// startFFIHostFull creates a fully-initialized Host running in Rust FFI mode,
// including health server and processing pipeline.
func startFFIHostFull(cfg *config.Config) (*Host, error) {
	newHost, err := startFFIHost(cfg)
	if err != nil {
		return nil, err
	}

	newHost.metrics = server.NewHostMetrics()

	// Initialize processing pipeline
	queueSize := cfg.Shinzo.CacheQueueSize
	if queueSize <= 0 {
		queueSize = 50000
	}

	newHost.processingPipeline = NewProcessingPipeline(
		context.Background(), newHost, queueSize,
		cfg.Shinzo.BatchWriterCount, cfg.Shinzo.BatchSize, cfg.Shinzo.BatchFlushInterval,
		cfg.Shinzo.UseBatchSignatures,
	)
	newHost.processingPipeline.Start()

	// Start health server
	port := cfg.HostConfig.HealthServerPort
	if port == 0 {
		port = 8080
	}
	newHost.healthServer = server.NewHealthServer(port, newHost, "", newHost.metrics)
	go func() {
		if err := newHost.healthServer.Start(); err != nil {
			logger.Sugar.Errorf("Health server failed: %v", err)
		}
	}()

	logger.Sugar.Infof("Host running in Rust FFI mode (health: http://localhost:%d/health)", port)
	return newHost, nil
}

// IsFFIMode returns true if the host is running with the Rust FFI backend.
func (h *Host) IsFFIMode() bool {
	return h.rustClient != nil
}

// FFIQuery executes a GraphQL query in FFI mode and returns the raw JSON response.
func (h *Host) FFIQuery(ctx context.Context, query string) (string, error) {
	if h.rustClient == nil {
		return "", fmt.Errorf("host is not in FFI mode")
	}
	return h.rustClient.Query(query)
}
