package defradb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/shinzonetwork/shinzo-host-client/pkg/defracontext"
	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
	"github.com/sourcenetwork/defradb/acp/identity"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/client/options"
	"github.com/sourcenetwork/defradb/crypto"
	"github.com/sourcenetwork/defradb/keyring"
	"github.com/sourcenetwork/defradb/node"
)

// DefaultConfig provides the default configuration used when no config is supplied.
var DefaultConfig = &Config{ //nolint:gochecknoglobals
	DefraDB: DefraDBConfig{
		URL:           "http://localhost:9181",
		KeyringSecret: os.Getenv("DEFRA_KEYRING_SECRET"),
		P2P: DefraP2PConfig{
			Enabled:             true,
			BootstrapPeers:      requiredPeers,
			ListenAddr:          defaultListenAddress,
			MaxRetries:          MaxRetries,
			RetryBaseDelayMs:    RetryBaseDelayMs,
			ReconnectIntervalMs: ReconnectIntervalsMs,
			EnableAutoReconnect: true,
		},
		Store: DefraStoreConfig{
			Path: ".defra",
		},
	},
	Logger: LoggerConfig{
		Development: false,
		LogsDir:     "./logs",
	},
}

//nolint:gochecknoglobals
var requiredPeers = []string{} // default bootstrap peers used to initialize the P2P network configuration
const (
	// defaultListenAddress is the default multiaddr on which the P2P node listens for incoming connections.
	defaultListenAddress string = "/ip4/127.0.0.1/tcp/9171"
	// NodeIdentityKeyName is the key name used to store and retrieve the node identity key from the keyring.
	NodeIdentityKeyName string = "node-identity-key"
)

// Key Management Implementation Notes:
//
// This implementation provides persistent DefraDB identity management using the keyring:
// 1. Extracting private key bytes from generated FullIdentity
// 2. Storing the raw key bytes in encrypted keyring storage (file-based keyring)
// 3. Reconstructing the same identity from stored private key bytes on subsequent runs
// 4. Ensuring the same cryptographic identity is used across application restarts
//
// Current Status: FULLY FUNCTIONAL
// - Private keys are properly extracted and stored in keyring
// - Identities are reconstructed from keyring, maintaining consistency
// - Keys are encrypted using PBES2_HS512_A256KW algorithm
// - Comprehensive error handling and logging
//
// Security Features:
// - Keys stored in encrypted keyring (default: {storePath}/keys/)
// - Encryption key derived from KeyringSecret
// - Proper error handling for corrupted or missing keys
// - Requires DEFRA_KEYRING_SECRET environment variable or config

// OpenKeyring opens the file-based keyring at {Store.Path}/keys using
// KeyringSecret as the encryption secret. Returns an error if cfg is nil or
// KeyringSecret is empty; callers that want a "no keyring → fall back" flow
// should detect that error at their own call site rather than relying on a
// silent nil return.
func OpenKeyring(cfg *Config) (keyring.Keyring, error) {
	if cfg == nil {
		return nil, ErrEmptyConfig
	}
	if cfg.DefraDB.KeyringSecret == "" {
		return nil, ErrNilKeyringSecret
	}

	// Use file-based keyring (default for DefraDB)
	// Keyring path defaults to "keys" directory in store path, or "keys" in current dir
	keyringPath := filepath.Join(cfg.DefraDB.Store.Path, "keys")
	if cfg.DefraDB.Store.Path == "" {
		keyringPath = "keys"
	}

	// Ensure directory exists
	if err := os.MkdirAll(keyringPath, 0o750); err != nil { //nolint:mnd
		return nil, err
	}

	secret := []byte(cfg.DefraDB.KeyringSecret)
	return keyring.OpenFileKeyring(keyringPath, secret)
}

// getOrCreateNodeIdentity retrieves an existing node identity from keyring or creates a new one.
func getOrCreateNodeIdentity(cfg *Config) (identity.Identity, error) {
	// Open keyring (required, no fallback)
	kr, err := OpenKeyring(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to open keyring: %w", err)
	}

	// Try to load existing identity from keyring
	identityBytes, err := kr.Get(NodeIdentityKeyName)
	if err != nil {
		if !errors.Is(err, keyring.ErrNotFound) {
			return nil, fmt.Errorf("failed to get identity from keyring: %w", err)
		}

		// Key not found, create new identity
		logger.Sugar.Info("Generating new DefraDB identity")
		nodeIdentity, err := identity.Generate(crypto.KeyTypeSecp256k1)
		if err != nil {
			return nil, fmt.Errorf("failed to generate new identity: %w", err)
		}

		// Save the new identity to keyring
		if err := saveNodeIdentityToKeyring(kr, nodeIdentity); err != nil {
			return nil, fmt.Errorf("failed to save identity to keyring: %w", err)
		}

		return nodeIdentity, nil
	}

	// Load existing identity from keyring
	logger.Sugar.Info("Loading existing DefraDB identity from keyring")
	return LoadIdentityFromBytes(identityBytes)
}

// saveNodeIdentityToKeyring saves the private key bytes of a node identity to the keyring.
func saveNodeIdentityToKeyring(kr keyring.Keyring, nodeIdentity identity.Identity) error {
	// Cast to FullIdentity to access private key
	fullIdentity, ok := nodeIdentity.(identity.FullIdentity)
	if !ok {
		return ErrIdentityNotFull
	}

	// Get the private key from the identity
	privateKey := fullIdentity.PrivateKey()
	if privateKey == nil {
		return ErrPrivateKeyFailure
	}

	// Get raw key bytes
	keyBytes := privateKey.Raw()
	if len(keyBytes) == 0 {
		return ErrPrivateKeyNoBytes
	}

	// Format: "keyType:rawKeyBytes" (same format as DefraDB CLI)
	keyType := string(privateKey.Type())
	identityBytes := append([]byte(keyType+":"), keyBytes...)

	// Save to keyring
	if err := kr.Set(NodeIdentityKeyName, identityBytes); err != nil {
		return err
	}

	logger.Sugar.Info("DefraDB identity private key saved to keyring")
	return nil
}

// LoadIdentityFromBytes parses identity bytes in the keyring's "keyType:rawKeyBytes"
// format (the same format DefraDB CLI writes) and rebuilds the FullIdentity.
// Pre-prefix bytes are assumed to be secp256k1 for backward compatibility.
func LoadIdentityFromBytes(identityBytes []byte) (identity.Identity, error) {
	sepPos := bytes.Index(identityBytes, []byte(":"))
	if sepPos == -1 {
		// Old format without key type prefix, assume secp256k1
		identityBytes = append([]byte(crypto.KeyTypeSecp256k1+":"), identityBytes...)
		sepPos = len(crypto.KeyTypeSecp256k1)
	}

	keyType := string(identityBytes[:sepPos])
	keyBytes := identityBytes[sepPos+1:]

	privateKey, err := crypto.PrivateKeyFromBytes(crypto.KeyType(keyType), keyBytes)
	if err != nil {
		var emptyIdentity identity.Identity
		return emptyIdentity, err
	}

	fullIdentity, err := identity.FromPrivateKey(privateKey)
	if err != nil {
		var emptyIdentity identity.Identity
		return emptyIdentity, err
	}

	return fullIdentity, nil
}

// LoadIdentityFromKeyring fetches the node-identity entry from kr and parses it
// into an Identity. Convenience wrapper for callers that already hold an open
// keyring and don't need the create-if-missing branch.
func LoadIdentityFromKeyring(kr keyring.Keyring) (identity.Identity, error) {
	identityBytes, err := kr.Get(NodeIdentityKeyName)
	if err != nil {
		return nil, err
	}
	return LoadIdentityFromBytes(identityBytes)
}

// GetOrCreateNodeIdentity retrieves an existing node identity from keyring or creates a new one.
// This is an exported version of getOrCreateNodeIdentity for use by external packages.
func GetOrCreateNodeIdentity(cfg *Config) (identity.Identity, error) {
	return getOrCreateNodeIdentity(cfg)
}

// WithIdentityContext returns a new context with the identity value set.
// Thin wrapper over defracontext.WithIdentity for callers that already import this package.
func WithIdentityContext(ctx context.Context, id identity.Identity) context.Context {
	return defracontext.WithIdentity(ctx, id)
}

// IdentityFromContext returns the identity stored in the context, if any.
// Thin wrapper over defracontext.IdentityFrom for callers that already import this package.
func IdentityFromContext(ctx context.Context) (identity.Identity, bool) {
	return defracontext.IdentityFrom(ctx)
}

// GetIdentityContext returns a context with the node identity attached.
func GetIdentityContext(ctx context.Context, cfg *Config) (context.Context, error) {
	nodeIdentity, err := getOrCreateNodeIdentity(cfg)
	if err != nil {
		return ctx, err
	}
	return defracontext.WithIdentity(ctx, nodeIdentity), nil
}

// CreateLibP2PKeyFromIdentity derives a deterministic libp2p Ed25519 private
// key from a DefraDB secp256k1 identity. The 32-byte secp256k1 key bytes are
// used as the seed, so the libp2p peer ID stays stable across restarts and
// matches across every component that calls this with the same identity.
func CreateLibP2PKeyFromIdentity(nodeIdentity identity.Identity) (libp2pcrypto.PrivKey, error) {
	// Cast to FullIdentity to access private key
	fullIdentity, ok := nodeIdentity.(identity.FullIdentity)
	if !ok {
		return nil, ErrIdentityNotFull
	}

	// Get the private key from the identity
	privateKey := fullIdentity.PrivateKey()
	if privateKey == nil {
		return nil, ErrPrivateKeyFailure
	}

	// Get raw key bytes
	keyBytes := privateKey.Raw()
	if len(keyBytes) == 0 {
		return nil, ErrPrivateKeyNoBytes
	}

	// DefraDB expects Ed25519 keys, but DefraDB identities use secp256k1
	// We need to derive an Ed25519 key deterministically from the secp256k1 key
	// Use the secp256k1 key bytes as seed for Ed25519 key generation
	if len(keyBytes) != 32 { // nolint:mnd
		return nil, ErrKeyLengthError
	}

	// Generate Ed25519 key from secp256k1 seed
	libp2pPrivKey, _, err := libp2pcrypto.GenerateEd25519Key(strings.NewReader(string(keyBytes)))
	if err != nil {
		return nil, err
	}

	return libp2pPrivKey, nil
}

// StartDefraInstance initializes and starts a DefraDB node instance with the provided configuration.
func StartDefraInstance(cfg *Config, schemaApplier SchemaApplier, nodeOpts []options.Enumerable[options.NodeOptions], replicationFilter client.ReplicationFilter, collectionsOfInterest ...string) (*node.Node, *NetworkHandler, error) { //nolint:funlen //TODO fix length
	ctx := context.Background()

	if cfg == nil {
		return nil, nil, ErrEmptyConfig
	}
	cfg.DefraDB.P2P.BootstrapPeers = append(cfg.DefraDB.P2P.BootstrapPeers, requiredPeers...)
	if len(cfg.DefraDB.P2P.ListenAddr) == 0 {
		cfg.DefraDB.P2P.ListenAddr = defaultListenAddress
	}

	logger.Init(cfg.Logger.Development, cfg.Logger.LogsDir)

	// Use persistent identity from keyring (required, no fallback)
	nodeIdentity, err := getOrCreateNodeIdentity(cfg)
	if err != nil {
		return nil, nil, err
	}

	// Create LibP2P private key from the same identity to ensure consistent peer ID
	libp2pPrivKey, err := CreateLibP2PKeyFromIdentity(nodeIdentity)
	if err != nil {
		return nil, nil, err
	}

	// Get raw bytes for P2P private key configuration (DefraDB 0.20 API TBD)
	libp2pKeyBytes, err := libp2pPrivKey.Raw()
	if err != nil {
		return nil, nil, err
	}

	// Get real IP address to replace loopback addresses
	ipAddress, err := getLANIP()
	if err != nil {
		return nil, nil, err
	}

	// Replace loopback addresses in URL with real IP
	defraURL := cfg.DefraDB.URL
	defraURL = strings.Replace(defraURL, "http://localhost", ipAddress, 1)
	defraURL = strings.Replace(defraURL, "http://127.0.0.1", ipAddress, 1)
	defraURL = strings.Replace(defraURL, "localhost", ipAddress, 1)
	defraURL = strings.Replace(defraURL, "127.0.0.1", ipAddress, 1)

	// Replace loopback addresses in listen address with real IP
	listenAddress := cfg.DefraDB.P2P.ListenAddr
	if len(listenAddress) > 0 {
		listenAddress = strings.Replace(listenAddress, "127.0.0.1", ipAddress, 1)
		listenAddress = strings.Replace(listenAddress, "localhost", ipAddress, 1)
	}

	// Create defra node options using builder pattern
	nb := options.Node().
		SetDisableAPI(false).
		SetDisableP2P(false) // Enable P2P networking
	nb.P2P().SetEnablePubSub(true)
	nb.Store().SetPath(cfg.DefraDB.Store.Path)
	nb.HTTP().SetAddress(defraURL)
	nb.DB().SetNodeIdentity(nodeIdentity)

	// Badger tuning knobs (BlockCacheMB, MemTableMB, IndexCacheMB, NumCompactors,
	// NumLevelZeroTables, NumLevelZeroTablesStall) were removed from NodeStoreOptions
	// in defradb v1.0.0-rc1. Any corresponding config fields are intentionally ignored.
	vlogSizeMB := cfg.DefraDB.Store.ValueLogFileSizeMB
	if vlogSizeMB <= 0 {
		vlogSizeMB = 64
	}
	nb.Store().SetBadgerFileSize(vlogSizeMB << BadgerFileSize)
	logger.Sugar.Infof("Badger value log file size: %dMB", vlogSizeMB)

	// Add P2P configuration options
	if len(listenAddress) > 0 {
		nb.P2P().SetListenAddresses(listenAddress)
		logger.Sugar.Infof("P2P Listen Address configured: %s", listenAddress)
	}

	if len(libp2pKeyBytes) > 0 {
		nb.P2P().SetPrivateKey(libp2pKeyBytes)
		logger.Sugar.Info("P2P Private Key configured for consistent peer ID")
	}

	// Collect all options: builder + badger extras + user-provided options
	allOpts := []options.Enumerable[options.NodeOptions]{nb}

	allOpts = append(allOpts, nodeOpts...)

	defraNode, err := node.New(ctx, allOpts...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create defra node: %w ", err)
	}

	if replicationFilter != nil {
		defraNode.ReplicationFilter = replicationFilter
	}

	err = defraNode.Start(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to start defra node: %w ", err)
	}

	err = schemaApplier.ApplySchema(ctx, defraNode)
	if err != nil {
		if strings.Contains(err.Error(), "collection already exists") {
			logger.Sugar.Warnf("Failed to apply schema: %v\nProceeding...", err)
		} else {
			defer func() { _ = defraNode.Close(ctx) }()
			return nil, nil, fmt.Errorf("failed to apply schema: %w", err)
		}
	}

	err = defraNode.DB.AddP2PCollections(ctx, collectionsOfInterest)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to add collections of interest %v: %w", collectionsOfInterest, err)
	}

	// Create network handler
	networkHandler := NewNetworkHandler(defraNode, cfg)

	// Conditionally start P2P networking based on config
	if cfg.DefraDB.P2P.Enabled {
		err = networkHandler.StartNetwork()
		if err != nil {
			logger.Sugar.Warnf("Failed to start P2P network: %v", err)
			// Don't fail completely, just log the warning
		}
	} else {
		logger.Sugar.Info("🔇 P2P networking disabled by configuration")
	}

	return defraNode, networkHandler, nil
}

// StartDefraInstanceWithTestConfig is a simple wrapper on StartDefraInstance that changes the configured defra store path to a temp directory for the test.
func StartDefraInstanceWithTestConfig(t *testing.T, cfg *Config, schemaApplier SchemaApplier, collectionsOfInterest ...string) (*node.Node, error) {
	ipAddress, err := getLANIP()
	if err != nil {
		return nil, err
	}
	listenAddress := fmt.Sprintf("/ip4/%s/tcp/0", ipAddress)
	defraURL := fmt.Sprintf("%s:0", ipAddress)
	if cfg == nil {
		cfg = DefaultConfig
	}
	cfg.DefraDB.Store.Path = t.TempDir()
	cfg.DefraDB.URL = defraURL
	cfg.DefraDB.P2P.ListenAddr = listenAddress
	cfg.DefraDB.KeyringSecret = "testSecret"
	node, _, err := StartDefraInstance(cfg, schemaApplier, nil, nil, collectionsOfInterest...)
	return node, err
}

// Subscribe creates a GraphQL subscription for real-time updates.
//
// This function uses non-blocking sends to prevent slow consumers from blocking subscription processing.
func Subscribe[T any](ctx context.Context, defraNode *node.Node, subscription string) (<-chan T, error) {
	result := defraNode.DB.ExecRequest(ctx, subscription)

	if result.Subscription == nil {
		// Check if there are GraphQL errors that explain why subscription is nil
		if result.GQL.Errors != nil {
			return nil, fmt.Errorf("subscription failed with GraphQL errors: %v", result.GQL.Errors) // nolint:err113
		}
		return nil, ErrNilSubscriptionChannel
	}

	resultChan := make(chan T, 100000) // nolint:mnd

	go func() {
		defer close(resultChan)

		for {
			select {
			case <-ctx.Done():
				return
			case gqlResult, ok := <-result.Subscription:
				if !ok {
					return
				}

				if gqlResult.Errors != nil {
					// log errors but continue
					logger.Sugar.Errorf("failed to subscribe: %s , errors: %v", subscription, gqlResult.Errors)
					continue
				}
				// Parse and send typed result
				var typedResult T
				if err := marshalUnmarshal(gqlResult.Data, &typedResult); err == nil {
					// Non-blocking send to prevent slow consumers from blocking subscription processing
					select {
					case resultChan <- typedResult:
					case <-ctx.Done():
						return
					default:
						logger.Sugar.Warnf("subscription buffer full, dropping event for query: %s", subscription)
					}
				} else {
					logger.Sugar.Errorf("failed to parse subscription data: %v, raw data: %+v", err, gqlResult.Data)
				}
			}
		}
	}()

	return resultChan, nil
}

// marshalUnmarshal converts a generic interface{} to a specific typed struct
// using JSON marshal/unmarshal. This is the same pattern used throughout the query client.
func marshalUnmarshal(data any, target any) error {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}
	return json.Unmarshal(dataBytes, target)
}

// ====================================================================
// NEW CLIENT API - Clean alternative to StartDefraInstance
// ====================================================================

// Client provides a clean interface for DefraDB operations.
type Client struct {
	node    *node.Node
	network *NetworkHandler
	config  *Config
}

// NewClient creates a new client instance (doesn't start anything).
func NewClient(cfg *Config) (*Client, error) {
	if cfg == nil {
		return nil, ErrEmptyConfig
	}

	return &Client{
		config: cfg,
	}, nil
}

// Start initializes key generation, node startup, and network handler.
func (c *Client) Start(ctx context.Context) error { // nolint:funlen
	if c.node != nil {
		return ErrClientStarted
	}

	// Use the same core logic as StartDefraInstance but without schema
	c.config.DefraDB.P2P.BootstrapPeers = append(c.config.DefraDB.P2P.BootstrapPeers, requiredPeers...)
	if len(c.config.DefraDB.P2P.ListenAddr) == 0 {
		c.config.DefraDB.P2P.ListenAddr = defaultListenAddress
	}

	logger.Init(c.config.Logger.Development, c.config.Logger.LogsDir)

	// Use persistent identity from keyring (required, no fallback)
	nodeIdentity, err := getOrCreateNodeIdentity(c.config)
	if err != nil {
		return fmt.Errorf("error getting or creating identity: %w", err)
	}

	// Create LibP2P private key from the same identity to ensure consistent peer ID
	libp2pPrivKey, err := CreateLibP2PKeyFromIdentity(nodeIdentity)
	if err != nil {
		return fmt.Errorf("error creating LibP2P private key from identity: %w", err)
	}

	// Get raw bytes for P2P private key configuration
	libp2pKeyBytes, err := libp2pPrivKey.Raw()
	if err != nil {
		return fmt.Errorf("error getting LibP2P private key bytes: %w", err)
	}

	// Get real IP address to replace loopback addresses
	ipAddress, err := getLANIP()
	if err != nil {
		return fmt.Errorf("failed to get LAN IP address: %w", err)
	}

	// Replace loopback addresses in URL with real IP
	defraURL := c.config.DefraDB.URL
	defraURL = strings.Replace(defraURL, "http://localhost", ipAddress, 1)
	defraURL = strings.Replace(defraURL, "http://127.0.0.1", ipAddress, 1)
	defraURL = strings.Replace(defraURL, "localhost", ipAddress, 1)
	defraURL = strings.Replace(defraURL, "127.0.0.1", ipAddress, 1)

	// Replace loopback addresses in listen address with real IP
	listenAddress := c.config.DefraDB.P2P.ListenAddr
	if len(listenAddress) > 0 {
		listenAddress = strings.Replace(listenAddress, "127.0.0.1", ipAddress, 1)
		listenAddress = strings.Replace(listenAddress, "localhost", ipAddress, 1)
	}

	// Create defra node options using builder pattern
	nb := options.Node().
		SetDisableAPI(false).
		SetDisableP2P(false)
	nb.P2P().SetEnablePubSub(true)
	nb.Store().SetPath(c.config.DefraDB.Store.Path)
	nb.HTTP().SetAddress(defraURL)
	nb.DB().SetNodeIdentity(nodeIdentity)

	// Badger tuning knobs (BlockCacheMB, MemTableMB, IndexCacheMB, NumCompactors,
	// NumLevelZeroTables, NumLevelZeroTablesStall) were removed from NodeStoreOptions
	// in defradb v1.0.0-rc1. Any corresponding config fields are intentionally ignored.
	{
		vlogSizeMB := c.config.DefraDB.Store.ValueLogFileSizeMB
		if vlogSizeMB <= 0 {
			vlogSizeMB = 64
		}
		nb.Store().SetBadgerFileSize(vlogSizeMB << BadgerFileSize)
	}

	if len(listenAddress) > 0 {
		nb.P2P().SetListenAddresses(listenAddress)
		logger.Sugar.Infof("P2P Listen Address configured: %s", listenAddress)
	}

	if len(libp2pKeyBytes) > 0 {
		nb.P2P().SetPrivateKey(libp2pKeyBytes)
		logger.Sugar.Info("P2P Private Key configured for consistent peer ID")
	}

	// Collect all options
	allOpts := []options.Enumerable[options.NodeOptions]{nb}

	c.node, err = node.New(ctx, allOpts...)
	if err != nil {
		return fmt.Errorf("failed to create defra node: %w", err)
	}

	err = c.node.Start(ctx)
	if err != nil {
		return fmt.Errorf("failed to start defra node: %w", err)
	}

	// Create network handler
	c.network = NewNetworkHandler(c.node, c.config)

	if c.config.DefraDB.P2P.Enabled {
		err = c.network.StartNetwork()
		if err != nil {
			logger.Sugar.Warnf("Failed to start P2P network: %v", err)
		}
	} else {
		logger.Sugar.Info("🔇 P2P networking disabled by configuration")
	}

	return nil
}

// Stop cleanly shuts down the client.
func (c *Client) Stop(ctx context.Context) error {
	if c.node == nil {
		return nil
	}

	err := c.node.Close(ctx)
	c.node = nil
	c.network = nil
	return err
}

// ApplySchema applies a GraphQL schema string to the started node.
func (c *Client) ApplySchema(ctx context.Context, schema string) error {
	if c.node == nil {
		return ErrClientMustBeStarted
	}

	if len(schema) == 0 {
		return ErrEmptySchema
	}

	_, err := c.node.DB.AddCollection(ctx, schema)
	if err != nil {
		if strings.Contains(err.Error(), "collection already exists") {
			logger.Sugar.Warnf("Failed to apply schema: %v\nProceeding...", err)
			return nil
		}
		return fmt.Errorf("failed to apply schema: %w", err)
	}

	return nil
}

// GetNode returns the underlying DefraDB node.
func (c *Client) GetNode() *node.Node {
	return c.node
}

// GetNetworkHandler returns the network handler.
func (c *Client) GetNetworkHandler() *NetworkHandler {
	return c.network
}
