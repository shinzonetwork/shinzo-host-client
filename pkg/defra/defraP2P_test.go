package defra

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/shinzonetwork/host/pkg/attestation"
	"github.com/shinzonetwork/indexer/pkg/defra"
	"github.com/shinzonetwork/indexer/pkg/logger"
	"github.com/shinzonetwork/indexer/pkg/types"
	"github.com/sourcenetwork/defradb/acp/identity"
	"github.com/sourcenetwork/defradb/crypto"
	"github.com/sourcenetwork/defradb/http"
	netConfig "github.com/sourcenetwork/defradb/net/config"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	logger.Init(true)
	exitCode := m.Run()
	os.Exit(exitCode)
}

func TestSimpleP2PReplication(t *testing.T) {
	listenAddress := "/ip4/127.0.0.1/tcp/0"
	defraUrl := "127.0.0.1:0"
	options := []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(false),
		node.WithStorePath(t.TempDir()),
		http.WithAddress(defraUrl),
		netConfig.WithListenAddresses(listenAddress),
	}
	ctx := context.Background()
	writerDefra := StartDefraInstance(t, ctx, options)
	defer writerDefra.Close(ctx)

	options = []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(false),
		node.WithStorePath(t.TempDir()),
		http.WithAddress(defraUrl),
		netConfig.WithListenAddresses(listenAddress),
	}
	readerDefra := StartDefraInstance(t, ctx, options)
	defer readerDefra.Close(ctx)

	addSchema(t, ctx, writerDefra)
	addSchema(t, ctx, readerDefra)

	writerPort := GetPort(writerDefra)
	require.NotEqual(t, -1, writerPort, "Unable to retrieve writer port")

	postBasicData(t, ctx, writerPort)

	result, err := getUserName(ctx, writerPort)
	require.NoError(t, err)
	require.Equal(t, "Quinn", result)

	time.Sleep(10 * time.Second) // Allow some time to give the data a chance to sync to the readerDefra instance (it won't since they aren't connected, but we give it time anyways just in case)

	readerPort := GetPort(readerDefra)
	require.NotEqual(t, -1, readerPort, "Unable to retrieve reader port")
	result, err = getUserName(ctx, readerPort)
	require.Error(t, err)

	err = writerDefra.DB.SetReplicator(ctx, readerDefra.DB.PeerInfo())
	require.NoError(t, err)

	result, err = getUserName(ctx, readerPort)
	for attempts := 1; attempts < 100; attempts++ { // It may take some time to sync now that we are connected
		if err == nil {
			break
		}
		t.Logf("Attempt %d to query username from readerDefra failed. Trying again...", attempts)
		time.Sleep(1 * time.Second)
		result, err = getUserName(ctx, readerPort)
	}
	require.NoError(t, err)
	require.Equal(t, "Quinn", result)
}

func postQuery(ctx context.Context, port int, request types.Request) (string, error) {
	handler, err := defra.NewBlockHandler(fmt.Sprintf("http://localhost:%d", port))
	if err != nil {
		return "", fmt.Errorf("Error building block handler: %v", err)
	}
	result, err := handler.SendToGraphql(ctx, request)
	if err != nil {
		return "", fmt.Errorf("Error sending graphql query %s : %v", request.Query, err)
	}
	return string(result), nil
}

func getUserName(ctx context.Context, port int) (string, error) {
	query := `query GetUserName{
		User(limit: 1) {
			name
		}
	}`
	request := types.Request{
		Type:  "POST",
		Query: query,
	}

	result, err := postQuery(ctx, port, request)
	if err != nil {
		return "", fmt.Errorf("Error sending query: %v", err)
	}

	var rawResponse map[string]interface{}
	if err := json.Unmarshal([]byte(result), &rawResponse); err != nil {
		return "", fmt.Errorf("Error unmarshalling reponse: %v", err)
	}
	data, ok := rawResponse["data"].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("Data field not found in response: %s", result)
	}
	userBlock, ok := data["User"].([]interface{})
	if !ok {
		return "", fmt.Errorf("User field not found in response: %s", result)
	}
	if len(userBlock) == 0 {
		return "", fmt.Errorf("No users found in response: %s", result)
	}
	userNameBlock, ok := userBlock[0].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("name field not found in response: %s", result)
	}
	userName, ok := userNameBlock["name"]
	if !ok {
		return "", fmt.Errorf("name field not found in response: %s", result)
	}
	name, ok := userName.(string)
	if !ok {
		return "", fmt.Errorf("name field not found in response: %s", result)
	}
	return name, nil
}

func addSchema(t *testing.T, ctx context.Context, writerDefra *node.Node) {
	schema := "type User { name: String }"
	_, err := writerDefra.DB.AddSchema(ctx, schema)
	require.NoError(t, err)
}

func postBasicData(t *testing.T, ctx context.Context, writerPort int) {
	query := `mutation {
		create_User(input: { name: "Quinn" }) {
			name
		}
	}`
	mutation := types.Request{
		Type:  "POST",
		Query: query,
	}

	result, err := postQuery(ctx, writerPort, mutation)
	require.NoError(t, err)
	require.True(t, strings.Contains(result, "Quinn"))
}

func TestMultiTenantP2PReplication_ConnectToPeers(t *testing.T) {
	listenAddress := "/ip4/127.0.0.1/tcp/0"
	defraUrl := "127.0.0.1:0"
	ctx := context.Background()
	writerDefra := createWriterDefraInstanceAndApplySchema(t, ctx, defraUrl, listenAddress)
	defer writerDefra.Close(ctx)
	err := writerDefra.DB.AddP2PCollections(ctx, "User")
	require.NoError(t, err)

	previousDefra := writerDefra
	readerDefraInstances := []*node.Node{}
	for i := 0; i < 10; i++ {
		readerDefraOptions := []node.Option{
			node.WithDisableAPI(false),
			node.WithDisableP2P(false),
			node.WithStorePath(t.TempDir()),
			http.WithAddress(defraUrl),
			netConfig.WithListenAddresses(listenAddress),
		}
		newDefraInstance := StartDefraInstance(t, ctx, readerDefraOptions)
		defer newDefraInstance.Close(ctx)

		err = newDefraInstance.DB.Connect(ctx, previousDefra.DB.PeerInfo())
		require.NoError(t, err)

		addSchema(t, ctx, newDefraInstance)

		err = newDefraInstance.DB.AddP2PCollections(ctx, "User")
		require.NoError(t, err)

		readerDefraInstances = append(readerDefraInstances, newDefraInstance)
		previousDefra = newDefraInstance
	}

	writerPort := GetPort(writerDefra)
	require.NotEqual(t, -1, writerPort, "Unable to retrieve writer port")

	postBasicData(t, ctx, writerPort)

	result, err := getUserName(ctx, writerPort)
	require.NoError(t, err)
	require.Equal(t, "Quinn", result)

	assertReaderDefraInstancesHaveLatestData(t, ctx, readerDefraInstances)
}

func TestMultiTenantP2PReplication_ManualReplicatorAssignment(t *testing.T) {
	listenAddress := "/ip4/127.0.0.1/tcp/0"
	defraUrl := "127.0.0.1:0"
	ctx := context.Background()
	writerDefra := createWriterDefraInstanceAndPostBasicData(t, ctx, defraUrl, listenAddress)
	defer writerDefra.Close(ctx)

	previousDefra := writerDefra
	readerDefraInstances := []*node.Node{}
	for i := 0; i < 10; i++ {
		readerDefraOptions := []node.Option{
			node.WithDisableAPI(false),
			node.WithDisableP2P(false),
			node.WithStorePath(t.TempDir()),
			http.WithAddress(defraUrl),
			netConfig.WithListenAddresses(listenAddress),
		}
		newDefraInstance := StartDefraInstance(t, ctx, readerDefraOptions)
		defer newDefraInstance.Close(ctx)

		assertDefraInstanceDoesNotHaveData(t, ctx, newDefraInstance)

		addSchema(t, ctx, newDefraInstance)

		assertDefraInstanceDoesNotHaveData(t, ctx, newDefraInstance)

		err := previousDefra.DB.SetReplicator(ctx, newDefraInstance.DB.PeerInfo())
		require.NoError(t, err)
		readerDefraInstances = append(readerDefraInstances, newDefraInstance)
		previousDefra = newDefraInstance
	}

	assertReaderDefraInstancesHaveLatestData(t, ctx, readerDefraInstances)
}

func createWriterDefraInstanceAndPostBasicData(t *testing.T, ctx context.Context, defraUrl string, listenAddress string) *node.Node {
	writerDefra := createWriterDefraInstanceAndApplySchema(t, ctx, defraUrl, listenAddress)

	writerPort := GetPort(writerDefra)
	require.NotEqual(t, -1, writerPort, "Unable to retrieve writer port")

	postBasicData(t, ctx, writerPort)

	result, err := getUserName(ctx, writerPort)
	require.NoError(t, err)
	require.Equal(t, "Quinn", result)

	return writerDefra
}

func createWriterDefraInstanceAndApplySchema(t *testing.T, ctx context.Context, defraUrl string, listenAddress string) *node.Node {
	options := []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(false),
		node.WithStorePath(t.TempDir()),
		http.WithAddress(defraUrl),
		netConfig.WithListenAddresses(listenAddress),
	}
	writerDefra := StartDefraInstance(t, ctx, options)

	addSchema(t, ctx, writerDefra)
	return writerDefra
}

func createDefraInstanceAndApplySchema(t *testing.T, ctx context.Context, options []node.Option) *node.Node {
	instance := StartDefraInstance(t, ctx, options)
	addSchema(t, ctx, instance)
	return instance
}

func assertReaderDefraInstancesHaveLatestData(t *testing.T, ctx context.Context, readerDefraInstances []*node.Node) {
	for i, readerDefra := range readerDefraInstances {
		readerPort := GetPort(readerDefra)
		require.NotEqual(t, -1, readerPort, "Unable to retrieve reader port")
		result, err := getUserName(ctx, readerPort)
		for attempts := 1; attempts < 60; attempts++ { // It may take some time to sync now that we are connected
			if err == nil {
				break
			}
			t.Logf("Attempt %d to query username from readerDefra %d failed. Trying again...", attempts, i)
			time.Sleep(1 * time.Second)
			result, err = getUserName(ctx, readerPort)
		}
		require.NoError(t, err, fmt.Sprintf("Received unexpected error when checking user name for node %d: %v", i, err))
		require.Equal(t, "Quinn", result)
	}
}

func TestMultiTenantP2PReplication_ConnectToBigPeer(t *testing.T) {
	listenAddress := "/ip4/127.0.0.1/tcp/0"
	defraUrl := "127.0.0.1:0"
	ctx := context.Background()

	bigPeer := createWriterDefraInstanceAndApplySchema(t, ctx, defraUrl, listenAddress)
	defer bigPeer.Close(ctx)
	err := bigPeer.DB.AddP2PCollections(ctx, "User")
	require.NoError(t, err)

	options := []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(false),
		node.WithStorePath(t.TempDir()),
		http.WithAddress(defraUrl),
		netConfig.WithListenAddresses(listenAddress),
	}
	writerDefra := createDefraInstanceAndApplySchema(t, ctx, options)
	defer writerDefra.Close(ctx)
	err = writerDefra.DB.AddP2PCollections(ctx, "User")
	require.NoError(t, err)

	err = writerDefra.DB.Connect(ctx, bigPeer.DB.PeerInfo())
	require.NoError(t, err)

	readerDefraInstances := []*node.Node{}
	for i := 0; i < 10; i++ {
		readerDefraOptions := []node.Option{
			node.WithDisableAPI(false),
			node.WithDisableP2P(false),
			node.WithStorePath(t.TempDir()),
			http.WithAddress(defraUrl),
			netConfig.WithListenAddresses(listenAddress),
		}
		newDefraInstance := StartDefraInstance(t, ctx, readerDefraOptions)
		defer newDefraInstance.Close(ctx)

		assertDefraInstanceDoesNotHaveData(t, ctx, newDefraInstance)

		err = newDefraInstance.DB.Connect(ctx, bigPeer.DB.PeerInfo())
		require.NoError(t, err)

		addSchema(t, ctx, newDefraInstance)

		assertDefraInstanceDoesNotHaveData(t, ctx, newDefraInstance)

		err = newDefraInstance.DB.AddP2PCollections(ctx, "User")
		require.NoError(t, err)

		readerDefraInstances = append(readerDefraInstances, newDefraInstance)
	}

	writerPort := GetPort(writerDefra)
	require.NotEqual(t, -1, writerPort, "Unable to retrieve writer port")

	postBasicData(t, ctx, writerPort)

	result, err := getUserName(ctx, writerPort)
	require.NoError(t, err)
	require.Equal(t, "Quinn", result)

	assertReaderDefraInstancesHaveLatestData(t, ctx, readerDefraInstances)
}

func assertDefraInstanceDoesNotHaveData(t *testing.T, ctx context.Context, readerDefra *node.Node) {
	port := GetPort(readerDefra)
	require.NotEqual(t, -1, port)

	_, err := getUserName(ctx, port)
	require.Error(t, err)
}

func TestSyncFromMultipleWriters(t *testing.T) {
	listenAddress := "/ip4/127.0.0.1/tcp/0"
	defraUrl := "127.0.0.1:0"
	ctx := context.Background()

	writerDefras := []*node.Node{}
	defraNodes := 10
	for i := 0; i < defraNodes; i++ {
		nodeIdentity, err := identity.Generate(crypto.KeyTypeSecp256k1)
		require.NoError(t, err)
		options := []node.Option{
			node.WithDisableAPI(false),
			node.WithDisableP2P(false),
			node.WithStorePath(t.TempDir()),
			http.WithAddress(defraUrl),
			netConfig.WithListenAddresses(listenAddress),
			node.WithNodeIdentity(identity.Identity(nodeIdentity)),
		}
		writerDefra := StartDefraInstance(t, ctx, options)
		defer writerDefra.Close(ctx)

		addSchema(t, ctx, writerDefra)
		err = writerDefra.DB.AddP2PCollections(ctx, "User")
		require.NoError(t, err)

		writerDefras = append(writerDefras, writerDefra)
	}

	// Create a reader instance
	readerOptions := []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(false),
		node.WithStorePath(t.TempDir()),
		http.WithAddress(defraUrl),
		netConfig.WithListenAddresses(listenAddress),
	}
	readerDefra := StartDefraInstance(t, ctx, readerOptions)
	defer readerDefra.Close(ctx)

	for _, writer := range writerDefras {
		err := readerDefra.DB.Connect(ctx, writer.DB.PeerInfo())
		require.NoError(t, err)
	}

	addSchema(t, ctx, readerDefra)

	assertDefraInstanceDoesNotHaveData(t, ctx, readerDefra)

	err := readerDefra.DB.AddP2PCollections(ctx, "User")
	require.NoError(t, err)

	// Write data to each writer
	for _, writer := range writerDefras {
		writerPort := GetPort(writer)
		require.NotEqual(t, -1, writerPort, "Unable to retrieve writer port")
		postBasicData(t, ctx, writerPort)

		// Verify the data was written to this writer
		result, err := getUserName(ctx, writerPort)
		require.NoError(t, err)
		require.Equal(t, "Quinn", result)
	}

	// Wait for sync and verify reader has all data
	readerPort := GetPort(readerDefra)
	require.NotEqual(t, -1, readerPort, "Unable to retrieve reader port")
	result, err := getUserName(ctx, readerPort)
	for attempts := 1; attempts < 60; attempts++ { // It may take some time to sync now that we are connected
		if err == nil {
			break
		}
		t.Logf("Attempt %d to query username from readerDefra failed. Trying again...", attempts)
		time.Sleep(1 * time.Second)
		result, err = getUserName(ctx, readerPort)
	}
	require.Equal(t, "Quinn", result)

	// Verify we can get the _version field
	queryClient, err := NewQueryClientFromPort(readerPort)
	require.NoError(t, err)

	userWithVersion, err := getUserWithVersion(queryClient, ctx)
	require.NoError(t, err)
	require.Equal(t, "Quinn", userWithVersion.Name)
	require.Equal(t, defraNodes, len(userWithVersion.Version))
}

type UserWithVersion struct {
	Name    string                `json:"name"`
	Version []attestation.Version `json:"_version"`
}

func getUserWithVersion(queryClient *QueryClient, ctx context.Context) (UserWithVersion, error) {
	query := `query GetUserWithVersion{
		User(limit: 1) {
			name
			_version {
				cid
				signature {
					type
					identity
					value
					__typename
				}
			}
		}
	}`

	user, err := QuerySingle[UserWithVersion](queryClient, ctx, query)
	if err != nil {
		return UserWithVersion{}, fmt.Errorf("Error querying user with version: %v", err)
	}

	return user, nil
}
