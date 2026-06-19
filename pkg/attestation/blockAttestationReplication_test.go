package attestation

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	localschema "github.com/shinzonetwork/shinzo-host-client/pkg/schema"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/client/options"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/require"
)

// replReceiverEnv flags the re-executed copy of this test binary to act as the
// receiving (accounting-service-like) node in a separate process. Two real processes
// are used rather than two in-process nodes because two DefraDB peers in one process
// connect but do not complete DAG-sync in this environment.
const replReceiverEnv = "BA_REPL_RECEIVER"

// buildBANode brings up a DefraDB node bound to loopback with P2P enabled and only the
// BlockAttestation collection registered. The collection is in no pubsub set, so the
// only inbound path is a directed replicator. P2P options are set as one struct via
// SetAll because separate options.Node() builders do not merge their P2P config.
func buildBANode(ctx context.Context, dir string) (*node.Node, error) {
	nb := options.Node().SetDisableAPI(true).SetDisableP2P(false)
	nb.Store().SetPath(dir)
	nb.P2P().SetAll(options.NodeP2POptions{
		ListenAddresses:           []string{"/ip4/127.0.0.1/tcp/0"},
		EnablePubSub:              true,
		EnableRelay:               true,
		EnableClearBackoffOnRetry: true,
	})
	n, err := node.New(ctx, nb)
	if err != nil {
		return nil, err
	}
	if err := n.Start(ctx); err != nil {
		return nil, err
	}
	if _, err := n.DB.AddSchema(ctx, localschema.BlockAttestationSchema); err != nil {
		return nil, err
	}
	return n, nil
}

func startBANode(t *testing.T) *node.Node {
	t.Helper()
	n, err := buildBANode(context.Background(), t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = n.Close(context.Background()) })
	return n
}

func blockAttestationCount(ctx context.Context, n *node.Node) int {
	res := n.DB.ExecRequest(ctx, `query {
		`+constants.CollectionBlockAttestation+` { hostId blockNumber voteCount signerIdentities cids }
	}`)
	if res.GQL.Errors != nil {
		return -1
	}
	data, ok := res.GQL.Data.(map[string]any)
	if !ok {
		return -1
	}
	rows, _ := data[constants.CollectionBlockAttestation].([]map[string]any)
	return len(rows)
}

// runReplicationReceiver runs in the re-executed subprocess: it brings up a node,
// prints its multiaddr on stdout for the parent, then waits for the host's directed
// push to land the record. It signals the outcome through the process exit code (0 =
// the record replicated, 1 = timeout) so the parent can assert on it.
func runReplicationReceiver() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "ba-recv-*")
	if err != nil {
		fmt.Println("RECV_ERR", err)
		os.Exit(2)
	}
	n, err := buildBANode(ctx, dir)
	if err != nil {
		fmt.Println("RECV_ERR", err)
		os.Exit(2)
	}
	defer func() { _ = n.Close(ctx) }()

	addrs, err := n.DB.PeerInfo(ctx)
	if err != nil || len(addrs) == 0 {
		fmt.Println("RECV_ERR no peer address")
		os.Exit(2)
	}
	fmt.Println("ADDR=" + addrs[0])

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if blockAttestationCount(ctx, n) == 1 {
			fmt.Println("RESULT=REPLICATED")
			os.Exit(0)
		}
		time.Sleep(250 * time.Millisecond)
	}
	fmt.Println("RESULT=TIMEOUT")
	os.Exit(1)
}

// TestBlockAttestationDirectedReplication is the live proof of the host -> accounting
// service delivery hop: a host node that registers a second node (running in a separate
// process) as a directed replicator for the BlockAttestation collection pushes its
// writes there automatically. The collection is in no pubsub set, so the directed
// replicator is the only delivery path, matching the production shape.
func TestBlockAttestationDirectedReplication(t *testing.T) {
	if os.Getenv(replReceiverEnv) == "1" {
		runReplicationReceiver()
		return
	}
	ctx := context.Background()

	// Re-exec this test binary as the receiving (accounting-service-like) process.
	cmd := exec.Command(os.Args[0], "-test.run=^TestBlockAttestationDirectedReplication$", "-test.timeout=120s")
	cmd.Env = append(os.Environ(), replReceiverEnv+"=1")
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	defer func() { _ = cmd.Process.Kill() }()

	addrCh := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if line := sc.Text(); strings.HasPrefix(line, "ADDR=") {
				select {
				case addrCh <- strings.TrimPrefix(line, "ADDR="):
				default:
				}
			}
		}
	}()

	var addr string
	select {
	case addr = <-addrCh:
	case <-time.After(40 * time.Second):
		t.Fatal("receiver subprocess did not report its address")
	}

	// Host-like sender: connect, register the directed replicator, write one record.
	hostNode := startBANode(t)
	require.NoError(t, hostNode.DB.Connect(ctx, []string{addr}))
	require.NoError(t, hostNode.DB.CreateReplicator(ctx, []string{addr},
		options.CreateReplicator().SetCollectionNames([]string{constants.CollectionBlockAttestation})))
	// CreateReplicator registers the in-memory replicator (consulted by the write-event
	// push) in the transaction's async success callback; let it land before writing or
	// the first write races it and is missed.
	time.Sleep(2 * time.Second)

	hostCol, err := hostNode.DB.GetCollectionByName(ctx, constants.CollectionBlockAttestation)
	require.NoError(t, err)
	doc, err := client.NewDocFromMap(ctx, map[string]any{
		"hostId":           "host-A",
		"blockNumber":      int64(18_000_000),
		"blockHash":        "0xhash",
		"merkleRoot":       "0xroot",
		"signerIdentities": []any{"indexer-a", "indexer-b"},
		"cids":             []any{"cid-1", "cid-2"},
		"voteCount":        int64(2),
		"hostSignature":    "sig",
		"shinzoHeight":     int64(12345),
		"createdAt":        int64(1718800000),
	}, hostCol.Version())
	require.NoError(t, err)
	require.NoError(t, hostCol.Save(ctx, doc))

	// The receiver process exits 0 once the record lands, non-zero on timeout.
	require.NoError(t, cmd.Wait(), "BlockAttestation did not replicate to the receiver process")
}
