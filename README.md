# shinzo-host-client

The Shinzo Host Client facilitates the hosting of **Views**, which are structured collections of data useful to applications. **Views** are defined using the [view-creator tool](https://github.com/shinzonetwork/shinzo-view-creator) and consists of SDLs, Lens transforms, and source data.

The Host Client receives **primitive data** (blocks, transactions, logs, etc.) from Shinzo Indexers, applies Lens transforms, writes to Views, and serves requests from subscribers. Hosts also contribute to network security by producing Attestation Records.

## Installation & Running

Clone the repository:

```bash
git clone https://github.com/shinzonetwork/shinzo-host-client.git
cd shinzo-host-client
```

Run directly:

```bash
go run cmd/main.go
```

Or, build and run:

```bash
go build -o bin/host cmd/main.go
./bin/host
```

## Running with GraphQL Playground (Optional)

The host includes an optional GraphQL Playground that provides a web-based interface for querying the embedded DefraDB instance. This is useful for development and debugging.

**To enable the playground:**

1. Download the playground assets:

```bash
make deps-playground
```

2. Build or run with the hostplayground tag:

```bash
# Build with playground
make build-playground
./bin/host
   
# Or run directly with playground
make start-playground
```

Once the host is running with the playground enabled, you'll see a message in the logs indicating the playground URL (typically `http://127.0.0.1:9182`). Open this URL in your browser to access the GraphQL Playground.

The playground provides:

- Interactive GraphQL query editor
- Schema exploration and documentation
- Query execution and result viewing
- Support for subscriptions

> **Note:** The playground runs on a separate port (9182) from defradb's API server (9181). The playground UI proxies all GraphQL API requests to defradb's API server, so you can use the playground to interact with your embedded DefraDB instance. For more details, see the [playground README](playground/README.md).

The host is designed to transform data it receives from Shinzo Indexers. So, when running the host app, you'll want to make sure to connect it to Indexer client(s) (or a mock Indexer - a defra instance that posts dummy primitives as if it were a real Indexer).

## Configuration

The Host Client reads from [config.yaml](/config.yaml), which comes with sensible defaults. The only field you need to set is `defradb.keyring_secret` which can alternatively be set with the following command in the terminal window.

```bash
export DEFRA_KEYRING_SECRET=<make_a_password>
```

### Key Fields

* **defradb.url** – API endpoint of your local DefraDB node. Defaults work for most setups.
* **defradb.keyring_secret** – Requires a secret to generate your private keys. 
* **p2p.bootstrap_peers** – Seed peers for joining the Shinzo network. Defaults include a reliable bootstrap peer.
* **p2p.listen_addr** – Default is suitable for local runs. Override when containerizing.
* **store.path** – Directory where local DefraDB data is stored.
* **shinzo.web_socket_url** – Defaults to a hosted ShinzoHub node. Only change if connecting to a different node.
* **logger.development** – Set to `false` for production.
* **host.lens_registry_path** – Where received WASM lens files are stored.

> The default `config.yaml` is ready for most local development workflows. Modify only if connecting to custom peers or storage paths.

## Connecting to Indexer(s)

Hosts receive primitive data from Indexers and serve Views to subscribers. To connect your Host client to Indexer(s) (or mocks), you can either

1) Connect them directly to the Indexer
2) Connect them to the Indexer by hopping through a "big peer"

### Direct Connection

If your goal is to connect to the rest of the Shinzo network, the correct approach is to connect directly to a deployed and running Indexer. When a Defra instance is first started, it will log its peer info, and Indexer clients do the same.

You will need to include this peer info in the `bootstrap_peers` array of your `config.yaml`. For example:

```bash
/ip4/183.123.3.12/tcp/9171/p2p/12D3KooWLttXvtbokAphdVWL6hx7VEviDnHYwQs5SmAw1Y1yfcZT
```

Replace the `IP address`, `port`, and `PeerId` with the values from your logs.  If there are other peers you'd like to connect with (e.g. more Indexers, your application client, etc.), you may also include their info as well. Connecting directly ensures that your Host client is fully integrated with the live network and receives all the relevant primitives from deployed Indexers.

### Connection via Big Peer

For local development, it is often more convenient to connect to an Indexer through a big peer. This approach simplifies DevEx by providing a single entrypoint for all your local Defra nodes and reducing the need to manually configure multiple bootstrap peers. You can use [the example big peer](https://github.com/shinzonetwork/app-sdk/tree/Examples/hostDemo/examples/bigPeer) provided in the repository.

To set it up, clone the [big peer repository](https://github.com/shinzonetwork/app-sdk/tree/Examples/hostDemo/examples/bigPeer) and run it as a standard Go program using either `go run` or `go build` followed by executing the binary. Retrieve the **peer info** from the big peer’s logs, and include it in the `boostrap_peers` array in your `config.yaml` for the Host client, Indexer client, and any other local Shinzo apps you are working with. The big peer will serve as an entrypoint for all your Defra nodes and serve as a means of facilitation their discovery and connection to one another. It is recommended to keep this lightweight peer running at all times during development to avoid repeatedly reconfiguring bootstrap peers.

This approach is particularly useful when you are testing or experimenting locally and do not need to connect to the live Shinzo network. It ensures your Host client can still receive primitive data from local Indexers and participate in passive replication without the complexity of managing multiple direct peer connections.

## Connect to an Indexer (Step-by-Step)

Below is a concrete example of connecting the Host client to a running Indexer.

**PEER ID INFO**
IP_ADDRESS: 136.115.148.56
PORT: 9171
PEER_ID: 12D3KooWT2wVhxc7ySePpFoomm1SengPYdAa1P6iUiAypN5TRijD

Peer info format:

`['/ip4/<IP_ADDRESS>/tcp/<PORT>/p2p/<PEER_ID>']`

Step 1: Update `config.yaml`

```yaml
#... existing config.yaml ...
defradb:
    p2p:
        bootstrap_peers: ['/ip4/<IP_ADDRESS>/tcp/9171/p2p/<PeerID>']
        listen_addr: "/ip4/0.0.0.0/tcp/0"
#... existing config.yaml ...
```

Step 2: Run the Host Client

```bash
make build
make start
```

Step 3: Verify the connection

Check the Host logs. You should see messages indicating successful pub/sub communication with the Indexer:

```bash
Nov 12 16:10:15.363 INF p2p Received new pubsub message
PeerID=12D3KooWSfFo4Dr3T4AFupCGmDyEosFFcZeo7ozfm6itULeTmTDS
SenderId=12D3KooWT2wVhxc7ySePpFoomm1SengPYdAa1P6iUiAypN5TRijD
Topic=bafyreidmrvhwhvnjcrucj7qz26mlsxi4cwjevzzpnb4rr23ighgqxn2n7i
```

This confirms the Host is connected and receiving data.

## How the Host Client works

The Host client app is built on top of the [app-sdk](https://github.com/shinzonetwork/app-sdk). This means the Host client includes an **embedded DefraDB instance**, and there is no need to clone, build, or run a separate Defra node.

The Host's embedded Defra instance (and the rest of Shinzo) leverages [libp2p's pub/sub system](https://docs.libp2p.io/concepts/pubsub/overview/) which allows the network participants to discover one another and gossip about their topics of interest. Through this mechanism, Hosts connect to Indexers and to subscriber nodes that are interested in the Views being hosted.

Once connected, Hosts begin receiving primitive data from the Indexers and sending the various View collections they are Hosting to subscriber nodes. Data propagation between Indexers, Hosts, and application clients is handled by [Defra's Passive Replication](https://docs.source.network/defradb/guides/peer-to-peer#passive-replication).

Since the transmission of data from one node to another (moving from Indexer -> Host -> app clients) is facilitated by Defra's Passive Replication, this leaves the Host with only a few remaining responsibilities:

1) Keeping track of the available View definitions that could be Hosted and downloading + storing their required Lens wasm files
2) Hosting Views -> transforming primitives as described in the View definition, applying Lens transforms as required
3) Managing attestation records from the Indexers

### View Discovery and Setup

If the `web_socket_url` field in `config.yaml` is set, the Host client will attempt to establish a websocket connection with a ShinzoHub node. At the time of writing, if you are running ShinzoHub locally, this would typically be:

```bash
ws://localhost:26657/websocket
```

Once connected, the Host subscribes to new View created events. When a new View event is received, the Host will automatically begin hosting that View. This behavior is currently automatic, though more configurability will be introduced before Shinzo goes live.

**Setting up a View for hosting involves several steps:**

a) If the View includes Lenses, the Host writes the Lens WASM binaries (included in the event) to .wasm files and registers them with the Lens Registry. These WASM files are later used by DefraDB to apply Lens transforms during View processing.
b) The Host adds the View’s SDL to the embedded Defra instance's schema. This enables the creation of new documents in the View’s collection.
c) The Host adds the View’s name as a P2P collection of interest. This configures DefraDB to begin sharing documents for that View with other nodes that have declared the View as a collection of interest (i.e. enabling passive replication of the View).

Once these steps are completed, the View is added to the Host’s list of hosted Views.

> Note: In the current implementation, this is an ephemeral list, meaning that it will not persist between separate runs of the Host (it is only stored in runtime memory). If you restart a Host, you will need to re-create the View(s) (and thus re-fire the new View creation event(s)). This behavior is intentional for development and will become configurable in future versions.

### Hosting and Processing Views

After the Host has started its embedded DefraDB instance and established its websocket connection to Shinzo node, it will begin to Host any Views it has added to its list and prepared.

The Host maintains a mapping between each View and a stack of block number timestamps indicating when that View was last processed. The Host continuously queries its embedded DefraDB instance to determine the most recent block number it has received.

If the most recent block number received exceeds the block number at the top of a given View’s timestamp stack, the Host will:

- Process the View on all unprocessed blocks, starting from one block past the last processed block
- Retrieve raw primitive documents using the View’s query
- Apply any configured Lens transforms
- Write the transformed data to the View’s collection (as identified by the SDL) before pushing the current block number onto the stack
- Push the current block number onto the View’s timestamp stack

Note that while only the top item in the stacks are used, having the entire stack can be quite useful when debugging.

> Important: With this approach to Hosting Views, any block data that is received late (after other versions of that block have been received and processed) will not be processed into Views. This limitation is known and will be addressed in future iterations.

### Attestation Records

Hosts are also responsible for managing attestation records posted by Indexers. When an Indexer submits an attestation, the Host performs the following steps:

1. Validate the attestation’s signature against its corresponding CID
2. Post the CID, along with the associated attested_doc and source_doc, to an AttestationRecord collection

To minimize data requirements for application clients, the Host creates a **separate AttestationRecord collection for each View collection**. This ensures that clients only need to subscribe to attestation records relevant to the Views they care about.

Application clients that require attestations can subscribe to these collections using the `attestation.AddAttestationRecordCollection` method provided by the app-sdk’s attestation package.

Further design details and background on attestations can be found in ADRs [#02](/adr/02-AttestationRecords.md) and [#03](/adr/03-AttestationRecordAmmendment.md).
