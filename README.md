# shinzo-host-client
The Host Client's role is to facilitate the hosting of `View`s. A `View` is a collection of data that is useful to an application. `View`s are described using [the view-creator tool](https://github.com/shinzonetwork/view-creator) and comprise of SDLs, Lens transforms, and source data. A Host performs Lens transforms on Primitive data (blocks, transactions, logs, etc.) to write to a `View` and serve requests from subscribers.

## How to Use

To run the host client app

```bash
go run cmd/main.go
```

Or, build and run with
```bash
go build -o bin/host cmd/main.go
./bin/host
```

The host is designed to transform data it receives from Shinzo Indexers. So, when running the host app, you'll want to make sure to connect it to Indexer client(s) (or a mock Indexer - a defra instance that posts dummy primitives as if it were a real Indexer).

### Connecting to Indexer(s)

To connect your Host client to Indexer(s) (or mocks), you can either

1) Connect them directly to the Indexer
2) Connect them to the Indexer by hopping through a "big peer"

If you are looking to connect to the rest of the Shinzo network, connecting directly to a deployed and running Indexer is the correct approach. When a defra instance is first started, they will log their "peer info" - the Indexer client will do this as well.

In your config.yaml, you'll want to include the Indexer client's "peer info" in your `bootstrap_peers` array. An example "peer info" looks like `"/ip4/183.123.3.12/tcp/9171/p2p/12D3KooWLttXvtbokAphdVWL6hx7VEviDnHYwQs5SmAw1Y1yfcZT"` - with the ip address, port, and `PeerId` replaced accordingly with what was seen in the logs. If there are other peers you'd like to connect with (e.g. more Indexers, your application client, etc.), you may also include their info as well.

If you are developing locally, then, while you can connect directly to an Indexer, you will likely prefer the devex of connecting to an Indexer by hopping through a "big peer". It is recommended to use [the example big peer](https://github.com/shinzonetwork/app-sdk/tree/Examples/hostDemo/examples/bigPeer). You'll want to clone it and run it like a standard go program (with `go run` or `go build` and running the built binary, as above with the host client app). You'll want to retrieve the "peer info" from the big peer's logs and include it in the `boostrap_peers` array in your `config.yaml` for the Host client, the Indexer client, and any other Shinzo apps you may be working with. The big peer will serve as an entrypoint for all your Defra nodes and serve as a means of facilitation their discovery and connection to one another. When developing locally, it is recommended to keep the (lightweight) big peer running at all times to save you the hassle of re-configuring all your bootstrap peers again.

## How it Works

The Host client app is built on top of the [app-sdk](https://github.com/shinzonetwork/app-sdk), meaning the Host client features an embedded Defra instance (there is no need to clone, build, and run a separate Defra instance). The Host's embedded Defra instance (and the rest of Shinzo) leverages [libp2p's pub/sub system](https://docs.libp2p.io/concepts/pubsub/overview/) which allows the network participants to find one another and "gossip" about their topics of interest; through this, Hosts are connected with Indexers and subscribers to the Views they are Hosting. Once connected, Hosts begin receiving primitive data from the Indexers and sending the various View collections they are Hosting to subscriber nodes - this happens via a pub/sub relationship facilitated by [Defra's Passive Replication](https://docs.source.network/defradb/guides/peer-to-peer#passive-replication). 

Since the transmission of data from one node to another (moving from Indexer -> Host -> app clients) is facilitated by Defra's Passive Replication, this leaves the Host with only a few remaining responsibilities:

1) Keeping track of the available View definitions that could be Hosted and downloading + storing their required Lens wasm files
2) Hosting Views -> transforming primitives as described in the View definition, applying Lens transforms as required
3) Managing attestation records from the Indexers (coming soon!)

Currently, with respect to 1), the Host client will, assuming `web_socket_url` in `config.yaml` is not nil, attempt to establish a websocket with a ShinzoHub node (please provide the websocket to one - at time of writing, if you run ShinzoHub locally, that would be "ws://localhost:26657/websocket") and subscribe to the new View created event. Currently, when the Host receives a new View event, they will automatically begin Hosting it (eventually, we will provide more optionality around this). 

Setting up a View for Hosting involves:

a) If the View has Lenses, writing the wasm (included in the event) for each Lens to a .wasm file and registering the Lens with the Lens Registry - when processing a View, the Defra instance will apply the Lenses from the .wasm files
b) Adding the SDL from the View to the embedded Defra instance's schema - this allows the creation of new documents in the View's collection
c) Adding the View's name as a P2P collection of interest - this configures the Defra instance to begin sharing documents with other nodes that have declared the View as a collection of interest (i.e. enabling passive replication of the View)

Once a)-c) are completed, the View is added to a list of Hosted Views that the Host will maintain. In the current implementation, this is an ephemeral list, meaning that it will not persist between separate runs of the Host (it is only stored in runtime memory). If you restart a Host, you will need to re-create the View(s) (and thus re-fire the new View creation event(s)). This behaviour is useful for development and will eventually be configurable, certainly before Shinzo goes live.

After the Host has started its embedded Defra instance and has attempted to setup a websocket with a Shinzo node, it will begin to Host any Views it has added to its list and prepared.

The Host maintains a mapping of Views to a stack of block number timestamps at which that View was processed. The Host is repeatedly querying against its embedded Defra instance to figure out the most recent block number. If the most recent block number received exceeds the block number at the top of a given View's timestamp stack, then the Host will process the View on all un-processed blocks (starting one past the block number at the top of the stack) up until the most recently received block number; the Host will retrieve the raw documents using the View's query, apply any Lenses, and finally write the resulting data to the View's collection (as identified in the SDL) before pushing the current block number onto the stack.
Note that while only the top item in the stacks are used, having the entire stack can be quite useful when debugging.

With this approach to Hosting Views, any block data that is received late (after other versions of that block have been received and processed) will not be processed into Views. In future iterations, when we add the attestation logic to the Host, we will have the means to identify newly updated documents as well as any late block data (as we will need to process the attestation logic on them as well); from here, we can trigger retrospective View processing on any late data received.
