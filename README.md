# Shinzo Host Client - Embedded DefraDB Application

## **Core Purpose**

The **Shinzo Host Client** is an **embedded DefraDB application** that provides **decentralized data attestation and networking** for blockchain indexers. It acts as both a **data verification hub** and **P2P networking layer**, enabling secure, consensus-driven data sharing across multiple blockchain indexers.

## **Key Capabilities**

### **🔗 Embedded DefraDB Engine**
- **Self-contained database**: Runs DefraDB internally, eliminating external dependencies
- **Automatic schema management**: Handles AttestationRecord and IndexerSignature schemas
- **P2P networking**: Built-in libp2p integration for decentralized data replication & indexer subscription
- **Multi-indexer support**: Aggregates data from multiple blockchain validators, giving high data verification

### **⚡ Dual Connection Architecture**
**Option 1: DefraDB-to-DefraDB Replication**
- Indexers run their own DefraDB instances
- Host Client subscribes to indexer DefraDB collections via native P2P
- Automatic data synchronization using DefraDB's built-in replication

**Option 2: Direct API Integration**
- Indexers connect directly via HTTP/GraphQL API
- Host Client provides storage and attestation services
- Ideal for indexers without DefraDB infrastructure

### **🛡️ Attestation & Consensus System**
```
Indexer → IndexerSignature → AttestationRecord → Consensus → Verified Data
```

1. **Indexer creates signature**: Each document gets an `IndexerSignature` with cryptographic proof
2. **Attestation aggregation**: Multiple signatures accumulate in `AttestationRecord` using P-counters
3. **Consensus verification**: Configurable thresholds determine data validity
4. **Decentralized storage**: Verified data replicates across the P2P network

### **🌐 Gateway & Access Layer**
- **GraphQL API**: Rich query interface for applications and curators
- **REST endpoints**: Simple HTTP access for basic operations
- **Real-time subscriptions**: Live data feeds for applications
- **Access control**: Integration with Shinzo Hub chain for permissions

## **Architecture Flow**

```
Multiple Indexers  → Host Client (Embedded DefraDB) → P2P Network   →   Curators/Apps
     ↓                        ↓                            ↓                  ↓
Ethereum Indexer#1         Attestation Engine         Peer Replication     GraphQL API
Ethereum Indexer#2     →   P-Counter Consensus    →   Data Verification →  REST Endpoints  
Arbitrum Indexer#N         Signature Validation       Network Discovery    Live Subscriptions
```

## **Use Cases**

- **Multi-chain data aggregation**: Collect and verify data from multiple ethereum validators
- **Decentralized indexing**: Provide redundant, consensus-verified blockchain data
- **Data marketplace**: Enable secure data sharing with cryptographic attestation
- **Application backends**: Serve verified blockchain data to dApps and analytics tools

## **Benefits Over Traditional Approaches**

- **No external database dependencies**: Self-contained DefraDB eliminates infrastructure complexity
- **Built-in P2P networking**: Native replication without custom networking code
- **Cryptographic verification**: Every piece of data includes attestation proofs
- **Horizontal scalability**: Add indexers and peers without architectural changes
- **Consensus-driven reliability**: Multiple attestations ensure data accuracy

The Host Client transforms raw blockchain data into **verified, consensus-backed information** that applications can trust, while providing the **networking infrastructure** for decentralized data sharing at scale.

## **Getting Started**

### **Quick Start**
```bash
# Build the application
make build

# Run with default configuration
./bin/shinzo-host-client --config configs/config.yaml

# Run in development mode
./bin/shinzo-host-client --dev
```

### **Configuration**
See `configs/config.yaml` for multi-indexer setup examples and `docs/DEFRADB_REPLICATION.md` for detailed setup instructions.

### **Testing**
Follow the comprehensive testing guide in `docs/TESTING_GUIDE.md` to test the complete indexer → DefraDB → host-client → curator data flow.

## **Documentation**

- **[DefraDB Replication Setup](docs/DEFRADB_REPLICATION.md)** - Complete P2P replication configuration
- **[Testing Guide](docs/TESTING_GUIDE.md)** - End-to-end testing instructions  
- **[API Documentation](docs/API.md)** - GraphQL and REST endpoint reference
- **[Schema Documentation](docs/SCHEMA.md)** - Database schema and relationships
- **[Multi-Machine Deployment](docs/MULTI_MACHINE_DEPLOYMENT.md)** - Production deployment guide

## **Architecture**

The Host Client operates as an embedded DefraDB application with two connection modes:

1. **DefraDB-to-DefraDB**: Native P2P replication between DefraDB instances
2. **Direct API**: HTTP/GraphQL connections for indexers without DefraDB

Both modes support the same attestation flow: `IndexerSignature` → `AttestationRecord` → consensus verification.
