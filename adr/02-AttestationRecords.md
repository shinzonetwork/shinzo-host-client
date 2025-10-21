# 01 - Attestation Records

## Status
Written by Quinn Purdy on October 21, 2025

## Context
As a decentralized protocol, particularly one that focuses on data collation and transformation for high-value data, Shinzo is ripe for malicious intent. A malicious actor, through clever data manipulation, has the potential to extract (steal) high value (in $ term) from the Shinzo ecosystem. For this reason, data manipulation in the Shinzo network is expressly forbidden and will be heavily policed by the network (we'll discuss this further in future ADRs).

However, even with the shared understanding that Shinzo, as a protocol, will make significant efforts to police the network and aggressively punish bad actors, it is still important to provide application developers with some means for data self-verification so that they can feel confident making transactions in their application(s). We have identified the need for what we have so far been calling the "attestation system".

There are a few different ways via which Shinzo can expose view data self-verification to application developers/users - i.e. there are a few different ways for the attestation system to work - each with tradeoffs. As we assess these options in this ADR, it is important to keep in mind some important context:

1) It is very important to understand where the data is at each step in the Shinzo pipeline and what that data looks like. Please see `#01 - Host Application Design` for some context there. Perhaps the most important thing to recognize is that the application clients are expected to be subscribing the view collections only - they should have view collection data on their device, but not the underlying/source (primitive) data.
2) It is important to recognize that both the Host and Indexer roles have the potential (and motive) to behave maliciously. However, it is worth noting that Indexers, which are intended to be run on validator nodes, are (by necessity) more invested in the network (e.g. 32 staked ETH) and have undergone more vetting by nature of their validator role. Indexers are also further from where data gets consumed; while they provide the initial data, they don't provide the data ultimately used by applications; this makes any data manipulation attacks more difficult to pull off. The takeaway is that, while Indexer certainly aren't to be blindly trusted, they are more trustworthy than Hosts. 
3) Defra has a built-in "attestation system". While insufficient for our needs, understanding the built-in system is important context for how we build Shinzo's. In short, any Defra node configured with an identity will have a signing public/private key pair; when they write to a collection, the document they write to is signed. This is exposed to Defra users in the form of a hidden, system defined, field in the collection's schema; `_version`, the hidden field, is an array which contains the signature objects and CID of the document. Readers of a collection can expect to see the same number of `_version` instances as the number of Defra nodes (with identities) that have written to that document - e.g. if 5 Indexers have written the same block data, Hosts will receive a Block document with 5 `_version` instances attached to it, one signed by each of the 5 Indexers. The problem with this system is that the `_version` instances signed by the Indexer do not cary over by default to the View collections written to by the Hosts (instead we have the Host's signatures and `_version` blobs).
4) It is important to anticipate the kinds of developers and applications that will care the most about the attestation system as this will help to inform how this system works and is exposed to users (developers). We anticipate that there are a few relevant motivations why developers will choose Shinzo as opposed to other indexers:
    a) Enhanced data accuracy (a presumed by-product of decentralization)
    b) Faster data availability (with Indexer client piggybacking off validator, Shinzo should have latest blocks before network consensus - e.g. before Geth has it)
    c) Faster query responses (as queries are done against pre-processed data stored locally)
Notice that a) is in conflict with b) and c) 
-> If, as in b), you receive block data before network consensus, then you are, by necessity, compromising data accuracy as you would be trusting the word of one (or a small group of) Indexer(s), a potentially malicious actor. Waiting for more attestations from more Indexers to increase the accuracy increases the time it takes for data to be available to users.
-> If, in an attempt to increase data accuracy a), additional data, used for data self-verification, is sent to the application clients, then the query response time will increase (simply because there is more data to query against), harming c). We need to be careful not to increase query response times too much when improving our data accuracy.
When designing our attestation system, we aim to reach a happy medium between these opposing (in this instance) motivations. 
A key insight that makes reasoning about this much simpler is that these motivations, while not mutually exclusive, vary significantly in terms of importance depending on the application. Some examples:
i) Games, often real-time applications, or social media apps are likely to heavily value fast query response times. Depending on the game, getting the latest data faster b) may also be very important. Since games usually deal with frequent, micro-transactions as opposed to infrequent large transactions, having the most accurate data tends to be less important for this use case.
ii) User-facing financial applications are likely to place a high value on having highly accurate data. As a user-facing app, these types of financial apps aren't likely to benefit much from having the latest data sooner b) as typically you would rely on algorithmic trading bots (or similar) to benefit from this information, as human actors would be too slow to react favourably to this info. While users do care about fast query response times, they are typically more willing to accept more latency in a financial app than in a game for instance.
iii) Automated trading apps on the other hand are likely to favour getting new data available as fast as possible as they can use algorithms to take advantage of this new information and gain an edge on non-Shinzo using algo trading apps. While they likely value data accuracy as well, with recognition that higher data accuracy directly causes slower data availability, some automated trading apps, especially those who trade at high frequencies, are likely to favour data availability over accuracy. Similarly, when operating at high trading frequencies, where transaction values tend to be a bit lower as well, faster query response time is likely to be favoured over higher data accuracy.
With these examples, we see that a flexible and configurable attestation system, one that allows for tuning the ratio between data accuracy and performance (both in data availability and query response time), is highly desirable for Shinzo.
5) Even if we could safely assume that each Indexer isn't behaving maliciously, attestations from Indexers are still important. As Shinzo plans for Indexers to piggyback off of validators, block re-orgs (and similar) are likely occurences. This means that some Indexers may occasionally post incorrect information even when not acting maliciously!

## Decision

There were quite a few different approaches to the attestation system that were considered. Some thoughts are laid out in a flow chart here: https://excalidraw.com/#json=XPx0XZaEjMc5RHoMqaX1R,_GrTPoreplWcd0hxBxjVBQ. This can serve as a useful reference.

Of the options considered, the most flexible and configurable option seems to be to create a separate `AttestationRecord` collection for each View collection. Applications that care about having highly accurate data can subscribe to these collections - the app-sdk should do this on their behalf during the setup phase. From here, they can perform queries against the `AttestationRecord` collection to get the number of attestations for their underlying data.

```
type AttestationRecord_ViewNameAndUniqueIdentifier{
    attested_doc_id: String
    source_doc_ids: []String
    signatures: []IndexerSignature
}

type IndexerSignature{
    identity: String
    value: String
    type: String
}
```

The app-sdk will be updated to be made configurable such that you can configure a minimum required number of Indexer + Host attestations. With this, the SDK would perform the requested query and then perform an additional query against the respective `AttestationRecord` collection for that View and filter out data from any documents that don't have the desired number of `signatures` (Indexer attestation number) and/or the desired `_version` amount (Host attestation number). Similarly, the SDK can also be made configurable such that, on occasion, it will prune out any data it has received that do not meet the required attestation requirements - aiming to maintain fast query response times while improving accuracy.

Here, since the underlying primitive data is not included and therefore the app clients have no means to self-verify the Indexers' signatures, the attestations from the Indexer are being "observed" by the Hosts and essentially just passed along. However, the Hosts could lie about these attestations in an attempt to manipulate data - for this reason, checking for Host atttestations is also important. To protect against a collusion from malicious Hosts, Shinzo will also need to police its Hosts heavily and punish malicious behaviour. Including both the `attested_doc_id` and the `source_doc_ids` in the `AttestationRecord` makes verifying this information easy to do for an independent verifier, allowing for a trustless approach to policing (which we will examine more in-depth in the future).

This approach is unintrusive to apps that don't care about improved data accuracy; they can continue to enjoy fast data availability and fast query response times without any impact (aside from a very minimal lag in processing Views introduced by creating the attestation records). It also allows for apps to configure/specify the allowable attestation thresholds for each collection, allowing them to decide on the appropriate level of accuracy required for their app. Finally, the filter vs prune approach allows apps to choose where those tradeoffs are most acceptable - with filter making accurate data available sooner but queries slower while prune maintains query speed but takes longer to achieve higher accuracy on the latest data.

## Consequences

This attestation system requires updates to the Host client app such that they now create the attestation records. Similarly, the system also requires updates to the app-sdk to facilitate exposing this data to developers.

As we examined potential options (see excalidraw link above), we notice that solutions that don't require saddling the application clients with lots of extra data, require some level of Host policing. Shinzo will need to account for Host+Indexer policing and for the new police role in its tokenomic design.
