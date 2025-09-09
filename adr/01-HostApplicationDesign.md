# 01 - Host Application Design

## Status
Written by Quinn Purdy on September 9, 2025

## Context
Throughout the design and development of the Shinzo protocol, the Shinzo team has always conceptualized a Host actor whose primary responsibility is to process primitive data (blocks/transactions/logs) written by the indexers into the outputs defined by the various views created by users of the view_creator tool. Originally, we expected this scope of the Hosts' responsibilities to be rather minimal - something that could be setup with ease. However, as our design is maturing, it has become clear that the Host will need to do a bit more heavy lifting than previously expected - as a result, it is necessary that the Shinzo team provide a Host application that can be easily installed and ran by Shinzo's Hosts.

The responsibilities of the Host client application are:
1) Applying Lens transforms on Primitives to produce the requested Views
2) When the Indexer clients write the Primitives, we will have some duplicated data and we may also have some mismatches in our data (Indexers may not always agree on the content of a block for example). When Indexers submit Primitive data, they will also submit and sign an attestation indicating that they believe the block data to be correct. Hosts will need to keep track of, and forward along, the attestations made by the indexers. Note: instead of rejecting any versions of primitive data with less attestations, our Hosts will instead keep it and all related information (this will become useful later on should we begin to police the network and punish bad behaviour).
3) Hosts have the potential to behave maliciously. A Host could write incorrect data to a view. As a result, we also require an attestation system for Hosts - where Hosts vote on what they think the correct output of a given Lens transform and input data is - i.e. Hosts vote on the correct document version for each view. Hosts will need to submit signed attestations with their data.

In addition to understanding these responsibilities, in designing our Host client application, we need to also understand some of the related requirements of our system and how they can be fulfilled. These include:
i) Hosts must be aware of newly created Views
ii) Hosts must be aware that new data is requested for an existing view so that they can fulfill the request
iii) There must be a means via which subscribers can receive the data they have paid for
iv) Subscribers must be cut off from receiving new data once their funds run out
v) Hosts will need a mechanism to query and find out the views that can be hosted so that they can make informed decisions on which views to host

## Decision

The Shinzo team will provide a Host client application with an in-process Defra instance. To accompany this written description of what the Host application will do and look like, please refer to [this diagram](https://excalidraw.com/#json=alvFlIJ7SxU5wJ8SR8AYN,7wiLPpRdK0DkxW1B70XFIQ).

In order to fulfill its responsibilities and account for other considerations, our Host application client must implement the following:

a) When a view is submitted by the view_creator, ShinzoHub will perform some validation. If the submitted view is approved and created, ShinzoHub will emit an event. During startup, our Host client app will establish a webhook with a ShinzoHub node RPC (supplied in config) - this will allow our Host client app to respond anytime there is a newly created view.
b) Following a), ShinzoHub node RPCs will expose functionality such that they can be queried for a complete list of the published views (and related info).
c) Our Host will need to keep a list of the views they are hosting and cache any newly created views to prompt the user on whether they'd like to Host them or not.
d) Following c), our Host client app will need some form of UI to display this information to the user. A simple CLI will be sufficient for these purposes, at least for now.
e) Our Host will need to be able to apply Lens transforms on data before writing to their Defra instance
f) Our Host will need to be able to sign its attestations - their Defra node's private key should be sufficient, we just need to use the exposed functionality
g) Our Host will need to append attestation info to the Lens transform outputs
h) When someone purchases new data for an existing view via the Outpost contract, ShinzoHub will respond by granting that DID read access to that view and will emit an event. Hosts, who, following a), have a webhook established with a ShinzoHub node RPC will also listen to this event. If it is a view that the Host is actively hosting, they will begin processing that data for the view to fulfill the request.
i) In many cases, the DID that pays for access to the data in a view won't be the DID(s) actually using the data. Instead, in most cases, application developers will be paying for access to the data on behalf of the users of their application. Because of this, we can't simply include the P2P connection details for the user in the purchase request in the event emitted by h) (privacy risk). However, we still require a means for a user, who needs data, to get connected with a Host who is capable of fulfilling the request. Luckily, the gossip system in Defra's passive replication protocol has peers constantly sharing the "topics" (or in our case Views) that one another cares about - through a few "hops" a user requesting data should be able to identify and connect with a Host who can fulfill their request for data. However, we have been given the caveatt that this passive discovery can sometimes take a while and we may find that we want some form of gateway or router who can facilitate this for us - a "Host Repository" node of sorts that keeps track of which Hosts are hosting which View. For now, for the sake of simplicity, we will rely on the built-in passive replication system and add additional routing logic should the need arise.
j) While payment is still somewhat of an unknown, for now, we will assume that subscribers will be buying access to the latest data. Once their payment dries up, ShinzoHub can revoke their subscriber role (and by extension, their read access). Defra should stop sending them data at this point - will need to test and confirm.

## Consequences

This design allows the Host client application to fulfill the Host's responsibilities and accounts for the related considerations. However, it introduces a few consequences:

- a)-d)require that, during setup, each Host client have a ShinzoHub node RPC endpoint for their configuration - adding additional responsibility for the ShinzoHub node who must expose these methods
- Following j), ShinzoHub will need to keep track of (and take action when) a subscriber runs out of funds
- ShinzoHub needs to be updated such that it emits the required events
- Indexer client application needs to be updated such that it submits attestations
- Since Hosts will have the option to choose which Views to host, this means there is the possibility that there are Views that are not being hosted by anyone or Views that may have their last remaining Host leave. These Views may or may not have subscribers who will be unable to sync the latest data (as there is no Host to calculate it). While this may seem like a problem (and likely is for any subscriber), it isn't problematic for Shinzo; if no Host is willing to host a given View, then it likely isn't economically viable for them to do so. 
