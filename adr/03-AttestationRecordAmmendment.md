# 03 - Ammendment to "02 - Attestation Records"

## Status
Written by Quinn Purdy on November 7, 2025

## Context

The Shinzo team has conducted a number of experiments around Defra's built in "attestation" system, described in the prior ADR. These experiments, and the learnings from each of them, can be found [here](https://github.com/shinzonetwork/shinzo-app-sdk/tree/Experiments/attestations/experiments).

The Shinzo team has also made a first pass at the attestation system previously described and has received additional feedback from stakeholders on the Defra team.

## Decision

As advised by the Defra team, we're simplifying the SDL for the Attestation Record collections.

where previously we had
```
type AttestationRecord_ViewNameAndUniqueIdentifier{
    attested_doc_id: String
    source_doc_ids: []String
    signatures: []IndexerSignature_ViewNameAndUniqueIdentifier
}

type IndexerSignature_ViewNameAndUniqueIdentifier{
    identity: String
    value: String
    type: String
}
```
now, we have
```
type AttestationRecord_ViewNameAndUniqueIdentifier{
    attested_doc_id: String
    source_doc_ids: []String
    CIDs: []String
}
```

CIDs (Commit Identifiers) can be thought of a more condensed attestation. Each signed commit, regardless of document contents, will produce a unique CID - the CID is the hash of the document contents, including the signatures. These CIDs are included in the built-in `_version` array which Defra includes in each document. When creating the Attestation Records, the Hosts will now be responsible for validating that each signature included in the `_version` array is a valid signature.

In our experiments, we discovered that a (potentially malicious) node can modify a document an arbitrary amount of times, adding additional `_version` entries, signatures, and CIDs. However, we've been advised that through a combination of checking the `height` in the `_version` array (which indicates how many times a document has been modified - height of 1 = original document, height greater than 1 = the document has been modified) and time-travelling queries (which allow us to view a document from a given CID), we can reject any modifications to Primitive documents during the view hosting process.

We also discovered that if multiple nodes publish an attestation record for a given document but have a different set of signatures, they will produce separate documents. [This issue](https://github.com/shinzonetwork/shinzo-host-client/issues/27) describes this scenario in more detail and provides examples. As a result, some CRDT work must be done to collate the attestation records, requiring minor updates to the AttestationRecord SDL (not modifying the data types).

## Consequences

Since Hosts will be validating the signatures and only passing along this CIDs, this shifts more trust towards the hosts simply because the application clients will not be able to verify the Indexer signatures themselves. In exchange, this approach reduces the extra data requirements for application clients that care about attestations and it also increases their query speeds by making attestation validation quicker on their end. This shift in trust is acceptable for the following reasons:
1) The Shinzo devnet and early launch will require an in-depth KYC onboarding process for Hosts, making them more trustworthy. Note that the team does plan on opening this up to be a trustless onboarding process once Shinzo has grown sufficiently for policing + economic security to prove effective
2) The Shinzo team will build out a robust policing system to identify and punish malishious behaviour. Policing the attestation system will be an important part of the attestation system. 
3) As Shinzo grows (economically) and the policing system becomes more robust, Shinzo can increasingly open up the Host onboarding process - allowing us to shift trust from the host and towards the Shinzo network's policing system and economic security.

To facilitate the collation of AttestationRecord documents, Defra will likely need a new CRDT type to merge the CID []string. The Defra team is currently very busy preparing for their V1 launch, so the Shinzo team will most likely need to be adding this to Defra themselves, with some support from the Defra team.
