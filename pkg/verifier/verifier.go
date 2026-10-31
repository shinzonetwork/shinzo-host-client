// Package verifier records a tokens-mode chain's verified blocks: heights where two of the chain's
// signers agree on the block and the host holds exactly the documents they signed.
package verifier

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/node"

	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
)

const (
	// requiredSigners is how many of the chain's signers must sign the same documents for a block to
	// be verified.
	requiredSigners = 2

	// retryInterval is how long the verifier waits before it checks an unverified height again.
	retryInterval = time.Second

	// repairAfter is how long a height may stay unverified, while the host holds a later block,
	// before the verifier fetches the documents the generators hold for it that the host does not.
	repairAfter = 10 * time.Second

	// repairHeights is how many heights, from the unverified one up, one repair covers: the most a
	// generator lists in one request.
	repairHeights = 1000

	// syncTimeout bounds one document sync, so a peer that never answers cannot hold up the
	// verifier.
	syncTimeout = 30 * time.Second
)

var (
	errTooFewSigners   = errors.New("fewer than two signers agree on the block")
	errSignersDisagree = errors.New("signers agree on more than one version of the block")
	errDocumentsDiffer = errors.New("the documents held differ from the signed ones")
)

// Verifier checks a tokens-mode chain's heights in order and records each one that passes in the
// chain's VerifiedBlock table.
type Verifier struct {
	node        *node.Node
	collections chain.Collections
	count       func(ctx context.Context, signatureIDs []string)
	documents   func(ctx context.Context, from, to int64) (map[string][]string, error)
	repairAfter time.Duration
}

// New returns a verifier for the tokens-mode chain whose collections are c. count attests the
// BlockSignature documents with the given IDs; the verifier calls it for signatures the host holds
// but has not counted. documents lists the IDs of the documents the chain's generators hold at the
// heights from through to, by collection; the verifier fetches those the host is missing.
func New(
	n *node.Node,
	c chain.Collections,
	count func(ctx context.Context, signatureIDs []string),
	documents func(ctx context.Context, from, to int64) (map[string][]string, error),
) *Verifier {
	return &Verifier{node: n, collections: c, count: count, documents: documents, repairAfter: repairAfter}
}

// Run verifies heights in order until ctx is done. It starts above the highest verified height or,
// on a chain with none, at the lowest height whose Header the host holds. A height that does not
// pass holds back every height above it. It is checked again at once after the verifier counts
// signatures the host holds for it or fetches missing documents, and otherwise every
// retryInterval.
func (v *Verifier) Run(ctx context.Context) {
	height, err := v.firstHeight(ctx)
	if err != nil {
		return
	}
	var reported string
	handed := make(map[string]bool)
	waitingSince := time.Now()
	for {
		err := v.verify(ctx, height)
		if err == nil {
			height++
			reported = ""
			clear(handed)
			waitingSince = time.Now()
			continue
		}
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, errTooFewSigners) && v.countHeldSignatures(ctx, height, handed) {
			continue
		}
		if time.Since(waitingSince) >= v.repairAfter && v.heldAbove(ctx, height) {
			v.repair(ctx, height)
			waitingSince = time.Now()
			continue
		}
		if msg := err.Error(); msg != reported {
			reported = msg
			logger.Sugar.Debugf("Block %d not verified yet: %v (chain: %s)", height, err, v.collections.Prefix)
		}
		if !wait(ctx) {
			return
		}
	}
}

// firstHeight returns the height Run starts at, waiting until the host holds a Header when it holds
// neither a Header nor a verified block. It returns ctx's error once ctx is done.
func (v *Verifier) firstHeight(ctx context.Context) (int64, error) {
	verified, header := v.collections.VerifiedBlock, v.collections.Header
	// The _geq filters skip rows a purge left without a height.
	request := fmt.Sprintf(`query { `+
		`verified: %s(filter: {%s: {_geq: 0}}, order: {%s: DESC}, limit: 1) { height: %s } `+
		`header: %s(filter: {%s: {_geq: 0}}, order: {%s: ASC}, limit: 1) { height: %s } }`,
		verified.Name, verified.HeightField, verified.HeightField, verified.HeightField,
		header.Name, header.HeightField, header.HeightField, header.HeightField)
	for {
		rows, err := v.query(ctx, request)
		switch {
		case err != nil:
			logger.Sugar.Warnf("Block verification cannot find its first height: %v (chain: %s)", err, v.collections.Prefix)
		case len(rows["verified"]) > 0:
			return rows["verified"][0].Height + 1, nil
		case len(rows["header"]) > 0:
			return rows["header"][0].Height, nil
		}
		if !wait(ctx) {
			return 0, ctx.Err()
		}
	}
}

// verify records height in the chain's VerifiedBlock table when requiredSigners of the chain's
// signers agree on the block there and the host holds exactly the documents they signed: every one
// at its signed version, and no other. Otherwise it returns why not.
func (v *Verifier) verify(ctx context.Context, height int64) error {
	c := v.collections
	var query strings.Builder
	fmt.Fprintf(&query, `query { headers: %s(filter: {%s: {_eq: %d}}) { hash _version { cid } } `,
		c.Header.Name, c.Header.HeightField, height)
	for i, log := range c.ContractLogs {
		fmt.Fprintf(&query, `log%d: %s(filter: {%s: {_eq: %d}}) { _version { cid } } `, i, log.Name, log.HeightField, height)
	}
	fmt.Fprintf(&query, `attestations: %s(filter: {%s: {_eq: %d}}) { source_doc CIDs } }`,
		c.AttestationRecord.Name, c.AttestationRecord.HeightField, height)
	rows, err := v.query(ctx, query.String())
	if err != nil {
		return err
	}

	agreed, err := agreedAttestation(rows["attestations"])
	if err != nil {
		return err
	}
	headers := rows["headers"]
	var held []string
	for name, docs := range rows {
		if name == "attestations" {
			continue
		}
		for _, doc := range docs {
			for _, version := range doc.Version {
				held = append(held, version.CID)
			}
		}
	}
	slices.Sort(held)
	signed := slices.Sorted(slices.Values(agreed.CIDs))
	if len(headers) != 1 || !slices.Equal(held, signed) {
		return fmt.Errorf("%w: %d Header documents, %d documents held, %d signed", errDocumentsDiffer, len(headers), len(held), len(signed))
	}
	return v.record(ctx, height, headers[0].Hash, agreed.Signers)
}

// countHeldSignatures hands count the BlockSignature documents held at height whose signer no
// attestation record there counts yet, each document at most once while Run waits on that height,
// and reports whether it handed any. The attestation listener counts only signatures a peer sends
// as an update, so a signature fetched another way, such as by a document sync, is counted only
// through here.
func (v *Verifier) countHeldSignatures(ctx context.Context, height int64, handed map[string]bool) bool {
	c := v.collections
	rows, err := v.query(ctx, fmt.Sprintf(`query { `+
		`signatures: %s(filter: {%s: {_eq: %d}}) { _docID signatureIdentity } `+
		`attestations: %s(filter: {%s: {_eq: %d}}) { source_doc } }`,
		c.BlockSignature.Name, c.BlockSignature.HeightField, height,
		c.AttestationRecord.Name, c.AttestationRecord.HeightField, height))
	if err != nil {
		logger.Sugar.Warnf("Cannot read the signatures held at block %d: %v (chain: %s)", height, err, c.Prefix)
		return false
	}
	counted := make(map[string]bool)
	for _, record := range rows["attestations"] {
		for _, signer := range record.Signers {
			counted[signer] = true
		}
	}
	var uncounted []string
	for _, signature := range rows["signatures"] {
		if !counted[signature.Signer] && !handed[signature.DocID] {
			handed[signature.DocID] = true
			uncounted = append(uncounted, signature.DocID)
		}
	}
	if len(uncounted) == 0 {
		return false
	}
	logger.Sugar.Infof("Counting %d held signatures at block %d (chain: %s)", len(uncounted), height, c.Prefix)
	v.count(ctx, uncounted)
	return true
}

// heldAbove reports whether the host holds a Header above height: the chain has moved past height,
// so its documents exist even if the host has not received them.
func (v *Verifier) heldAbove(ctx context.Context, height int64) bool {
	h := v.collections.Header
	rows, err := v.query(ctx, fmt.Sprintf(`query { headers: %s(filter: {%s: {_gt: %d}}, limit: 1) { height: %s } }`,
		h.Name, h.HeightField, height, h.HeightField))
	if err != nil {
		logger.Sugar.Warnf("Cannot read the Headers held above block %d: %v (chain: %s)", height, err, v.collections.Prefix)
		return false
	}
	return len(rows["headers"]) > 0
}

// repair asks peers for the documents the chain's generators hold at height and the heights above
// it, up to repairHeights in all, that the host does not hold. Fetched signatures are counted when
// Run next checks their height.
func (v *Verifier) repair(ctx context.Context, height int64) {
	c := v.collections
	to := height + repairHeights - 1
	listed, err := v.documents(ctx, height, to)
	if err != nil {
		logger.Sugar.Warnf("Cannot list the generators' documents at blocks %d-%d: %v (chain: %s)", height, to, err, c.Prefix)
		return
	}
	held, err := v.heldIDs(ctx, height, to)
	if err != nil {
		logger.Sugar.Warnf("Cannot read the documents held at blocks %d-%d: %v (chain: %s)", height, to, err, c.Prefix)
		return
	}
	for _, col := range c.Generated() {
		var missing []string
		for _, id := range listed[col.Name] {
			if !held[id] {
				missing = append(missing, id)
			}
		}
		if len(missing) == 0 {
			continue
		}
		syncCtx, cancel := context.WithTimeout(ctx, syncTimeout)
		err := v.node.DB.SyncDocuments(syncCtx, col.Name, missing)
		cancel()
		if err != nil {
			logger.Sugar.Warnf("Cannot fetch %d missing %s documents at blocks %d-%d: %v", len(missing), col.Name, height, to, err)
			continue
		}
		logger.Sugar.Infof("Requested %d missing %s documents at blocks %d-%d from peers", len(missing), col.Name, height, to)
	}
}

// heldIDs returns the IDs of the documents the host holds in the chain's generated collections at
// the heights from through to.
func (v *Verifier) heldIDs(ctx context.Context, from, to int64) (map[string]bool, error) {
	var query strings.Builder
	query.WriteString("query { ")
	for i, col := range v.collections.Generated() {
		fmt.Fprintf(&query, `c%d: %s(filter: {%s: {_geq: %d, _leq: %d}}) { _docID } `, i, col.Name, col.HeightField, from, to)
	}
	query.WriteString("}")
	rows, err := v.query(ctx, query.String())
	if err != nil {
		return nil, err
	}
	held := make(map[string]bool)
	for _, docs := range rows {
		for _, doc := range docs {
			held[doc.DocID] = true
		}
	}
	return held, nil
}

// agreedAttestation returns the one attestation record among records that requiredSigners of the
// chain's signers back. Records hold only signatures that passed the chain's signer and signature
// checks, one record per signed set of documents, each signer counted once.
func agreedAttestation(records []row) (row, error) {
	var agreed []row
	for _, r := range records {
		if len(r.Signers) >= requiredSigners {
			agreed = append(agreed, r)
		}
	}
	switch len(agreed) {
	case 0:
		return row{}, errTooFewSigners
	case 1:
		return agreed[0], nil
	default:
		return row{}, errSignersDisagree
	}
}

// record writes height's VerifiedBlock document.
func (v *Verifier) record(ctx context.Context, height int64, hash string, signers []string) error {
	col, err := v.node.DB.GetCollectionByName(ctx, v.collections.VerifiedBlock.Name)
	if err != nil {
		return fmt.Errorf("get %s: %w", v.collections.VerifiedBlock.Name, err)
	}
	signersAny := make([]any, len(signers))
	for i, s := range signers {
		signersAny[i] = s
	}
	doc, err := client.NewDocFromMap(ctx, map[string]any{
		v.collections.VerifiedBlock.HeightField: height,
		"blockHash":                             hash,
		"signers":                               signersAny,
	}, col.Version())
	if err != nil {
		return fmt.Errorf("build verified block %d: %w", height, err)
	}
	if err := col.AddDocument(ctx, doc); err != nil {
		return fmt.Errorf("record verified block %d: %w", height, err)
	}
	return nil
}

// row is a document a verifier query returns, with the fields that query selects: a height, a
// Header's hash, a Header's or log's current version, an attestation record's signers and signed
// CIDs, or a BlockSignature's ID and signer.
type row struct {
	Height  int64  `json:"height"`
	Hash    string `json:"hash"`
	Version []struct {
		CID string `json:"cid"`
	} `json:"_version"`
	Signers []string `json:"source_doc"`
	CIDs    []string `json:"CIDs"`
	DocID   string   `json:"_docID"`
	Signer  string   `json:"signatureIdentity"`
}

// query runs a GraphQL request and returns its rows by the name each table is selected under.
func (v *Verifier) query(ctx context.Context, request string) (map[string][]row, error) {
	result := v.node.DB.ExecRequest(ctx, request)
	if len(result.GQL.Errors) > 0 {
		return nil, fmt.Errorf("query %s tables: %w", v.collections.Prefix, result.GQL.Errors[0])
	}
	data, err := json.Marshal(result.GQL.Data)
	if err != nil {
		return nil, fmt.Errorf("read query result: %w", err)
	}
	var rows map[string][]row
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, fmt.Errorf("read query result: %w", err)
	}
	return rows, nil
}

// wait waits retryInterval and reports whether ctx is still live.
func wait(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(retryInterval):
		return true
	}
}
