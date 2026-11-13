// Package verifier records a tokens-mode chain's verified blocks: heights where the evidence codec's
// rule accepts the block from the documents the host holds.
package verifier

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/node"

	"github.com/shinzonetwork/shinzo-evidence-codec/rule"
	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
)

const (
	// retryInterval is how long the verifier waits before it checks an unverified height again, or
	// retries a VerifiedBlock write that failed.
	retryInterval = time.Second

	// repairAfter is how long a height may stay unverified, while the host holds a later Header,
	// before the verifier fetches the documents the generators hold for it that the host does not.
	repairAfter = 10 * time.Second

	// repairHeights is how many heights, from the unverified one up, one repair covers: the most a
	// generator's documents endpoint lists in one request.
	repairHeights = 1000

	// syncTimeout bounds one document sync, so a peer that never answers cannot hold up the
	// verifier.
	syncTimeout = 30 * time.Second

	// syncCalls is how many document syncs a repair runs at once.
	syncCalls = 8

	// syncBatch is how many documents one sync asks for.
	syncBatch = 50
)

// Why the rule left out a listed signer's attestation, as Hooks.Dropped reports it.
const (
	DroppedSignature    = "signature"
	DroppedStatement    = "statement"
	DroppedWrongBlock   = "wrong_block"
	DroppedHeader       = "header"
	DroppedBadHeader    = "bad_header"
	DroppedMissingClaim = "missing_claim"
	DroppedEmptyClaim   = "empty_claim"
	DroppedBelowK       = "below_k"
	DroppedEvidence     = "evidence"
	DroppedCapture      = "capture"
	DroppedBadCapture   = "bad_capture"
	DroppedOther        = "other" // a reason this host does not know, from a newer codec
)

var (
	errNotAccepted = errors.New("the rule does not accept the block")
	// errAwaitingDocuments is returned for a height the rule may still accept once more of its
	// documents arrive; see awaitsDocuments.
	errAwaitingDocuments = errors.New("the rule cannot decide until more documents arrive")
	errStartHash         = errors.New("the agreed block at the start height is not start.hash")
)

// Hooks are called as the verifier makes progress. A nil hook is skipped.
type Hooks struct {
	// Verified is called with each block once its VerifiedBlock document is written, and the
	// signers who agreed on it.
	Verified func(b rule.Block, signers []string)
	// Unverified is called with the lowest height not yet verified: when Run starts and after each
	// verified block.
	Unverified func(height uint64)
	// Held is called with a height when it first holds and whenever the reason it holds changes.
	Held func(height uint64)
	// Dropped is called with one of the Dropped constants for each listed signer left out of a
	// verified block, once per signer and reason. A held height reports nothing, since documents
	// still to come can bring a signer into the agreed group.
	Dropped func(reason string)
	// Unlisted is called for an attestation from a signer the chain does not list, once per height
	// and signer.
	Unlisted func()
	// Documents lists the IDs of the documents the chain's generators hold at the heights from
	// through to, by collection. Without it the verifier does not fetch missing documents.
	Documents func(ctx context.Context, from, to uint64) (map[string][]string, error)
}

// Verifier checks a tokens-mode chain's heights in order and records each one the rule accepts in
// the chain's VerifiedBlock table.
type Verifier struct {
	node        *node.Node
	collections chain.Collections
	rule        *rule.Chain
	// assets are the IDs of the assets the rule checks, which each VerifiedBlock document names.
	assets []string
	// required is how many listed signers must agree on a block.
	required int
	// startHeight and startHash are start.block and start.hash.
	startHeight uint64
	startHash   string
	hooks       Hooks
	repairAfter time.Duration
	// reported holds the unlisted signers already passed to the Unlisted hook at reportedHeight,
	// so a height checked many times reports each of them once.
	reportedHeight uint64
	reported       map[string]bool
	// write records an accepted block. It is v.record except in one test, which makes a single
	// write fail; no store failure a test can cause fails once and then clears.
	write func(ctx context.Context, b rule.Block, signers []string) error
}

// New returns a verifier for the tokens-mode chain c, whose collections are collections, that calls
// hooks. It returns an error when the evidence codec refuses c's rule configuration.
func New(n *node.Node, c chain.Config, collections chain.Collections, hooks Hooks) (*Verifier, error) {
	config := c.RuleConfig()
	r, err := rule.New(config)
	if err != nil {
		return nil, fmt.Errorf("chain %s: %w", c.Prefix, err)
	}
	assets := make([]string, len(c.Assets))
	for i, a := range c.Assets {
		assets[i] = a.ID
	}
	v := &Verifier{
		node:        n,
		collections: collections,
		rule:        r,
		assets:      assets,
		required:    config.Required,
		startHeight: uint64(c.Start.Block), //nolint:gosec // chain.Validate refuses a negative start.block
		startHash:   c.Start.Hash,
		hooks:       hooks,
		repairAfter: repairAfter,
		reported:    make(map[string]bool),
	}
	v.write = v.record
	return v, nil
}

// Run verifies heights in order until ctx is done, starting above the last verified block, or at
// start.block on an empty chain. A height that fails is retried every retryInterval and holds back
// the heights above it. After repairAfter, if a later Header is held, the verifier fetches the
// height's missing documents from the generators.
func (v *Verifier) Run(ctx context.Context) {
	last, found, err := v.lastVerified(ctx)
	if err != nil {
		return
	}
	height := v.startHeight
	var parent *rule.Block
	if found {
		parent, height = &last, last.Number+1
	}
	v.unverified(height)
	var reported string
	waitingSince := time.Now()
	for {
		block, signers, err := v.verify(ctx, height, parent)
		if err == nil {
			if !v.writeUntilDone(ctx, block, signers) {
				return
			}
			if v.hooks.Verified != nil {
				v.hooks.Verified(block, signers)
			}
			parent = &block
			height++
			v.unverified(height)
			reported = ""
			waitingSince = time.Now()
			continue
		}
		if ctx.Err() != nil {
			return
		}
		if v.hooks.Documents != nil && time.Since(waitingSince) >= v.repairAfter && v.heldAbove(ctx, height) {
			v.repair(ctx, height)
			waitingSince = time.Now()
			continue
		}
		if msg := err.Error(); msg != reported {
			reported = msg
			if errors.Is(err, errAwaitingDocuments) {
				logger.Sugar.Infof("Block %d is held: %v (chain: %s)", height, err, v.collections.Prefix)
			} else {
				logger.Sugar.Errorf("Block %d is held: %v (chain: %s)", height, err, v.collections.Prefix)
			}
			if v.hooks.Held != nil {
				v.hooks.Held(height)
			}
		}
		if !wait(ctx) {
			return
		}
	}
}

// unverified calls the Unverified hook, if any, with height.
func (v *Verifier) unverified(height uint64) {
	if v.hooks.Unverified != nil {
		v.hooks.Unverified(height)
	}
}

// lastVerified returns the highest verified block, and false when the chain has none, retrying a
// failed read every retryInterval. It returns ctx's error once ctx is done.
func (v *Verifier) lastVerified(ctx context.Context) (rule.Block, bool, error) {
	vb := v.collections.VerifiedBlock
	request := fmt.Sprintf(`query { blocks: %s(order: {%s: DESC}, limit: 1) { height: %s blockHash } }`,
		vb.Name, vb.HeightField, vb.HeightField)
	for {
		rows, err := v.query(ctx, request)
		if err == nil {
			if len(rows["blocks"]) == 0 {
				return rule.Block{}, false, nil
			}
			last := rows["blocks"][0]
			return rule.Block{Number: last.Height, Hash: last.BlockHash}, true, nil
		}
		logger.Sugar.Warnf("Block verification cannot read its last verified block: %v (chain: %s)", err, v.collections.Prefix)
		if !wait(ctx) {
			return rule.Block{}, false, ctx.Err()
		}
	}
}

// verify returns the block at height and its agreeing signers when the rule accepts it from the
// documents held there, with parent as the previous verified block. With no parent, height is
// start.block, and the block must also have start.hash. Otherwise it returns why not.
func (v *Verifier) verify(ctx context.Context, height uint64, parent *rule.Block) (rule.Block, []string, error) {
	docs, err := v.documents(ctx, height)
	if err != nil {
		return rule.Block{}, nil, err
	}
	verdict := v.rule.Check(height, parent, docs)
	v.reportUnlisted(height, verdict.Results)
	if !verdict.Accepted {
		if awaitsDocuments(verdict, v.required) {
			return rule.Block{}, nil, fmt.Errorf("%w: %s", errAwaitingDocuments, verdict.Reason)
		}
		return rule.Block{}, nil, fmt.Errorf("%w: %s", errNotAccepted, verdict.Reason)
	}
	if parent == nil && verdict.Block.Hash != v.startHash {
		return rule.Block{}, nil, fmt.Errorf("%w: agreed %s, start.hash %s", errStartHash, verdict.Block.Hash, v.startHash)
	}
	v.reportDropped(verdict)
	return verdict.Block, verdict.Signers, nil
}

// awaitsDocuments reports whether the rule may accept a height it did not accept once more
// documents arrive: each listed signer's attestation waits for a document or is in a group below k,
// and those groups name one block and hold fewer than required signers between them.
func awaitsDocuments(verdict rule.Verdict, required int) bool {
	if verdict.Reason != rule.ReasonNoAgreement {
		return false
	}
	short := make(map[string]bool)
	var block string
	for _, r := range verdict.Results {
		switch r.Reason {
		case rule.ReasonUnlisted, rule.ReasonHeader, rule.ReasonEvidence, rule.ReasonCapture:
			// An unlisted signer never counts, and the others wait for a document.
		case rule.ReasonBelowK:
			if block != "" && r.Hash != block {
				return false
			}
			block = r.Hash
			short[r.Signer] = true
		default:
			return false
		}
	}
	return len(short) < required
}

// reportUnlisted passes each attestation from a signer the chain does not list to the Unlisted
// hook, once per height and signer.
func (v *Verifier) reportUnlisted(height uint64, results []rule.Result) {
	if height != v.reportedHeight {
		v.reportedHeight = height
		clear(v.reported)
	}
	for _, r := range results {
		if r.Reason != rule.ReasonUnlisted || v.reported[r.Signer] {
			continue
		}
		v.reported[r.Signer] = true
		if v.hooks.Unlisted != nil {
			v.hooks.Unlisted()
		}
	}
}

// reportDropped passes to the Dropped hook why each listed signer that attested at a verified
// block's height, but is not among the signers it was verified with, did not count, once per signer
// and reason. A signer with an attestation that counted is not dropped.
func (v *Verifier) reportDropped(verdict rule.Verdict) {
	if v.hooks.Dropped == nil {
		return
	}
	seen := make(map[rule.Result]bool)
	for _, r := range verdict.Results {
		key := rule.Result{Signer: r.Signer, Reason: r.Reason}
		if r.Reason == "" || r.Reason == rule.ReasonUnlisted || slices.Contains(verdict.Signers, r.Signer) || seen[key] {
			continue
		}
		seen[key] = true
		v.hooks.Dropped(droppedReason(r.Reason))
	}
}

// droppedReason returns the Dropped constant for one of the rule's reasons for leaving out an
// attestation.
func droppedReason(reason string) string {
	switch reason {
	case rule.ReasonSignature:
		return DroppedSignature
	case rule.ReasonStatement:
		return DroppedStatement
	case rule.ReasonWrongBlock:
		return DroppedWrongBlock
	case rule.ReasonHeader:
		return DroppedHeader
	case rule.ReasonBadHeader:
		return DroppedBadHeader
	case rule.ReasonMissingClaim:
		return DroppedMissingClaim
	case rule.ReasonEmptyClaim:
		return DroppedEmptyClaim
	case rule.ReasonBelowK:
		return DroppedBelowK
	case rule.ReasonEvidence:
		return DroppedEvidence
	case rule.ReasonCapture:
		return DroppedCapture
	case rule.ReasonBadCapture:
		return DroppedBadCapture
	default:
		return DroppedOther
	}
}

// documents returns the Header, Evidence, StateCapture and BlockAttestation documents held at
// height in the form the rule takes. A document whose Blob field does not decode is left out, as if
// the host did not hold it.
func (v *Verifier) documents(ctx context.Context, height uint64) (rule.Documents, error) {
	c := v.collections
	rows, err := v.query(ctx, fmt.Sprintf(`query { `+
		`headers: %s(filter: {%s: {_eq: %d}}) { hash raw } `+
		`evidence: %s(filter: {%s: {_eq: %d}}) { blockHash asset records proof } `+
		`captures: %s(filter: {%s: {_eq: %d}}) { blockHash asset value proof } `+
		`attestations: %s(filter: {%s: {_eq: %d}}) { signer signatureType signature statement } }`,
		c.Header.Name, c.Header.HeightField, height,
		c.Evidence.Name, c.Evidence.HeightField, height,
		c.StateCapture.Name, c.StateCapture.HeightField, height,
		c.BlockAttestation.Name, c.BlockAttestation.HeightField, height))
	if err != nil {
		return rule.Documents{}, err
	}

	var docs rule.Documents
	for _, r := range rows["headers"] {
		if raw, ok := decodeBlob(r.Raw); ok {
			docs.Headers = append(docs.Headers, rule.Header{Hash: r.Hash, Raw: raw})
		}
	}
	for _, r := range rows["evidence"] {
		records, okRecords := decodeBlob(r.Records)
		proof, okProof := decodeBlob(r.Proof)
		if okRecords && okProof {
			docs.Evidence = append(docs.Evidence, rule.Evidence{BlockHash: r.BlockHash, Asset: r.Asset, Records: records, Proof: proof})
		}
	}
	for _, r := range rows["captures"] {
		if proof, ok := decodeBlob(r.Proof); ok {
			docs.Captures = append(docs.Captures, rule.Capture{BlockHash: r.BlockHash, Asset: r.Asset, Value: r.Value, Proof: proof})
		}
	}
	for _, r := range rows["attestations"] {
		if statement, ok := decodeBlob(r.Statement); ok {
			docs.Attestations = append(docs.Attestations, rule.Attestation{
				Signer: r.Signer, SignatureType: r.SignatureType, Signature: r.Signature, Statement: statement,
			})
		}
	}
	return docs, nil
}

// decodeBlob decodes a Blob field, which generators write as hex without 0x and a query returns as
// stored. It reports false for a value that is empty or not hex.
func decodeBlob(s string) ([]byte, bool) {
	b, err := hex.DecodeString(s)
	if err != nil || len(b) == 0 {
		return nil, false
	}
	return b, true
}

// writeUntilDone writes an accepted block, retrying every retryInterval until the write succeeds.
// It reports false when ctx is done first.
func (v *Verifier) writeUntilDone(ctx context.Context, b rule.Block, signers []string) bool {
	for {
		err := v.write(ctx, b, signers)
		if err == nil {
			return true
		}
		logger.Sugar.Warnf("Block %d verified but not recorded, retrying: %v (chain: %s)", b.Number, err, v.collections.Prefix)
		if !wait(ctx) {
			return false
		}
	}
}

// record writes b's VerifiedBlock document unless the table already holds one at b's height, so a
// write retried after it committed adds nothing. defradb's ErrDocumentAlreadyExists is internal and
// cannot be matched, so the existing document is found by query.
func (v *Verifier) record(ctx context.Context, b rule.Block, signers []string) error {
	vb := v.collections.VerifiedBlock
	rows, err := v.query(ctx, fmt.Sprintf(`query { held: %s(filter: {%s: {_eq: %d}}) { height: %s } }`,
		vb.Name, vb.HeightField, b.Number, vb.HeightField))
	if err != nil {
		return err
	}
	if len(rows["held"]) > 0 {
		return nil
	}
	col, err := v.node.DB.GetCollectionByName(ctx, vb.Name)
	if err != nil {
		return fmt.Errorf("get %s: %w", vb.Name, err)
	}
	doc, err := client.NewDocFromMap(ctx, map[string]any{
		vb.HeightField: int64(b.Number), //nolint:gosec // b.Number counts up by one from the int64 start.block
		"blockHash":    b.Hash,
		"signers":      listField(signers),
		"assets":       listField(v.assets),
	}, col.Version())
	if err != nil {
		return fmt.Errorf("build verified block %d: %w", b.Number, err)
	}
	if err := col.AddDocument(ctx, doc); err != nil {
		return fmt.Errorf("record verified block %d: %w", b.Number, err)
	}
	return nil
}

// listField returns values in the form a [String] document field takes.
func listField(values []string) []any {
	list := make([]any, len(values))
	for i, s := range values {
		list[i] = s
	}
	return list
}

// heldAbove reports whether the host holds a Header above height: the chain has moved past height,
// so its documents exist even if the host has not received them.
func (v *Verifier) heldAbove(ctx context.Context, height uint64) bool {
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
// it, up to repairHeights in all, that the host does not hold. Each collection's missing documents
// are fetched in batches of syncBatch, syncCalls at once, each within syncTimeout.
func (v *Verifier) repair(ctx context.Context, height uint64) {
	c := v.collections
	to := height + repairHeights - 1
	listed, err := v.hooks.Documents(ctx, height, to)
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
		var requested atomic.Int64
		var wg sync.WaitGroup
		calls := make(chan struct{}, syncCalls)
		for batch := range slices.Chunk(missing, syncBatch) {
			calls <- struct{}{}
			wg.Go(func() {
				defer func() { <-calls }()
				syncCtx, cancel := context.WithTimeout(ctx, syncTimeout)
				defer cancel()
				if err := v.node.DB.SyncDocuments(syncCtx, col.Name, batch); err != nil {
					logger.Sugar.Warnf("Cannot fetch %d missing %s documents at blocks %d-%d: %v", len(batch), col.Name, height, to, err)
					return
				}
				requested.Add(int64(len(batch)))
			})
		}
		wg.Wait()
		if n := requested.Load(); n > 0 {
			logger.Sugar.Infof("Requested %d missing %s documents at blocks %d-%d from peers", n, col.Name, height, to)
		}
	}
}

// heldIDs returns the IDs of the documents the host holds in the chain's generated collections at
// the heights from through to.
func (v *Verifier) heldIDs(ctx context.Context, from, to uint64) (map[string]bool, error) {
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

// row is a document a verifier query returns, with the fields that query selects. Blob fields are
// hex strings.
type row struct {
	Height        uint64 `json:"height"`
	Hash          string `json:"hash"`
	Raw           string `json:"raw"`
	BlockHash     string `json:"blockHash"`
	Asset         string `json:"asset"`
	Records       string `json:"records"`
	Proof         string `json:"proof"`
	Value         string `json:"value"`
	Signer        string `json:"signer"`
	SignatureType string `json:"signatureType"`
	Signature     string `json:"signature"`
	Statement     string `json:"statement"`
	DocID         string `json:"_docID"`
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
