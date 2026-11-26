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

	// repairAfter is how long a height may wait for documents, while the host holds a later Header,
	// before the verifier fetches the documents the generators hold for it that the host does not.
	repairAfter = 10 * time.Second

	// maxRepairWait caps the wait before the next repair of a height, which doubles from
	// repairAfter after each repair that requested nothing.
	maxRepairWait = 5 * time.Minute

	// stallAfter is how long a height may wait for documents before it holds although no later
	// Header is held, as when the chain's generators stop. It is longer than the gap between
	// Ethereum's finalized batches, which a height waits through without holding.
	stallAfter = 10 * time.Minute

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

// ErrNotStored is wrapped by a Documents hook's error when no generator stores the heights asked
// for any longer, so a repair cannot fetch them.
var ErrNotStored = errors.New("no generator stores the heights asked for")

// Hooks are called as the verifier makes progress. A nil hook is skipped.
type Hooks struct {
	// Verified is called with the height of each block once its VerifiedBlock document is written.
	Verified func(height uint64)
	// Unverified is called with the lowest height not yet verified: when Run starts and after each
	// verified block.
	Unverified func(height uint64)
	// Held is called with a height when it starts to hold and whenever the reason it holds changes.
	// A height holds when the rule refuses it, or while it waits for documents once it has waited
	// repairAfter with a later Header held, or stallAfter.
	Held func(height uint64)
	// Dropped is called with one of the Dropped constants for each listed signer left out of a
	// verified block, once per signer and reason. A held height reports nothing, since documents
	// still to come can bring a signer into the agreed group.
	Dropped func(reason string)
	// Unlisted is called for an attestation from a signer the chain does not list, once per height
	// and signer.
	Unlisted func()
	// Repairing is called each time the verifier asks the generators for a held height's documents.
	Repairing func()
	// Documents lists the IDs of the documents the chain's generators hold at the heights from
	// through to, by collection. Its error wraps ErrNotStored when no generator stores from any
	// longer. Without it the verifier does not fetch missing documents.
	Documents func(ctx context.Context, from, to uint64) (map[string][]string, error)
}

// repairResult is how a background repair of height ended: the documents it requested from peers,
// and whether no generator stores height any longer.
type repairResult struct {
	height    uint64
	requested int64
	notStored bool
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
	stallAfter  time.Duration
	// failingSince is when the height being checked first failed, zero until it does. held is
	// whether it holds, and holdReason the reason last logged for it.
	failingSince time.Time
	held         bool
	holdReason   string
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
		stallAfter:  stallAfter,
		reported:    make(map[string]bool),
	}
	v.write = v.record
	return v, nil
}

// Run verifies heights in order until ctx is done, starting above the last verified block, or at
// start.block on an empty chain. A height that fails is retried every retryInterval and holds back
// the heights above it. A height that waits for documents while a later Header is held is repaired
// in the background, one repair at a time, while the verifier keeps checking it: its missing
// documents are fetched from the generators after repairAfter, and again after a wait that doubles,
// up to maxRepairWait, while repairs request nothing. A height no generator stores any longer is not
// repaired again. A height that holds, as Hooks.Held defines it, is logged at
// error level, and at info level once it verifies.
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
	var repairs sync.WaitGroup
	defer repairs.Wait()
	repaired := make(chan repairResult, 1)
	repairing, notStored := false, false
	repairWait, waitingSince := v.repairAfter, time.Now()
	for {
		block, signers, err := v.verify(ctx, height, parent)
		if err == nil {
			if !v.writeUntilDone(ctx, block, signers) {
				return
			}
			v.endHold(block.Number)
			if v.hooks.Verified != nil {
				v.hooks.Verified(block.Number)
			}
			parent = &block
			height++
			v.unverified(height)
			repairWait, waitingSince, notStored = v.repairAfter, time.Now(), false
			continue
		}
		if ctx.Err() != nil {
			return
		}
		behind := v.checkHold(ctx, height, err)
		select {
		case r := <-repaired:
			repairing = false
			if r.height == height {
				repairWait, waitingSince, notStored = v.repairWaitAfter(repairWait, r.requested), time.Now(), r.notStored
			}
		default:
		}
		if !repairing && !notStored && v.hooks.Documents != nil && behind && time.Since(waitingSince) >= repairWait {
			if v.hooks.Repairing != nil {
				v.hooks.Repairing()
			}
			repairing = true
			h := height
			repairs.Go(func() { repaired <- v.repairInBackground(ctx, h) })
		}
		if !wait(ctx) {
			return
		}
	}
}

// checkHold records that height failed with err, and reports whether it has fallen behind: it
// waits for documents, has waited repairAfter, and a later Header is held. Once height holds, each
// new reason is logged at error level and passed to the Held hook.
func (v *Verifier) checkHold(ctx context.Context, height uint64, err error) (behind bool) {
	if v.failingSince.IsZero() {
		v.failingSince = time.Now()
	}
	waited := time.Since(v.failingSince)
	awaiting := errors.Is(err, errAwaitingDocuments)
	behind = awaiting && waited >= v.repairAfter && v.heldAbove(ctx, height)
	v.held = v.held || !awaiting || behind || waited >= v.stallAfter
	if v.held && err.Error() != v.holdReason {
		v.holdReason = err.Error()
		logger.Sugar.Errorf("Block %d is held: %v (chain: %s)", height, err, v.collections.Prefix)
		if v.hooks.Held != nil {
			v.hooks.Held(height)
		}
	}
	return behind
}

// endHold logs that the block at height verified after holding, if it held, and clears the hold for
// the next height.
func (v *Verifier) endHold(height uint64) {
	if v.held {
		logger.Sugar.Infof("Block %d verified after being held for %v (chain: %s)", height,
			time.Since(v.failingSince).Round(time.Second), v.collections.Prefix)
	}
	v.failingSince, v.held, v.holdReason = time.Time{}, false, ""
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
// start.block, and the block must also have start.hash. Otherwise it returns why not, wrapping
// errAwaitingDocuments when the rule may accept the block once more documents arrive.
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

// repairWaitAfter returns the wait before the next repair of a height, given the wait before the
// last one and how many documents it requested.
func (v *Verifier) repairWaitAfter(wait time.Duration, requested int64) time.Duration {
	if requested > 0 {
		return v.repairAfter
	}
	return min(2*wait, maxRepairWait) //nolint:mnd // the wait doubles
}

// repairInBackground repairs height and logs why it could not, at Error when no generator stores
// height any longer.
func (v *Verifier) repairInBackground(ctx context.Context, height uint64) repairResult {
	requested, err := v.repair(ctx, height)
	notStored := errors.Is(err, ErrNotStored)
	switch {
	case notStored:
		logger.Sugar.Errorf("Block %d cannot be repaired: %v (chain: %s)", height, err, v.collections.Prefix)
	case err != nil && ctx.Err() == nil:
		logger.Sugar.Warnf("Block %d not repaired: %v (chain: %s)", height, err, v.collections.Prefix)
	}
	return repairResult{height: height, requested: requested, notStored: notStored}
}

// repair asks peers for the documents the chain's generators hold at height and the heights above
// it, up to repairHeights in all, that the host does not hold. Each collection's missing documents
// are fetched in batches of syncBatch, syncCalls at once, each within syncTimeout. It returns how
// many documents it requested, or why it could not list them. When the host holds every listed
// document, it logs that at Info, since the height then waits for something other than a missing
// document.
func (v *Verifier) repair(ctx context.Context, height uint64) (int64, error) {
	c := v.collections
	to := height + repairHeights - 1
	listed, err := v.hooks.Documents(ctx, height, to)
	if err != nil {
		return 0, fmt.Errorf("list the generators' documents at blocks %d-%d: %w", height, to, err)
	}
	held, err := v.heldIDs(ctx, height, to)
	if err != nil {
		return 0, fmt.Errorf("read the documents held at blocks %d-%d: %w", height, to, err)
	}
	var total int64
	anyMissing := false
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
		anyMissing = true
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
			total += n
		}
	}
	if !anyMissing {
		logger.Sugar.Infof("Block %d not repaired: the host already holds every document the generators list at blocks %d-%d (chain: %s)",
			height, height, to, c.Prefix)
	}
	return total, nil
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
