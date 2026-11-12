// Package verifier records a tokens-mode chain's verified blocks: heights where the evidence codec's
// rule accepts the block from the documents the host holds.
package verifier

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/node"

	"github.com/shinzonetwork/shinzo-evidence-codec/rule"
	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
)

// retryInterval is how long the verifier waits before it checks an unverified height again, or
// retries a VerifiedBlock write that failed.
const retryInterval = time.Second

var (
	errNotAccepted = errors.New("the rule does not accept the block")
	errStartHash   = errors.New("the agreed block at the start height is not start.hash")
)

// Hooks are called as the verifier makes progress. A nil hook is skipped.
type Hooks struct {
	// Verified is called with each block once its VerifiedBlock document is written, and the
	// signers who agreed on it.
	Verified func(b rule.Block, signers []string)
}

// Verifier checks a tokens-mode chain's heights in order and records each one the rule accepts in
// the chain's VerifiedBlock table.
type Verifier struct {
	node        *node.Node
	collections chain.Collections
	rule        *rule.Chain
	// assets are the IDs of the assets the rule checks, which each VerifiedBlock document names.
	assets []string
	// startHeight and startHash are start.block and start.hash.
	startHeight uint64
	startHash   string
	hooks       Hooks
	// write records an accepted block. It is v.record except in one test, which makes a single
	// write fail; no store failure a test can cause fails once and then clears.
	write func(ctx context.Context, b rule.Block, signers []string) error
}

// New returns a verifier for the tokens-mode chain c, whose collections are collections, that calls
// hooks. It returns an error when the evidence codec refuses c's rule configuration.
func New(n *node.Node, c chain.Config, collections chain.Collections, hooks Hooks) (*Verifier, error) {
	r, err := rule.New(c.RuleConfig())
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
		startHeight: uint64(c.Start.Block), //nolint:gosec // chain.Validate refuses a negative start.block
		startHash:   c.Start.Hash,
		hooks:       hooks,
	}
	v.write = v.record
	return v, nil
}

// Run verifies heights in order until ctx is done, starting above the last verified block, or at
// start.block on an empty chain. A height that fails is retried every retryInterval and holds back
// the heights above it.
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
	var reported string
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
			reported = ""
			continue
		}
		if ctx.Err() != nil {
			return
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
	if !verdict.Accepted {
		return rule.Block{}, nil, fmt.Errorf("%w: %s", errNotAccepted, verdict.Reason)
	}
	if parent == nil && verdict.Block.Hash != v.startHash {
		return rule.Block{}, nil, fmt.Errorf("%w: agreed %s, start.hash %s", errStartHash, verdict.Block.Hash, v.startHash)
	}
	return verdict.Block, verdict.Signers, nil
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
