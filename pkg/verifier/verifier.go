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
}

// New returns a verifier for the tokens-mode chain whose collections are c.
func New(n *node.Node, c chain.Collections) *Verifier {
	return &Verifier{node: n, collections: c}
}

// Run verifies heights in order until ctx is done. It starts above the highest verified height or,
// on a chain with none, at the lowest height whose Header the host holds. A height that does not
// pass is checked again every retryInterval and holds back every height above it.
func (v *Verifier) Run(ctx context.Context) {
	height, err := v.firstHeight(ctx)
	if err != nil {
		return
	}
	var reported string
	for {
		err := v.verify(ctx, height)
		if err == nil {
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
// Header's hash, a Header's or log's current version, or an attestation record's signers and
// signed CIDs.
type row struct {
	Height  int64  `json:"height"`
	Hash    string `json:"hash"`
	Version []struct {
		CID string `json:"cid"`
	} `json:"_version"`
	Signers []string `json:"source_doc"`
	CIDs    []string `json:"CIDs"`
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
