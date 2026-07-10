// Package acp implements the host-side query-billing middleware. The middleware
// recovers the payer from each request's signature, confirms the served query
// matches the signed query_hash, and authorizes the payer against their on-chain
// query balance before forwarding to defradb.
package acp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/pkg/accounting"
	"github.com/shinzonetwork/shinzo-querysig/billing"
)

// GraphQLPath is the request path the middleware gates. Requests with any other
// path are passed through to the wrapped handler unchanged.
const GraphQLPath = "/api/v0/graphql"

// DefaultRequestMaxAge bounds how far a request's signed timestamp may be from
// the host clock before the gate rejects it. Two minutes absorbs client/host
// clock skew while bounding how long a captured signature can be replayed.
const DefaultRequestMaxAge = 2 * time.Minute

// DefaultRecordTimeout bounds each service-record POST to the accounting service.
// Recording runs off the request path, so this caps a record goroutine's lifetime,
// not the client's response.
const DefaultRecordTimeout = 5 * time.Second

// defaultRecordConcurrency caps how many service records post at once, so a slow
// accounting service cannot spawn unbounded goroutines. Past the cap the gate
// blocks rather than drop a record.
const defaultRecordConcurrency = 64

// Authorizer decides whether a payer may run a query.
type Authorizer interface {
	Authorize(ctx context.Context, payer common.Address) (bool, error)
}

// Recorder submits a service record for a served query.
type Recorder interface {
	Record(ctx context.Context, in accounting.RecordInput) error
}

// Recording configures service-record submission on the allow path. A nil
// Recording (or nil Recorder) means the gate serves without recording. Attesters
// returns the host's observed attesting set at serve time, or nil.
type Recording struct {
	Recorder  Recorder
	Attesters func() []string
}

// Middleware gates incoming GraphQL queries on the billing model.
//
// Decision tree for a request to GraphQLPath:
//   - Body parse fails            -> 400 Bad Request
//   - No view collections         -> pass through (nothing to bill)
//   - More than one view          -> 400 Bad Request (a record attributes one pool)
//   - Missing/invalid signature   -> 403 Forbidden
//   - Stale or future request     -> 403 Forbidden
//   - Query/signature mismatch    -> 403 Forbidden
//   - Authorizer reports an error -> 503 Service Unavailable
//   - Balance below the minimum   -> 402 Payment Required
//   - Funded                      -> pass through
//
// Non-GraphQL paths bypass every step above.
type Middleware struct {
	authz         Authorizer
	registry      ViewRegistry
	chainID       uint64
	maxRequestAge time.Duration
	recording     *Recording
	log           *zap.SugaredLogger

	recordSem chan struct{}  // bounds concurrent record submissions
	recordWG  sync.WaitGroup // tracks in-flight records for Drain
}

// NewMiddleware returns a Middleware that authorizes via authz, resolves
// collection names to view addresses via registry, and verifies request
// signatures under chainID. maxRequestAge bounds how far a request's signed
// timestamp may be from the host clock (0 disables the freshness check). A nil
// recording serves without submitting service records. A nil log is replaced
// with a no-op logger.
func NewMiddleware(authz Authorizer, registry ViewRegistry, chainID uint64, maxRequestAge time.Duration, recording *Recording, log *zap.SugaredLogger) *Middleware {
	if log == nil {
		log = zap.NewNop().Sugar()
	}
	return &Middleware{
		authz:         authz,
		registry:      registry,
		chainID:       chainID,
		maxRequestAge: maxRequestAge,
		recording:     recording,
		log:           log,
		recordSem:     make(chan struct{}, defaultRecordConcurrency),
	}
}

// Wrap returns an http.Handler that runs the gating policy before delegating to
// next.
func (m *Middleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != GraphQLPath {
			next.ServeHTTP(w, r)
			return
		}
		m.handleGraphQL(w, r, next)
	})
}

// viewLookup pairs a collection name with its resolved contract address.
type viewLookup struct {
	Name    string
	Address string
}

func (m *Middleware) handleGraphQL(w http.ResponseWriter, r *http.Request, next http.Handler) { //nolint:funlen // linear guard-clause pipeline; splitting obscures the request flow
	req, err := parseGraphQLRequest(r)
	if err != nil {
		m.log.Warnw("billing.parse_failed", "err", err, "path", r.URL.Path)
		http.Error(w, "bad graphql request", http.StatusBadRequest)
		return
	}

	fields, err := topLevelFields(req.Query, req.OperationName)
	if err != nil {
		m.log.Warnw("billing.parse_failed", "err", err, "path", r.URL.Path)
		http.Error(w, "bad graphql request", http.StatusBadRequest)
		return
	}

	lookups, ok := m.collectViewLookups(w, collectionNames(fields))
	if !ok {
		return
	}
	if len(lookups) == 0 {
		// No view collections in this request: nothing to bill, pass through.
		next.ServeHTTP(w, r)
		return
	}
	if len(lookups) > 1 {
		// A single service record attributes one pool, so a billed query may
		// touch only one view.
		m.log.Infow("billing.deny", "reason", "multi_view", "views", len(lookups))
		http.Error(w, "a billed query may touch only one view", http.StatusBadRequest)
		return
	}

	ext, err := parseExtensions(req.Extensions)
	if err != nil {
		m.log.Infow("billing.deny", "reason", "no_signature", "err", err.Error())
		http.Error(w, "forbidden: missing request signature", http.StatusForbidden)
		return
	}

	if err := billing.CheckFreshness(ext.RequestTimestamp, time.Now(), m.maxRequestAge); err != nil {
		m.log.Infow("billing.deny", "reason", "stale_request", "err", err.Error())
		http.Error(w, "forbidden: stale or future request", http.StatusForbidden)
		return
	}

	payer, err := billing.VerifyRequest(m.chainID, req.Query, req.Variables, ext)
	if err != nil {
		m.log.Infow("billing.deny", "reason", "verify_failed", "err", err.Error())
		http.Error(w, "forbidden: request verification failed", http.StatusForbidden)
		return
	}

	allowed, err := m.authz.Authorize(r.Context(), payer)
	if err != nil {
		m.log.Errorw("billing.error", "payer", payer.Hex(), "view", lookups[0].Name, "err", err)
		http.Error(w, "authorization backend unavailable", http.StatusServiceUnavailable)
		return
	}
	if !allowed {
		m.log.Infow("billing.deny", "reason", "insufficient_balance", "payer", payer.Hex(), "view", lookups[0].Name)
		http.Error(w, "payment required: insufficient query balance", http.StatusPaymentRequired)
		return
	}

	// Capture the response to count the rows the view served (its response key(s);
	// other top-level fields are ignored).
	cw := newCaptureWriter(w)
	next.ServeHTTP(cw, r)
	rows, served := viewRows(cw.body.Bytes(), responseKeys(fields, lookups[0].Name))
	m.log.Infow("billing.allow",
		"payer", payer.Hex(),
		"view", lookups[0].Name,
		"rows", rows,
		"served", served,
		"status", cw.status,
	)
	// Bill only a 2xx response: a well-formed body can accompany a non-2xx
	// status, and the client treats that as a failure.
	if served && cw.status/100 == 2 {
		m.submitRecord(payer, ext, lookups[0], rows) //nolint:contextcheck // record runs on a background context by design so a client disconnect does not cancel billing submission
	}
}

// submitRecord posts the service record on a background goroutine so a slow
// accounting service does not delay the client's response. Concurrency is bounded:
// past the limit it blocks rather than drop a record. Drain waits for the in-flight
// records at shutdown so none is lost.
func (m *Middleware) submitRecord(payer common.Address, ext billing.Extensions, view viewLookup, rows uint64) {
	if m.recording == nil || m.recording.Recorder == nil {
		return
	}
	m.recordSem <- struct{}{}
	m.recordWG.Go(func() {
		defer func() {
			<-m.recordSem
			// net/http recovers a panic in the request goroutine but not one from a
			// goroutine the handler starts, so recover here to keep a bad record from
			// taking down the host.
			if r := recover(); r != nil {
				m.log.Errorw("billing.record_panic", "payer", payer.Hex(), "view", view.Name, "panic", r)
			}
		}()
		m.record(payer, ext, view, rows)
	})
}

// Drain waits for in-flight record submissions to finish, or until ctx is done.
// The host calls it after the GraphQL server stops so records already accepted are
// not lost on shutdown.
func (m *Middleware) Drain(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		m.recordWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// record builds and submits the service record for a served query. A failure is
// logged, not returned: recording is best-effort and must not change what the
// client receives. It runs on a context derived from context.Background(), not the
// request context, so a client that disconnects does not cancel the submission.
func (m *Middleware) record(payer common.Address, ext billing.Extensions, view viewLookup, rows uint64) {
	var attested []string
	if m.recording.Attesters != nil {
		attested = m.recording.Attesters()
	}
	ctx, cancel := context.WithTimeout(context.Background(), DefaultRecordTimeout)
	defer cancel()

	err := m.recording.Recorder.Record(ctx, accounting.RecordInput{
		Extensions:       ext,
		ViewAddress:      common.HexToAddress(view.Address),
		RowsQueried:      rows,
		AttestedIndexers: attested,
	})
	if err != nil {
		m.log.Errorw("billing.record_failed", "payer", payer.Hex(), "view", view.Name, "err", err)
	}
}

// collectViewLookups resolves each top-level collection name through the
// registry. Returns (lookups, true) when every view in the request has an
// on-chain address. Returns (nil, false) after writing a 503 response when a
// registered view is missing its address (fail-closed).
func (m *Middleware) collectViewLookups(w http.ResponseWriter, collections []string) ([]viewLookup, bool) {
	var lookups []viewLookup
	for _, name := range collections {
		if !m.registry.IsView(name) {
			continue
		}
		addr, ok := m.registry.ContractAddress(name)
		if !ok {
			m.log.Errorw("billing.view_without_address", "view", name, "action", "fail_closed")
			http.Error(w, "view metadata unavailable", http.StatusServiceUnavailable)
			return nil, false
		}
		lookups = append(lookups, viewLookup{Name: name, Address: addr})
	}
	return lookups, true
}

var (
	errNoExtensions       = errors.New("no extensions")
	errNoRequestSignature = errors.New("no request signature")
)

// parseExtensions decodes the signed billing envelope from a request's
// extensions. An absent envelope, or one without a signature, is an error: a
// billed query must carry a request signature.
func parseExtensions(raw json.RawMessage) (billing.Extensions, error) {
	if len(raw) == 0 {
		return billing.Extensions{}, errNoExtensions
	}
	var ext billing.Extensions
	if err := json.Unmarshal(raw, &ext); err != nil {
		return billing.Extensions{}, fmt.Errorf("parse extensions: %w", err)
	}
	if ext.RequestSignature == "" {
		return billing.Extensions{}, errNoRequestSignature
	}
	return ext, nil
}
