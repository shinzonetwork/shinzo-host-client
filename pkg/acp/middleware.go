package acp

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	acpIdentity "github.com/sourcenetwork/defradb/acp/identity"
	"go.uber.org/zap"
)

// GraphQLPath is the request path the middleware gates. Requests with any
// other path are passed through to the wrapped handler unchanged.
const GraphQLPath = "/api/v0/graphql"

const (
	authHeader   = "Authorization"
	bearerPrefix = "Bearer "
)

// ErrAuthRequired is the error returned by an Authenticator when the
// caller has not provided a valid bearer token. The middleware maps it
// to a 403 response.
var ErrAuthRequired = errors.New("authentication required")

// Authenticator extracts and verifies the caller identity from an HTTP
// request. The production implementation parses the bearer JWT and
// checks the audience claim; tests may substitute a stub.
type Authenticator interface {
	CallerDID(r *http.Request) (string, error)
}

// Middleware applies access-control to incoming GraphQL requests.
//
// Decision tree for a request to GraphQLPath:
//   - Body parse fails           -> 400 Bad Request
//   - No view collections        -> pass through (auth optional)
//   - View(s) but no/invalid JWT -> 403 Forbidden
//   - Authorizer reports error   -> 503 Service Unavailable
//   - Any view denied            -> 403 Forbidden
//   - All views allowed          -> pass through
//
// Non-GraphQL paths bypass every step above.
type Middleware struct {
	auth     Authenticator
	authz    Authorizer
	registry ViewRegistry
	log      *zap.SugaredLogger
}

// NewMiddleware returns a Middleware that uses auth to identify the
// caller, authz for per-view permission checks, and registry to resolve
// collection names to on-chain addresses. A nil log argument is replaced
// with a no-op logger so callers in tests do not have to initialize the
// package-level logger.
func NewMiddleware(auth Authenticator, authz Authorizer, registry ViewRegistry, log *zap.SugaredLogger) *Middleware {
	if log == nil {
		log = zap.NewNop().Sugar()
	}
	return &Middleware{auth: auth, authz: authz, registry: registry, log: log}
}

// Wrap returns an http.Handler that runs the gating policy before
// delegating to next.
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

func (m *Middleware) handleGraphQL(w http.ResponseWriter, r *http.Request, next http.Handler) {
	collections, err := ExtractCollections(r)
	if err != nil {
		m.log.Warnw("acp.parse_failed", "err", err, "path", r.URL.Path)
		http.Error(w, "bad graphql request", http.StatusBadRequest)
		return
	}

	lookups, ok := m.collectViewLookups(w, collections)
	if !ok {
		return
	}
	if len(lookups) == 0 {
		// No view collections in this request. Authentication is the
		// downstream handler's concern when there is nothing to gate.
		next.ServeHTTP(w, r)
		return
	}

	callerDID, err := m.auth.CallerDID(r)
	if err != nil {
		m.log.Infow("acp.deny",
			"reason", "no_identity",
			"err", err.Error(),
			"path", r.URL.Path)
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	for _, lookup := range lookups {
		if !m.authorizeOne(w, r, callerDID, lookup) {
			return
		}
	}

	next.ServeHTTP(w, r)
}

// collectViewLookups resolves each top-level collection name through the
// registry. Returns (lookups, true) when every view in the request has an
// on-chain address. Returns (nil, false) after writing a 503 response
// when a registered view is missing its address (fail-closed).
func (m *Middleware) collectViewLookups(w http.ResponseWriter, collections []string) ([]viewLookup, bool) {
	var lookups []viewLookup
	for _, name := range collections {
		if !m.registry.IsView(name) {
			continue
		}
		addr, ok := m.registry.ContractAddress(name)
		if !ok {
			m.log.Errorw("acp.view_without_address",
				"view", name,
				"action", "fail_closed")
			http.Error(w, "view metadata unavailable", http.StatusServiceUnavailable)
			return nil, false
		}
		lookups = append(lookups, viewLookup{Name: name, Address: addr})
	}
	return lookups, true
}

// authorizeOne issues a single AuthorizeView call. Returns true on allow,
// false after writing the appropriate error response on deny or error.
func (m *Middleware) authorizeOne(w http.ResponseWriter, r *http.Request, callerDID string, lookup viewLookup) bool {
	start := time.Now()
	allowed, err := m.authz.AuthorizeView(r.Context(), callerDID, lookup.Address)
	latencyMS := time.Since(start).Milliseconds()
	switch {
	case err != nil:
		m.log.Errorw("acp.error",
			"view", lookup.Name,
			"address", lookup.Address,
			"did", callerDID,
			"latency_ms", latencyMS,
			"err", err)
		http.Error(w, "authorization backend unavailable", http.StatusServiceUnavailable)
		return false
	case !allowed:
		m.log.Infow("acp.deny",
			"reason", "no_subscription",
			"view", lookup.Name,
			"address", lookup.Address,
			"did", callerDID,
			"latency_ms", latencyMS)
		http.Error(w, "forbidden: no subscription", http.StatusForbidden)
		return false
	default:
		m.log.Infow("acp.allow",
			"view", lookup.Name,
			"address", lookup.Address,
			"did", callerDID,
			"latency_ms", latencyMS)
		return true
	}
}

// BearerJWTAuth is the production Authenticator. It parses the
// Authorization header as a defradb-format JWT, verifies the signature
// against the issuer DID's public key, and checks the audience matches
// the request's Host header. The type is stateless; callers construct
// it directly with BearerJWTAuth{}.
type BearerJWTAuth struct{}

// CallerDID returns the issuer DID from the request's bearer token after
// verifying the signature and audience. All failure modes wrap
// ErrAuthRequired so callers can branch on it via errors.Is.
func (BearerJWTAuth) CallerDID(r *http.Request) (string, error) {
	raw := r.Header.Get(authHeader)
	if raw == "" {
		return "", fmt.Errorf("%w: missing header", ErrAuthRequired)
	}
	if !strings.HasPrefix(raw, bearerPrefix) {
		return "", fmt.Errorf("%w: missing Bearer prefix", ErrAuthRequired)
	}
	token := strings.TrimPrefix(raw, bearerPrefix)
	ident, err := acpIdentity.FromToken([]byte(token))
	if err != nil {
		return "", fmt.Errorf("%w: parse: %w", ErrAuthRequired, err)
	}
	audience := strings.ToLower(r.Host)
	if err := acpIdentity.VerifyAuthToken(ident, audience); err != nil {
		return "", fmt.Errorf("%w: verify: %w", ErrAuthRequired, err)
	}
	return ident.DID(), nil
}
