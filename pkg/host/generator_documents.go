package host

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"

	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
	"github.com/shinzonetwork/shinzo-host-client/pkg/verifier"
)

const (
	// documentsEndpoint is the generator endpoint that lists the documents stored at a range of
	// block heights.
	documentsEndpoint = "/api/v1/documents"

	// maxDocumentsAnswerBytes caps a generator's documents answer. A 1,000-height answer for one
	// token is about 133 KB, 43 bytes an ID, and each further token adds about 43 KB, so the cap
	// leaves room for about 95 tokens on a chain.
	maxDocumentsAnswerBytes = 4 << 20
)

// generatorDocuments returns a function that lists the IDs of the documents generators stored at
// the heights from through to, by collection, merged across generators. A generator that does not
// answer is logged and skipped; the function fails only when none answers, with an error wrapping
// verifier.ErrNotStored when every generator answers that it no longer stores from.
func generatorDocuments(generators []chain.Generator, client *http.Client) func(ctx context.Context, from, to uint64) (map[string][]string, error) {
	return func(ctx context.Context, from, to uint64) (map[string][]string, error) {
		merged := make(map[string][]string)
		seen := make(map[string]bool)
		var errs []error
		for _, g := range generators {
			listed, err := listDocuments(ctx, client, g.URL, from, to)
			if err != nil {
				errs = append(errs, fmt.Errorf("generator %s: %w", g.URL, err))
				continue
			}
			for collection, ids := range listed {
				for _, id := range ids {
					if !seen[id] {
						seen[id] = true
						merged[collection] = append(merged[collection], id)
					}
				}
			}
		}
		if len(errs) > 0 && len(errs) == len(generators) {
			err := errors.Join(errs...)
			if !slices.ContainsFunc(errs, func(e error) bool { return !errors.Is(e, errDocumentsNotStored) }) {
				return nil, fmt.Errorf("%w: %w", verifier.ErrNotStored, err)
			}
			return nil, err
		}
		for _, err := range errs {
			logger.Sugar.Warnf("Documents at blocks %d-%d not listed: %v", from, to, err)
		}
		return merged, nil
	}
}

// listDocuments asks the generator whose HTTP API is at base for the IDs of the documents it stored
// at the heights from through to, by collection. A 410 answer, for heights below the lowest the
// generator stores, returns errDocumentsNotStored with that height, and an answer over
// maxDocumentsAnswerBytes returns errDocumentsTooLarge.
func listDocuments(ctx context.Context, client *http.Client, base string, from, to uint64) (map[string][]string, error) {
	endpoint, err := url.JoinPath(base, documentsEndpoint)
	if err != nil {
		return nil, err
	}
	query := url.Values{"from": {strconv.FormatUint(from, 10)}, "to": {strconv.FormatUint(to, 10)}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?"+query.Encode(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxDocumentsAnswerBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read the documents list: %w", err)
	}
	if len(body) > maxDocumentsAnswerBytes {
		return nil, fmt.Errorf("%w: over %d bytes", errDocumentsTooLarge, maxDocumentsAnswerBytes)
	}
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusGone:
		var gone struct {
			LowestHeight uint64 `json:"lowestHeight"`
		}
		if err := json.Unmarshal(body, &gone); err != nil {
			return nil, errDocumentsNotStored
		}
		return nil, fmt.Errorf("%w: its lowest stored height is %d", errDocumentsNotStored, gone.LowestHeight)
	default:
		return nil, fmt.Errorf("%w: %s", errDocumentsRefused, resp.Status)
	}
	var listed map[string][]string
	if err := json.Unmarshal(body, &listed); err != nil {
		return nil, fmt.Errorf("read the documents list: %w", err)
	}
	return listed, nil
}
