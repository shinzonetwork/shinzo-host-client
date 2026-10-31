package host

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
)

// documentsEndpoint is the generator endpoint that lists the documents stored at a range of block
// heights.
const documentsEndpoint = "/api/v1/documents"

// generatorDocuments returns a function that lists the IDs of the documents generators stored at
// the heights from through to, by collection, merged across generators. A generator that does not
// answer is logged and skipped; the function fails only when none answers.
func generatorDocuments(generators []chain.Generator, client *http.Client) func(ctx context.Context, from, to int64) (map[string][]string, error) {
	return func(ctx context.Context, from, to int64) (map[string][]string, error) {
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
			return nil, errors.Join(errs...)
		}
		for _, err := range errs {
			logger.Sugar.Warnf("Documents at blocks %d-%d not listed: %v", from, to, err)
		}
		return merged, nil
	}
}

// listDocuments asks the generator whose HTTP API is at base for the IDs of the documents it stored
// at the heights from through to, by collection.
func listDocuments(ctx context.Context, client *http.Client, base string, from, to int64) (map[string][]string, error) {
	endpoint, err := url.JoinPath(base, documentsEndpoint)
	if err != nil {
		return nil, err
	}
	query := url.Values{"from": {strconv.FormatInt(from, 10)}, "to": {strconv.FormatInt(to, 10)}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?"+query.Encode(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: %s", errDocumentsRefused, resp.Status)
	}
	var listed map[string][]string
	if err := json.NewDecoder(resp.Body).Decode(&listed); err != nil {
		return nil, fmt.Errorf("read the documents list: %w", err)
	}
	return listed, nil
}
