package host

import (
	"context"
	"strings"

	"github.com/shinzonetwork/shinzo-host-client/config"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
)

// EventReplicationFilter implements client.ReplicationFilter using the
// host's EventFilterConfig. It inspects incoming P2P document fields and
// decides whether the document should be stored based on contract addresses,
// event topics, and block-number ranges defined in the configuration.
type EventReplicationFilter struct {
	cfg config.EventFilterConfig
}

// NewEventReplicationFilter creates a filter from the given config.
// Returns nil if the filter is not enabled so callers can pass nil directly
// to StartDefraInstance (which disables filtering).
func NewEventReplicationFilter(cfg config.EventFilterConfig) *EventReplicationFilter {
	if !cfg.Enabled {
		return nil
	}
	return &EventReplicationFilter{cfg: cfg}
}

// AllowReplication implements client.ReplicationFilter.
func (f *EventReplicationFilter) AllowReplication(
	_ context.Context,
	collectionID string,
	_ string,
	fields map[string]any,
) bool {
	// Structural collections always pass — we never filter blocks or batch signatures.
	switch collectionID {
	case constants.CollectionBlock:
		return f.allowBlock(fields)
	case constants.CollectionBatchSignature, constants.CollectionSnapshotSignature:
		return true
	}

	// Block-range gate: if the document carries a blockNumber and it is outside
	// the configured range, reject immediately.
	if !f.inBlockRange(fields) {
		return false
	}

	switch collectionID {
	case constants.CollectionTransaction:
		return f.allowTransaction(fields)
	case constants.CollectionLog:
		return f.allowLog(fields)
	case constants.CollectionAccessListEntry:
		return f.allowAccessListEntry(fields)
	default:
		// Unknown collection — let it through.
		return true
	}
}

// ---------------------------------------------------------------------------
// Per-collection helpers
// ---------------------------------------------------------------------------

func (f *EventReplicationFilter) allowBlock(fields map[string]any) bool {
	if f.cfg.BlockRange == nil {
		return true
	}
	num, ok := fieldUint64(fields, "number")
	if !ok {
		return true // can't determine — allow
	}
	if num < f.cfg.BlockRange.MinBlock {
		return false
	}
	if f.cfg.BlockRange.MaxBlock > 0 && num > f.cfg.BlockRange.MaxBlock {
		return false
	}
	return true
}

func (f *EventReplicationFilter) allowTransaction(fields map[string]any) bool {
	to, _ := fieldString(fields, "to")
	return f.matchesGroups(to, nil, "transaction")
}

func (f *EventReplicationFilter) allowLog(fields map[string]any) bool {
	addr, _ := fieldString(fields, "address")
	topics := fieldStringSlice(fields, "topics")
	return f.matchesGroups(addr, topics, "log")
}

func (f *EventReplicationFilter) allowAccessListEntry(fields map[string]any) bool {
	addr, _ := fieldString(fields, "address")
	return f.matchesGroups(addr, nil, "accessListEntry")
}

// ---------------------------------------------------------------------------
// Core matching logic
// ---------------------------------------------------------------------------

// matchesGroups checks whether a document (identified by address, topics, and
// collection type) matches any enabled filter group.
//
// In allowlist mode the document must match at least one group to be accepted.
// In blocklist mode the document is rejected when it matches any group.
func (f *EventReplicationFilter) matchesGroups(address string, topics []string, colType string) bool {
	isAllowlist := f.cfg.Mode != "blocklist" // default to allowlist

	matched := false
	for i := range f.cfg.Groups {
		g := &f.cfg.Groups[i]
		if !g.Enabled {
			continue
		}
		if f.groupMatches(g, address, topics, colType) {
			matched = true
			break
		}
	}

	if isAllowlist {
		// No enabled groups at all → nothing to filter → allow everything.
		if !f.hasEnabledGroups() {
			return true
		}
		return matched
	}
	// blocklist: reject when matched
	return !matched
}

func (f *EventReplicationFilter) hasEnabledGroups() bool {
	for i := range f.cfg.Groups {
		if f.cfg.Groups[i].Enabled {
			return true
		}
	}
	return false
}

// groupMatches returns true when the document matches this filter group.
// A group matches when any of its contract filters or topic filters hit.
func (f *EventReplicationFilter) groupMatches(
	g *config.FilterGroup,
	address string,
	topics []string,
	colType string,
) bool {
	// Contract address matching.
	if address != "" {
		for _, cf := range g.Contracts {
			if !contractAppliesToType(cf, colType, f.cfg.CascadeFilters) {
				continue
			}
			if strings.EqualFold(cf.Address, address) {
				return true
			}
		}
	}

	// Topic matching (logs only).
	if colType == "log" && len(topics) > 0 {
		for _, tf := range g.Topics {
			if topicFilterMatches(tf, topics) {
				return true
			}
		}
	}

	return false
}

// contractAppliesToType checks if the ContractFilter is relevant for the given
// collection type. When cascade is true, a filter targeting "transaction" also
// applies to "log" and "accessListEntry".
func contractAppliesToType(cf config.ContractFilter, colType string, cascade bool) bool {
	for _, t := range cf.Types {
		if strings.EqualFold(t, colType) {
			return true
		}
		if cascade && strings.EqualFold(t, "transaction") &&
			(colType == "log" || colType == "accessListEntry") {
			return true
		}
	}
	return false
}

// topicFilterMatches checks if a log's topics match a TopicFilter.
// topic0 is required; topic1-3 are optional (empty = wildcard).
func topicFilterMatches(tf config.TopicFilter, topics []string) bool {
	if len(topics) == 0 || tf.Topic0 == "" {
		return false
	}
	if !strings.EqualFold(tf.Topic0, topics[0]) {
		return false
	}
	if tf.Topic1 != "" && (len(topics) < 2 || !strings.EqualFold(tf.Topic1, topics[1])) {
		return false
	}
	if tf.Topic2 != "" && (len(topics) < 3 || !strings.EqualFold(tf.Topic2, topics[2])) {
		return false
	}
	if tf.Topic3 != "" && (len(topics) < 4 || !strings.EqualFold(tf.Topic3, topics[3])) {
		return false
	}
	return true
}

// ---------------------------------------------------------------------------
// Block range helpers
// ---------------------------------------------------------------------------

func (f *EventReplicationFilter) inBlockRange(fields map[string]any) bool {
	if f.cfg.BlockRange == nil {
		return true
	}
	num, ok := fieldUint64(fields, "blockNumber")
	if !ok {
		return true // can't determine — allow
	}
	if num < f.cfg.BlockRange.MinBlock {
		return false
	}
	if f.cfg.BlockRange.MaxBlock > 0 && num > f.cfg.BlockRange.MaxBlock {
		return false
	}
	return true
}

// ---------------------------------------------------------------------------
// Field extraction helpers
// ---------------------------------------------------------------------------

func fieldString(fields map[string]any, key string) (string, bool) {
	v, ok := fields[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

func fieldUint64(fields map[string]any, key string) (uint64, bool) {
	v, ok := fields[key]
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case int64:
		return uint64(n), true
	case uint64:
		return n, true
	case float64:
		return uint64(n), true
	case int:
		return uint64(n), true
	}
	return 0, false
}

func fieldStringSlice(fields map[string]any, key string) []string {
	v, ok := fields[key]
	if !ok {
		return nil
	}
	switch s := v.(type) {
	case []string:
		return s
	case []any:
		out := make([]string, 0, len(s))
		for _, item := range s {
			if str, ok := item.(string); ok {
				out = append(out, str)
			}
		}
		return out
	}
	return nil
}
