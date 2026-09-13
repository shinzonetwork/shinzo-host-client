package host

import (
	"context"
	"strings"
	"sync/atomic"

	"github.com/shinzonetwork/shinzo-host-client/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
)

type EventFilter struct {
	mode           string
	cascadeFilters bool
	rules          atomic.Pointer[hostconfig.FilterSet]
}

func NewEventFilter(cfg hostconfig.EventFilterConfig, rules *hostconfig.FilterSet) *EventFilter {
	if !cfg.Enabled {
		return nil
	}
	f := &EventFilter{
		mode:           cfg.Mode,
		cascadeFilters: cfg.CascadeFilters,
	}
	f.rules.Store(rules)
	return f
}

func (f *EventFilter) UpdateRules(rules *hostconfig.FilterSet) {
	f.rules.Store(rules)
}

func (f *EventFilter) AllowReplication(
	_ context.Context,
	collectionID string,
	_ string,
	fields map[string]any,
) bool {
	rules := f.rules.Load()

	switch collectionID {
	case constants.CollectionBlock:
		return f.allowBlock(rules, fields)
	case constants.CollectionBlockSignature, constants.CollectionSnapshotSignature:
		return true
	}

	if !f.inBlockRange(rules, fields) {
		return false
	}

	switch collectionID {
	case constants.CollectionTransaction:
		return f.allowTransaction(rules, fields)
	case constants.CollectionLog:
		return f.allowLog(rules, fields)
	case constants.CollectionAccessListEntry:
		return f.allowAccessListEntry(rules, fields)
	default:
		return true
	}
}

func (f *EventFilter) allowBlock(rules *hostconfig.FilterSet, fields map[string]any) bool {
	if rules == nil || rules.BlockRange == nil {
		return true
	}
	num, ok := fieldUint64(fields, "number")
	if !ok {
		return true
	}
	return inRange(rules.BlockRange, num)
}

func (f *EventFilter) inBlockRange(rules *hostconfig.FilterSet, fields map[string]any) bool {
	if rules == nil || rules.BlockRange == nil {
		return true
	}
	num, ok := fieldUint64(fields, "blockNumber")
	if !ok {
		return true
	}
	return inRange(rules.BlockRange, num)
}

func inRange(r *hostconfig.BlockRangeFilter, num uint64) bool {
	if int64(num) < r.Start { //nolint:gosec
		return false
	}
	if r.End > 0 && int64(num) > r.End { //nolint:gosec
		return false
	}
	return true
}

func (f *EventFilter) allowTransaction(rules *hostconfig.FilterSet, fields map[string]any) bool {
	to, _ := fieldString(fields, "to")
	return f.matchesGroups(rules, to, nil, colTypeTransaction)
}

func (f *EventFilter) allowLog(rules *hostconfig.FilterSet, fields map[string]any) bool {
	addr, _ := fieldString(fields, "address")
	topics := fieldStringSlice(fields, "topics")
	return f.matchesGroups(rules, addr, topics, colTypeLog)
}

func (f *EventFilter) allowAccessListEntry(rules *hostconfig.FilterSet, fields map[string]any) bool {
	addr, _ := fieldString(fields, "address")
	return f.matchesGroups(rules, addr, nil, colTypeAccessListEntry)
}

func (f *EventFilter) matchesGroups(rules *hostconfig.FilterSet, address string, topics []string, colType string) bool {
	isAllowlist := f.mode != filterModeBlocklist

	matched := false
	hasEnabled := false
	if rules != nil {
		for i := range rules.Groups {
			g := &rules.Groups[i]
			if !g.Enabled {
				continue
			}
			hasEnabled = true
			if f.groupMatches(g, address, topics, colType) {
				matched = true
				break
			}
		}
	}

	if isAllowlist {
		if !hasEnabled {
			return true
		}
		return matched
	}
	return !matched
}

func (f *EventFilter) groupMatches(g *hostconfig.FilterGroup, address string, topics []string, colType string) bool {
	if address != "" {
		for _, cf := range g.Contracts {
			if !contractMatchesType(cf, colType, f.cascadeFilters) {
				continue
			}
			if strings.EqualFold(cf.Address, address) {
				return true
			}
		}
	}

	if colType == colTypeLog && len(topics) > 0 {
		for _, tf := range g.Topics {
			if topicMatches(tf, topics) {
				return true
			}
		}
	}

	return false
}

func contractMatchesType(cf hostconfig.ContractFilter, colType string, cascade bool) bool {
	for _, t := range cf.Types {
		if strings.EqualFold(t, colType) {
			return true
		}
		if cascade && strings.EqualFold(t, colTypeTransaction) &&
			(colType == colTypeLog || colType == colTypeAccessListEntry) {
			return true
		}
	}
	return false
}

func topicMatches(tf hostconfig.TopicFilter, topics []string) bool {
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
