package host

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/sec"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/sourcenetwork/defradb/event"
	"github.com/sourcenetwork/defradb/node"
	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/hostconfig"
)

const (
	defaultPeerDiscoveryTimeout = 10 * time.Second
	defaultP2PPort              = "9171"
	defaultConnectRetries       = 5
	defaultConnectBaseDelay     = time.Second
	maxConnectBackoff           = 30 * time.Second
	defaultReconnectInterval    = 60 * time.Second
)

func (s *defraService) MaintainPeerConnections(ctx context.Context) {
	if s.node == nil || !s.cfg.P2P.Enabled || len(s.cfg.P2P.BootstrapPeers) == 0 {
		return
	}
	go s.maintainPeerConnections(ctx, s.node)
}

func (s *defraService) maintainPeerConnections(ctx context.Context, defraNode *node.Node) {
	log := s.log.Sugar()
	p2pCfg := s.cfg.P2P

	discoveryTimeout := time.Duration(p2pCfg.PeerDiscoveryTimeoutMs) * time.Millisecond
	if discoveryTimeout <= 0 {
		discoveryTimeout = defaultPeerDiscoveryTimeout
	}
	peers := resolvePeerAddrs(ctx, p2pCfg.BootstrapPeers, discoveryTimeout, log)
	if len(peers) == 0 {
		log.Warn("no bootstrap peers resolved, nothing to connect to")
		return
	}

	for _, addr := range peers {
		connectPeerWithRetry(ctx, defraNode, addr, p2pCfg, log)
	}

	if !p2pCfg.EnableAutoReconnect {
		return
	}

	interval := time.Duration(p2pCfg.ReconnectIntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = defaultReconnectInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var noPeersCh <-chan event.Message
	if sub, err := defraNode.DB.Events().Subscribe(event.P2PNoPeersName); err != nil {
		log.Warnw("mesh-loss listener disabled, event subscription failed", "error", err)
	} else {
		noPeersCh = sub.Message()
	}

	log.Infow("maintaining peer connections", "peers", len(peers), "reconnect_interval", interval)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reconnectMissingPeers(ctx, defraNode, peers, p2pCfg, log)
		case msg, ok := <-noPeersCh:
			if !ok {
				noPeersCh = nil
				continue
			}
			if _, ok := msg.Data.(event.P2PNoPeers); !ok {
				continue
			}

			if active, err := defraNode.DB.ActivePeers(ctx); err == nil && len(active) == 0 {
				reconnectMissingPeers(ctx, defraNode, peers, p2pCfg, log)
			}
		}
	}
}

func connectPeerWithRetry(ctx context.Context, defraNode *node.Node, peerAddr string, cfg hostconfig.P2PConfig, log *zap.SugaredLogger) {
	maxRetries := cfg.MaxRetries
	if maxRetries <= 0 {
		maxRetries = defaultConnectRetries
	}
	baseDelay := time.Duration(cfg.RetryBaseDelayMs) * time.Millisecond
	if baseDelay <= 0 {
		baseDelay = defaultConnectBaseDelay
	}

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if err := defraNode.DB.Connect(ctx, []string{peerAddr}); err == nil {
			log.Infow("connected to bootstrap peer", "peer", peerAddr, "attempt", attempt+1)
			return
		} else {
			lastErr = err
		}

		if attempt == maxRetries-1 {
			break
		}
		delay := min(baseDelay*time.Duration(int64(1)<<attempt), maxConnectBackoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
	log.Warnw("failed to connect to bootstrap peer", "peer", peerAddr, "attempts", maxRetries, "error", lastErr)
}

func reconnectMissingPeers(ctx context.Context, defraNode *node.Node, bootstrapPeers []string, cfg hostconfig.P2PConfig, log *zap.SugaredLogger) {
	active, err := defraNode.DB.ActivePeers(ctx)
	if err != nil {
		log.Warnw("failed to check active peers", "error", err)
		return
	}
	activeSet := make(map[string]bool, len(active)*2)
	for _, addr := range active {
		activeSet[addr] = true
		if id := peerIDFromMultiaddr(addr); id != "" {
			activeSet[id] = true
		}
	}

	for _, addr := range bootstrapPeers {
		if activeSet[addr] {
			continue
		}
		if id := peerIDFromMultiaddr(addr); id != "" && activeSet[id] {
			continue
		}
		go connectPeerWithRetry(ctx, defraNode, addr, cfg, log)
	}
}

func peerIDFromMultiaddr(multiaddr string) string {
	const p2pSuffix = "/p2p/"
	i := strings.LastIndex(multiaddr, p2pSuffix)
	if i == -1 {
		return ""
	}
	return multiaddr[i+len(p2pSuffix):]
}

type resolvedPeerAddr struct {
	index int
	addr  string
}

func resolvePeerAddrs(ctx context.Context, peers []string, timeout time.Duration, log *zap.SugaredLogger) []string {
	results := make([]resolvedPeerAddr, 0, len(peers))
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i, raw := range peers {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}

		maddr, err := normalizePeerAddr(raw)
		if err != nil {
			log.Warnw("invalid bootstrap peer address", "addr", raw, "error", err)
			continue
		}

		if multiaddrHasPeerID(maddr) {
			mu.Lock()
			results = append(results, resolvedPeerAddr{index: i, addr: maddr.String()})
			mu.Unlock()
			continue
		}

		wg.Add(1)
		go func(idx int, addr ma.Multiaddr) {
			defer wg.Done()
			full, err := probePeerID(ctx, addr, timeout)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				log.Warnw("failed to discover peer id", "addr", addr.String(), "error", err)
				return
			}
			results = append(results, resolvedPeerAddr{index: idx, addr: full})
		}(i, maddr)
	}
	wg.Wait()

	sort.Slice(results, func(i, j int) bool { return results[i].index < results[j].index })
	resolved := make([]string, len(results))
	for i, r := range results {
		resolved[i] = r.addr
	}
	return resolved
}

func normalizePeerAddr(addr string) (ma.Multiaddr, error) {
	if strings.HasPrefix(addr, "/") {
		return ma.NewMultiaddr(addr)
	}

	host, port, err := net.SplitHostPort(addr)
	if err == nil {
		return buildPeerMultiaddr(host, port)
	}

	host = strings.TrimPrefix(addr, "[")
	host = strings.TrimSuffix(host, "]")
	return buildPeerMultiaddr(host, defaultP2PPort)
}

func buildPeerMultiaddr(host, port string) (ma.Multiaddr, error) {
	ip := net.ParseIP(host)
	if ip == nil {
		return nil, fmt.Errorf("%q: %w", host, ErrInvalidIPAddress)
	}
	proto := "ip4"
	if ip.To4() == nil {
		proto = "ip6"
	}
	return ma.NewMultiaddr(fmt.Sprintf("/%s/%s/tcp/%s", proto, host, port))
}

func multiaddrHasPeerID(maddr ma.Multiaddr) bool {
	for _, p := range maddr.Protocols() {
		if p.Code == ma.P_P2P {
			return true
		}
	}
	return false
}

func probePeerID(ctx context.Context, targetAddr ma.Multiaddr, timeout time.Duration) (string, error) {
	tmpHost, err := libp2p.New(libp2p.NoListenAddrs)
	if err != nil {
		return "", fmt.Errorf("creating discovery host: %w", err)
	}
	defer func() { _ = tmpHost.Close() }()

	bogusID, err := peer.Decode("12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN")
	if err != nil {
		return "", fmt.Errorf("decoding placeholder peer id: %w", err)
	}

	dialCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	err = tmpHost.Connect(dialCtx, peer.AddrInfo{ID: bogusID, Addrs: []ma.Multiaddr{targetAddr}})
	if err == nil {

		return fmt.Sprintf("%s/p2p/%s", targetAddr, bogusID), nil
	}

	actual, extractErr := peerIDFromDialError(err)
	if extractErr != nil {
		return "", fmt.Errorf("%w (dial error: %w)", extractErr, err)
	}
	return fmt.Sprintf("%s/p2p/%s", targetAddr, actual), nil
}

func peerIDFromDialError(err error) (peer.ID, error) {
	var mismatch sec.ErrPeerIDMismatch
	if errors.As(err, &mismatch) {
		return mismatch.Actual, nil
	}

	const marker = "but remote key matches "
	_, after, ok := strings.Cut(err.Error(), marker)
	if !ok {
		return "", ErrNoPeerIDMismatchInfo
	}
	idStr := after
	for _, delim := range []string{".", ",", " ", "\n", ")"} {
		if i := strings.Index(idStr, delim); i != -1 {
			idStr = idStr[:i]
		}
	}
	idStr = strings.TrimSpace(idStr)

	id, err := peer.Decode(idStr)
	if err != nil {
		return "", fmt.Errorf("extracted invalid peer id %q: %w", idStr, err)
	}
	return id, nil
}
