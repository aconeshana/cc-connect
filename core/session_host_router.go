package core

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const sessionHostRouteVersion = 1

const defaultSessionHostRouteLease = 30 * time.Second

const defaultSessionHostInteractionLease = 24 * time.Hour

const (
	defaultSessionHostClaimTTL           = 24 * time.Hour
	defaultSessionHostClaimSweepInterval = time.Hour
)

var ErrSessionHostRouteOwned = errors.New("session-host route has a live owner")

// SessionHostRoute identifies the cc-connect process that owns one host-bound
// IM thread. The route is persisted outside the per-process API socket so any
// Feishu WebSocket consumer can forward an event to the correct TUI instance.
type SessionHostRoute struct {
	Version    int       `json:"version"`
	SessionKey string    `json:"session_key"`
	Project    string    `json:"project"`
	SocketPath string    `json:"socket_path"`
	OwnerToken string    `json:"owner_token"`
	Generation uint64    `json:"generation"`
	LeaseUntil time.Time `json:"lease_until"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// SessionHostInteractionRoute binds one permission/question request to the
// exact cc-connect process and Java host session that published its card. IM
// card callbacks can be delivered to any sibling WebSocket consumer, so the
// chat session key alone is not sufficient when multiple terminal processes
// are active.
type SessionHostInteractionRoute struct {
	Version       int       `json:"version"`
	RequestID     string    `json:"request_id"`
	HostSessionID string    `json:"host_session_id"`
	SessionKey    string    `json:"session_key"`
	Project       string    `json:"project"`
	SocketPath    string    `json:"socket_path"`
	OwnerToken    string    `json:"owner_token"`
	LeaseUntil    time.Time `json:"lease_until"`
	UpdatedAt     time.Time `json:"updated_at"`
}

func (route *SessionHostInteractionRoute) Alive(now time.Time) bool {
	return route != nil && !route.LeaseUntil.IsZero() && now.Before(route.LeaseUntil)
}

func (route *SessionHostInteractionRoute) sessionRoute() *SessionHostRoute {
	if route == nil {
		return nil
	}
	return &SessionHostRoute{
		Version: route.Version, SessionKey: route.SessionKey, Project: route.Project,
		SocketPath: route.SocketPath, OwnerToken: route.OwnerToken,
		Generation: 1, LeaseUntil: route.LeaseUntil, UpdatedAt: route.UpdatedAt,
	}
}

func (route *SessionHostRoute) Alive(now time.Time) bool {
	return route != nil && !route.LeaseUntil.IsZero() && now.Before(route.LeaseUntil)
}

type SessionHostRouter struct {
	routeDir           string
	project            string
	localSocket        string
	ownerToken         string
	lease              time.Duration
	now                func() time.Time
	claimTTL           time.Duration
	claimSweepInterval time.Duration
	claimSweepMu       sync.Mutex
	lastClaimSweep     time.Time
}

func NewSessionHostRouter(dataDir, project, localSocket string) *SessionHostRouter {
	return &SessionHostRouter{
		routeDir:           filepath.Join(dataDir, "session-host", "routes"),
		project:            strings.TrimSpace(project),
		localSocket:        strings.TrimSpace(localSocket),
		ownerToken:         newSessionHostOwnerToken(),
		lease:              defaultSessionHostRouteLease,
		now:                time.Now,
		claimTTL:           defaultSessionHostClaimTTL,
		claimSweepInterval: defaultSessionHostClaimSweepInterval,
	}
}

func newSessionHostOwnerToken() string {
	var token [16]byte
	if _, err := rand.Read(token[:]); err == nil {
		return hex.EncodeToString(token[:])
	}
	return fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
}

func SessionHostRouteExists(dataDir, sessionKey string) bool {
	router := NewSessionHostRouter(dataDir, "", "")
	route, err := router.Lookup(sessionKey)
	return err == nil && route != nil && route.Alive(router.now())
}

func (r *SessionHostRouter) Register(sessionKey string) error {
	_, err := r.RegisterRoute(sessionKey)
	return err
}

// RegisterRoute is a cross-process compare-and-swap. A live foreign owner is
// preserved; the same owner renews its lease; an expired owner is replaced and
// advances the generation so stale readers can detect the handoff.
// This is a cc-connect Java Session Host extension and has no upstream TS equivalent.
func (r *SessionHostRouter) RegisterRoute(sessionKey string) (*SessionHostRoute, error) {
	if r == nil || strings.TrimSpace(sessionKey) == "" || r.project == "" || r.localSocket == "" {
		return nil, nil
	}
	if err := os.MkdirAll(r.routeDir, 0o700); err != nil {
		return nil, fmt.Errorf("create session-host route directory: %w", err)
	}
	var registered *SessionHostRoute
	err := r.withRouteLock(sessionKey, func() error {
		now := r.now()
		current, err := r.lookupFile(sessionKey)
		if err != nil {
			// Routes written before owner-token/generation leases (or interrupted
			// writes from a dead process) cannot represent a live CAS owner. Replace
			// them while holding the route transaction lock.
			slog.Warn("replacing invalid legacy session-host route",
				"session_key", sessionKey, "error", err)
			current = nil
		}
		if current != nil && current.Alive(now) && current.OwnerToken != r.ownerToken {
			return ErrSessionHostRouteOwned
		}
		generation := uint64(1)
		if current != nil {
			generation = current.Generation
			if current.OwnerToken != r.ownerToken {
				generation++
			}
		}
		route := &SessionHostRoute{
			Version: sessionHostRouteVersion, SessionKey: sessionKey,
			Project: r.project, SocketPath: r.localSocket, OwnerToken: r.ownerToken,
			Generation: generation, UpdatedAt: now, LeaseUntil: now.Add(r.lease),
		}
		if err := r.writeRoute(route); err != nil {
			return err
		}
		registered = route
		return nil
	})
	return registered, err
}

func (r *SessionHostRouter) Lookup(sessionKey string) (*SessionHostRoute, error) {
	if r == nil || strings.TrimSpace(sessionKey) == "" {
		return nil, nil
	}
	return r.lookupFile(sessionKey)
}

// RegisterInteraction binds one host-session/request pair to this process. The
// pair remains unambiguous even when separate Java processes generate the same
// request ID, and supported card callbacks carry both values.
func (r *SessionHostRouter) RegisterInteraction(
	requestID, hostSessionID, sessionKey string, routeGeneration uint64,
) (*SessionHostInteractionRoute, error) {
	requestID = strings.TrimSpace(requestID)
	hostSessionID = strings.TrimSpace(hostSessionID)
	sessionKey = strings.TrimSpace(sessionKey)
	if r == nil || requestID == "" || hostSessionID == "" || sessionKey == "" ||
		r.project == "" || r.localSocket == "" {
		return nil, nil
	}
	if !r.Owns(sessionKey, routeGeneration) {
		return nil, fmt.Errorf("cannot register interaction for unowned session-host route %q", sessionKey)
	}
	interactionDir := filepath.Join(r.routeDir, "interactions")
	if err := os.MkdirAll(interactionDir, 0o700); err != nil {
		return nil, fmt.Errorf("create session-host interaction route directory: %w", err)
	}
	path := r.interactionPath(requestID, hostSessionID)
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	defer lock.Close()
	if err := lockFile(lock); err != nil {
		return nil, err
	}
	defer unlockFile(lock)

	now := r.now()
	current, err := r.lookupInteractionFile(requestID, hostSessionID)
	if err != nil {
		current = nil
	}
	if current != nil && current.Alive(now) && current.OwnerToken != r.ownerToken {
		return nil, ErrSessionHostRouteOwned
	}
	route := &SessionHostInteractionRoute{
		Version: sessionHostRouteVersion, RequestID: requestID, HostSessionID: hostSessionID,
		SessionKey: sessionKey, Project: r.project, SocketPath: r.localSocket,
		OwnerToken: r.ownerToken, UpdatedAt: now,
		LeaseUntil: now.Add(defaultSessionHostInteractionLease),
	}
	data, err := json.MarshalIndent(route, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode session-host interaction route: %w", err)
	}
	if err := AtomicWriteFile(path, data, 0o600); err != nil {
		return nil, fmt.Errorf("save session-host interaction route: %w", err)
	}
	return route, nil
}

func (r *SessionHostRouter) LookupInteraction(
	requestID, hostSessionID string,
) (*SessionHostInteractionRoute, error) {
	if r == nil || strings.TrimSpace(requestID) == "" {
		return nil, nil
	}
	requestID = strings.TrimSpace(requestID)
	hostSessionID = strings.TrimSpace(hostSessionID)
	if hostSessionID != "" {
		return r.lookupInteractionFile(requestID, hostSessionID)
	}

	interactionDir := filepath.Join(r.routeDir, "interactions")
	entries, err := os.ReadDir(interactionDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read session-host interaction routes: %w", err)
	}
	var matched *SessionHostInteractionRoute
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		data, readErr := os.ReadFile(filepath.Join(interactionDir, entry.Name()))
		if readErr != nil {
			continue
		}
		var route SessionHostInteractionRoute
		if json.Unmarshal(data, &route) != nil || route.Version != sessionHostRouteVersion ||
			route.RequestID != requestID || strings.TrimSpace(route.HostSessionID) == "" ||
			strings.TrimSpace(route.SessionKey) == "" || strings.TrimSpace(route.Project) == "" ||
			strings.TrimSpace(route.SocketPath) == "" || strings.TrimSpace(route.OwnerToken) == "" ||
			!route.Alive(r.now()) {
			continue
		}
		if matched != nil {
			return nil, fmt.Errorf("ambiguous session-host interaction route for %q", requestID)
		}
		routeCopy := route
		matched = &routeCopy
	}
	return matched, nil
}

func (r *SessionHostRouter) lookupInteractionFile(
	requestID, hostSessionID string,
) (*SessionHostInteractionRoute, error) {
	data, err := os.ReadFile(r.interactionPath(requestID, hostSessionID))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read session-host interaction route: %w", err)
	}
	var route SessionHostInteractionRoute
	if err := json.Unmarshal(data, &route); err != nil {
		return nil, fmt.Errorf("decode session-host interaction route: %w", err)
	}
	if route.Version != sessionHostRouteVersion || route.RequestID != requestID ||
		route.HostSessionID != hostSessionID || strings.TrimSpace(route.SessionKey) == "" ||
		strings.TrimSpace(route.Project) == "" || strings.TrimSpace(route.SocketPath) == "" ||
		strings.TrimSpace(route.OwnerToken) == "" || route.LeaseUntil.IsZero() {
		return nil, fmt.Errorf("invalid session-host interaction route for %q", requestID)
	}
	return &route, nil
}

func (r *SessionHostRouter) IsLocalInteraction(route *SessionHostInteractionRoute) bool {
	return r == nil || route == nil ||
		(route.SocketPath == r.localSocket && route.OwnerToken == r.ownerToken)
}

func (r *SessionHostRouter) DeleteInteraction(requestID, hostSessionID string) {
	route, err := r.LookupInteraction(requestID, hostSessionID)
	if err != nil || route == nil || !r.IsLocalInteraction(route) ||
		(hostSessionID != "" && route.HostSessionID != hostSessionID) {
		return
	}
	if err := os.Remove(r.interactionPath(requestID, route.HostSessionID)); err != nil && !os.IsNotExist(err) {
		slog.Warn("delete session-host interaction route", "request_id", requestID, "error", err)
	}
}

// ActiveThreadRoutes returns the live Session Host thread owners beneath one
// platform base-chat key. It is used to prevent a main-chat message from being
// consumed independently by multiple TUI processes.
func (r *SessionHostRouter) ActiveThreadRoutes(baseSessionKey string) ([]*SessionHostRoute, error) {
	if r == nil || strings.TrimSpace(baseSessionKey) == "" {
		return nil, nil
	}
	entries, err := os.ReadDir(r.routeDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read session-host routes: %w", err)
	}
	prefix := strings.TrimSuffix(baseSessionKey, ":") + ":root:"
	now := r.now()
	routes := make([]*SessionHostRoute, 0)
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(r.routeDir, entry.Name()))
		if err != nil {
			continue
		}
		var route SessionHostRoute
		if json.Unmarshal(data, &route) != nil || route.Version != sessionHostRouteVersion ||
			!strings.HasPrefix(route.SessionKey, prefix) || !route.Alive(now) ||
			strings.TrimSpace(route.Project) == "" || strings.TrimSpace(route.SocketPath) == "" ||
			strings.TrimSpace(route.OwnerToken) == "" || route.Generation == 0 {
			continue
		}
		routeCopy := route
		routes = append(routes, &routeCopy)
	}
	sort.Slice(routes, func(i, j int) bool {
		return routes[i].SessionKey < routes[j].SessionKey
	})
	return routes, nil
}

func (r *SessionHostRouter) lookupFile(sessionKey string) (*SessionHostRoute, error) {
	data, err := os.ReadFile(r.routePath(sessionKey))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read session-host route: %w", err)
	}
	var route SessionHostRoute
	if err := json.Unmarshal(data, &route); err != nil {
		return nil, fmt.Errorf("decode session-host route: %w", err)
	}
	if route.Version != sessionHostRouteVersion || route.SessionKey != sessionKey ||
		strings.TrimSpace(route.Project) == "" || strings.TrimSpace(route.SocketPath) == "" ||
		strings.TrimSpace(route.OwnerToken) == "" || route.Generation == 0 || route.LeaseUntil.IsZero() {
		return nil, fmt.Errorf("invalid session-host route for %q", sessionKey)
	}
	return &route, nil
}

func (r *SessionHostRouter) IsLocal(route *SessionHostRoute) bool {
	return r == nil || route == nil || (route.SocketPath == r.localSocket && route.OwnerToken == r.ownerToken)
}

func (r *SessionHostRouter) Owns(sessionKey string, generation uint64) bool {
	route, err := r.Lookup(sessionKey)
	if err != nil || route == nil || !r.IsLocal(route) ||
		(generation != 0 && route.Generation != generation) {
		return false
	}
	if now := r.now(); route.Alive(now) && route.LeaseUntil.Sub(now) > r.lease/2 {
		return true
	}
	renewed, err := r.RegisterRoute(sessionKey)
	return err == nil && renewed != nil && renewed.Generation == route.Generation
}

func (r *SessionHostRouter) CompareAndDelete(sessionKey, ownerToken string, generation uint64) (bool, error) {
	deleted := false
	err := r.withRouteLock(sessionKey, func() error {
		route, err := r.lookupFile(sessionKey)
		if err != nil || route == nil {
			return err
		}
		if route.OwnerToken != ownerToken || route.Generation != generation {
			return nil
		}
		if err := os.Remove(r.routePath(sessionKey)); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("delete session-host route: %w", err)
		}
		deleted = true
		return nil
	})
	return deleted, err
}

func (r *SessionHostRouter) ClaimMessage(sessionKey, messageID string, generation uint64) (bool, error) {
	if strings.TrimSpace(messageID) == "" {
		return true, nil
	}
	if !r.Owns(sessionKey, generation) {
		return false, nil
	}
	claimDir := filepath.Join(r.routeDir, "claims")
	if err := os.MkdirAll(claimDir, 0o700); err != nil {
		return false, fmt.Errorf("create routed-message claim directory: %w", err)
	}
	r.sweepExpiredClaims(claimDir)
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%s", sessionKey, generation, messageID)))
	file, err := os.OpenFile(filepath.Join(claimDir, hex.EncodeToString(sum[:])), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		if os.IsExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("claim routed message: %w", err)
	}
	return true, file.Close()
}

func (r *SessionHostRouter) sweepExpiredClaims(claimDir string) {
	if r == nil || r.claimTTL <= 0 || r.claimSweepInterval <= 0 {
		return
	}
	now := r.now()
	r.claimSweepMu.Lock()
	defer r.claimSweepMu.Unlock()
	if !r.lastClaimSweep.IsZero() && now.Sub(r.lastClaimSweep) < r.claimSweepInterval {
		return
	}
	r.lastClaimSweep = now
	entries, err := os.ReadDir(claimDir)
	if err != nil {
		slog.Warn("read routed-message claims for cleanup", "error", err)
		return
	}
	cutoff := now.Add(-r.claimTTL)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.ModTime().Before(cutoff) {
			continue
		}
		if err := os.Remove(filepath.Join(claimDir, entry.Name())); err != nil && !os.IsNotExist(err) {
			slog.Warn("remove expired routed-message claim", "claim", entry.Name(), "error", err)
		}
	}
}

func (r *SessionHostRouter) writeRoute(route *SessionHostRoute) error {
	data, err := json.MarshalIndent(route, "", "  ")
	if err != nil {
		return fmt.Errorf("encode session-host route: %w", err)
	}
	if err := AtomicWriteFile(r.routePath(route.SessionKey), data, 0o600); err != nil {
		return fmt.Errorf("save session-host route: %w", err)
	}
	return nil
}

func (r *SessionHostRouter) withRouteLock(sessionKey string, fn func() error) error {
	if err := os.MkdirAll(r.routeDir, 0o700); err != nil {
		return err
	}
	file, err := os.OpenFile(r.routePath(sessionKey)+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := lockFile(file); err != nil {
		return err
	}
	defer unlockFile(file)
	return fn()
}

func (r *SessionHostRouter) Forward(ctx context.Context, route *SessionHostRoute, msg *Message) (InteractionOutcome, error) {
	if route == nil || msg == nil {
		return InteractionOutcome{}, fmt.Errorf("session-host forward requires route and message")
	}
	if !route.Alive(r.now()) {
		return InteractionOutcome{}, fmt.Errorf("session-host route lease expired")
	}
	req := InboundMessageRequest{Project: route.Project, Platform: msg.Platform, Message: *msg}
	req.Message.ReplyCtx = nil
	req.Message.OnAccepted = nil
	req.Message.InteractionResult = nil
	req.Message.CrossProcessRouted = true
	body, err := json.Marshal(req)
	if err != nil {
		return InteractionOutcome{}, fmt.Errorf("encode routed message: %w", err)
	}
	dialer := &net.Dialer{Timeout: 2 * time.Second}
	transport := &http.Transport{
		DialContext: func(dialCtx context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(dialCtx, "unix", route.SocketPath)
		},
	}
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
	defer transport.CloseIdleConnections()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://unix/inbound", bytes.NewReader(body))
	if err != nil {
		return InteractionOutcome{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(httpReq)
	if err != nil {
		return InteractionOutcome{}, fmt.Errorf("forward to owning session-host: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return InteractionOutcome{}, fmt.Errorf("owning session-host returned %s", resp.Status)
	}
	var result InboundMessageResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return InteractionOutcome{}, fmt.Errorf("decode owning session-host response: %w", err)
	}
	return result.Interaction, nil
}

func (r *SessionHostRouter) ForwardCardAction(
	ctx context.Context, route *SessionHostRoute, sessionKey, action string,
) (*Card, error) {
	if route == nil || strings.TrimSpace(sessionKey) == "" || strings.TrimSpace(action) == "" {
		return nil, fmt.Errorf("session-host card action forward requires route, session key, and action")
	}
	if !route.Alive(r.now()) {
		return nil, fmt.Errorf("session-host route lease expired")
	}
	body, err := json.Marshal(CardActionRequest{
		Project: route.Project, SessionKey: sessionKey, Action: action,
	})
	if err != nil {
		return nil, fmt.Errorf("encode routed card action: %w", err)
	}
	dialer := &net.Dialer{Timeout: 2 * time.Second}
	transport := &http.Transport{
		DialContext: func(dialCtx context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(dialCtx, "unix", route.SocketPath)
		},
	}
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
	defer transport.CloseIdleConnections()
	httpReq, err := http.NewRequestWithContext(
		ctx, http.MethodPost, "http://unix/card-action", bytes.NewReader(body),
	)
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("forward card action to owning session-host: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("owning session-host returned %s", resp.Status)
	}
	var result CardActionResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode owning card action response: %w", err)
	}
	return cardFromWire(result.Card)
}

func (r *SessionHostRouter) routePath(sessionKey string) string {
	sum := sha256.Sum256([]byte(sessionKey))
	return filepath.Join(r.routeDir, hex.EncodeToString(sum[:])+".json")
}

func (r *SessionHostRouter) interactionPath(requestID, hostSessionID string) string {
	sum := sha256.Sum256([]byte(hostSessionID + "\x00" + requestID))
	return filepath.Join(r.routeDir, "interactions", hex.EncodeToString(sum[:])+".json")
}
