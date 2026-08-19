// Package sessionhost connects cc-connect to an application-owned semantic
// session host over the versioned Session Link Protocol. Unlike CLI adapters,
// it never parses terminal rendering or starts a headless coding-agent process.
package sessionhost

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/chenhg5/cc-connect/core"
)

const defaultAuthTokenEnv = "CC_SESSION_LINK_TOKEN"

var envNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
var collaborationChannelPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)
var activationIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

type dialFunc func(context.Context, string, string) (net.Conn, error)

type agentConfig struct {
	endpoint             string
	network              string
	address              string
	authToken            string
	workDir              string
	maxFrameBytes        int
	requestTimeout       time.Duration
	bindSessionKey       string
	collaborationTargets map[string]string
}

// Agent owns one multiplexed Session Link connection shared by all sessions.
type Agent struct {
	cfg  agentConfig
	dial dialFunc

	mu                   sync.Mutex
	client               *linkClient
	collaborationChannel string

	stopped atomic.Bool
}

func init() {
	core.RegisterAgent("sessionhost", New)
}

// New creates a Session Host agent. The endpoint must be a local Unix socket;
// the authentication token is read from auth_token_env and is never accepted
// inline in config, so project files do not become secret stores.
func New(opts map[string]any) (core.Agent, error) {
	endpoint, _ := opts["endpoint"].(string)
	if injected := strings.TrimSpace(os.Getenv("CC_SESSION_LINK_ENDPOINT")); injected != "" {
		endpoint = injected
	}
	network, address, err := parseEndpoint(endpoint)
	if err != nil {
		return nil, err
	}

	authTokenEnv, _ := opts["auth_token_env"].(string)
	authTokenEnv = strings.TrimSpace(authTokenEnv)
	if authTokenEnv == "" {
		authTokenEnv = defaultAuthTokenEnv
	}
	if !envNamePattern.MatchString(authTokenEnv) {
		return nil, fmt.Errorf("session-host: invalid auth_token_env %q", authTokenEnv)
	}
	authToken := os.Getenv(authTokenEnv)
	if strings.TrimSpace(authToken) == "" {
		return nil, fmt.Errorf("session-host: authentication token environment variable %s is required", authTokenEnv)
	}
	if len(authToken) > 4096 {
		return nil, fmt.Errorf("session-host: authentication token from %s exceeds limit", authTokenEnv)
	}

	workDir, _ := opts["work_dir"].(string)
	if injected := strings.TrimSpace(os.Getenv("CC_SESSION_WORK_DIR")); injected != "" {
		workDir = injected
	}
	if strings.TrimSpace(workDir) == "" {
		workDir = "."
	}

	maxFrameBytes, err := intOption(opts, "max_frame_bytes", defaultMaxFrameBytes)
	if err != nil {
		return nil, err
	}
	if maxFrameBytes < 4096 || maxFrameBytes > maxMaxFrameBytes {
		return nil, fmt.Errorf("session-host: max_frame_bytes must be between 4096 and %d", maxMaxFrameBytes)
	}

	requestTimeoutSeconds, err := intOption(opts, "request_timeout_seconds", 30)
	if err != nil {
		return nil, err
	}
	if requestTimeoutSeconds < 1 || requestTimeoutSeconds > 300 {
		return nil, fmt.Errorf("session-host: request_timeout_seconds must be between 1 and 300")
	}

	bindSessionKey, _ := opts["bind_session_key"].(string)
	bindSessionKey = strings.TrimSpace(bindSessionKey)
	collaborationTargets, err := parseCollaborationTargets(opts["collaboration_targets"])
	if err != nil {
		return nil, err
	}
	if bindSessionKey != "" {
		channel := strings.SplitN(bindSessionKey, ":", 2)[0]
		if _, exists := collaborationTargets[channel]; !exists {
			collaborationTargets[channel] = bindSessionKey
		}
	}

	return newAgent(agentConfig{
		endpoint:             endpoint,
		network:              network,
		address:              address,
		authToken:            authToken,
		workDir:              workDir,
		maxFrameBytes:        maxFrameBytes,
		requestTimeout:       time.Duration(requestTimeoutSeconds) * time.Second,
		bindSessionKey:       bindSessionKey,
		collaborationTargets: collaborationTargets,
	}, (&net.Dialer{}).DialContext), nil
}

func newAgent(cfg agentConfig, dial dialFunc) *Agent {
	if cfg.network == "" {
		cfg.network = "unix"
	}
	if cfg.address == "" {
		cfg.address = cfg.endpoint
	}
	if cfg.maxFrameBytes == 0 {
		cfg.maxFrameBytes = defaultMaxFrameBytes
	}
	if cfg.requestTimeout == 0 {
		cfg.requestTimeout = 30 * time.Second
	}
	return &Agent{cfg: cfg, dial: dial}
}

func parseEndpoint(raw string) (string, string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", fmt.Errorf("session-host: endpoint is required")
	}
	address := raw
	if strings.HasPrefix(address, "unix://") {
		address = strings.TrimPrefix(address, "unix://")
	} else if strings.Contains(address, "://") {
		return "", "", fmt.Errorf("session-host: only local unix:// endpoints are supported")
	}
	if !filepath.IsAbs(address) {
		return "", "", fmt.Errorf("session-host: Unix socket endpoint must be an absolute path")
	}
	return "unix", address, nil
}

func intOption(opts map[string]any, name string, fallback int) (int, error) {
	value, ok := opts[name]
	if !ok || value == nil {
		return fallback, nil
	}
	switch typed := value.(type) {
	case int:
		return typed, nil
	case int64:
		return int(typed), nil
	case float64:
		if typed != float64(int(typed)) {
			return 0, fmt.Errorf("session-host: %s must be an integer", name)
		}
		return int(typed), nil
	default:
		return 0, fmt.Errorf("session-host: %s must be an integer", name)
	}
}

func (a *Agent) Name() string { return "sessionhost" }

func (a *Agent) StartSession(ctx context.Context, sessionID string) (core.AgentSession, error) {
	activation, err := a.openHostSession(ctx, sessionID, "", false)
	if err != nil {
		return nil, err
	}
	return activation.Session, nil
}

// ResumeHostSession prepares an existing application-owned session attachment.
// The caller commits it to the durable IM thread only after generation fencing.
func (a *Agent) ResumeHostSession(
	ctx context.Context, sessionID, activationID string,
) (core.HostSessionActivation, error) {
	sessionID, err := validateSessionID(sessionID, false)
	if err != nil {
		return core.HostSessionActivation{}, err
	}
	activationID = strings.TrimSpace(activationID)
	if !activationIDPattern.MatchString(activationID) {
		return core.HostSessionActivation{}, fmt.Errorf("session-host: invalid activation ID")
	}
	return a.openHostSession(ctx, sessionID, activationID, true)
}

func (a *Agent) openHostSession(
	ctx context.Context, sessionID, activationID string, requireCausal bool,
) (core.HostSessionActivation, error) {
	var err error
	sessionID, err = validateSessionID(sessionID, true)
	if err != nil {
		return core.HostSessionActivation{}, err
	}
	client, err := a.ensureClient(ctx)
	if err != nil {
		return core.HostSessionActivation{}, err
	}

	requestCtx, cancel := context.WithTimeout(ctx, a.cfg.requestTimeout)
	defer cancel()
	client.sessionCallMu.Lock()
	defer client.sessionCallMu.Unlock()
	var opened sessionOpenResult
	if err := client.call(requestCtx, messageSessionOpen, "", sessionOpenPayload{
		RequestedSessionID:   sessionID,
		WorkDir:              a.currentWorkDir(),
		CollaborationChannel: a.currentCollaborationChannel(),
		ActivationID:         activationID,
	}, &opened); err != nil {
		return core.HostSessionActivation{}, fmt.Errorf("session-host: open session: %w", err)
	}
	if strings.TrimSpace(opened.SessionID) == "" {
		return core.HostSessionActivation{}, fmt.Errorf("session-host: host returned an empty session ID")
	}
	if requireCausal && sessionID != "" && opened.SessionID != sessionID {
		return core.HostSessionActivation{}, fmt.Errorf(
			"session-host: activation returned session %q, want %q", opened.SessionID, sessionID)
	}
	if requireCausal && (opened.ActivationID != activationID || opened.ActivationGeneration == 0) {
		return core.HostSessionActivation{}, fmt.Errorf(
			"session-host: host returned invalid activation metadata")
	}
	if !client.activateSession(
		opened.SessionID, opened.ActivationID, opened.ActivationGeneration,
	) {
		return core.HostSessionActivation{}, fmt.Errorf(
			"session-host: activation was superseded by generation %d",
			opened.ActivationGeneration)
	}

	session := newSession(ctx, client, opened.SessionID, a.cfg.requestTimeout)
	if err := client.registerSession(session); err != nil {
		session.closeFromClient(err)
		return core.HostSessionActivation{}, err
	}
	return core.HostSessionActivation{
		Session: session, SessionID: opened.SessionID,
		ActivationID:         opened.ActivationID,
		ActivationGeneration: opened.ActivationGeneration,
	}, nil
}

func validateSessionID(value string, allowEmpty bool) (string, error) {
	value = strings.TrimSpace(value)
	if (!allowEmpty && value == "") || len(value) > 1024 {
		return "", fmt.Errorf("session-host: invalid session ID")
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return "", fmt.Errorf("session-host: invalid session ID")
		}
	}
	return value, nil
}

func (a *Agent) HostSessionEvents() <-chan core.HostSessionLifecycle {
	ctx, cancel := context.WithTimeout(context.Background(), a.cfg.requestTimeout)
	defer cancel()
	client, err := a.ensureClient(ctx)
	if err != nil {
		failed := make(chan core.HostSessionLifecycle)
		close(failed)
		return failed
	}
	return client.hostSessions
}

func (a *Agent) HostSessionBindingTarget() string { return a.cfg.bindSessionKey }

func (a *Agent) HostSessionCollaborationEvents() <-chan core.HostSessionCollaboration {
	ctx, cancel := context.WithTimeout(context.Background(), a.cfg.requestTimeout)
	defer cancel()
	client, err := a.ensureClient(ctx)
	if err != nil {
		failed := make(chan core.HostSessionCollaboration)
		close(failed)
		return failed
	}
	return client.collaboration
}

func (a *Agent) HostSessionBindingTargetFor(channel string) string {
	return a.cfg.collaborationTargets[strings.ToLower(strings.TrimSpace(channel))]
}

func (a *Agent) HostSessionCollaborationChannels() []string {
	return sortedTargetChannels(a.cfg.collaborationTargets)
}

func parseCollaborationTargets(raw any) (map[string]string, error) {
	result := make(map[string]string)
	if raw == nil {
		return result, nil
	}
	values, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("session-host: collaboration_targets must be a table")
	}
	for channel, value := range values {
		target, ok := value.(string)
		channel = strings.ToLower(strings.TrimSpace(channel))
		target = strings.TrimSpace(target)
		if !ok || !collaborationChannelPattern.MatchString(channel) || target == "" || len(target) > 2048 {
			return nil, fmt.Errorf("session-host: collaboration target %q must be a non-empty string", channel)
		}
		result[channel] = target
	}
	return result, nil
}

func sortedTargetChannels(targets map[string]string) []string {
	channels := make([]string, 0, len(targets))
	for channel := range targets {
		channels = append(channels, channel)
	}
	sort.Strings(channels)
	return channels
}

func (a *Agent) ListSessions(ctx context.Context) ([]core.AgentSessionInfo, error) {
	client, err := a.ensureClient(ctx)
	if err != nil {
		return nil, err
	}
	requestCtx, cancel := context.WithTimeout(ctx, a.cfg.requestTimeout)
	defer cancel()
	var result sessionListResult
	if err := client.call(requestCtx, messageSessionList, "", struct{}{}, &result); err != nil {
		return nil, fmt.Errorf("session-host: list sessions: %w", err)
	}

	sessions := make([]core.AgentSessionInfo, 0, len(result.Sessions))
	for _, item := range result.Sessions {
		var modifiedAt time.Time
		if item.ModifiedAt != "" {
			parsed, err := time.Parse(time.RFC3339Nano, item.ModifiedAt)
			if err != nil {
				return nil, fmt.Errorf("session-host: parse session %q modified_at: %w", item.ID, err)
			}
			modifiedAt = parsed
		}
		sessions = append(sessions, core.AgentSessionInfo{
			ID:           item.ID,
			Summary:      item.Summary,
			MessageCount: item.MessageCount,
			ModifiedAt:   modifiedAt,
			GitBranch:    item.GitBranch,
		})
	}
	return sessions, nil
}

func (a *Agent) GetSessionModel(ctx context.Context, sessionID string) (core.SessionModelState, error) {
	return a.sessionModelCall(ctx, sessionID, "", false)
}

func (a *Agent) SetSessionModel(ctx context.Context, sessionID, model string) (core.SessionModelState, error) {
	return a.sessionModelCall(ctx, sessionID, model, true)
}

func (a *Agent) GetSessionReasoningEffort(
	ctx context.Context, sessionID string,
) (core.SessionReasoningEffortState, error) {
	return a.sessionEffortCall(ctx, sessionID, "", false)
}

func (a *Agent) SetSessionReasoningEffort(
	ctx context.Context, sessionID, effort string,
) (core.SessionReasoningEffortState, error) {
	return a.sessionEffortCall(ctx, sessionID, effort, true)
}

func (a *Agent) CompactSession(
	ctx context.Context, sessionID, instructions string,
) (core.SessionCompactionResult, error) {
	client, err := a.ensureClient(ctx)
	if err != nil {
		return core.SessionCompactionResult{}, err
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return core.SessionCompactionResult{}, fmt.Errorf("session-host: session ID is required")
	}
	instructions, err = validateCompactInstructions(instructions)
	if err != nil {
		return core.SessionCompactionResult{}, err
	}
	if session := client.attachedSession(sessionID); session != nil && session.Alive() {
		return session.Compact(ctx, instructions)
	}

	client.sessionCallMu.Lock()
	defer client.sessionCallMu.Unlock()
	var opened sessionOpenResult
	if err := client.call(ctx, messageSessionOpen, "", sessionOpenPayload{
		RequestedSessionID: sessionID, WorkDir: a.currentWorkDir(),
		CollaborationChannel: a.currentCollaborationChannel(),
	}, &opened); err != nil {
		return core.SessionCompactionResult{}, fmt.Errorf("session-host: activate compact session: %w", err)
	}
	if opened.SessionID != sessionID {
		return core.SessionCompactionResult{}, fmt.Errorf(
			"session-host: activation returned session %q, want %q", opened.SessionID, sessionID)
	}
	if !client.activateSession(opened.SessionID, opened.ActivationID, opened.ActivationGeneration) {
		return core.SessionCompactionResult{}, fmt.Errorf("session-host: compact activation was superseded")
	}
	var result compactRunResult
	if err := client.call(ctx, messageCompactRun, sessionID,
		compactRunPayload{Instructions: instructions}, &result); err != nil {
		return core.SessionCompactionResult{}, fmt.Errorf("session-host: compact session: %w", err)
	}
	return core.SessionCompactionResult{Message: strings.TrimSpace(result.Message)}, nil
}

func validateCompactInstructions(value string) (string, error) {
	value = strings.TrimSpace(value)
	if len(value) > 32*1024 {
		return "", fmt.Errorf("session-host: compact instructions are too long")
	}
	for _, r := range value {
		if unicode.IsControl(r) && r != '\n' && r != '\r' && r != '\t' {
			return "", fmt.Errorf("session-host: compact instructions contain control characters")
		}
	}
	return value, nil
}

func (a *Agent) sessionEffortCall(
	ctx context.Context, sessionID, effort string, set bool,
) (core.SessionReasoningEffortState, error) {
	client, err := a.ensureClient(ctx)
	if err != nil {
		return core.SessionReasoningEffortState{}, err
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return core.SessionReasoningEffortState{}, fmt.Errorf("session-host: session ID is required")
	}
	if set {
		effort = strings.ToLower(strings.TrimSpace(effort))
		valid := effort == "auto" || effort == "none" || effort == "minimal" ||
			effort == "low" || effort == "medium" || effort == "high" ||
			effort == "xhigh" || effort == "max"
		if !valid {
			return core.SessionReasoningEffortState{}, fmt.Errorf("session-host: invalid effort selection")
		}
	}
	if session := client.attachedSession(sessionID); session != nil && session.Alive() {
		if set {
			return session.SetEffort(ctx, effort)
		}
		return session.EffortState(ctx)
	}
	return a.unattachedSessionEffortCall(ctx, client, sessionID, effort, set)
}

func (a *Agent) unattachedSessionEffortCall(
	ctx context.Context, client *linkClient, sessionID, effort string, set bool,
) (core.SessionReasoningEffortState, error) {
	requestCtx, cancel := context.WithTimeout(ctx, a.cfg.requestTimeout)
	defer cancel()
	client.sessionCallMu.Lock()
	defer client.sessionCallMu.Unlock()
	var opened sessionOpenResult
	if err := client.call(requestCtx, messageSessionOpen, "", sessionOpenPayload{
		RequestedSessionID: sessionID, WorkDir: a.currentWorkDir(),
		CollaborationChannel: a.currentCollaborationChannel(),
	}, &opened); err != nil {
		return core.SessionReasoningEffortState{}, fmt.Errorf("session-host: activate effort session: %w", err)
	}
	if opened.SessionID != sessionID {
		return core.SessionReasoningEffortState{}, fmt.Errorf(
			"session-host: activation returned session %q, want %q", opened.SessionID, sessionID)
	}
	if !client.activateSession(opened.SessionID, opened.ActivationID, opened.ActivationGeneration) {
		return core.SessionReasoningEffortState{}, fmt.Errorf("session-host: effort activation was superseded")
	}
	var result effortStateResult
	if set {
		if err := client.call(requestCtx, messageEffortSet, sessionID,
			effortSetPayload{Effort: effort}, &result); err != nil {
			return core.SessionReasoningEffortState{}, fmt.Errorf("session-host: set session effort: %w", err)
		}
	} else if err := client.call(requestCtx, messageEffortGet, sessionID, struct{}{}, &result); err != nil {
		return core.SessionReasoningEffortState{}, fmt.Errorf("session-host: get session effort: %w", err)
	}
	return mapEffortState(result), nil
}

func (a *Agent) sessionModelCall(ctx context.Context, sessionID, model string, set bool) (core.SessionModelState, error) {
	client, err := a.ensureClient(ctx)
	if err != nil {
		return core.SessionModelState{}, err
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return core.SessionModelState{}, fmt.Errorf("session-host: session ID is required")
	}
	session := client.attachedSession(sessionID)
	if set {
		model = strings.TrimSpace(model)
		if model == "" || len(model) > 1024 || strings.IndexFunc(model, unicode.IsControl) >= 0 {
			return core.SessionModelState{}, fmt.Errorf("session-host: invalid model selection")
		}
		if session != nil && session.Alive() {
			return session.SetModel(ctx, model)
		}
		return a.unattachedSessionModelCall(ctx, client, sessionID, model, true)
	}
	if session != nil && session.Alive() {
		return session.ModelState(ctx)
	}
	return a.unattachedSessionModelCall(ctx, client, sessionID, "", false)
}

// unattachedSessionModelCall lets a persisted IM thread reactivate its Java
// transcript before cc-connect has recreated the local AgentSession wrapper.
// The next normal turn will register that wrapper and drain any early events.
func (a *Agent) unattachedSessionModelCall(
	ctx context.Context,
	client *linkClient,
	sessionID string,
	model string,
	set bool,
) (core.SessionModelState, error) {
	requestCtx, cancel := context.WithTimeout(ctx, a.cfg.requestTimeout)
	defer cancel()
	client.sessionCallMu.Lock()
	defer client.sessionCallMu.Unlock()

	var opened sessionOpenResult
	if err := client.call(requestCtx, messageSessionOpen, "", sessionOpenPayload{
		RequestedSessionID:   sessionID,
		WorkDir:              a.currentWorkDir(),
		CollaborationChannel: a.currentCollaborationChannel(),
	}, &opened); err != nil {
		return core.SessionModelState{}, fmt.Errorf("session-host: activate model session: %w", err)
	}
	if opened.SessionID != sessionID {
		return core.SessionModelState{}, fmt.Errorf(
			"session-host: activation returned session %q, want %q", opened.SessionID, sessionID,
		)
	}
	if !client.activateSession(opened.SessionID, opened.ActivationID, opened.ActivationGeneration) {
		return core.SessionModelState{}, fmt.Errorf("session-host: model activation was superseded")
	}

	var result modelStateResult
	if set {
		if err := client.call(requestCtx, messageModelSet, sessionID, modelSetPayload{Model: model}, &result); err != nil {
			return core.SessionModelState{}, fmt.Errorf("session-host: set session model: %w", err)
		}
	} else if err := client.call(requestCtx, messageModelGet, sessionID, struct{}{}, &result); err != nil {
		return core.SessionModelState{}, fmt.Errorf("session-host: get session model: %w", err)
	}
	return mapModelState(result), nil
}

func (a *Agent) ensureClient(ctx context.Context) (*linkClient, error) {
	if a.stopped.Load() {
		return nil, fmt.Errorf("session-host: agent is stopped")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.client != nil && a.client.Alive() {
		return a.client, nil
	}
	client, err := connectLink(ctx, a.cfg, a.dial)
	if err != nil {
		return nil, err
	}
	a.client = client
	return client, nil
}

func (a *Agent) Stop() error {
	if !a.stopped.CompareAndSwap(false, true) {
		return nil
	}
	a.mu.Lock()
	client := a.client
	a.client = nil
	a.mu.Unlock()
	if client != nil {
		return client.Close()
	}
	return nil
}

func (a *Agent) SetWorkDir(dir string) {
	a.mu.Lock()
	a.cfg.workDir = dir
	a.mu.Unlock()
}

func (a *Agent) GetWorkDir() string {
	return a.currentWorkDir()
}

func (a *Agent) currentWorkDir() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cfg.workDir
}

func (a *Agent) SetSessionEnv(env []string) {
	channel := ""
	for _, entry := range env {
		if strings.HasPrefix(entry, "CC_SESSION_KEY=") {
			key := strings.TrimPrefix(entry, "CC_SESSION_KEY=")
			channel = strings.ToLower(strings.TrimSpace(strings.SplitN(key, ":", 2)[0]))
			break
		}
	}
	a.mu.Lock()
	a.collaborationChannel = channel
	a.mu.Unlock()
}

func (a *Agent) currentCollaborationChannel() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.collaborationChannel
}

var _ core.Agent = (*Agent)(nil)
var _ core.WorkDirSwitcher = (*Agent)(nil)
var _ core.SessionEnvInjector = (*Agent)(nil)
var _ core.HostSessionLifecycleSource = (*Agent)(nil)
var _ core.HostSessionCollaborationSource = (*Agent)(nil)
var _ core.SessionModelSwitcher = (*Agent)(nil)
var _ core.SessionReasoningEffortSwitcher = (*Agent)(nil)
var _ core.SessionContextCompactor = (*Agent)(nil)
