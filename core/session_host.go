package core

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
)

// HostSessionLifecycle is emitted by agents whose conversation is owned by an
// external application (for example Claude Code Java's semantic Session Host).
type HostSessionLifecycle struct {
	SessionID string
	// PreviousSessionID identifies the host session that was authoritative
	// immediately before this activation. The Session Link client derives it
	// from its causal active-session fence so a local TUI resume can move the
	// exact current IM collaboration surface to the new session.
	PreviousSessionID    string
	WorkDir              string
	Summary              string
	MessageCount         int
	GitBranch            string
	Origin               string
	ActivationID         string
	ActivationGeneration uint64
	Ended                bool
	Reason               string
	Acknowledge          func()
	MetadataOnly         bool
}

// HostSessionLifecycleSource exposes application-owned session activation.
// The channel remains open until the agent stops.
type HostSessionLifecycleSource interface {
	HostSessionEvents() <-chan HostSessionLifecycle
	HostSessionBindingTarget() string
}

// HostSessionActivation is a prepared application-owned session attachment.
// The engine commits it to an IM thread only after generation fencing succeeds.
type HostSessionActivation struct {
	Session              AgentSession
	SessionID            string
	ActivationID         string
	ActivationGeneration uint64
}

// HostSessionResumer is implemented by semantic Session Host agents that can
// resume an existing application session without starting a new CLI process.
type HostSessionResumer interface {
	ResumeHostSession(
		ctx context.Context, sessionID, activationID string,
	) (HostSessionActivation, error)
}

// HostSessionCollaboration is an immediate, model-independent request from the
// host UI to enable, switch, or disable one IM surface for one host session.
type HostSessionCollaboration struct {
	SessionID    string
	Channel      string
	Enabled      bool
	WorkDir      string
	Summary      string
	MessageCount int
	GitBranch    string
	Origin       string
}

type HostSessionCollaborationSource interface {
	HostSessionCollaborationEvents() <-chan HostSessionCollaboration
	HostSessionBindingTargetFor(channel string) string
	HostSessionCollaborationChannels() []string
}

// InteractionResolution is an out-of-band first-responder notification. It
// never becomes model-visible user input.
type InteractionResolution struct {
	RequestID    string
	Behavior     string
	Origin       string
	UpdatedInput map[string]any
	Message      string
}

type InteractionResolutionSource interface {
	InteractionResolutions() <-chan InteractionResolution
}

// SessionThreadBinder lets a platform create a durable sub-conversation for a
// host-owned session. Implementations must be idempotent when baseSessionKey is
// already thread-scoped: restore and return that thread instead of creating a
// nested one. Platforms without native thread creation can return a
// reconstructed reply context and reuse the base session key.
type SessionThreadBinder interface {
	BindSessionThread(ctx context.Context, baseSessionKey, sessionID, title string) (sessionKey string, replyCtx any, err error)
}

// SessionThreadTitleUpdater refreshes the visible identity of an existing
// platform thread without creating a message or changing its session binding.
type SessionThreadTitleUpdater interface {
	UpdateSessionThreadTitle(ctx context.Context, sessionKey, sessionID, title string) error
}

// FreshSessionThreadBinder is the optional platform primitive for workflows
// that explicitly request a sibling sub-conversation. Session Host `/new` does
// not use it: that command switches the session bound to the current thread.
type FreshSessionThreadBinder interface {
	CreateSessionThread(ctx context.Context, baseSessionKey, title string) (sessionKey string, replyCtx any, err error)
}

// StartHostSessionCoordinator binds active host sessions to the configured IM
// target and attaches an unsolicited event reader before any IM turn arrives.
func (e *Engine) StartHostSessionCoordinator(source HostSessionLifecycleSource) {
	if source == nil {
		return
	}
	collaboration, collaborative := source.(HostSessionCollaborationSource)
	go func() {
		for {
			select {
			case <-e.ctx.Done():
				return
			case event, ok := <-source.HostSessionEvents():
				if !ok {
					return
				}
				if event.Ended {
					e.handleHostSessionEnded(event)
					continue
				}
				if event.MetadataOnly {
					e.handleHostSessionMetadataUpdated(event)
					continue
				}
				if strings.EqualFold(strings.TrimSpace(event.Origin), "remote") {
					continue
				}
				target, channel, keepCurrentThread, restore :=
					e.localHostActivationBinding(event, collaboration, collaborative)
				if !restore {
					continue
				}
				var err error
				if keepCurrentThread {
					err = e.bindHostSessionToCurrentSurface(target, event)
				} else {
					err = e.bindHostSession(target, event)
				}
				if err != nil {
					slog.Error("host session resume binding failed", "project", e.name,
						"session_id", event.SessionID, "channel", channel, "error", err)
					continue
				}
				e.notifyHostSessionResumed(event)
			case change, ok := <-collaborationEvents(collaboration, collaborative):
				if !ok {
					return
				}
				if strings.EqualFold(strings.TrimSpace(change.Origin), "remote") {
					continue
				}
				if !change.Enabled {
					e.disableHostSessionCollaboration(change.SessionID, true)
					continue
				}
				target := collaboration.HostSessionBindingTargetFor(change.Channel)
				if strings.TrimSpace(target) == "" {
					slog.Error("host collaboration channel has no binding target",
						"project", e.name, "channel", change.Channel)
					continue
				}
				event := HostSessionLifecycle{
					SessionID: change.SessionID, WorkDir: change.WorkDir,
					Summary: change.Summary, MessageCount: change.MessageCount,
					GitBranch: change.GitBranch, Origin: change.Origin,
				}
				previousChannel, explicit := e.persistedHostCollaboration(change.SessionID)
				if !explicit || previousChannel != strings.ToLower(strings.TrimSpace(change.Channel)) {
					e.disableHostSessionCollaboration(change.SessionID, false)
				}
				if err := e.bindHostSession(target, event); err != nil {
					slog.Error("host session binding failed", "project", e.name,
						"session_id", change.SessionID, "channel", change.Channel,
						"error", err)
				}
			}
		}
	}()
}

func (e *Engine) handleHostSessionMetadataUpdated(event HostSessionLifecycle) {
	key := e.findBoundHostSessionKey(event.SessionID)
	if key == "" {
		return
	}
	title := hostSessionDisplayTitle(event.WorkDir, event.Summary, event.SessionID)
	for _, session := range e.sessions.AllSessions() {
		if session.GetAgentSessionID() == event.SessionID {
			session.SetName(title)
		}
	}
	e.sessions.SetSessionName(event.SessionID, title)

	platformName := strings.SplitN(key, ":", 2)[0]
	for _, platform := range e.platforms {
		if platform.Name() != platformName {
			continue
		}
		if updater, ok := platform.(SessionThreadTitleUpdater); ok {
			if err := updater.UpdateSessionThreadTitle(
				e.ctx, key, event.SessionID, title); err != nil {
				slog.Warn("host session thread title update failed",
					"session_id", event.SessionID, "session_key", key, "error", err)
			}
		}
		break
	}
}

func (e *Engine) handleHostSessionEnded(event HostSessionLifecycle) {
	acknowledge := func() {
		if event.Acknowledge != nil {
			event.Acknowledge()
		}
	}
	key := e.findBoundHostSessionKey(event.SessionID)
	if key == "" {
		acknowledge()
		return
	}
	interactiveKey := e.interactiveKeyForSessionKey(key)
	if e.sessionHostRouter != nil {
		route, err := e.sessionHostRouter.Lookup(key)
		if err != nil {
			slog.Warn("terminal-end route lookup failed; suppressing notification",
				"session_id", event.SessionID, "session_key", key, "error", err)
			acknowledge()
			e.cleanupInteractiveState(interactiveKey)
			return
		}
		if route != nil && !e.sessionHostRouter.Owns(key, route.Generation) {
			// Multiple sidecars share the durable session index. A stale local
			// attachment must never announce another process owner's shutdown.
			acknowledge()
			e.cleanupInteractiveState(interactiveKey)
			return
		}
	}
	e.interactiveMu.Lock()
	state := e.interactiveStates[interactiveKey]
	e.interactiveMu.Unlock()
	if state == nil {
		acknowledge()
		return
	}
	state.mu.Lock()
	platform, replyCtx := state.platform, state.replyCtx
	routeGeneration := state.sessionHostRouteGeneration
	state.mu.Unlock()
	if platform != nil && replyCtx != nil {
		_ = e.sendWithError(platform, replyCtx,
			"⏹ Terminal session ended · resume is available")
	}
	// Tell the host that the visible IM marker has finished sending before it
	// tears down the bundled sidecar. Live-state cleanup may continue locally.
	acknowledge()
	// Keep the durable session/thread/channel mapping. Only the live reader and
	// application-owned AgentSession wrapper end with the terminal process.
	e.cleanupInteractiveState(interactiveKey, state)
	if e.sessionHostRouter != nil && routeGeneration != 0 {
		if _, err := e.sessionHostRouter.CompareAndDelete(
			key, e.sessionHostRouter.ownerToken, routeGeneration); err != nil {
			slog.Warn("terminal-end route cleanup failed", "session_key", key, "error", err)
		}
	}
}

func collaborationEvents(source HostSessionCollaborationSource, ok bool) <-chan HostSessionCollaboration {
	if !ok || source == nil {
		return nil
	}
	return source.HostSessionCollaborationEvents()
}

// localHostActivationBinding resolves where a terminal-origin activation must
// remain visible. A live collaboration thread belongs to the TUI surface, not
// permanently to the transcript that happened to be active when it was
// enabled. Therefore a local /resume or /new first inherits the exact thread
// bound to PreviousSessionID. Persisted target-session state is only a restart
// fallback when there is no live collaboration surface to carry forward.
func (e *Engine) localHostActivationBinding(
	event HostSessionLifecycle,
	source HostSessionCollaborationSource,
	collaborative bool,
) (target, channel string, keepCurrentThread, restore bool) {
	if !collaborative || source == nil {
		return "", "", false, false
	}
	previousID := strings.TrimSpace(event.PreviousSessionID)
	if previousID != "" && previousID != strings.TrimSpace(event.SessionID) {
		if previousChannel, explicit := e.persistedHostCollaboration(previousID); explicit && previousChannel != "" {
			previousKey := e.findBoundHostSessionKeyForPlatform(previousID, previousChannel)
			if previousKey != "" && e.isLiveHostCollaborationSurface(previousKey, previousID) {
				return previousKey, previousChannel, true, true
			}
		}
	}

	targetChannel, explicit := e.persistedHostCollaboration(event.SessionID)
	if !explicit || targetChannel == "" {
		return "", "", false, false
	}
	targetKey := strings.TrimSpace(source.HostSessionBindingTargetFor(targetChannel))
	if targetKey == "" {
		return "", "", false, false
	}
	return targetKey, targetChannel, false, true
}

func (e *Engine) isLiveHostCollaborationSurface(sessionKey, sessionID string) bool {
	interactiveKey := e.interactiveKeyForSessionKey(sessionKey)
	e.interactiveMu.Lock()
	state := e.interactiveStates[interactiveKey]
	e.interactiveMu.Unlock()
	if state == nil {
		return false
	}
	state.mu.Lock()
	agentSession := state.agentSession
	platform := state.platform
	replyCtx := state.replyCtx
	routeKey := state.sessionHostRouteKey
	routeGeneration := state.sessionHostRouteGeneration
	state.mu.Unlock()
	if agentSession == nil || !agentSession.Alive() ||
		agentSession.CurrentSessionID() != sessionID || platform == nil || replyCtx == nil {
		return false
	}
	if routeKey == "" {
		routeKey = sessionKey
	}
	if e.sessionHostRouter != nil {
		// A shared Feishu app can deliver the same event to sibling sidecars.
		// Only a fully registered route owned at the captured generation may be
		// inherited; zero/foreign generations fail closed instead of guessing.
		if routeGeneration == 0 || !e.sessionHostRouter.Owns(routeKey, routeGeneration) {
			return false
		}
	}
	return true
}

func (e *Engine) disableHostSessionCollaboration(sessionID string, notify bool) {
	if strings.TrimSpace(sessionID) == "" {
		return
	}
	e.interactiveMu.Lock()
	matching := make(map[string]*interactiveState)
	for key, state := range e.interactiveStates {
		if state == nil {
			continue
		}
		state.mu.Lock()
		matches := state.agentSession != nil &&
			state.agentSession.CurrentSessionID() == sessionID
		state.mu.Unlock()
		if matches {
			matching[key] = state
		}
	}
	e.interactiveMu.Unlock()
	if notify {
		for _, state := range matching {
			notifyCollaborationDisabled(e, state)
		}
	}
	for key, state := range matching {
		e.cleanupInteractiveState(key, state)
	}
	for _, session := range e.sessions.AllSessions() {
		if session.GetAgentSessionID() == sessionID {
			session.SetCollaborationChannel("")
		}
	}
	e.sessions.Save()
}

func notifyCollaborationDisabled(e *Engine, state *interactiveState) {
	state.mu.Lock()
	platform, replyCtx := state.platform, state.replyCtx
	state.mu.Unlock()
	if platform == nil || replyCtx == nil {
		return
	}
	e.send(platform, replyCtx, "↪ Collaboration disabled on terminal")
}

// prepareInboundHostSessionThread moves a normal inbound IM turn into the
// platform's durable sub-conversation before cc-connect creates its local
// Session or asks the external host to open one. sessionID is intentionally
// empty here: the host allocates it during StartSession, after this routing
// decision. Existing thread-scoped keys are restored without creating a new
// root when the platform supports that behavior.
func (e *Engine) prepareInboundHostSessionThread(p Platform, msg *Message, agent Agent) (bool, error) {
	if p == nil || msg == nil || agent == nil {
		return false, nil
	}
	if _, ok := agent.(HostSessionLifecycleSource); !ok {
		return false, nil
	}
	binder, ok := p.(SessionThreadBinder)
	if !ok {
		return false, nil
	}
	baseKey := strings.TrimSpace(msg.SessionKey)
	if baseKey == "" {
		return false, fmt.Errorf("host session inbound message has an empty session key")
	}
	title := strings.TrimSpace(msg.Content)
	if title == "" {
		title = "Claude Code"
	} else {
		title = truncateStr(title, 80)
	}
	boundKey, replyCtx, err := binder.BindSessionThread(e.ctx, baseKey, "", title)
	if err != nil {
		return false, fmt.Errorf("prepare host session thread: %w", err)
	}
	if strings.TrimSpace(boundKey) == "" || replyCtx == nil {
		return false, fmt.Errorf("platform returned an empty host session thread binding")
	}
	msg.SessionKey = boundKey
	msg.ReplyCtx = replyCtx
	return true, nil
}

func (e *Engine) bindHostSession(baseSessionKey string, event HostSessionLifecycle) error {
	return e.bindHostSessionWithPolicy(baseSessionKey, event, false)
}

func (e *Engine) bindHostSessionToCurrentSurface(
	baseSessionKey string, event HostSessionLifecycle,
) error {
	return e.bindHostSessionWithPolicy(baseSessionKey, event, true)
}

func (e *Engine) bindHostSessionWithPolicy(
	baseSessionKey string, event HostSessionLifecycle, keepBaseThread bool,
) error {
	if strings.TrimSpace(event.SessionID) == "" {
		return fmt.Errorf("host session ID is empty")
	}
	platformName := strings.SplitN(baseSessionKey, ":", 2)[0]
	var platform Platform
	for _, candidate := range e.platforms {
		if candidate.Name() == platformName {
			platform = candidate
			break
		}
	}
	if platform == nil {
		return fmt.Errorf("platform %q not found for binding target", platformName)
	}

	title := hostSessionDisplayTitle(event.WorkDir, event.Summary, event.SessionID)
	boundKey := baseSessionKey
	var replyCtx any
	if keepBaseThread {
		if binder, ok := platform.(SessionThreadBinder); ok {
			var err error
			boundKey, replyCtx, err = binder.BindSessionThread(
				e.ctx, boundKey, event.SessionID, title)
			if err != nil {
				return fmt.Errorf("keep current session thread: %w", err)
			}
		} else {
			reconstructor, ok := platform.(ReplyContextReconstructor)
			if !ok {
				return fmt.Errorf("platform %q cannot reconstruct current thread", platform.Name())
			}
			var err error
			replyCtx, err = reconstructor.ReconstructReplyCtx(boundKey)
			if err != nil {
				return fmt.Errorf("reconstruct current thread: %w", err)
			}
		}
	} else if existingKey := e.findBoundHostSessionKeyForPlatform(event.SessionID, platformName); existingKey != "" {
		boundKey = existingKey
		if binder, ok := platform.(SessionThreadBinder); ok {
			var err error
			boundKey, replyCtx, err = binder.BindSessionThread(e.ctx, boundKey, event.SessionID, title)
			if err != nil {
				return fmt.Errorf("restore existing session thread: %w", err)
			}
		} else {
			reconstructor, ok := platform.(ReplyContextReconstructor)
			if !ok {
				return fmt.Errorf("platform %q cannot reconstruct existing thread", platform.Name())
			}
			var err error
			replyCtx, err = reconstructor.ReconstructReplyCtx(boundKey)
			if err != nil {
				return fmt.Errorf("reconstruct existing thread: %w", err)
			}
		}
	} else if binder, ok := platform.(SessionThreadBinder); ok {
		var err error
		boundKey, replyCtx, err = binder.BindSessionThread(e.ctx, baseSessionKey, event.SessionID, title)
		if err != nil {
			return fmt.Errorf("create session thread: %w", err)
		}
	} else {
		reconstructor, ok := platform.(ReplyContextReconstructor)
		if !ok {
			return fmt.Errorf("platform %q cannot reconstruct proactive replies", platform.Name())
		}
		var err error
		replyCtx, err = reconstructor.ReconstructReplyCtx(boundKey)
		if err != nil {
			return fmt.Errorf("reconstruct reply context: %w", err)
		}
	}
	if strings.TrimSpace(boundKey) == "" || replyCtx == nil {
		return fmt.Errorf("platform returned an empty session thread binding")
	}
	if strings.TrimSpace(event.Summary) == "" {
		if activeID := e.sessions.ActiveSessionID(boundKey); activeID != "" {
			if existing := e.sessions.FindByID(activeID); existing != nil {
				if existingTitle := sessionProgressTitle(existing); existingTitle != "" {
					title = existingTitle
				}
			}
		}
	}

	interactiveKey := e.interactiveKeyForSessionKey(boundKey)
	resumeLock := e.hostResumeLock(boundKey)
	resumeLock.Lock()
	defer resumeLock.Unlock()
	e.interactiveMu.Lock()
	existingBeforeResume := e.interactiveStates[interactiveKey]
	e.interactiveMu.Unlock()
	if existingBeforeResume != nil && event.ActivationGeneration > 0 {
		existingBeforeResume.mu.Lock()
		currentGeneration := existingBeforeResume.sessionHostActivationGeneration
		currentSessionID := ""
		if existingBeforeResume.agentSession != nil {
			currentSessionID = existingBeforeResume.agentSession.CurrentSessionID()
		}
		existingBeforeResume.mu.Unlock()
		if currentGeneration > event.ActivationGeneration ||
			(currentGeneration == event.ActivationGeneration && currentGeneration != 0 &&
				currentSessionID != "" && currentSessionID != event.SessionID) {
			slog.Debug("ignore stale host activation binding",
				"session_key", boundKey, "session_id", event.SessionID,
				"activation_generation", event.ActivationGeneration,
				"current_generation", currentGeneration)
			return nil
		}
	}
	if !keepBaseThread {
		moved, err := e.moveBoundHostInteractiveState(
			event.SessionID, interactiveKey, boundKey, platform, replyCtx, event.WorkDir)
		if err != nil {
			return err
		}
		if moved {
			localSession := e.sessions.SwitchToAgentSession(
				boundKey, event.SessionID, e.agent.Name(), title)
			localSession.SetCollaborationChannel(platformName)
			e.sessions.Save()
			movedState := e.interactiveState(interactiveKey)
			if movedState != nil {
				movedState.mu.Lock()
				movedState.sessionHostActivationGeneration = event.ActivationGeneration
				movedState.mu.Unlock()
				e.startUnsolicitedReader(
					movedState, localSession, e.sessions, interactiveKey, event.WorkDir)
			}
			slog.Info("host session binding moved", "project", e.name, "session_id", event.SessionID,
				"session_key", boundKey, "origin", event.Origin)
			return nil
		}
	}

	localSession := e.sessions.SwitchToAgentSession(
		boundKey, event.SessionID, e.agent.Name(), title)
	localSession.SetCollaborationChannel(platformName)
	e.sessions.Save()
	e.interactiveMu.Lock()
	existing := e.interactiveStates[interactiveKey]
	e.interactiveMu.Unlock()
	if existing != nil {
		existing.mu.Lock()
		same := existing.agentSession != nil && existing.agentSession.Alive() &&
			existing.agentSession.CurrentSessionID() == event.SessionID
		if same {
			existing.platform = platform
			existing.replyCtx = replyCtx
			existing.sessionHostActivationGeneration = event.ActivationGeneration
		}
		existing.mu.Unlock()
		if same {
			if e.sessionHostRouter != nil {
				route, err := e.sessionHostRouter.RegisterRoute(boundKey)
				if err != nil {
					return fmt.Errorf("renew session-host route: %w", err)
				}
				existing.mu.Lock()
				existing.sessionHostRouteGeneration = route.Generation
				existing.sessionHostRouteKey = boundKey
				existing.mu.Unlock()
			}
			return nil
		}
		e.cleanupInteractiveState(interactiveKey, existing)
	}

	state := e.getOrCreateInteractiveStateWith(
		interactiveKey, platform, replyCtx, localSession, e.sessions, nil, boundKey)
	state.mu.Lock()
	alive := state.agentSession != nil && state.agentSession.Alive()
	if alive {
		// A host session may already be in the middle of a local PTY turn when
		// binding occurs. Early events are preserved by the Session Link client.
		state.eventsNeedResync = false
	}
	state.sessionHostActivationGeneration = event.ActivationGeneration
	state.mu.Unlock()
	if !alive {
		return fmt.Errorf("failed to attach host session %q", event.SessionID)
	}
	if e.sessionHostRouter != nil {
		route, err := e.sessionHostRouter.RegisterRoute(boundKey)
		if err != nil {
			return fmt.Errorf("register session-host route: %w", err)
		}
		state.mu.Lock()
		state.sessionHostRouteGeneration = route.Generation
		state.sessionHostRouteKey = boundKey
		state.mu.Unlock()
	}
	e.startUnsolicitedReader(state, localSession, e.sessions, interactiveKey, event.WorkDir)
	slog.Info("host session bound", "project", e.name, "session_id", event.SessionID,
		"session_key", boundKey, "origin", event.Origin)
	return nil
}

// moveBoundHostInteractiveState transfers an idle live Session Link state when
// the same Java session is rebound to a different IM thread. The AgentSession
// must not be closed: both bindings refer to the same underlying Java TUI.
// A foreground IM turn keeps its original routing until completion, so moving
// a busy state is rejected instead of allowing two map entries/readers to race.
func (e *Engine) moveBoundHostInteractiveState(
	agentSessionID, targetInteractiveKey, targetRouteKey string,
	platform Platform, replyCtx any, workspaceDir string,
) (bool, error) {
	e.interactiveMu.Lock()
	var sourceKey string
	var state *interactiveState
	targetOccupied := e.interactiveStates[targetInteractiveKey] != nil
	for key, candidate := range e.interactiveStates {
		if key == targetInteractiveKey || candidate == nil {
			continue
		}
		candidate.mu.Lock()
		matches := candidate.agentSession != nil && candidate.agentSession.Alive() &&
			candidate.agentSession.CurrentSessionID() == agentSessionID
		candidate.mu.Unlock()
		if matches {
			sourceKey = key
			state = candidate
			break
		}
	}
	e.interactiveMu.Unlock()
	if state == nil {
		return false, nil
	}
	if targetOccupied {
		return false, fmt.Errorf("target host thread %q already has a live session", targetRouteKey)
	}

	var sourceSession *Session
	if sourceSessionKey, ok := e.sessions.SessionKeyForAgentSessionID(agentSessionID); ok {
		sourceSession = e.sessions.FindByID(e.sessions.ActiveSessionID(sourceSessionKey))
		if sourceSession != nil && sourceSession.Busy() {
			return false, fmt.Errorf("host session %q is busy on %q", agentSessionID, sourceSessionKey)
		}
	}

	var route *SessionHostRoute
	var err error
	if e.sessionHostRouter != nil {
		route, err = e.sessionHostRouter.RegisterRoute(targetRouteKey)
		if err != nil {
			return false, fmt.Errorf("register moved session-host route: %w", err)
		}
	}

	oldRouteKey, oldGeneration := state.hostRouteIdentity(sourceKey)
	e.stopUnsolicitedReader(state)

	e.interactiveMu.Lock()
	if e.interactiveStates[sourceKey] != state {
		e.interactiveMu.Unlock()
		if route != nil {
			_, _ = e.sessionHostRouter.CompareAndDelete(
				targetRouteKey, route.OwnerToken, route.Generation)
		}
		e.startUnsolicitedReader(state, sourceSession, e.sessions, sourceKey, workspaceDir)
		return false, fmt.Errorf("host session %q binding changed concurrently", agentSessionID)
	}
	if target := e.interactiveStates[targetInteractiveKey]; target != nil && target != state {
		e.interactiveMu.Unlock()
		if route != nil {
			_, _ = e.sessionHostRouter.CompareAndDelete(
				targetRouteKey, route.OwnerToken, route.Generation)
		}
		e.startUnsolicitedReader(state, sourceSession, e.sessions, sourceKey, workspaceDir)
		return false, fmt.Errorf("target host thread %q already has a live session", targetRouteKey)
	}
	delete(e.interactiveStates, sourceKey)
	e.interactiveStates[targetInteractiveKey] = state
	e.interactiveMu.Unlock()

	state.mu.Lock()
	state.platform = platform
	state.replyCtx = replyCtx
	state.workspaceDir = workspaceDir
	state.sessionHostRouteKey = targetRouteKey
	if route != nil {
		state.sessionHostRouteGeneration = route.Generation
	} else {
		state.sessionHostRouteGeneration = 0
	}
	state.mu.Unlock()

	if e.sessionHostRouter != nil && oldRouteKey != "" && oldRouteKey != targetRouteKey && oldGeneration != 0 {
		if _, err := e.sessionHostRouter.CompareAndDelete(
			oldRouteKey, e.sessionHostRouter.ownerToken, oldGeneration); err != nil {
			slog.Warn("delete previous session-host route after move",
				"session_key", oldRouteKey, "error", err)
		}
	}
	return true, nil
}

func (e *Engine) interactiveState(sessionKey string) *interactiveState {
	e.interactiveMu.Lock()
	defer e.interactiveMu.Unlock()
	return e.interactiveStates[sessionKey]
}

func (e *Engine) findBoundHostSessionKey(agentSessionID string) string {
	return e.findBoundHostSessionKeyForPlatform(agentSessionID, "")
}

func (e *Engine) findBoundHostSessionKeyForPlatform(agentSessionID, platformName string) string {
	if platformName == "" {
		if key, ok := e.sessions.SessionKeyForAgentSessionID(agentSessionID); ok {
			return key
		}
		return ""
	}
	idToKey, _ := e.sessions.SessionKeyMap()
	for _, session := range e.sessions.AllSessions() {
		if session.GetAgentSessionID() == agentSessionID {
			key := idToKey[session.ID]
			if platformName == "" || strings.HasPrefix(key, platformName+":") {
				return key
			}
		}
	}
	return ""
}

// persistedHostCollaboration returns the explicit per-session choice. Legacy
// snapshots infer the channel once from their durable thread key, then persist
// it so subsequent explicit Off selections remain authoritative.
func (e *Engine) persistedHostCollaboration(agentSessionID string) (string, bool) {
	idToKey, _ := e.sessions.SessionKeyMap()
	for _, session := range e.sessions.AllSessions() {
		if session.GetAgentSessionID() != agentSessionID {
			continue
		}
		if channel, explicit := session.GetCollaborationChannel(); explicit {
			return channel, true
		}
		key := idToKey[session.ID]
		channel := strings.SplitN(key, ":", 2)[0]
		if channel == "" {
			return "", false
		}
		session.SetCollaborationChannel(channel)
		e.sessions.Save()
		return channel, true
	}
	return "", false
}

// notifyHostSessionResumed leaves one compact, non-model-visible marker in the
// restored IM thread. The marker uses the same human-readable title as the
// thread so multiple resumed sessions remain distinguishable. Transcript
// history is not replayed; subsequent semantic output continues through the
// existing unsolicited reader.
func (e *Engine) notifyHostSessionResumed(event HostSessionLifecycle) {
	key := e.findBoundHostSessionKey(event.SessionID)
	if key == "" {
		return
	}
	resumeLock := e.hostResumeLock(key)
	resumeLock.Lock()
	defer resumeLock.Unlock()
	interactiveKey := e.interactiveKeyForSessionKey(key)
	e.interactiveMu.Lock()
	state := e.interactiveStates[interactiveKey]
	e.interactiveMu.Unlock()
	if state == nil {
		return
	}
	state.mu.Lock()
	platform, replyCtx := state.platform, state.replyCtx
	currentSessionID := ""
	if state.agentSession != nil {
		currentSessionID = state.agentSession.CurrentSessionID()
	}
	currentGeneration := state.sessionHostActivationGeneration
	state.mu.Unlock()
	if platform == nil || replyCtx == nil || currentSessionID != event.SessionID ||
		(event.ActivationGeneration > 0 && currentGeneration != event.ActivationGeneration) {
		return
	}
	title := hostSessionDisplayTitle(event.WorkDir, event.Summary, event.SessionID)
	e.send(platform, replyCtx, e.i18n.Tf(MsgResumeTerminalNotice, title, event.MessageCount))
	e.refreshLatestResumeCard(key)
}

func shortSessionID(id string) string {
	id = strings.TrimSpace(id)
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}

func hostSessionDisplayTitle(workDir, summary, sessionID string) string {
	summary = strings.Join(strings.Fields(strings.TrimSpace(summary)), " ")
	if summary == "" {
		summary = "Claude Code · " + shortSessionID(sessionID)
	} else {
		summary = truncateStr(summary, 56)
	}
	project := strings.TrimSpace(filepath.Base(filepath.Clean(strings.TrimSpace(workDir))))
	if project == "" || project == "." || project == string(filepath.Separator) ||
		strings.HasPrefix(strings.ToLower(summary), strings.ToLower(project)+" ·") {
		return summary
	}
	return truncateStr(project+" · "+summary, 72)
}
