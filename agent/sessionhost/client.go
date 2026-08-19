package sessionhost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"

	"github.com/chenhg5/cc-connect/core"
)

var errLinkClosed = errors.New("session-host: link closed")

const (
	maxEarlyEventSessions    = 64
	maxEarlyEventsPerSession = 2048
)

type callResult struct {
	frame frame
	err   error
}

type hostCallError struct {
	name    string
	code    string
	message string
}

func (e *hostCallError) Error() string {
	return fmt.Sprintf("session-host: host rejected %s (%s): %s", e.name, e.code, e.message)
}

type linkClient struct {
	conn  net.Conn
	codec *frameCodec

	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
	alive   atomic.Bool
	ids     atomic.Uint64
	workDir string

	mu                         sync.RWMutex
	sessionCallMu              sync.Mutex
	pending                    map[string]chan callResult
	sessions                   map[string]*Session
	earlyEvents                map[string][]frame
	hostSessions               chan core.HostSessionLifecycle
	collaboration              chan core.HostSessionCollaboration
	activeSession              string
	activeActivationID         string
	activeActivationGeneration uint64

	closeOnce sync.Once
}

func connectLink(ctx context.Context, cfg agentConfig, dial dialFunc) (*linkClient, error) {
	conn, err := dial(ctx, cfg.network, cfg.address)
	if err != nil {
		return nil, fmt.Errorf("session-host: connect to %s endpoint: %w", cfg.network, err)
	}

	clientCtx, cancel := context.WithCancel(context.Background())
	client := &linkClient{
		conn:          conn,
		codec:         newFrameCodec(conn, cfg.maxFrameBytes),
		ctx:           clientCtx,
		cancel:        cancel,
		done:          make(chan struct{}),
		workDir:       cfg.workDir,
		pending:       make(map[string]chan callResult),
		sessions:      make(map[string]*Session),
		earlyEvents:   make(map[string][]frame),
		hostSessions:  make(chan core.HostSessionLifecycle, 32),
		collaboration: make(chan core.HostSessionCollaboration, 32),
	}
	client.alive.Store(true)
	go client.readLoop()

	var hello linkHelloResult
	if err := client.call(ctx, messageLinkHello, "", linkHelloPayload{
		Client: clientName, AuthToken: cfg.authToken,
		CollaborationChannels: sortedTargetChannels(cfg.collaborationTargets),
	}, &hello); err != nil {
		client.closeWithError(err)
		return nil, fmt.Errorf("session-host: handshake: %w", err)
	}
	slog.Info("session-host: link established", "host", hello.Host, "network", cfg.network)
	return client, nil
}

func (c *linkClient) Alive() bool {
	return c != nil && c.alive.Load()
}

func (c *linkClient) call(ctx context.Context, name, sessionID string, payload, result any) error {
	if !c.Alive() {
		return errLinkClosed
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("session-host: encode %s payload: %w", name, err)
	}

	id := fmt.Sprintf("cc-%d", c.ids.Add(1))
	response := make(chan callResult, 1)
	c.mu.Lock()
	if !c.alive.Load() {
		c.mu.Unlock()
		return errLinkClosed
	}
	c.pending[id] = response
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
	}()

	if err := c.codec.write(frame{
		Protocol:  protocolName,
		Version:   protocolVersion,
		Kind:      frameKindRequest,
		Name:      name,
		ID:        id,
		SessionID: sessionID,
		Payload:   raw,
	}); err != nil {
		c.closeWithError(err)
		return err
	}

	select {
	case received := <-response:
		if received.err != nil {
			return received.err
		}
		if result == nil || len(received.frame.Payload) == 0 {
			return nil
		}
		decoder := json.NewDecoder(bytes.NewReader(received.frame.Payload))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(result); err != nil {
			return fmt.Errorf("session-host: decode %s response: %w", name, err)
		}
		if err := ensureJSONEOF(decoder); err != nil {
			return err
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("session-host: %s: %w", name, ctx.Err())
	case <-c.done:
		return errLinkClosed
	}
}

func (c *linkClient) registerSession(session *Session) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.alive.Load() {
		return errLinkClosed
	}
	if _, exists := c.sessions[session.id]; exists {
		return fmt.Errorf("session-host: session %q is already attached", session.id)
	}
	c.sessions[session.id] = session
	early := c.earlyEvents[session.id]
	delete(c.earlyEvents, session.id)

	for _, value := range early {
		event, known, err := mapWireEvent(value)
		if err != nil {
			delete(c.sessions, session.id)
			return fmt.Errorf("session-host: replay early event %s: %w", value.Name, err)
		}
		if known {
			session.emit(event)
		}
	}
	return nil
}

func (c *linkClient) setActiveSession(sessionID string) {
	c.mu.Lock()
	c.activeSession = sessionID
	c.mu.Unlock()
}

// activateSession applies a causal activation without allowing an older
// response/event to overwrite a newer host lifecycle decision.
func (c *linkClient) activateSession(
	sessionID, activationID string, generation uint64,
) bool {
	_, activated := c.activateSessionTransition(sessionID, activationID, generation)
	return activated
}

// activateSessionTransition is the event-path variant of activateSession. It
// returns the session that was authoritative immediately before the accepted
// transition so the engine can move the exact current IM thread on a local TUI
// resume. The value and the activation fence are read/updated under one lock.
func (c *linkClient) activateSessionTransition(
	sessionID, activationID string, generation uint64,
) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	previous := c.activeSession
	if generation == 0 {
		if c.activeActivationGeneration > 0 && c.activeSession != sessionID {
			return "", false
		}
		c.activeSession = sessionID
		return previous, true
	}
	if generation < c.activeActivationGeneration {
		return "", false
	}
	if generation == c.activeActivationGeneration &&
		c.activeSession != "" && c.activeSession != sessionID {
		return "", false
	}
	c.activeSession = sessionID
	c.activeActivationID = activationID
	c.activeActivationGeneration = generation
	return previous, true
}

func (c *linkClient) isActiveSession(sessionID string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.activeSession == sessionID
}

func (c *linkClient) captureSessionModel(sessionID, model string) {
	c.mu.RLock()
	session := c.sessions[sessionID]
	c.mu.RUnlock()
	if session != nil {
		session.captureModel(model)
	}
}

func (c *linkClient) attachedSession(sessionID string) *Session {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.sessions[sessionID]
}

func (c *linkClient) unregisterSession(sessionID string, expected *Session) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if current := c.sessions[sessionID]; current == expected {
		delete(c.sessions, sessionID)
	}
}

func (c *linkClient) readLoop() {
	for {
		value, err := c.codec.read()
		if err != nil {
			c.closeWithError(err)
			return
		}

		switch value.Kind {
		case frameKindResponse, frameKindError:
			c.deliverResponse(value)
		case frameKindEvent:
			if err := c.deliverEvent(value); err != nil {
				c.closeWithError(err)
				return
			}
		}
	}
}

func (c *linkClient) deliverResponse(value frame) {
	c.mu.RLock()
	pending := c.pending[value.ReplyTo]
	c.mu.RUnlock()
	if pending == nil {
		slog.Debug("session-host: dropping late response", "reply_to", value.ReplyTo, "name", value.Name)
		return
	}

	result := callResult{frame: value}
	if value.Kind == frameKindError {
		result.err = &hostCallError{name: value.Name, code: value.Error.Code, message: value.Error.Message}
	}
	select {
	case pending <- result:
	default:
	}
}

func (c *linkClient) deliverEvent(value frame) error {
	if value.Name == eventSessionActivated {
		payload, err := decodePayload[sessionActivatedPayload](value.Payload)
		if err != nil {
			return err
		}
		previousSessionID, activated := c.activateSessionTransition(
			payload.ID, payload.ActivationID, payload.ActivationGeneration,
		)
		if !activated {
			slog.Debug("session-host: dropping stale activation event",
				"session_id", payload.ID,
				"activation_generation", payload.ActivationGeneration)
			return nil
		}
		event := core.HostSessionLifecycle{
			SessionID: payload.ID, WorkDir: payload.WorkDir, Summary: payload.Summary,
			MessageCount: payload.MessageCount, GitBranch: payload.GitBranch, Origin: payload.Origin,
			PreviousSessionID:    previousSessionID,
			ActivationID:         payload.ActivationID,
			ActivationGeneration: payload.ActivationGeneration,
		}
		select {
		case c.hostSessions <- event:
		case <-c.ctx.Done():
		}
		return nil
	}
	if value.Name == eventSessionUpdated {
		payload, err := decodePayload[sessionUpdatedPayload](value.Payload)
		if err != nil {
			return err
		}
		event := core.HostSessionLifecycle{
			SessionID: value.SessionID, WorkDir: payload.WorkDir, Summary: payload.Summary,
			MessageCount: payload.MessageCount, GitBranch: payload.GitBranch,
			Origin: payload.Origin, MetadataOnly: true,
		}
		select {
		case c.hostSessions <- event:
		case <-c.ctx.Done():
		}
		return nil
	}
	if value.Name == eventSessionEnded {
		payload, err := decodePayload[sessionEndedPayload](value.Payload)
		if err != nil {
			return err
		}
		var acknowledgeOnce sync.Once
		event := core.HostSessionLifecycle{
			SessionID: value.SessionID, WorkDir: payload.WorkDir, Summary: payload.Summary,
			MessageCount: payload.MessageCount, GitBranch: payload.GitBranch,
			Origin: payload.Origin, Ended: true, Reason: payload.Reason,
			Acknowledge: func() {
				acknowledgeOnce.Do(func() {
					id := fmt.Sprintf("cc-%d", c.ids.Add(1))
					raw, _ := json.Marshal(sessionEndedAckPayload{
						NotificationID: payload.NotificationID,
					})
					if err := c.codec.write(frame{
						Protocol: protocolName, Version: protocolVersion,
						Kind: frameKindRequest, Name: "session.ended.ack", ID: id,
						SessionID: value.SessionID, Payload: raw,
					}); err != nil {
						slog.Debug("session-host: terminal-end acknowledgement failed", "error", err)
					}
				})
			},
		}
		select {
		case c.hostSessions <- event:
		case <-c.ctx.Done():
			event.Acknowledge()
		}
		return nil
	}
	if value.Name == eventCollaborationChanged {
		payload, err := decodePayload[collaborationChangedPayload](value.Payload)
		if err != nil {
			return err
		}
		event := core.HostSessionCollaboration{
			SessionID: value.SessionID, Channel: payload.Channel, Enabled: payload.Enabled,
			WorkDir: payload.WorkDir, Summary: payload.Summary,
			MessageCount: payload.MessageCount, GitBranch: payload.GitBranch,
			Origin: payload.Origin,
		}
		select {
		case c.collaboration <- event:
		case <-c.ctx.Done():
		}
		return nil
	}
	c.mu.Lock()
	session := c.sessions[value.SessionID]
	if session == nil {
		if value.SessionID == "" {
			c.mu.Unlock()
			slog.Debug("session-host: host-wide event has no coordinator", "name", value.Name)
			return nil
		}
		buffered := c.earlyEvents[value.SessionID]
		if len(buffered) >= maxEarlyEventsPerSession {
			c.mu.Unlock()
			return fmt.Errorf("session-host: too many early events for session %q", value.SessionID)
		}
		if len(buffered) == 0 && len(c.earlyEvents) >= maxEarlyEventSessions {
			c.mu.Unlock()
			return fmt.Errorf("session-host: too many sessions with early events")
		}
		c.earlyEvents[value.SessionID] = append(buffered, value)
		c.mu.Unlock()
		slog.Debug("session-host: buffered event for attaching session", "session_id", value.SessionID, "name", value.Name)
		return nil
	}
	c.mu.Unlock()
	if value.Name == eventInteractionResolved {
		payload, err := decodePayload[interactionResolvedPayload](value.Payload)
		if err != nil {
			return err
		}
		session.emitResolution(payload)
		return nil
	}
	event, known, err := mapEvent(value)
	if err != nil {
		return err
	}
	if known {
		session.emit(event)
	}
	return nil
}

func mapEvent(value frame) (coreEvent, bool, error) {
	return mapWireEvent(value)
}

func (c *linkClient) closeWithError(cause error) {
	c.closeOnce.Do(func() {
		c.alive.Store(false)
		c.cancel()
		_ = c.conn.Close()

		c.mu.Lock()
		pending := make([]chan callResult, 0, len(c.pending))
		for _, response := range c.pending {
			pending = append(pending, response)
		}
		sessions := make([]*Session, 0, len(c.sessions))
		for _, session := range c.sessions {
			sessions = append(sessions, session)
		}
		c.pending = make(map[string]chan callResult)
		c.sessions = make(map[string]*Session)
		c.earlyEvents = make(map[string][]frame)
		c.mu.Unlock()

		if cause == nil {
			cause = errLinkClosed
		}
		for _, response := range pending {
			select {
			case response <- callResult{err: cause}:
			default:
			}
		}
		for _, session := range sessions {
			session.closeFromClient(cause)
		}
		if c.hostSessions != nil {
			close(c.hostSessions)
		}
		if c.collaboration != nil {
			close(c.collaboration)
		}
		close(c.done)
	})
}

func (c *linkClient) Close() error {
	c.closeWithError(errLinkClosed)
	return nil
}
