package sessionhost

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/chenhg5/cc-connect/core"
)

type coreEvent = core.Event

// Session is a lightweight attachment to an application-owned session. It
// does not own the underlying Session Link connection.
type Session struct {
	id      string
	client  *linkClient
	ctx     context.Context
	cancel  context.CancelFunc
	timeout time.Duration

	events               chan core.Event
	resolved             chan core.InteractionResolution
	alive                atomic.Bool
	eventsMu             sync.RWMutex
	metaMu               sync.RWMutex
	model                string
	effort               string
	mode                 string
	workDir              string
	collaborationChannel string

	sendMu    sync.Mutex
	closeOnce sync.Once
}

func newSession(parent context.Context, client *linkClient, id string, timeout time.Duration) *Session {
	ctx, cancel := context.WithCancel(parent)
	session := &Session{
		id:      id,
		client:  client,
		ctx:     ctx,
		cancel:  cancel,
		timeout: timeout,
		// A local PTY can emit output substantially faster than an IM API can
		// deliver it. Keep Session Link reads independent from the platform
		// sender so transient Feishu latency cannot immediately backpressure the
		// Java terminal. The engine still consumes events in order.
		events:   make(chan core.Event, 4096),
		resolved: make(chan core.InteractionResolution, 32),
		workDir:  client.workDir,
	}
	session.alive.Store(true)
	return session
}

func (s *Session) Send(prompt, messageID string, images []core.ImageAttachment, files []core.FileAttachment) error {
	if !s.Alive() {
		return fmt.Errorf("session-host: session is closed")
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	s.client.sessionCallMu.Lock()
	defer s.client.sessionCallMu.Unlock()
	ctx, cancel := context.WithTimeout(s.ctx, s.timeout)
	defer cancel()
	if err := s.ensureActiveLocked(ctx); err != nil {
		return err
	}

	payload := turnSubmitPayload{
		Prompt:      prompt,
		MessageID:   messageID,
		Images:      imageAttachments(images),
		Attachments: fileAttachments(files),
	}
	if err := s.client.call(ctx, messageTurnSubmit, s.id, payload, nil); err != nil {
		return fmt.Errorf("session-host: submit turn: %w", err)
	}
	return nil
}

func (s *Session) RespondPermission(requestID string, result core.PermissionResult) error {
	if !s.Alive() {
		return fmt.Errorf("session-host: session is closed")
	}
	if result.Behavior != "allow" && result.Behavior != "deny" {
		return fmt.Errorf("session-host: unsupported interaction behavior %q", result.Behavior)
	}
	ctx, cancel := context.WithTimeout(s.ctx, s.timeout)
	defer cancel()
	if err := s.client.call(ctx, messageInteractionRespond, s.id, interactionRespondPayload{
		RequestID:    requestID,
		Behavior:     result.Behavior,
		UpdatedInput: result.UpdatedInput,
		Message:      result.Message,
	}, nil); err != nil {
		var hostErr *hostCallError
		if errors.As(err, &hostErr) && hostErr.code == "stale_interaction" {
			return fmt.Errorf("%w: %s", core.ErrInteractionAlreadyResolved, hostErr.message)
		}
		return fmt.Errorf("session-host: respond to interaction: %w", err)
	}
	return nil
}

func (s *Session) Events() <-chan core.Event { return s.events }

func (s *Session) CurrentSessionID() string { return s.id }

func (s *Session) GetModel() string {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	return s.model
}

func (s *Session) ModelState(ctx context.Context) (core.SessionModelState, error) {
	if !s.Alive() {
		return core.SessionModelState{}, fmt.Errorf("session-host: session is closed")
	}
	requestCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	s.client.sessionCallMu.Lock()
	defer s.client.sessionCallMu.Unlock()
	if err := s.ensureActiveLocked(requestCtx); err != nil {
		return core.SessionModelState{}, err
	}
	var result modelStateResult
	if err := s.client.call(requestCtx, messageModelGet, s.id, struct{}{}, &result); err != nil {
		return core.SessionModelState{}, fmt.Errorf("session-host: get session model: %w", err)
	}
	state := mapModelState(result)
	s.captureModel(state.Current)
	return state, nil
}

func (s *Session) SetModel(ctx context.Context, model string) (core.SessionModelState, error) {
	if !s.Alive() {
		return core.SessionModelState{}, fmt.Errorf("session-host: session is closed")
	}
	model = strings.TrimSpace(model)
	if model == "" || len(model) > 1024 || strings.IndexFunc(model, unicode.IsControl) >= 0 {
		return core.SessionModelState{}, fmt.Errorf("session-host: invalid model selection")
	}
	requestCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	s.client.sessionCallMu.Lock()
	defer s.client.sessionCallMu.Unlock()
	if err := s.ensureActiveLocked(requestCtx); err != nil {
		return core.SessionModelState{}, err
	}
	var result modelStateResult
	if err := s.client.call(requestCtx, messageModelSet, s.id, modelSetPayload{Model: model}, &result); err != nil {
		return core.SessionModelState{}, fmt.Errorf("session-host: set session model: %w", err)
	}
	state := mapModelState(result)
	s.captureModel(state.Current)
	return state, nil
}

func (s *Session) EffortState(ctx context.Context) (core.SessionReasoningEffortState, error) {
	if !s.Alive() {
		return core.SessionReasoningEffortState{}, fmt.Errorf("session-host: session is closed")
	}
	requestCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	s.client.sessionCallMu.Lock()
	defer s.client.sessionCallMu.Unlock()
	if err := s.ensureActiveLocked(requestCtx); err != nil {
		return core.SessionReasoningEffortState{}, err
	}
	var result effortStateResult
	if err := s.client.call(requestCtx, messageEffortGet, s.id, struct{}{}, &result); err != nil {
		return core.SessionReasoningEffortState{}, fmt.Errorf("session-host: get session effort: %w", err)
	}
	return mapEffortState(result), nil
}

func (s *Session) SetEffort(ctx context.Context, effort string) (core.SessionReasoningEffortState, error) {
	if !s.Alive() {
		return core.SessionReasoningEffortState{}, fmt.Errorf("session-host: session is closed")
	}
	requestCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	s.client.sessionCallMu.Lock()
	defer s.client.sessionCallMu.Unlock()
	if err := s.ensureActiveLocked(requestCtx); err != nil {
		return core.SessionReasoningEffortState{}, err
	}
	var result effortStateResult
	if err := s.client.call(requestCtx, messageEffortSet, s.id,
		effortSetPayload{Effort: effort}, &result); err != nil {
		return core.SessionReasoningEffortState{}, fmt.Errorf("session-host: set session effort: %w", err)
	}
	return mapEffortState(result), nil
}

func (s *Session) Compact(ctx context.Context, instructions string) (core.SessionCompactionResult, error) {
	if !s.Alive() {
		return core.SessionCompactionResult{}, fmt.Errorf("session-host: session is closed")
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	s.client.sessionCallMu.Lock()
	defer s.client.sessionCallMu.Unlock()
	if err := s.ensureActiveLocked(ctx); err != nil {
		return core.SessionCompactionResult{}, err
	}
	var result compactRunResult
	if err := s.client.call(ctx, messageCompactRun, s.id,
		compactRunPayload{Instructions: instructions}, &result); err != nil {
		return core.SessionCompactionResult{}, fmt.Errorf("session-host: compact session: %w", err)
	}
	return core.SessionCompactionResult{Message: strings.TrimSpace(result.Message)}, nil
}

func mapModelState(result modelStateResult) core.SessionModelState {
	models := make([]core.ModelOption, 0, len(result.Models))
	for _, option := range result.Models {
		desc := strings.TrimSpace(option.Description)
		if desc == "" && option.Label != "" && option.Label != option.Name {
			desc = option.Label
		}
		models = append(models, core.ModelOption{
			Name: option.Name, Desc: desc, Alias: option.Alias,
		})
	}
	return core.SessionModelState{Current: result.Current, Models: models}
}

func mapEffortState(result effortStateResult) core.SessionReasoningEffortState {
	return core.SessionReasoningEffortState{
		Current: strings.TrimSpace(result.Current), Effective: strings.TrimSpace(result.Effective),
		Efforts: append([]string(nil), result.Efforts...),
	}
}

func (s *Session) captureModel(model string) {
	s.metaMu.Lock()
	s.model = model
	s.metaMu.Unlock()
}

// ensureActiveLocked requires client.sessionCallMu to be held by the caller so
// activation and the immediately following session-scoped request cannot be
// interleaved with another attachment on the shared Session Link connection.
func (s *Session) ensureActiveLocked(ctx context.Context) error {
	if s.client.isActiveSession(s.id) {
		return nil
	}
	var opened sessionOpenResult
	if err := s.client.call(ctx, messageSessionOpen, "", sessionOpenPayload{
		RequestedSessionID:   s.id,
		WorkDir:              s.client.workDir,
		CollaborationChannel: s.currentCollaborationChannel(),
	}, &opened); err != nil {
		return fmt.Errorf("session-host: activate session: %w", err)
	}
	if opened.SessionID != s.id {
		return fmt.Errorf("session-host: activation returned session %q, want %q", opened.SessionID, s.id)
	}
	if !s.client.activateSession(
		opened.SessionID, opened.ActivationID, opened.ActivationGeneration,
	) {
		return fmt.Errorf("session-host: activation was superseded")
	}
	return nil
}

func (s *Session) GetReasoningEffort() string {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	return s.effort
}

func (s *Session) GetWorkDir() string {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	return s.workDir
}

func (s *Session) SetSessionEnv(env []string) {
	channel := ""
	for _, entry := range env {
		if strings.HasPrefix(entry, "CC_SESSION_KEY=") {
			key := strings.TrimPrefix(entry, "CC_SESSION_KEY=")
			channel = strings.ToLower(strings.TrimSpace(strings.SplitN(key, ":", 2)[0]))
			break
		}
	}
	s.metaMu.Lock()
	s.collaborationChannel = channel
	s.metaMu.Unlock()
}

func (s *Session) currentCollaborationChannel() string {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	return s.collaborationChannel
}

func (s *Session) Alive() bool { return s.alive.Load() && s.client.Alive() }

var _ core.SessionEnvInjector = (*Session)(nil)

func (s *Session) Close() error {
	if !s.alive.CompareAndSwap(true, false) {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), minDuration(s.timeout, 2*time.Second))
	defer cancel()
	err := s.client.call(ctx, messageSessionDetach, s.id, struct{}{}, nil)
	s.finish()
	if err != nil && s.client.Alive() {
		return fmt.Errorf("session-host: detach session: %w", err)
	}
	return nil
}

func (s *Session) emit(event core.Event) {
	s.captureMetadata(event)
	s.eventsMu.RLock()
	defer s.eventsMu.RUnlock()
	if event.SessionID == "" {
		event.SessionID = s.id
	}
	select {
	case s.events <- event:
	case <-s.ctx.Done():
	}
}

func (s *Session) captureMetadata(event core.Event) {
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	switch event.Type {
	case core.EventUserInput:
		if event.Model != "" {
			s.model = event.Model
		}
		if event.ReasoningEffort != "" {
			s.effort = event.ReasoningEffort
		}
		if event.PermissionMode != "" {
			s.mode = event.PermissionMode
		}
	case core.EventModel:
		if event.Model != "" {
			s.model = event.Model
		}
	case core.EventPermissionRequest:
		if event.PermissionMode != "" {
			s.mode = event.PermissionMode
		}
	}
}

func (s *Session) emitResolution(event interactionResolvedPayload) {
	s.eventsMu.RLock()
	defer s.eventsMu.RUnlock()
	select {
	case s.resolved <- core.InteractionResolution{
		RequestID: event.RequestID, Behavior: event.Behavior, Origin: event.Origin,
		UpdatedInput: event.UpdatedInput, Message: event.Message,
	}:
	case <-s.ctx.Done():
	}
}

func (s *Session) InteractionResolutions() <-chan core.InteractionResolution {
	return s.resolved
}

func (s *Session) closeFromClient(cause error) {
	if s.alive.CompareAndSwap(true, false) && cause != nil && !isExpectedClose(cause) {
		s.emit(core.Event{Type: core.EventError, Error: cause, SessionID: s.id})
	}
	s.finish()
}

func (s *Session) finish() {
	s.closeOnce.Do(func() {
		s.client.unregisterSession(s.id, s)
		s.cancel()
		s.eventsMu.Lock()
		close(s.events)
		if s.resolved != nil {
			close(s.resolved)
		}
		s.eventsMu.Unlock()
	})
}

func isExpectedClose(err error) bool {
	return err == nil || err == errLinkClosed
}

func imageAttachments(images []core.ImageAttachment) []wireAttachment {
	result := make([]wireAttachment, 0, len(images))
	for _, image := range images {
		result = append(result, wireAttachment{
			MimeType: image.MimeType,
			FileName: image.FileName,
			Data:     image.Data,
		})
	}
	return result
}

func fileAttachments(files []core.FileAttachment) []wireAttachment {
	result := make([]wireAttachment, 0, len(files))
	for _, file := range files {
		result = append(result, wireAttachment{
			MimeType: file.MimeType,
			FileName: file.FileName,
			Data:     file.Data,
		})
	}
	return result
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func mapWireEvent(value frame) (core.Event, bool, error) {
	switch value.Name {
	case eventTurnStarted:
		payload, err := decodePayload[turnStartedPayload](value.Payload)
		return core.Event{
			Type: core.EventUserInput, Content: payload.DisplayText, SessionID: value.SessionID,
			InputOrigin:    payload.Origin,
			PermissionMode: payload.PermissionMode, Model: payload.ModelOverride,
			ReasoningEffort: payload.EffortOverride,
		}, true, err
	case eventTurnModel:
		payload, err := decodePayload[turnModelPayload](value.Payload)
		return core.Event{Type: core.EventModel, Model: payload.Model, SessionID: value.SessionID}, true, err
	case eventOutputText:
		payload, err := decodePayload[outputTextPayload](value.Payload)
		return core.Event{Type: core.EventText, Content: payload.Content, SessionID: value.SessionID, Synthetic: payload.Synthetic, Metadata: payload.Metadata}, true, err
	case eventOutputThinking:
		payload, err := decodePayload[outputThinkingPayload](value.Payload)
		return core.Event{Type: core.EventThinking, Content: payload.Content, SessionID: value.SessionID}, true, err
	case eventToolStarted:
		payload, err := decodePayload[toolStartedPayload](value.Payload)
		return core.Event{Type: core.EventToolUse, ToolName: payload.Name, ToolInput: payload.Input, ToolInputRaw: payload.InputRaw, SessionID: value.SessionID}, true, err
	case eventToolCompleted:
		payload, err := decodePayload[toolCompletedPayload](value.Payload)
		return core.Event{Type: core.EventToolResult, ToolName: payload.Name, ToolResult: payload.Result, Content: payload.Result, ToolStatus: payload.Status, ToolExitCode: payload.ExitCode, ToolSuccess: payload.Success, SessionID: value.SessionID}, true, err
	case eventInteractionRequested:
		payload, err := decodePayload[interactionRequestedPayload](value.Payload)
		questions := make([]core.UserQuestion, 0, len(payload.Questions))
		for _, question := range payload.Questions {
			options := make([]core.UserQuestionOption, 0, len(question.Options))
			for _, option := range question.Options {
				options = append(options, core.UserQuestionOption{Label: option.Label, Description: option.Description})
			}
			questions = append(questions, core.UserQuestion{
				Question:    question.Question,
				Header:      question.Header,
				Options:     options,
				MultiSelect: question.MultiSelect,
			})
		}
		return core.Event{
			Type: core.EventPermissionRequest, RequestID: payload.RequestID,
			ToolName: payload.ToolName, ToolInput: payload.ToolInput,
			ToolInputRaw: payload.InputRaw, Questions: questions, SessionID: value.SessionID,
			DecisionReasonType:    payload.DecisionReasonType,
			DecisionReasonDetail:  payload.DecisionReasonDetail,
			SuggestionRuleContent: payload.SuggestionRuleContent,
			SuggestionLabel:       payload.SuggestionLabel,
			WorkerID:              payload.WorkerID, WorkerColor: payload.WorkerColor,
			DestructiveWarning: payload.DestructiveWarning, BlockedPath: payload.BlockedPath,
			CustomMessage: payload.CustomMessage, ToolDescription: payload.ToolDescription,
		}, true, err
	case eventInteractionUnsupported:
		payload, err := decodePayload[interactionUnsupportedPayload](value.Payload)
		if err != nil {
			return core.Event{}, true, err
		}
		if strings.TrimSpace(payload.RequestID) == "" ||
			strings.TrimSpace(payload.InteractionKind) == "" ||
			strings.TrimSpace(payload.Action) == "" {
			return core.Event{}, true, fmt.Errorf("session-host: invalid unsupported interaction")
		}
		return core.Event{
			Type: core.EventInteractionUnsupported, RequestID: payload.RequestID,
			InteractionKind: payload.InteractionKind, InteractionAction: payload.Action,
			SessionID: value.SessionID,
		}, true, nil
	case eventTurnCompleted:
		payload, err := decodePayload[turnCompletedPayload](value.Payload)
		return core.Event{Type: core.EventResult, Content: payload.Content, Done: payload.Done, SessionID: value.SessionID, InputTokens: payload.InputTokens, OutputTokens: payload.OutputTokens, CacheCreationInputTokens: payload.CacheCreationInputTokens, CacheReadInputTokens: payload.CacheReadInputTokens, Metadata: payload.Metadata}, true, err
	case eventSessionError:
		payload, err := decodePayload[sessionErrorPayload](value.Payload)
		if err != nil {
			return core.Event{}, true, err
		}
		return core.Event{Type: core.EventError, Error: fmt.Errorf("session-host: %s", payload.Message), SessionID: value.SessionID}, true, nil
	default:
		return core.Event{}, false, nil
	}
}

var _ core.AgentSession = (*Session)(nil)
var _ core.InteractionResolutionSource = (*Session)(nil)
