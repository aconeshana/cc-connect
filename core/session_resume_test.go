package core

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

type resumeHostTestSession struct {
	id     string
	events chan Event
	sent   chan string
	mu     sync.Mutex
	closed bool
}

func newResumeHostTestSession(id string) *resumeHostTestSession {
	return &resumeHostTestSession{
		id: id, events: make(chan Event, 32), sent: make(chan string, 8),
	}
}

func (s *resumeHostTestSession) Send(
	prompt, _ string, _ []ImageAttachment, _ []FileAttachment,
) error {
	s.sent <- prompt
	s.events <- Event{Type: EventText, Content: "answer:" + prompt, SessionID: s.id}
	s.events <- Event{Type: EventResult, Content: "answer:" + prompt, Done: true, SessionID: s.id}
	return nil
}

func (s *resumeHostTestSession) RespondPermission(string, PermissionResult) error { return nil }
func (s *resumeHostTestSession) Events() <-chan Event                             { return s.events }
func (s *resumeHostTestSession) CurrentSessionID() string                         { return s.id }
func (s *resumeHostTestSession) Alive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.closed
}
func (s *resumeHostTestSession) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}
func (s *resumeHostTestSession) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

type resumeHostTestAgent struct {
	events        chan HostSessionLifecycle
	collaboration chan HostSessionCollaboration
	bindingTarget string
	sessions      []AgentSessionInfo
	prepared      map[string]*resumeHostTestSession
	gens          map[string]uint64
	resumeErr     error
	started       chan struct{}
	release       chan struct{}

	mu          sync.Mutex
	resumeCalls []string
	startCalls  []string
}

func (a *resumeHostTestAgent) Name() string { return "sessionhost" }
func (a *resumeHostTestAgent) StartSession(_ context.Context, sessionID string) (AgentSession, error) {
	a.mu.Lock()
	a.startCalls = append(a.startCalls, sessionID)
	a.mu.Unlock()
	if session := a.prepared[sessionID]; session != nil {
		return session, nil
	}
	return nil, errors.New("unexpected StartSession")
}
func (a *resumeHostTestAgent) ListSessions(context.Context) ([]AgentSessionInfo, error) {
	return append([]AgentSessionInfo(nil), a.sessions...), nil
}
func (a *resumeHostTestAgent) Stop() error { return nil }
func (a *resumeHostTestAgent) HostSessionEvents() <-chan HostSessionLifecycle {
	return a.events
}
func (a *resumeHostTestAgent) HostSessionBindingTarget() string { return "" }
func (a *resumeHostTestAgent) HostSessionCollaborationEvents() <-chan HostSessionCollaboration {
	return a.collaboration
}
func (a *resumeHostTestAgent) HostSessionBindingTargetFor(channel string) string {
	if channel == "feishu" {
		return a.bindingTarget
	}
	return ""
}
func (a *resumeHostTestAgent) HostSessionCollaborationChannels() []string {
	return []string{"feishu"}
}
func (a *resumeHostTestAgent) ResumeHostSession(
	_ context.Context, sessionID, activationID string,
) (HostSessionActivation, error) {
	a.mu.Lock()
	a.resumeCalls = append(a.resumeCalls, sessionID+":"+activationID)
	a.mu.Unlock()
	if a.started != nil {
		select {
		case a.started <- struct{}{}:
		default:
		}
	}
	if a.release != nil {
		<-a.release
	}
	if a.resumeErr != nil {
		return HostSessionActivation{}, a.resumeErr
	}
	session := a.prepared[sessionID]
	if session == nil {
		return HostSessionActivation{}, errors.New("target session is unavailable")
	}
	return HostSessionActivation{
		Session: session, SessionID: sessionID, ActivationID: activationID,
		ActivationGeneration: a.gens[sessionID],
	}, nil
}
func (a *resumeHostTestAgent) callCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.resumeCalls)
}

func (a *resumeHostTestAgent) startCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.startCalls)
}

func resumeSessionInfo(id, summary string, count int) AgentSessionInfo {
	return AgentSessionInfo{
		ID: id, Summary: summary, MessageCount: count,
		ModifiedAt: time.Date(2026, 8, 17, 12, count, 0, 0, time.UTC),
	}
}

func TestRenderHostResumeCardMarksCurrentAndUsesFullSessionIDActions(t *testing.T) {
	const currentID = "11111111-2222-3333-4444-555555555555"
	const targetID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	agent := &resumeHostTestAgent{
		events: make(chan HostSessionLifecycle),
		sessions: []AgentSessionInfo{
			resumeSessionInfo(currentID, "Current work", 3),
			resumeSessionInfo(targetID, "Earlier work", 9),
		},
	}
	engine := NewEngine("test", agent, nil, "", LangEnglish)
	defer engine.Stop()
	engine.sessions.GetOrCreateActive("feishu:chat:root:thread").
		SetAgentSessionID(currentID, agent.Name())

	card, err := engine.renderHostResumeCard("feishu:chat:root:thread", 1, "", "")
	if err != nil {
		t.Fatalf("renderHostResumeCard: %v", err)
	}
	text := card.RenderText()
	if !strings.Contains(text, "▶") || !strings.Contains(text, "Current work") {
		t.Fatalf("resume card current marker = %q", text)
	}
	buttons := card.CollectButtons()
	var actions []string
	for _, row := range buttons {
		for _, button := range row {
			actions = append(actions, button.Data)
		}
	}
	if !containsString(actions, "act:/resume "+currentID) ||
		!containsString(actions, "act:/resume "+targetID) {
		t.Fatalf("resume card actions = %#v", actions)
	}
}

func TestRenderHostResumeCardPaginatesWithStableFullIDActions(t *testing.T) {
	agent := &resumeHostTestAgent{events: make(chan HostSessionLifecycle)}
	for i := 1; i <= listPageSize+1; i++ {
		agent.sessions = append(agent.sessions, resumeSessionInfo(
			fmt.Sprintf("session-%02d-full-id", i), fmt.Sprintf("Session %02d", i), i))
	}
	engine := NewEngine("test", agent, nil, "", LangEnglish)
	defer engine.Stop()
	key := "feishu:chat:root:thread"
	engine.sessions.GetOrCreateActive(key).
		SetAgentSessionID("session-21-full-id", agent.Name())

	card, err := engine.renderHostResumeCard(key, 2, "", "")
	if err != nil {
		t.Fatalf("renderHostResumeCard page 2: %v", err)
	}
	if text := card.RenderText(); !strings.Contains(text, "2/2") ||
		!strings.Contains(text, "Session 21") || !strings.Contains(text, "▶") {
		t.Fatalf("resume page 2 = %q", text)
	}
	var actions []string
	for _, row := range card.CollectButtons() {
		for _, button := range row {
			actions = append(actions, button.Data)
		}
	}
	if !containsString(actions, "act:/resume session-21-full-id") ||
		!containsString(actions, "nav:/resume 1") {
		t.Fatalf("resume page 2 actions = %#v", actions)
	}
}

func TestSessionHostResumeCommandMatchesNumberNameAndIDPrefix(t *testing.T) {
	selectors := map[string][]string{
		"number":    {"2"},
		"name":      {"Archived", "Work"},
		"id-prefix": {"target-session"},
	}
	for name, selector := range selectors {
		t.Run(name, func(t *testing.T) {
			const (
				key      = "feishu:chat:root:thread"
				oldID    = "old-session-full-id"
				targetID = "target-session-full-id"
			)
			platform := &stubTrackableCardPlatform{stubCardPlatform: stubCardPlatform{
				stubPlatformEngine: stubPlatformEngine{n: "feishu"},
			}}
			oldRemote := newResumeHostTestSession(oldID)
			targetRemote := newResumeHostTestSession(targetID)
			agent := &resumeHostTestAgent{
				events: make(chan HostSessionLifecycle),
				sessions: []AgentSessionInfo{
					resumeSessionInfo(oldID, "Current", 2),
					resumeSessionInfo(targetID, "Archived summary", 11),
				},
				prepared: map[string]*resumeHostTestSession{targetID: targetRemote},
				gens:     map[string]uint64{targetID: 7},
			}
			engine := NewEngine("test", agent, []Platform{platform}, "", LangEnglish)
			defer engine.Stop()
			current := engine.sessions.GetOrCreateActive(key)
			current.SetAgentSessionID(oldID, agent.Name())
			targetLocal := engine.sessions.NewSession(key, "Archived Work")
			targetLocal.SetAgentSessionID(targetID, agent.Name())
			engine.sessions.SwitchToAgentSession(key, oldID, agent.Name(), "Current")
			engine.sessions.SetSessionName(targetID, "Archived Work")
			engine.interactiveStates[key] = &interactiveState{
				agentSession: oldRemote, platform: platform, replyCtx: "thread",
				eventsNeedResync: false,
			}

			engine.cmdResume(platform, &Message{
				SessionKey: key, Platform: "feishu", ReplyCtx: "thread",
			}, selector)

			if got := engine.sessions.GetOrCreateActive(key).GetAgentSessionID(); got != targetID {
				t.Fatalf("/resume %v selected %q", selector, got)
			}
			if agent.callCount() != 1 {
				t.Fatalf("/resume %v host calls = %d", selector, agent.callCount())
			}
			platform.mu.Lock()
			cards := append([]*Card(nil), platform.repliedCards...)
			platform.mu.Unlock()
			if len(cards) != 1 || !strings.Contains(cards[0].RenderText(), "Resumed") {
				t.Fatalf("/resume %v result cards = %#v", selector, cards)
			}
		})
	}
}

func TestSessionHostListReusesResumeCardAndSwitchIsDisabled(t *testing.T) {
	const currentID = "session-current"
	platform := &stubTrackableCardPlatform{stubCardPlatform: stubCardPlatform{
		stubPlatformEngine: stubPlatformEngine{n: "feishu"},
	}}
	agent := &resumeHostTestAgent{
		events:   make(chan HostSessionLifecycle),
		sessions: []AgentSessionInfo{resumeSessionInfo(currentID, "Current", 1)},
	}
	engine := NewEngine("test", agent, []Platform{platform}, "", LangEnglish)
	defer engine.Stop()
	key := "feishu:chat:root:thread"
	engine.sessions.GetOrCreateActive(key).SetAgentSessionID(currentID, agent.Name())
	msg := &Message{SessionKey: key, ReplyCtx: "thread", Platform: "feishu"}

	engine.cmdList(platform, msg, nil)
	platform.mu.Lock()
	if len(platform.sentCards) != 1 {
		platform.mu.Unlock()
		t.Fatalf("tracked resume cards = %d, want 1", len(platform.sentCards))
	}
	listCard := platform.sentCards[0]
	platform.mu.Unlock()
	if !strings.Contains(listCard.RenderText(), "Current") {
		t.Fatalf("/list resume card = %q", listCard.RenderText())
	}

	engine.cmdSwitch(platform, msg, []string{"1"})
	if got := strings.Join(platform.getSent(), "\n"); !strings.Contains(got, "/resume") {
		t.Fatalf("Session Host /switch guidance = %q", got)
	}
	if agent.callCount() != 0 {
		t.Fatalf("/switch invoked resume %d times", agent.callCount())
	}
	help := engine.renderHelpCard().RenderText()
	if !strings.Contains(help, "/resume") || strings.Contains(help, "/switch") {
		t.Fatalf("Session Host help = %q", help)
	}
	for _, command := range engine.GetAllCommands() {
		if command.Command == "switch" {
			t.Fatal("Session Host bot menu still exposes /switch")
		}
	}
}

func TestResumeHostSessionCommitsSameThreadAndPreservesOldHistory(t *testing.T) {
	const key = "feishu:chat:root:thread"
	oldRemote := newResumeHostTestSession("session-old")
	newRemote := newResumeHostTestSession("session-new")
	target := resumeSessionInfo("session-new", "New target", 12)
	agent := &resumeHostTestAgent{
		events: make(chan HostSessionLifecycle), sessions: []AgentSessionInfo{target},
		prepared: map[string]*resumeHostTestSession{"session-new": newRemote},
		gens:     map[string]uint64{"session-new": 8},
	}
	engine := NewEngine("test", agent, nil, "", LangEnglish)
	defer engine.Stop()
	oldLocal := engine.sessions.GetOrCreateActive(key)
	oldLocal.SetAgentSessionID("session-old", agent.Name())
	oldLocal.AddHistory("assistant", "old history remains")
	state := &interactiveState{agentSession: oldRemote, eventsNeedResync: false}
	engine.interactiveStates[key] = state

	outcome, err := engine.resumeHostSession(key, target)
	if err != nil {
		t.Fatalf("resumeHostSession: %v", err)
	}
	if outcome.Session.ID != "session-new" || outcome.AlreadyActive || outcome.ActivationGeneration != 8 {
		t.Fatalf("resume outcome = %#v", outcome)
	}
	if got := engine.sessions.GetOrCreateActive(key).GetAgentSessionID(); got != "session-new" {
		t.Fatalf("thread active host session = %q", got)
	}
	if history := oldLocal.GetHistory(10); len(history) != 1 || history[0].Content != "old history remains" {
		t.Fatalf("old history = %#v", history)
	}
	state.mu.Lock()
	activeRemote := state.agentSession
	generation := state.sessionHostActivationGeneration
	state.mu.Unlock()
	if activeRemote != newRemote || generation != 8 {
		t.Fatalf("live state session=%T generation=%d", activeRemote, generation)
	}
	if !oldRemote.isClosed() {
		t.Fatal("old Session Link attachment was not detached after commit")
	}
	select {
	case prompt := <-newRemote.sent:
		t.Fatalf("resume unexpectedly generated a model prompt: %q", prompt)
	default:
	}
}

func TestResumeHostSessionRejectsBusyTurnWithoutChangingState(t *testing.T) {
	const key = "feishu:chat:root:thread"
	oldRemote := newResumeHostTestSession("session-old")
	newRemote := newResumeHostTestSession("session-new")
	target := resumeSessionInfo("session-new", "New target", 2)
	agent := &resumeHostTestAgent{
		events: make(chan HostSessionLifecycle), sessions: []AgentSessionInfo{target},
		prepared: map[string]*resumeHostTestSession{"session-new": newRemote},
		gens:     map[string]uint64{"session-new": 2},
	}
	engine := NewEngine("test", agent, nil, "", LangEnglish)
	defer engine.Stop()
	oldLocal := engine.sessions.GetOrCreateActive(key)
	oldLocal.SetAgentSessionID("session-old", agent.Name())
	if !oldLocal.TryLock() {
		t.Fatal("failed to simulate active turn")
	}
	defer oldLocal.Unlock()
	engine.interactiveStates[key] = &interactiveState{agentSession: oldRemote}

	if _, err := engine.resumeHostSession(key, target); err == nil {
		t.Fatal("busy resume unexpectedly succeeded")
	}
	if agent.callCount() != 0 {
		t.Fatalf("busy resume called host %d times", agent.callCount())
	}
	if got := engine.sessions.GetOrCreateActive(key).GetAgentSessionID(); got != "session-old" {
		t.Fatalf("busy resume changed active session to %q", got)
	}
	if oldRemote.isClosed() || newRemote.isClosed() {
		t.Fatal("busy resume detached an attachment")
	}
}

func TestResumeHostSessionRejectsStaleGenerationAndKeepsWinner(t *testing.T) {
	const key = "feishu:chat:root:thread"
	oldRemote := newResumeHostTestSession("session-winner")
	staleRemote := newResumeHostTestSession("session-stale")
	target := resumeSessionInfo("session-stale", "Stale target", 1)
	agent := &resumeHostTestAgent{
		events: make(chan HostSessionLifecycle), sessions: []AgentSessionInfo{target},
		prepared: map[string]*resumeHostTestSession{"session-stale": staleRemote},
		gens:     map[string]uint64{"session-stale": 4},
	}
	engine := NewEngine("test", agent, nil, "", LangEnglish)
	defer engine.Stop()
	engine.sessions.GetOrCreateActive(key).SetAgentSessionID("session-winner", agent.Name())
	engine.interactiveStates[key] = &interactiveState{
		agentSession: oldRemote, sessionHostActivationGeneration: 5,
	}

	if _, err := engine.resumeHostSession(key, target); err == nil {
		t.Fatal("stale resume unexpectedly succeeded")
	}
	if got := engine.sessions.GetOrCreateActive(key).GetAgentSessionID(); got != "session-winner" {
		t.Fatalf("stale resume changed active session to %q", got)
	}
	if !staleRemote.isClosed() || oldRemote.isClosed() {
		t.Fatalf("stale attachment closed=%v winner closed=%v",
			staleRemote.isClosed(), oldRemote.isClosed())
	}
}

func TestResumeHostSessionTransfersMessagesQueuedDuringActivation(t *testing.T) {
	const key = "feishu:chat:root:thread"
	platform := &stubPlatformEngine{n: "feishu"}
	oldRemote := newResumeHostTestSession("session-old")
	newRemote := newResumeHostTestSession("session-new")
	target := resumeSessionInfo("session-new", "New target", 4)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	agent := &resumeHostTestAgent{
		events: make(chan HostSessionLifecycle), sessions: []AgentSessionInfo{target},
		prepared: map[string]*resumeHostTestSession{"session-new": newRemote},
		gens:     map[string]uint64{"session-new": 6}, started: started, release: release,
	}
	engine := NewEngine("test", agent, []Platform{platform}, "", LangEnglish)
	defer engine.Stop()
	oldLocal := engine.sessions.GetOrCreateActive(key)
	oldLocal.SetAgentSessionID("session-old", agent.Name())
	engine.interactiveStates[key] = &interactiveState{
		agentSession: oldRemote, platform: platform, replyCtx: "thread", eventsNeedResync: false,
	}

	done := make(chan error, 1)
	go func() {
		_, err := engine.resumeHostSession(key, target)
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("resume did not reach host activation")
	}
	engine.handleMessage(platform, &Message{
		SessionKey: key, Platform: "feishu", MessageID: "queued-1", UserID: "user-1",
		UserName: "Alice", Content: "queued while resuming", ReplyCtx: "thread",
	})
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("resumeHostSession: %v", err)
	}
	select {
	case prompt := <-newRemote.sent:
		if !strings.Contains(prompt, "queued while resuming") {
			t.Fatalf("queued prompt = %q", prompt)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("queued message was not transferred to resumed session")
	}
}

func TestResumeHostSessionDoubleClickSameTargetIsIdempotent(t *testing.T) {
	const key = "feishu:chat:root:thread"
	oldRemote := newResumeHostTestSession("session-old")
	targetRemote := newResumeHostTestSession("session-target")
	target := resumeSessionInfo("session-target", "Target", 5)
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	agent := &resumeHostTestAgent{
		events: make(chan HostSessionLifecycle), sessions: []AgentSessionInfo{target},
		prepared: map[string]*resumeHostTestSession{"session-target": targetRemote},
		gens:     map[string]uint64{"session-target": 9}, started: started, release: release,
	}
	engine := NewEngine("test", agent, nil, "", LangEnglish)
	defer engine.Stop()
	engine.sessions.GetOrCreateActive(key).SetAgentSessionID("session-old", agent.Name())
	engine.interactiveStates[key] = &interactiveState{agentSession: oldRemote}

	type result struct {
		outcome hostResumeOutcome
		err     error
	}
	results := make(chan result, 2)
	go func() {
		outcome, err := engine.resumeHostSession(key, target)
		results <- result{outcome: outcome, err: err}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first resume did not reach host activation")
	}
	go func() {
		outcome, err := engine.resumeHostSession(key, target)
		results <- result{outcome: outcome, err: err}
	}()
	close(release)

	alreadyActive := 0
	for i := 0; i < 2; i++ {
		select {
		case got := <-results:
			if got.err != nil {
				t.Fatalf("double resume result %d: %v", i, got.err)
			}
			if got.outcome.AlreadyActive {
				alreadyActive++
			}
		case <-time.After(3 * time.Second):
			t.Fatal("double resume did not finish")
		}
	}
	if agent.callCount() != 1 || alreadyActive != 1 {
		t.Fatalf("double resume calls=%d idempotent_results=%d", agent.callCount(), alreadyActive)
	}
	if got := engine.sessions.GetOrCreateActive(key).GetAgentSessionID(); got != target.ID {
		t.Fatalf("double resume final session = %q", got)
	}
}

func TestResumeHostSessionSerializesDifferentTargetsByGeneration(t *testing.T) {
	const key = "feishu:chat:root:thread"
	oldRemote := newResumeHostTestSession("session-old")
	firstRemote := newResumeHostTestSession("session-first")
	secondRemote := newResumeHostTestSession("session-second")
	first := resumeSessionInfo("session-first", "First", 3)
	second := resumeSessionInfo("session-second", "Second", 4)
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	agent := &resumeHostTestAgent{
		events:   make(chan HostSessionLifecycle),
		sessions: []AgentSessionInfo{first, second},
		prepared: map[string]*resumeHostTestSession{
			first.ID: firstRemote, second.ID: secondRemote,
		},
		gens:    map[string]uint64{first.ID: 10, second.ID: 11},
		started: started, release: release,
	}
	engine := NewEngine("test", agent, nil, "", LangEnglish)
	defer engine.Stop()
	engine.sessions.GetOrCreateActive(key).SetAgentSessionID("session-old", agent.Name())
	state := &interactiveState{agentSession: oldRemote}
	engine.interactiveStates[key] = state

	done := make(chan error, 2)
	go func() {
		_, err := engine.resumeHostSession(key, first)
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first target did not reach host activation")
	}
	go func() {
		_, err := engine.resumeHostSession(key, second)
		done <- err
	}()
	close(release)
	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("serialized resume %d: %v", i, err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("serialized resumes did not finish")
		}
	}
	if got := engine.sessions.GetOrCreateActive(key).GetAgentSessionID(); got != second.ID {
		t.Fatalf("serialized resume final session = %q", got)
	}
	state.mu.Lock()
	remote, generation := state.agentSession, state.sessionHostActivationGeneration
	state.mu.Unlock()
	if remote != secondRemote || generation != 11 || agent.callCount() != 2 {
		t.Fatalf("serialized resume remote=%T generation=%d calls=%d",
			remote, generation, agent.callCount())
	}
	if !oldRemote.isClosed() || !firstRemote.isClosed() || secondRemote.isClosed() {
		t.Fatalf("attachment lifecycle old=%v first=%v second=%v",
			oldRemote.isClosed(), firstRemote.isClosed(), secondRemote.isClosed())
	}
}

func TestHostResumeCardActionLoadsThenUpdatesClickedAndLatestCards(t *testing.T) {
	const key = "feishu:chat:root:thread"
	platform := &stubTrackableCardPlatform{stubCardPlatform: stubCardPlatform{
		stubPlatformEngine: stubPlatformEngine{n: "feishu"},
	}}
	oldRemote := newResumeHostTestSession("session-old")
	newRemote := newResumeHostTestSession("session-new")
	target := resumeSessionInfo("session-new", "Target session", 14)
	agent := &resumeHostTestAgent{
		events: make(chan HostSessionLifecycle),
		sessions: []AgentSessionInfo{
			resumeSessionInfo("session-old", "Current session", 2), target,
		},
		prepared: map[string]*resumeHostTestSession{"session-new": newRemote},
		gens:     map[string]uint64{"session-new": 12},
	}
	engine := NewEngine("test", agent, []Platform{platform}, "", LangEnglish)
	defer engine.Stop()
	engine.sessions.GetOrCreateActive(key).SetAgentSessionID("session-old", agent.Name())
	engine.interactiveStates[key] = &interactiveState{
		agentSession: oldRemote, platform: platform, replyCtx: "thread", eventsNeedResync: false,
	}
	engine.sendHostResumeCard(platform, "thread", key, 1)

	result := engine.handleCardNavLocal("act:/resume session-new", key)
	if result == nil || !strings.Contains(result.RenderText(), "Resumed") {
		t.Fatalf("final resume card = %#v", result)
	}
	if !result.HasNoteTag(ResumeCardOwnerUpdatedTag) {
		t.Fatal("resume result did not mark the exact source card as already updated")
	}
	waitForCondition(t, 3*time.Second, func() bool {
		platform.mu.Lock()
		defer platform.mu.Unlock()
		return len(platform.refreshedCards) >= 2 && len(platform.updatedCards) >= 2 &&
			engine.sessions.GetOrCreateActive(key).GetAgentSessionID() == "session-new"
	}, "resume card did not converge after activation")
	platform.mu.Lock()
	loading := platform.refreshedCards[0]
	clicked := platform.refreshedCards[len(platform.refreshedCards)-1]
	latest := platform.updatedCards[len(platform.updatedCards)-1]
	platform.mu.Unlock()
	if !strings.Contains(loading.RenderText(), "Switching") ||
		!strings.Contains(clicked.RenderText(), "Resumed") ||
		!strings.Contains(latest.RenderText(), "Target session") ||
		!strings.Contains(latest.RenderText(), "▶") {
		t.Fatalf("clicked=%q latest=%q", clicked.RenderText(), latest.RenderText())
	}
	if sent := strings.Join(platform.getSent(), "\n"); strings.Contains(sent, "Resumed in TUI") {
		t.Fatalf("remote resume emitted duplicate terminal notice: %q", sent)
	}
}

func TestResumeFeedbackRejectsACompletionSupersededByNewerGeneration(t *testing.T) {
	const key = "feishu:chat:root:thread"
	agent := &resumeHostTestAgent{events: make(chan HostSessionLifecycle)}
	engine := NewEngine("test", agent, nil, "", LangEnglish)
	defer engine.Stop()
	engine.sessions.GetOrCreateActive(key).SetAgentSessionID("session-winner", agent.Name())
	engine.interactiveStates[key] = &interactiveState{
		agentSession:                    newResumeHostTestSession("session-winner"),
		sessionHostActivationGeneration: 22,
	}

	err := engine.fenceHostResumeFeedback(key, hostResumeOutcome{
		Session:              resumeSessionInfo("session-stale", "Stale", 3),
		ActivationGeneration: 21,
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "superseded") {
		t.Fatalf("stale feedback fence error = %v", err)
	}
}

func TestLocalTerminalResumeNotifiesThreadAndRefreshesLatestResumeCard(t *testing.T) {
	const key = "feishu:chat:root:thread"
	platform := &stubTrackableCardPlatform{stubCardPlatform: stubCardPlatform{
		stubPlatformEngine: stubPlatformEngine{n: "feishu"},
	}}
	currentRemote := newResumeHostTestSession("session-current")
	targetRemote := newResumeHostTestSession("session-target")
	agent := &resumeHostTestAgent{
		events: make(chan HostSessionLifecycle, 2), bindingTarget: key,
		sessions: []AgentSessionInfo{
			resumeSessionInfo("session-current", "Current", 3),
			resumeSessionInfo("session-target", "Target", 21),
		},
		prepared: map[string]*resumeHostTestSession{"session-target": targetRemote},
	}
	engine := NewEngine("test", agent, []Platform{platform}, "", LangEnglish)
	defer engine.Stop()
	targetLocal := engine.sessions.GetOrCreateActive(key)
	targetLocal.SetAgentSessionID("session-target", agent.Name())
	targetLocal.SetCollaborationChannel("feishu")
	currentLocal := engine.sessions.NewSession(key, "")
	currentLocal.SetAgentSessionID("session-current", agent.Name())
	currentLocal.SetCollaborationChannel("feishu")
	engine.sessions.Save()
	engine.interactiveStates[key] = &interactiveState{
		agentSession: currentRemote, platform: platform, replyCtx: "thread",
		eventsNeedResync: false, sessionHostActivationGeneration: 19,
	}
	engine.sendHostResumeCard(platform, "thread", key, 1)
	engine.StartHostSessionCoordinator(agent)

	agent.events <- HostSessionLifecycle{
		SessionID: "session-target", Origin: "local", MessageCount: 21,
		ActivationGeneration: 20,
	}
	waitForCondition(t, 3*time.Second, func() bool {
		return engine.sessions.GetOrCreateActive(key).GetAgentSessionID() == "session-target" &&
			strings.Contains(strings.Join(platform.getSent(), "\n"), "Resumed in TUI")
	}, "local terminal resume did not notify and switch the thread")
	waitForCondition(t, 3*time.Second, func() bool {
		platform.mu.Lock()
		defer platform.mu.Unlock()
		return len(platform.updatedCards) > 0
	}, "local terminal resume did not refresh the latest resume card")
	platform.mu.Lock()
	updated := platform.updatedCards[len(platform.updatedCards)-1]
	platform.mu.Unlock()
	if !strings.Contains(updated.RenderText(), "Target") || !strings.Contains(updated.RenderText(), "▶") {
		t.Fatalf("refreshed resume card = %q", updated.RenderText())
	}
}

func TestLocalTerminalResumeCarriesCurrentThreadToPreviouslyUnboundSession(t *testing.T) {
	const (
		key      = "feishu:chat:root:current-thread"
		oldID    = "session-before-local-resume"
		targetID = "session-never-bound-to-im"
	)
	platform := &stubTrackableCardPlatform{stubCardPlatform: stubCardPlatform{
		stubPlatformEngine: stubPlatformEngine{n: "feishu"},
	}}
	currentRemote := newResumeHostTestSession(oldID)
	targetRemote := newResumeHostTestSession(targetID)
	agent := &resumeHostTestAgent{
		events: make(chan HostSessionLifecycle, 1), bindingTarget: "feishu:chat:owner",
		sessions: []AgentSessionInfo{
			resumeSessionInfo(oldID, "Current", 5),
			resumeSessionInfo(targetID, "Target", 9),
		},
		prepared: map[string]*resumeHostTestSession{targetID: targetRemote},
	}
	engine := NewEngine("test", agent, []Platform{platform}, "", LangEnglish)
	defer engine.Stop()
	currentLocal := engine.sessions.GetOrCreateActive(key)
	currentLocal.SetAgentSessionID(oldID, agent.Name())
	currentLocal.SetCollaborationChannel("feishu")
	engine.sessions.Save()
	engine.interactiveStates[key] = &interactiveState{
		agentSession: currentRemote, platform: platform, replyCtx: "current-thread",
		eventsNeedResync: false, sessionHostActivationGeneration: 40,
		sessionHostRouteKey: key,
	}
	engine.StartHostSessionCoordinator(agent)

	agent.events <- HostSessionLifecycle{
		SessionID: targetID, PreviousSessionID: oldID, Origin: "local",
		MessageCount: 9, ActivationGeneration: 41,
	}
	waitForCondition(t, 3*time.Second, func() bool {
		state := engine.interactiveState(key)
		if state == nil {
			return false
		}
		state.mu.Lock()
		defer state.mu.Unlock()
		return engine.sessions.GetOrCreateActive(key).GetAgentSessionID() == targetID &&
			state.agentSession != nil && state.agentSession.CurrentSessionID() == targetID &&
			strings.Contains(strings.Join(platform.getSent(), "\n"), "Resumed in TUI")
	}, "local resume did not carry the current thread to an unbound target session")
	if got := engine.findBoundHostSessionKey(targetID); got != key {
		t.Fatalf("target session bound key = %q, want current thread %q", got, key)
	}
}

func TestLocalTerminalResumePrefersCurrentThreadOverTargetsHistoricalThread(t *testing.T) {
	const (
		currentKey = "feishu:chat:root:current-thread"
		historyKey = "feishu:chat:root:historical-thread"
		oldID      = "session-current"
		targetID   = "session-with-historical-thread"
	)
	platform := &stubTrackableCardPlatform{stubCardPlatform: stubCardPlatform{
		stubPlatformEngine: stubPlatformEngine{n: "feishu"},
	}}
	currentRemote := newResumeHostTestSession(oldID)
	targetRemote := newResumeHostTestSession(targetID)
	agent := &resumeHostTestAgent{
		events: make(chan HostSessionLifecycle, 1), bindingTarget: "feishu:chat:owner",
		sessions: []AgentSessionInfo{
			resumeSessionInfo(oldID, "Current", 5),
			resumeSessionInfo(targetID, "Historical", 12),
		},
		prepared: map[string]*resumeHostTestSession{targetID: targetRemote},
	}
	engine := NewEngine("test", agent, []Platform{platform}, "", LangEnglish)
	defer engine.Stop()
	currentLocal := engine.sessions.GetOrCreateActive(currentKey)
	currentLocal.SetAgentSessionID(oldID, agent.Name())
	currentLocal.SetCollaborationChannel("feishu")
	historicalLocal := engine.sessions.GetOrCreateActive(historyKey)
	historicalLocal.SetAgentSessionID(targetID, agent.Name())
	historicalLocal.SetCollaborationChannel("feishu")
	engine.sessions.Save()
	engine.interactiveStates[currentKey] = &interactiveState{
		agentSession: currentRemote, platform: platform, replyCtx: "current-thread",
		eventsNeedResync: false, sessionHostActivationGeneration: 50,
		sessionHostRouteKey: currentKey,
	}
	engine.StartHostSessionCoordinator(agent)

	agent.events <- HostSessionLifecycle{
		SessionID: targetID, PreviousSessionID: oldID, Origin: "local",
		MessageCount: 12, ActivationGeneration: 51,
	}
	waitForCondition(t, 3*time.Second, func() bool {
		state := engine.interactiveState(currentKey)
		if state == nil {
			return false
		}
		state.mu.Lock()
		defer state.mu.Unlock()
		return state.agentSession != nil && state.agentSession.CurrentSessionID() == targetID
	}, "local resume did not remain on the current collaboration thread")
	if got := engine.findBoundHostSessionKey(targetID); got != currentKey {
		t.Fatalf("target resumed into %q, want current thread %q", got, currentKey)
	}
}

func TestStaleLocalActivationCannotOverwriteNewerThreadGeneration(t *testing.T) {
	const key = "feishu:chat:root:thread"
	platform := &stubTrackableCardPlatform{stubCardPlatform: stubCardPlatform{
		stubPlatformEngine: stubPlatformEngine{n: "feishu"},
	}}
	winnerRemote := newResumeHostTestSession("session-winner")
	staleRemote := newResumeHostTestSession("session-stale")
	agent := &resumeHostTestAgent{
		events: make(chan HostSessionLifecycle, 1), bindingTarget: key,
		sessions: []AgentSessionInfo{
			resumeSessionInfo("session-winner", "Winner", 8),
			resumeSessionInfo("session-stale", "Stale", 4),
		},
		prepared: map[string]*resumeHostTestSession{"session-stale": staleRemote},
	}
	engine := NewEngine("test", agent, []Platform{platform}, "", LangEnglish)
	defer engine.Stop()
	staleLocal := engine.sessions.GetOrCreateActive(key)
	staleLocal.SetAgentSessionID("session-stale", agent.Name())
	staleLocal.SetCollaborationChannel("feishu")
	winnerLocal := engine.sessions.NewSession(key, "")
	winnerLocal.SetAgentSessionID("session-winner", agent.Name())
	winnerLocal.SetCollaborationChannel("feishu")
	engine.sessions.Save()
	engine.interactiveStates[key] = &interactiveState{
		agentSession: winnerRemote, platform: platform, replyCtx: "thread",
		eventsNeedResync: false, sessionHostActivationGeneration: 30,
	}
	engine.StartHostSessionCoordinator(agent)
	agent.events <- HostSessionLifecycle{
		SessionID: "session-stale", Origin: "local", MessageCount: 4,
		ActivationGeneration: 29,
	}
	time.Sleep(100 * time.Millisecond)

	if got := engine.sessions.GetOrCreateActive(key).GetAgentSessionID(); got != "session-winner" {
		t.Fatalf("stale local activation changed active session to %q", got)
	}
	if agent.startCount() != 0 {
		t.Fatalf("stale local activation opened %d attachments", agent.startCount())
	}
	if sent := platform.getSent(); len(sent) != 0 {
		t.Fatalf("stale local activation sent notifications: %#v", sent)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
