package core

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type hostLifecycleTestAgent struct {
	stubAgent
	events        chan HostSessionLifecycle
	collaboration chan HostSessionCollaboration
}

func (a *hostLifecycleTestAgent) HostSessionEvents() <-chan HostSessionLifecycle {
	return a.events
}

func (a *hostLifecycleTestAgent) HostSessionBindingTarget() string {
	return "test:chat:user"
}
func (a *hostLifecycleTestAgent) HostSessionCollaborationEvents() <-chan HostSessionCollaboration {
	return a.collaboration
}
func (a *hostLifecycleTestAgent) HostSessionBindingTargetFor(channel string) string {
	if channel == "test" {
		return "test:chat:user"
	}
	return ""
}
func (a *hostLifecycleTestAgent) HostSessionCollaborationChannels() []string {
	return []string{"test"}
}

type inboundHostTestAgent struct {
	events       chan HostSessionLifecycle
	session      *inboundHostTestSession
	platform     *hostThreadTestPlatform
	mu           sync.Mutex
	startID      string
	boundAtStart bool
	env          []string
}

func (a *inboundHostTestAgent) Name() string { return "sessionhost" }

func (a *inboundHostTestAgent) StartSession(_ context.Context, sessionID string) (AgentSession, error) {
	a.mu.Lock()
	a.startID = sessionID
	a.boundAtStart = a.platform.callCount() == 1
	a.mu.Unlock()
	return a.session, nil
}

func (a *inboundHostTestAgent) ListSessions(context.Context) ([]AgentSessionInfo, error) {
	return nil, nil
}

func (a *inboundHostTestAgent) Stop() error { return nil }

func (a *inboundHostTestAgent) HostSessionEvents() <-chan HostSessionLifecycle {
	return a.events
}

func (a *inboundHostTestAgent) HostSessionBindingTarget() string { return "" }

func (a *inboundHostTestAgent) SetSessionEnv(env []string) {
	a.mu.Lock()
	a.env = append([]string(nil), env...)
	a.mu.Unlock()
}

func (a *inboundHostTestAgent) snapshot() (string, bool, []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.startID, a.boundAtStart, append([]string(nil), a.env...)
}

type inboundHostTestSession struct {
	events chan Event
	sent   chan string
}

func newInboundHostTestSession() *inboundHostTestSession {
	return &inboundHostTestSession{events: make(chan Event, 1), sent: make(chan string, 1)}
}

func (s *inboundHostTestSession) Send(prompt, _ string, _ []ImageAttachment, _ []FileAttachment) error {
	s.sent <- prompt
	s.events <- Event{Type: EventResult, Content: "done", Done: true, SessionID: "java-session"}
	return nil
}

func (s *inboundHostTestSession) RespondPermission(string, PermissionResult) error { return nil }
func (s *inboundHostTestSession) Events() <-chan Event                             { return s.events }
func (s *inboundHostTestSession) CurrentSessionID() string                         { return "java-session" }
func (s *inboundHostTestSession) Alive() bool                                      { return true }
func (s *inboundHostTestSession) Close() error                                     { return nil }

type hostThreadTestPlatform struct {
	stubPlatformEngine
	mu       sync.Mutex
	calls    int
	baseKey  string
	session  string
	title    string
	boundKey string
	replyCtx any
	updated  string
}

type hostThreadE2EPlatform struct {
	stubTrackableCardPlatform
	mu          sync.Mutex
	created     int
	restored    int
	boundKey    string
	boundCtx    any
	lastBaseKey string
}

type hostProgressThreadE2EPlatform struct {
	hostThreadE2EPlatform
	previewMu     sync.Mutex
	previewStarts []string
	previewEdits  []string
}

func (p *hostProgressThreadE2EPlatform) ProgressStyle() string { return "card" }
func (p *hostProgressThreadE2EPlatform) SupportsProgressCardPayload() bool {
	return true
}
func (p *hostProgressThreadE2EPlatform) SendPreviewStart(
	_ context.Context, _ any, content string,
) (any, error) {
	p.previewMu.Lock()
	p.previewStarts = append(p.previewStarts, content)
	p.previewMu.Unlock()
	return "host-progress-card", nil
}
func (p *hostProgressThreadE2EPlatform) UpdateMessage(
	_ context.Context, _ any, content string,
) error {
	p.previewMu.Lock()
	p.previewEdits = append(p.previewEdits, content)
	p.previewMu.Unlock()
	return nil
}
func (p *hostProgressThreadE2EPlatform) progressSnapshots() ([]string, []string) {
	p.previewMu.Lock()
	defer p.previewMu.Unlock()
	return append([]string(nil), p.previewStarts...), append([]string(nil), p.previewEdits...)
}

func (p *hostThreadE2EPlatform) BindSessionThread(
	_ context.Context, baseSessionKey, _ string, _ string,
) (string, any, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastBaseKey = baseSessionKey
	if baseSessionKey == p.boundKey {
		p.restored++
		return p.boundKey, p.boundCtx, nil
	}
	p.created++
	return p.boundKey, p.boundCtx, nil
}

func (p *hostThreadE2EPlatform) bindingCounts() (int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.created, p.restored
}

type hostThreadE2ESession struct {
	id          string
	events      chan Event
	resolutions chan InteractionResolution
	sent        chan string
}

func newHostThreadE2ESession(id string) *hostThreadE2ESession {
	return &hostThreadE2ESession{
		id: id, events: make(chan Event, 64), resolutions: make(chan InteractionResolution, 8),
		sent: make(chan string, 8),
	}
}

func (s *hostThreadE2ESession) Send(prompt, _ string, _ []ImageAttachment, _ []FileAttachment) error {
	s.sent <- prompt
	s.events <- Event{Type: EventUserInput, Content: prompt, SessionID: s.id}
	s.events <- Event{Type: EventText, Content: "remote:" + prompt, SessionID: s.id}
	s.events <- Event{Type: EventResult, Content: "remote:" + prompt, Done: true, SessionID: s.id}
	return nil
}

func (s *hostThreadE2ESession) RespondPermission(string, PermissionResult) error { return nil }
func (s *hostThreadE2ESession) Events() <-chan Event                             { return s.events }
func (s *hostThreadE2ESession) InteractionResolutions() <-chan InteractionResolution {
	return s.resolutions
}
func (s *hostThreadE2ESession) CurrentSessionID() string { return s.id }
func (s *hostThreadE2ESession) Alive() bool              { return true }
func (s *hostThreadE2ESession) Close() error             { return nil }

type hostThreadE2EAgent struct {
	events        chan HostSessionLifecycle
	collaboration chan HostSessionCollaboration
	session       *hostThreadE2ESession
	mu            sync.Mutex
	starts        []string
}

func (a *hostThreadE2EAgent) Name() string { return "sessionhost" }
func (a *hostThreadE2EAgent) StartSession(_ context.Context, sessionID string) (AgentSession, error) {
	a.mu.Lock()
	a.starts = append(a.starts, sessionID)
	a.mu.Unlock()
	return a.session, nil
}
func (a *hostThreadE2EAgent) ListSessions(context.Context) ([]AgentSessionInfo, error) {
	return nil, nil
}
func (a *hostThreadE2EAgent) Stop() error { return nil }
func (a *hostThreadE2EAgent) HostSessionEvents() <-chan HostSessionLifecycle {
	return a.events
}
func (a *hostThreadE2EAgent) HostSessionBindingTarget() string {
	return "feishu:oc_project:ou_owner"
}
func (a *hostThreadE2EAgent) HostSessionCollaborationEvents() <-chan HostSessionCollaboration {
	return a.collaboration
}
func (a *hostThreadE2EAgent) HostSessionBindingTargetFor(channel string) string {
	if channel == "feishu" {
		return "feishu:oc_project:ou_owner"
	}
	return ""
}
func (a *hostThreadE2EAgent) HostSessionCollaborationChannels() []string {
	return []string{"feishu"}
}
func (a *hostThreadE2EAgent) startIDs() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.starts...)
}

func (p *hostThreadTestPlatform) BindSessionThread(
	_ context.Context, baseSessionKey, sessionID, title string,
) (string, any, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	p.baseKey = baseSessionKey
	p.session = sessionID
	p.title = title
	return p.boundKey, p.replyCtx, nil
}

func (p *hostThreadTestPlatform) CreateSessionThread(
	ctx context.Context, baseSessionKey, title string,
) (string, any, error) {
	return p.BindSessionThread(ctx, baseSessionKey, "", title)
}

func (p *hostThreadTestPlatform) UpdateSessionThreadTitle(
	_ context.Context, sessionKey, sessionID, title string,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.baseKey = sessionKey
	p.session = sessionID
	p.updated = title
	return nil
}

func (p *hostThreadTestPlatform) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func TestStartHostSessionCoordinatorIgnoresRemoteActivation(t *testing.T) {
	platform := &hostThreadTestPlatform{
		stubPlatformEngine: stubPlatformEngine{n: "test"},
		boundKey:           "test:chat:root:created",
		replyCtx:           "thread-context",
	}
	agent := &hostLifecycleTestAgent{
		events:        make(chan HostSessionLifecycle, 1),
		collaboration: make(chan HostSessionCollaboration, 1),
	}
	engine := NewEngine("test", agent, []Platform{platform}, "", LangEnglish)
	defer engine.cancel()

	engine.StartHostSessionCoordinator(agent)
	agent.events <- HostSessionLifecycle{SessionID: "remote-session", Origin: "remote"}
	time.Sleep(50 * time.Millisecond)

	if got := platform.callCount(); got != 0 {
		t.Fatalf("remote activation created %d thread bindings, want 0", got)
	}
}

func TestHostSessionMetadataUpdateRenamesBoundThreadWithoutRebinding(t *testing.T) {
	platform := &hostThreadTestPlatform{
		stubPlatformEngine: stubPlatformEngine{n: "test"},
		boundKey:           "test:chat:root:created",
		replyCtx:           "thread-context",
	}
	engine := NewEngine("test", &stubAgent{}, []Platform{platform}, "", LangEnglish)
	defer engine.cancel()
	session := engine.sessions.SwitchToAgentSession(
		platform.boundKey, "session-12345678", "sessionhost", "Claude Code")
	before := platform.callCount()

	engine.handleHostSessionMetadataUpdated(HostSessionLifecycle{
		SessionID: "session-12345678", WorkDir: "/workspace/claude-code-java",
		Summary: "Improve Feishu thread titles", MetadataOnly: true,
	})

	if got := session.GetName(); got != "claude-code-java · Improve Feishu thread titles" {
		t.Fatalf("session title = %q", got)
	}
	if platform.callCount() != before {
		t.Fatalf("metadata update rebound the thread: calls=%d before=%d", platform.callCount(), before)
	}
	platform.mu.Lock()
	updated := platform.updated
	platform.mu.Unlock()
	if updated != "claude-code-java · Improve Feishu thread titles" {
		t.Fatalf("updated thread title = %q", updated)
	}
}

func TestHostSessionDisplayTitleUsesProjectAndShortFallback(t *testing.T) {
	if got := hostSessionDisplayTitle("/workspace/claude-code-java", "Readable topic", "session-1"); got != "claude-code-java · Readable topic" {
		t.Fatalf("display title = %q", got)
	}
	if got := hostSessionDisplayTitle("/workspace/claude-code-java", "修复飞书消息循环", "session-2"); got != "claude-code-java · 修复飞书消息循环" {
		t.Fatalf("original-language title = %q", got)
	}
	if got := hostSessionDisplayTitle("", "", "12345678-aaaa-bbbb"); got != "Claude Code · 12345678" {
		t.Fatalf("fallback title = %q", got)
	}
}

func TestStartHostSessionCoordinatorBindsOnlyAfterCollaborationIsEnabled(t *testing.T) {
	platform := &hostThreadTestPlatform{
		stubPlatformEngine: stubPlatformEngine{n: "test"},
		boundKey:           "test:chat:root:created", replyCtx: "thread-context",
	}
	agent := &hostLifecycleTestAgent{
		events:        make(chan HostSessionLifecycle, 2),
		collaboration: make(chan HostSessionCollaboration, 2),
	}
	engine := NewEngine("test", agent, []Platform{platform}, "", LangEnglish)
	defer engine.cancel()
	engine.StartHostSessionCoordinator(agent)

	agent.events <- HostSessionLifecycle{SessionID: "local-session", Origin: "local"}
	time.Sleep(50 * time.Millisecond)
	if platform.callCount() != 0 {
		t.Fatal("local activation bound a channel before opt-in")
	}

	agent.collaboration <- HostSessionCollaboration{
		SessionID: "local-session", Channel: "test", Enabled: true,
		WorkDir: "/workspace", Origin: "local",
	}
	waitForCondition(t, time.Second, func() bool { return platform.callCount() == 1 },
		"collaboration enable did not bind the selected channel")
}

func TestStartHostSessionCoordinatorRestoresPersistedCollaborationOnLocalResume(t *testing.T) {
	const (
		hostID    = "local-session"
		threadKey = "feishu:chat:root:resumed"
	)
	platform := &hostThreadE2EPlatform{
		stubTrackableCardPlatform: stubTrackableCardPlatform{stubCardPlatform: stubCardPlatform{
			stubPlatformEngine: stubPlatformEngine{n: "feishu"},
		}},
		boundKey: threadKey, boundCtx: "thread-context",
	}
	agent := &hostThreadE2EAgent{
		events: make(chan HostSessionLifecycle, 2), collaboration: make(chan HostSessionCollaboration, 2),
		session: newHostThreadE2ESession(hostID),
	}
	storePath := filepath.Join(t.TempDir(), "sessions.json")
	engine := NewEngine("test", agent, []Platform{platform}, storePath, LangEnglish)
	defer engine.Stop()
	persisted := engine.sessions.SwitchToAgentSession(
		threadKey, hostID, agent.Name(), "resumed")
	persisted.SetCollaborationChannel("feishu")
	engine.sessions.Save()
	if channel, ok := engine.persistedHostCollaboration(hostID); !ok || channel != "feishu" {
		t.Fatalf("persisted collaboration = %q, %v", channel, ok)
	}

	engine.StartHostSessionCoordinator(agent)
	agent.events <- HostSessionLifecycle{
		SessionID: hostID, WorkDir: "/workspace", Summary: "Investigate resume display",
		MessageCount: 16, Origin: "local",
	}
	waitForCondition(t, time.Second, func() bool {
		_, restored := platform.bindingCounts()
		return restored == 1 && engine.findBoundHostSessionKey(hostID) == threadKey &&
			strings.Contains(strings.Join(platform.getSent(), "\n"),
				"Resumed in TUI · workspace · Investigate resume display · 16 messages loaded")
	},
		"local resume did not restore the persisted collaboration thread")
	if got := engine.findBoundHostSessionKey(hostID); got != threadKey {
		t.Fatalf("restored session key = %q", got)
	}
}

func TestBindHostSessionMovesOneAgentSessionToOnlyOneLiveThread(t *testing.T) {
	const hostID = "shared-host-session"
	platform := &hostThreadE2EPlatform{
		stubTrackableCardPlatform: stubTrackableCardPlatform{stubCardPlatform: stubCardPlatform{
			stubPlatformEngine: stubPlatformEngine{n: "feishu"},
		}},
		boundKey: "feishu:chat:root:first", boundCtx: "first-context",
	}
	agent := &hostThreadE2EAgent{
		events: make(chan HostSessionLifecycle, 1), collaboration: make(chan HostSessionCollaboration, 1),
		session: newHostThreadE2ESession(hostID),
	}
	engine := NewEngine("test", agent, []Platform{platform}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
	defer engine.Stop()
	event := HostSessionLifecycle{SessionID: hostID, Origin: "local"}
	if err := engine.bindHostSession("feishu:chat:owner", event); err != nil {
		t.Fatal(err)
	}
	platform.mu.Lock()
	platform.boundKey = "feishu:chat:root:second"
	platform.boundCtx = "second-context"
	platform.mu.Unlock()
	if err := engine.bindHostSession("feishu:chat:owner", event); err != nil {
		t.Fatal(err)
	}

	engine.interactiveMu.Lock()
	defer engine.interactiveMu.Unlock()
	if len(engine.interactiveStates) != 1 {
		t.Fatalf("live bindings = %d, want exactly one", len(engine.interactiveStates))
	}
	if _, ok := engine.interactiveStates["feishu:chat:root:second"]; !ok {
		t.Fatalf("live binding keys = %#v", engine.interactiveStates)
	}
}

func TestStartHostSessionCoordinatorDoesNotRestoreExplicitlyDisabledCollaboration(t *testing.T) {
	platform := &hostThreadTestPlatform{
		stubPlatformEngine: stubPlatformEngine{n: "test"},
		boundKey:           "test:chat:root:old", replyCtx: "thread-context",
	}
	agent := &hostLifecycleTestAgent{
		events: make(chan HostSessionLifecycle, 1), collaboration: make(chan HostSessionCollaboration, 1),
	}
	engine := NewEngine("test", agent, []Platform{platform}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
	defer engine.cancel()
	persisted := engine.sessions.SwitchToAgentSession(
		"test:chat:root:old", "local-session", agent.Name(), "old")
	persisted.SetCollaborationChannel("")
	engine.sessions.Save()

	engine.StartHostSessionCoordinator(agent)
	agent.events <- HostSessionLifecycle{SessionID: "local-session", Origin: "local"}
	time.Sleep(100 * time.Millisecond)
	if got := platform.callCount(); got != 0 {
		t.Fatalf("explicitly disabled collaboration restored %d bindings", got)
	}
}

func TestStartHostSessionCoordinatorDisableDetachesActiveSurface(t *testing.T) {
	platform := &hostThreadE2EPlatform{
		stubTrackableCardPlatform: stubTrackableCardPlatform{stubCardPlatform: stubCardPlatform{
			stubPlatformEngine: stubPlatformEngine{n: "feishu"},
		}},
		boundKey: "feishu:chat:root:created", boundCtx: "thread-context",
	}
	session := newHostThreadE2ESession("local-session")
	agent := &hostThreadE2EAgent{events: make(chan HostSessionLifecycle, 2),
		collaboration: make(chan HostSessionCollaboration, 2), session: session}
	engine := NewEngine("test", agent, []Platform{platform}, "", LangEnglish)
	defer engine.Stop()
	engine.StartHostSessionCoordinator(agent)
	agent.collaboration <- HostSessionCollaboration{
		SessionID: "local-session", Channel: "feishu", Enabled: true, Origin: "local",
	}
	waitForCondition(t, time.Second, func() bool {
		return engine.findBoundHostSessionKey("local-session") != ""
	}, "enabled collaboration was not attached")

	agent.collaboration <- HostSessionCollaboration{
		SessionID: "local-session", Channel: "feishu", Enabled: false, Origin: "local",
	}
	waitForCondition(t, time.Second, func() bool {
		engine.interactiveMu.Lock()
		detached := len(engine.interactiveStates) == 0
		engine.interactiveMu.Unlock()
		return detached && strings.Contains(strings.Join(platform.getSent(), "\n"),
			"Collaboration disabled on terminal")
	}, "disabled collaboration remained attached")
}

func TestStartHostSessionCoordinatorTerminalEndNotifiesAndPreservesResumeBinding(t *testing.T) {
	const (
		hostID    = "local-session"
		threadKey = "feishu:chat:root:created"
	)
	platform := &hostThreadE2EPlatform{
		stubTrackableCardPlatform: stubTrackableCardPlatform{stubCardPlatform: stubCardPlatform{
			stubPlatformEngine: stubPlatformEngine{n: "feishu"},
		}},
		boundKey: threadKey, boundCtx: "thread-context",
	}
	agent := &hostThreadE2EAgent{
		events: make(chan HostSessionLifecycle, 2), collaboration: make(chan HostSessionCollaboration, 2),
		session: newHostThreadE2ESession(hostID),
	}
	engine := NewEngine("test", agent, []Platform{platform}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
	defer engine.Stop()
	engine.StartHostSessionCoordinator(agent)
	agent.collaboration <- HostSessionCollaboration{
		SessionID: hostID, Channel: "feishu", Enabled: true, Origin: "local",
	}
	waitForCondition(t, time.Second, func() bool {
		return engine.findBoundHostSessionKey(hostID) == threadKey
	}, "collaboration did not bind before terminal exit")

	acknowledged := make(chan struct{}, 1)
	agent.events <- HostSessionLifecycle{
		SessionID: hostID, Origin: "local", Ended: true, Reason: "terminal_exit",
		Acknowledge: func() { acknowledged <- struct{}{} },
	}
	waitForCondition(t, time.Second, func() bool {
		return strings.Contains(strings.Join(platform.getSent(), "\n"),
			"Terminal session ended · resume is available")
	}, "terminal end was not visible in the bound IM thread")
	select {
	case <-acknowledged:
	case <-time.After(time.Second):
		t.Fatal("terminal end was not acknowledged after the IM send")
	}
	waitForCondition(t, time.Second, func() bool {
		engine.interactiveMu.Lock()
		defer engine.interactiveMu.Unlock()
		return len(engine.interactiveStates) == 0
	}, "terminal end left the live host session attached")
	if got := engine.findBoundHostSessionKey(hostID); got != threadKey {
		t.Fatalf("terminal end discarded resume thread binding: %q", got)
	}
	if channel, explicit := engine.persistedHostCollaboration(hostID); !explicit || channel != "feishu" {
		t.Fatalf("terminal end collaboration = %q, explicit=%v", channel, explicit)
	}
}

func TestStartHostSessionCoordinatorResumeWithoutPriorThreadRemainsOff(t *testing.T) {
	platform := &hostThreadTestPlatform{
		stubPlatformEngine: stubPlatformEngine{n: "test"},
		boundKey:           "test:chat:root:unexpected", replyCtx: "thread-context",
	}
	agent := &hostLifecycleTestAgent{
		events: make(chan HostSessionLifecycle, 1), collaboration: make(chan HostSessionCollaboration, 1),
	}
	engine := NewEngine("test", agent, []Platform{platform}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
	defer engine.cancel()
	engine.StartHostSessionCoordinator(agent)

	agent.events <- HostSessionLifecycle{
		SessionID: "never-bound-session", WorkDir: "/workspace", MessageCount: 12, Origin: "local",
	}
	time.Sleep(100 * time.Millisecond)
	if got := platform.callCount(); got != 0 {
		t.Fatalf("resume without prior thread created %d bindings", got)
	}
	if channel, explicit := engine.persistedHostCollaboration("never-bound-session"); explicit || channel != "" {
		t.Fatalf("unbound resumed collaboration = %q, explicit=%v", channel, explicit)
	}
}

func TestPrepareInboundHostSessionThreadRewritesMessageBeforeSessionStart(t *testing.T) {
	platform := &hostThreadTestPlatform{
		stubPlatformEngine: stubPlatformEngine{n: "test"},
		boundKey:           "test:chat:root:created",
		replyCtx:           "thread-context",
	}
	agent := &hostLifecycleTestAgent{events: make(chan HostSessionLifecycle)}
	engine := NewEngine("test", agent, []Platform{platform}, "", LangEnglish)
	defer engine.cancel()
	message := &Message{
		SessionKey: "test:chat:user",
		ReplyCtx:   "incoming-context",
		Content:    "Investigate the login regression",
	}

	prepared, err := engine.prepareInboundHostSessionThread(platform, message, agent)
	if err != nil {
		t.Fatalf("prepareInboundHostSessionThread() error = %v", err)
	}
	if !prepared {
		t.Fatal("prepareInboundHostSessionThread() = false, want true")
	}
	if message.SessionKey != "test:chat:root:created" || message.ReplyCtx != "thread-context" {
		t.Fatalf("rewritten message = %#v", message)
	}
	if platform.baseKey != "test:chat:user" || platform.session != "" {
		t.Fatalf("binding call base=%q session=%q", platform.baseKey, platform.session)
	}
	if platform.title != "Investigate the login regression" {
		t.Fatalf("binding title = %q", platform.title)
	}
}

func TestPrepareInboundHostSessionThreadSkipsNonHostAgent(t *testing.T) {
	platform := &hostThreadTestPlatform{
		stubPlatformEngine: stubPlatformEngine{n: "test"},
		boundKey:           "test:chat:root:created",
		replyCtx:           "thread-context",
	}
	engine := NewEngine("test", &stubAgent{}, []Platform{platform}, "", LangEnglish)
	defer engine.cancel()
	message := &Message{SessionKey: "test:chat:user", ReplyCtx: "incoming-context"}

	prepared, err := engine.prepareInboundHostSessionThread(platform, message, &stubAgent{})
	if err != nil || prepared {
		t.Fatalf("prepareInboundHostSessionThread() = (%v, %v), want (false, nil)", prepared, err)
	}
	if got := platform.callCount(); got != 0 {
		t.Fatalf("non-host agent created %d thread bindings, want 0", got)
	}
}

func TestHandleMessagePreparesHostThreadBeforeOpeningJavaSession(t *testing.T) {
	platform := &hostThreadTestPlatform{
		stubPlatformEngine: stubPlatformEngine{n: "test"},
		boundKey:           "test:chat:root:first-message",
		replyCtx:           "thread-context",
	}
	hostSession := newInboundHostTestSession()
	agent := &inboundHostTestAgent{
		events:   make(chan HostSessionLifecycle),
		session:  hostSession,
		platform: platform,
	}
	engine := NewEngine("test", agent, []Platform{platform}, "", LangEnglish)
	defer engine.cancel()

	engine.handleMessage(platform, &Message{
		SessionKey: "test:chat:user",
		Platform:   "test",
		MessageID:  "message-1",
		UserID:     "user-1",
		UserName:   "Alice",
		Content:    "Open the project and investigate",
		ReplyCtx:   "incoming-context",
	})

	select {
	case <-hostSession.sent:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for host turn submission")
	}
	startID, boundAtStart, env := agent.snapshot()
	if startID != "" {
		t.Fatalf("StartSession ID = %q, want fresh session", startID)
	}
	if !boundAtStart {
		t.Fatal("Java session was opened before the IM thread was prepared")
	}
	if !containsEnvValue(env, "CC_SESSION_KEY", "test:chat:root:first-message") {
		t.Fatalf("CC_SESSION_KEY env = %#v", env)
	}
	if got := engine.sessions.ActiveSessionID("test:chat:root:first-message"); got == "" {
		t.Fatal("thread-scoped cc-connect session was not created")
	}
	if got := engine.sessions.ActiveSessionID("test:chat:user"); got != "" {
		t.Fatalf("base chat unexpectedly owns session %q", got)
	}
}

func TestNewCommandReusesCurrentThreadAndSwitchesHostSession(t *testing.T) {
	platform := &hostThreadTestPlatform{
		stubPlatformEngine: stubPlatformEngine{n: "test"},
		boundKey:           "test:chat:root:new-command",
		replyCtx:           "new-thread-context",
	}
	hostSession := newInboundHostTestSession()
	agent := &inboundHostTestAgent{
		events:   make(chan HostSessionLifecycle),
		session:  hostSession,
		platform: platform,
	}
	engine := NewEngine("test", agent, []Platform{platform}, "", LangEnglish)
	defer engine.Stop()
	oldSession := engine.sessions.GetOrCreateActive("test:chat:root:existing")
	oldSession.SetAgentSessionID("java-session-old", agent.Name())
	oldSession.AddHistory("assistant", "previous turn")

	engine.handleMessage(platform, &Message{
		SessionKey: "test:chat:root:existing", Platform: "test", MessageID: "message-new",
		UserID: "user-1", UserName: "Alice", Content: "/new Login regression",
		ReplyCtx: "existing-thread-context",
	})

	startID, boundAtStart, env := agent.snapshot()
	if startID != "" {
		t.Fatalf("StartSession ID = %q, want fresh host session", startID)
	}
	if boundAtStart || platform.callCount() != 0 {
		t.Fatal("/new created or rebound an IM thread; want the current thread reused")
	}
	if !containsEnvValue(env, "CC_SESSION_KEY", "test:chat:root:existing") {
		t.Fatalf("CC_SESSION_KEY env = %#v", env)
	}
	if got := engine.sessions.ActiveSessionID("test:chat:root:existing"); got == "" {
		t.Fatal("/new did not switch the existing thread to a fresh cc-connect session")
	}
	active := engine.sessions.GetOrCreateActive("test:chat:root:existing")
	if got := active.GetAgentSessionID(); got != "java-session" {
		t.Fatalf("existing thread is bound to host session %q, want java-session", got)
	}
	if got := engine.sessions.ActiveSessionID("test:chat:root:new-command"); got != "" {
		t.Fatalf("/new unexpectedly created sibling-thread session %q", got)
	}
	if got := oldSession.GetAgentSessionID(); got != "java-session-old" {
		t.Fatalf("previous host session ID = %q, want it preserved for /resume", got)
	}
	if history := oldSession.History; len(history) != 1 || history[0].Content != "previous turn" {
		t.Fatalf("previous host session history = %#v, want it preserved", history)
	}
	sent := strings.Join(platform.getSent(), "\n")
	if !strings.Contains(sent, "New session created") {
		t.Fatalf("same-thread /new acknowledgement = %q", sent)
	}
	select {
	case prompt := <-hostSession.sent:
		t.Fatalf("/new unexpectedly sent a model prompt: %q", prompt)
	default:
	}
}

func TestHostSessionThreadE2E_LocalBindRemoteResumeAndOutputMirroring(t *testing.T) {
	const (
		hostID    = "java-host-session-e2e"
		threadKey = "feishu:oc_project:root:om_thread_e2e"
	)
	platform := &hostThreadE2EPlatform{
		stubTrackableCardPlatform: stubTrackableCardPlatform{stubCardPlatform: stubCardPlatform{
			stubPlatformEngine: stubPlatformEngine{n: "feishu"},
		}},
		boundKey: threadKey, boundCtx: "thread-context",
	}
	hostSession := newHostThreadE2ESession(hostID)
	agent := &hostThreadE2EAgent{events: make(chan HostSessionLifecycle, 4),
		collaboration: make(chan HostSessionCollaboration, 4), session: hostSession}
	storePath := filepath.Join(t.TempDir(), "sessions.json")
	engine := NewEngine("test", agent, []Platform{platform}, storePath, LangChinese)
	defer engine.Stop()
	if err := engine.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Scenario 2: a locally activated PTY session proactively creates and binds
	// one durable Feishu thread, then mirrors local user/model output into it.
	agent.collaboration <- HostSessionCollaboration{
		SessionID: hostID, Channel: "feishu", Enabled: true,
		WorkDir: "/workspace/e2e", Summary: "端到端会话", Origin: "local",
	}
	waitForCondition(t, 3*time.Second, func() bool {
		created, _ := platform.bindingCounts()
		return created == 1 && engine.findBoundHostSessionKey(hostID) == threadKey
	}, "local host session was not bound to a Feishu thread")
	hostSession.events <- Event{Type: EventUserInput, Content: "本机问题", SessionID: hostID}
	hostSession.events <- Event{Type: EventText, Content: "本机答案", SessionID: hostID}
	hostSession.events <- Event{Type: EventResult, Content: "本机答案", Done: true, SessionID: hostID}
	waitForCondition(t, 3*time.Second, func() bool {
		joined := strings.Join(platform.getSent(), "\n")
		return strings.Contains(joined, "本机问题") && strings.Contains(joined, "本机答案")
	}, "local PTY input/output was not mirrored to the bound thread")

	// Scenario 3: a reply in that same thread must resume the exact host session,
	// not create a nested thread or a fresh application session.
	platform.clearSent()
	engine.handleMessage(platform, &Message{
		SessionKey: threadKey, Platform: "feishu", MessageID: "om_remote_turn",
		UserID: "ou_owner", Content: "从飞书继续", ReplyCtx: "thread-context",
	})
	select {
	case got := <-hostSession.sent:
		if got != "从飞书继续" {
			t.Fatalf("remote prompt = %q", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("thread reply did not reach the attached Java session")
	}
	waitForCondition(t, 3*time.Second, func() bool {
		return strings.Contains(strings.Join(platform.getSent(), "\n"), "remote:从飞书继续")
	}, "remote-resumed turn output was not delivered to the same thread")
	created, restored := platform.bindingCounts()
	if created != 1 || restored == 0 {
		t.Fatalf("thread binding counts create=%d restore=%d, want one create and a restore", created, restored)
	}
	for _, startedWith := range agent.startIDs() {
		if startedWith != hostID {
			t.Fatalf("StartSession(%q), want exact host session %q", startedWith, hostID)
		}
	}
}

func TestHostSessionThreadE2E_CoalescesLocalTextBeforeFollowingTool(t *testing.T) {
	const (
		hostID    = "java-host-output-coalescing"
		threadKey = "feishu:oc_project:root:om_output_coalescing"
	)
	platform := &hostThreadE2EPlatform{
		stubTrackableCardPlatform: stubTrackableCardPlatform{stubCardPlatform: stubCardPlatform{
			stubPlatformEngine: stubPlatformEngine{n: "feishu"},
		}},
		boundKey: threadKey, boundCtx: "thread-context",
	}
	hostSession := newHostThreadE2ESession(hostID)
	agent := &hostThreadE2EAgent{events: make(chan HostSessionLifecycle, 2),
		collaboration: make(chan HostSessionCollaboration, 2), session: hostSession}
	engine := NewEngine("test", agent, []Platform{platform}, filepath.Join(t.TempDir(), "sessions.json"), LangChinese)
	engine.display.ToolMessages = true
	defer engine.Stop()
	if err := engine.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	agent.collaboration <- HostSessionCollaboration{SessionID: hostID, Channel: "feishu",
		Enabled: true, WorkDir: "/workspace/e2e", Origin: "local"}
	waitForCondition(t, 3*time.Second, func() bool {
		return engine.findBoundHostSessionKey(hostID) == threadKey
	}, "coalescing test session was not bound")

	platform.clearSent()
	hostSession.events <- Event{Type: EventText, Content: "alpha", SessionID: hostID}
	hostSession.events <- Event{Type: EventText, Content: "beta", SessionID: hostID}
	hostSession.events <- Event{
		Type: EventToolUse, ToolName: "Bash", ToolInput: "pwd", SessionID: hostID,
	}
	hostSession.events <- Event{Type: EventResult, Content: "alphabeta", Done: true, SessionID: hostID}
	waitForCondition(t, 3*time.Second, func() bool {
		return len(platform.getSent()) >= 2
	}, "coalesced text and following tool were not delivered")

	sent := platform.getSent()
	if sent[0] != "alphabeta" {
		t.Fatalf("first mirrored message = %q, want coalesced text", sent[0])
	}
	if !strings.Contains(sent[1], "Bash") {
		t.Fatalf("second mirrored message = %q, want following tool", sent[1])
	}
	if len(sent) != 2 {
		t.Fatalf("mirrored message count = %d, want 2: %#v", len(sent), sent)
	}
}

func TestHostSessionThreadE2E_LocalPTYUsesSingleProgressCard(t *testing.T) {
	const (
		hostID    = "java-host-single-progress-card"
		threadKey = "feishu:oc_project:root:om_single_progress_card"
	)
	platform := &hostProgressThreadE2EPlatform{hostThreadE2EPlatform: hostThreadE2EPlatform{
		stubTrackableCardPlatform: stubTrackableCardPlatform{stubCardPlatform: stubCardPlatform{
			stubPlatformEngine: stubPlatformEngine{n: "feishu"},
		}},
		boundKey: threadKey, boundCtx: "thread-context",
	}}
	hostSession := newHostThreadE2ESession(hostID)
	agent := &hostThreadE2EAgent{events: make(chan HostSessionLifecycle, 2),
		collaboration: make(chan HostSessionCollaboration, 2), session: hostSession}
	engine := NewEngine("test", agent, []Platform{platform}, filepath.Join(t.TempDir(), "sessions.json"), LangChinese)
	engine.display.ThinkingMessages = true
	engine.display.ToolMessages = true
	defer engine.Stop()
	if err := engine.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	agent.collaboration <- HostSessionCollaboration{SessionID: hostID, Channel: "feishu",
		Enabled: true, WorkDir: "/workspace/e2e", Origin: "local"}
	waitForCondition(t, 3*time.Second, func() bool {
		return engine.findBoundHostSessionKey(hostID) == threadKey
	}, "single-card test session was not bound")

	platform.clearSent()
	hostSession.events <- Event{Type: EventUserInput, Content: "检查项目", SessionID: hostID}
	hostSession.events <- Event{Type: EventModel, Model: "claude-sonnet", SessionID: hostID}
	hostSession.events <- Event{Type: EventThinking, Content: "正在分析", SessionID: hostID}
	hostSession.events <- Event{Type: EventToolUse, ToolName: "Read", ToolInput: "README.md", SessionID: hostID}
	hostSession.events <- Event{Type: EventToolResult, ToolName: "Read", ToolStatus: "completed", SessionID: hostID}
	hostSession.events <- Event{Type: EventToolUse, ToolName: "Bash", ToolInput: "mvn test", SessionID: hostID}
	hostSession.events <- Event{Type: EventToolResult, ToolName: "Bash", ToolStatus: "completed", SessionID: hostID}
	hostSession.events <- Event{Type: EventText, Content: "检查完成", SessionID: hostID}
	hostSession.events <- Event{Type: EventResult, Content: "检查完成", Done: true, SessionID: hostID}

	waitForCondition(t, 3*time.Second, func() bool {
		starts, edits := platform.progressSnapshots()
		return len(starts) == 1 && len(edits) > 0 && len(platform.getSent()) == 2
	}, "local PTY turn was not aggregated into one progress card")
	starts, edits := platform.progressSnapshots()
	if len(starts) != 1 {
		t.Fatalf("progress card starts = %d, want 1", len(starts))
	}
	started, ok := ParseProgressCardPayload(starts[0])
	if !ok || started.State != ProgressCardStateRunning {
		t.Fatalf("initial progress payload = %#v, parsed=%v", started, ok)
	}
	final, ok := ParseProgressCardPayload(edits[len(edits)-1])
	if !ok || final.State != ProgressCardStateCompleted {
		t.Fatalf("final progress payload = %#v, parsed=%v", final, ok)
	}
	if sent := platform.getSent(); len(sent) != 2 || !strings.Contains(sent[0], "检查项目") || sent[1] != "检查完成" {
		t.Fatalf("mirrored messages = %#v, want user input plus one final answer", sent)
	}
}

func TestHostSessionThreadE2E_RestartResumesPersistedThreadBinding(t *testing.T) {
	const (
		hostID    = "java-host-session-after-restart"
		threadKey = "feishu:oc_project:root:om_restart_resume"
	)
	storePath := filepath.Join(t.TempDir(), "sessions.json")
	platform := &hostThreadE2EPlatform{
		stubTrackableCardPlatform: stubTrackableCardPlatform{stubCardPlatform: stubCardPlatform{
			stubPlatformEngine: stubPlatformEngine{n: "feishu"},
		}},
		boundKey: threadKey, boundCtx: "thread-context",
	}

	firstSession := newHostThreadE2ESession(hostID)
	firstAgent := &hostThreadE2EAgent{events: make(chan HostSessionLifecycle, 2),
		collaboration: make(chan HostSessionCollaboration, 2), session: firstSession}
	firstEngine := NewEngine("test", firstAgent, []Platform{platform}, storePath, LangChinese)
	if err := firstEngine.Start(); err != nil {
		t.Fatalf("first Start() error = %v", err)
	}
	firstAgent.collaboration <- HostSessionCollaboration{SessionID: hostID, Channel: "feishu",
		Enabled: true, WorkDir: "/workspace/e2e", Origin: "local"}
	waitForCondition(t, 3*time.Second, func() bool {
		return firstEngine.findBoundHostSessionKey(hostID) == threadKey
	}, "initial host session was not persisted against its thread")
	if err := firstEngine.Stop(); err != nil {
		t.Fatalf("first Stop() error = %v", err)
	}

	secondSession := newHostThreadE2ESession(hostID)
	secondAgent := &hostThreadE2EAgent{events: make(chan HostSessionLifecycle, 2),
		collaboration: make(chan HostSessionCollaboration, 2), session: secondSession}
	secondEngine := NewEngine("test", secondAgent, []Platform{platform}, storePath, LangChinese)
	defer secondEngine.Stop()
	if err := secondEngine.Start(); err != nil {
		t.Fatalf("second Start() error = %v", err)
	}
	secondEngine.handleMessage(platform, &Message{
		SessionKey: threadKey, Platform: "feishu", MessageID: "om_after_restart",
		UserID: "ou_owner", Content: "重启后继续", ReplyCtx: "thread-context",
	})
	select {
	case got := <-secondSession.sent:
		if got != "重启后继续" {
			t.Fatalf("prompt after restart = %q", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("persisted thread did not resume its Java session after restart")
	}
	if starts := secondAgent.startIDs(); len(starts) != 1 || starts[0] != hostID {
		t.Fatalf("StartSession calls after restart = %#v, want [%q]", starts, hostID)
	}
}

func TestHostSessionThreadE2E_LocalInteractionResolutionUpdatesCards(t *testing.T) {
	const (
		hostID    = "java-host-interaction-e2e"
		threadKey = "feishu:oc_project:root:om_interaction_e2e"
	)
	platform := &hostThreadE2EPlatform{
		stubTrackableCardPlatform: stubTrackableCardPlatform{stubCardPlatform: stubCardPlatform{
			stubPlatformEngine: stubPlatformEngine{n: "feishu"},
		}},
		boundKey: threadKey, boundCtx: "thread-context",
	}
	hostSession := newHostThreadE2ESession(hostID)
	agent := &hostThreadE2EAgent{events: make(chan HostSessionLifecycle, 2),
		collaboration: make(chan HostSessionCollaboration, 2), session: hostSession}
	engine := NewEngine("test", agent, []Platform{platform}, filepath.Join(t.TempDir(), "sessions.json"), LangChinese)
	defer engine.Stop()
	if err := engine.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	agent.collaboration <- HostSessionCollaboration{SessionID: hostID, Channel: "feishu",
		Enabled: true, WorkDir: "/workspace/e2e", Origin: "local"}
	waitForCondition(t, 3*time.Second, func() bool {
		return engine.findBoundHostSessionKey(hostID) == threadKey
	}, "interaction test session was not bound")

	hostSession.events <- Event{
		Type: EventPermissionRequest, RequestID: "perm-local-e2e", ToolName: "Bash",
		ToolInput: "which claude", ToolInputRaw: map[string]any{"command": "which claude"},
	}
	waitForCondition(t, 3*time.Second, func() bool {
		platform.stubCardPlatform.mu.Lock()
		defer platform.stubCardPlatform.mu.Unlock()
		return len(platform.sentCards) == 1
	}, "permission card was not sent")
	hostSession.resolutions <- InteractionResolution{
		RequestID: "perm-local-e2e", Behavior: "allow", Origin: "local",
	}
	permissionCard := waitForUpdatedCard(t, &platform.stubTrackableCardPlatform)
	if permissionCard.HasButtons() || permissionCard.Header == nil ||
		!strings.Contains(permissionCard.Header.Title, "终端") {
		t.Fatalf("permission card did not converge after local response: %#v", permissionCard)
	}

	questions := []UserQuestion{{
		Question: "需要哪些检查？", Header: "检查", MultiSelect: true,
		Options: []UserQuestionOption{{Label: "单测"}, {Label: "集成测试"}},
	}}
	hostSession.events <- Event{
		Type: EventPermissionRequest, RequestID: "ask-local-e2e", ToolName: "AskUserQuestion",
		Questions: questions, ToolInputRaw: map[string]any{"questions": []any{}},
	}
	waitForCondition(t, 3*time.Second, func() bool {
		platform.stubCardPlatform.mu.Lock()
		defer platform.stubCardPlatform.mu.Unlock()
		return len(platform.sentCards) == 2
	}, "AskUserQuestion card was not sent")
	platform.stubCardPlatform.mu.Lock()
	askCard := platform.sentCards[1]
	platform.stubCardPlatform.mu.Unlock()
	if text := askCard.RenderText(); !strings.Contains(text, "Other") || !strings.Contains(text, "自然语言") {
		t.Fatalf("multi-select AskUserQuestion card lacks natural-language Other: %s", text)
	}
	hostSession.resolutions <- InteractionResolution{
		RequestID: "ask-local-e2e", Behavior: "allow", Origin: "local",
		UpdatedInput: map[string]any{"answers": map[string]any{
			"需要哪些检查？": "单测, 再补一轮人工烟测",
		}},
	}
	waitForCondition(t, 3*time.Second, func() bool {
		platform.stubCardPlatform.mu.Lock()
		defer platform.stubCardPlatform.mu.Unlock()
		return len(platform.updatedCards) >= 2
	}, "AskUserQuestion card was not updated after terminal answer")
	platform.stubCardPlatform.mu.Lock()
	resolvedAsk := platform.updatedCards[len(platform.updatedCards)-1]
	platform.stubCardPlatform.mu.Unlock()
	if resolvedAsk.HasButtons() || !strings.Contains(resolvedAsk.RenderText(), "再补一轮人工烟测") {
		t.Fatalf("resolved AskUserQuestion card = %s", resolvedAsk.RenderText())
	}
}

func waitForCondition(t *testing.T, timeout time.Duration, ready func() bool, message string) {
	t.Helper()
	deadline := time.After(timeout)
	for !ready() {
		select {
		case <-deadline:
			t.Fatal(message)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func containsEnvValue(env []string, key, value string) bool {
	want := key + "=" + value
	for _, entry := range env {
		if entry == want {
			return true
		}
	}
	return false
}
