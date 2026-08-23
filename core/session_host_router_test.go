package core

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSessionHostRouterPersistsOwnerAcrossProcesses(t *testing.T) {
	dataDir := t.TempDir()
	ownerSocket := filepath.Join(t.TempDir(), "owner.sock")
	otherSocket := filepath.Join(t.TempDir(), "other.sock")
	const key = "feishu:oc_chat:root:om_owner"

	owner := NewSessionHostRouter(dataDir, "project-a", ownerSocket)
	if err := owner.Register(key); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	other := NewSessionHostRouter(dataDir, "project-a", otherSocket)
	route, err := other.Lookup(key)
	if err != nil {
		t.Fatalf("Lookup() error = %v", err)
	}
	if route == nil || route.SocketPath != ownerSocket || route.Project != "project-a" {
		t.Fatalf("route = %#v", route)
	}
	if other.IsLocal(route) {
		t.Fatal("route owned by another process reported local")
	}
	if !owner.IsLocal(route) {
		t.Fatal("owner route reported remote")
	}
}

func TestSessionHostRouterLeaseCASAndCompareDelete(t *testing.T) {
	dataDir := t.TempDir()
	now := time.Unix(1_700_000_000, 0)
	owner := NewSessionHostRouter(dataDir, "project-a", filepath.Join(t.TempDir(), "owner.sock"))
	owner.now = func() time.Time { return now }
	owner.lease = time.Minute
	contender := NewSessionHostRouter(dataDir, "project-a", filepath.Join(t.TempDir(), "contender.sock"))
	contender.now = func() time.Time { return now }
	contender.lease = time.Minute
	const key = "feishu:chat:root:lease"

	first, err := owner.RegisterRoute(key)
	if err != nil {
		t.Fatalf("owner RegisterRoute() error = %v", err)
	}
	if first.OwnerToken == "" || first.Generation == 0 || !first.Alive(now) {
		t.Fatalf("registered route missing owner/generation/lease: %#v", first)
	}
	if _, err := contender.RegisterRoute(key); !errors.Is(err, ErrSessionHostRouteOwned) {
		t.Fatalf("live contender RegisterRoute() error = %v, want ErrSessionHostRouteOwned", err)
	}

	now = now.Add(2 * time.Minute)
	second, err := contender.RegisterRoute(key)
	if err != nil {
		t.Fatalf("expired takeover error = %v", err)
	}
	if second.Generation != first.Generation+1 || second.OwnerToken == first.OwnerToken {
		t.Fatalf("takeover route = %#v, first = %#v", second, first)
	}
	deleted, err := owner.CompareAndDelete(key, first.OwnerToken, first.Generation)
	if err != nil || deleted {
		t.Fatalf("stale compare-delete = %v, %v, want false, nil", deleted, err)
	}
	deleted, err = contender.CompareAndDelete(key, second.OwnerToken, second.Generation)
	if err != nil || !deleted {
		t.Fatalf("owner compare-delete = %v, %v, want true, nil", deleted, err)
	}
}

func TestSessionHostRouterRegisterMigratesLegacyRoute(t *testing.T) {
	router := NewSessionHostRouter(t.TempDir(), "project-a", filepath.Join(t.TempDir(), "owner.sock"))
	const key = "feishu:chat:root:legacy"
	if err := os.MkdirAll(router.routeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	legacy := `{"version":1,"session_key":"feishu:chat:root:legacy","project":"old","socket_path":"/tmp/old.sock","updated_at":"2026-01-01T00:00:00Z"}`
	if err := os.WriteFile(router.routePath(key), []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	route, err := router.RegisterRoute(key)
	if err != nil {
		t.Fatalf("RegisterRoute() legacy migration error = %v", err)
	}
	if route.OwnerToken == "" || route.Generation != 1 || !router.IsLocal(route) {
		t.Fatalf("migrated route = %#v", route)
	}
}

func TestSessionHostRouterClaimMessageDeduplicatesAcrossInstances(t *testing.T) {
	dataDir := t.TempDir()
	owner := NewSessionHostRouter(dataDir, "project-a", filepath.Join(t.TempDir(), "owner.sock"))
	const key = "feishu:chat:root:claim"
	route, err := owner.RegisterRoute(key)
	if err != nil {
		t.Fatal(err)
	}
	peer := NewSessionHostRouter(dataDir, "project-a", owner.localSocket)
	peer.ownerToken = owner.ownerToken

	if ok, err := owner.ClaimMessage(key, "message-1", route.Generation); err != nil || !ok {
		t.Fatalf("first claim = %v, %v", ok, err)
	}
	if ok, err := peer.ClaimMessage(key, "message-1", route.Generation); err != nil || ok {
		t.Fatalf("duplicate claim = %v, %v, want false, nil", ok, err)
	}
}

func TestSessionHostRouterClaimMessageSweepsExpiredClaims(t *testing.T) {
	now := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	router := NewSessionHostRouter(t.TempDir(), "project-a", filepath.Join(t.TempDir(), "owner.sock"))
	router.now = func() time.Time { return now }
	router.claimTTL = time.Hour
	router.claimSweepInterval = time.Minute
	const key = "feishu:chat:root:claim-gc"
	route, err := router.RegisterRoute(key)
	if err != nil {
		t.Fatal(err)
	}
	claimDir := filepath.Join(router.routeDir, "claims")
	if err := os.MkdirAll(claimDir, 0o700); err != nil {
		t.Fatal(err)
	}
	expired := filepath.Join(claimDir, "expired")
	fresh := filepath.Join(claimDir, "fresh")
	if err := os.WriteFile(expired, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fresh, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(expired, now.Add(-2*time.Hour), now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(fresh, now.Add(-30*time.Minute), now.Add(-30*time.Minute)); err != nil {
		t.Fatal(err)
	}

	if ok, err := router.ClaimMessage(key, "trigger-sweep", route.Generation); err != nil || !ok {
		t.Fatalf("claim trigger = %v, %v", ok, err)
	}
	if _, err := os.Stat(expired); !os.IsNotExist(err) {
		t.Fatalf("expired claim still exists: %v", err)
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Fatalf("fresh claim removed: %v", err)
	}
}

func TestSessionHostRouterActiveThreadRoutesFiltersBaseChatAndExpiry(t *testing.T) {
	now := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	dataDir := t.TempDir()
	first := NewSessionHostRouter(dataDir, "project-a", filepath.Join(t.TempDir(), "first.sock"))
	first.now = func() time.Time { return now }
	first.lease = time.Minute
	second := NewSessionHostRouter(dataDir, "project-b", filepath.Join(t.TempDir(), "second.sock"))
	second.now = func() time.Time { return now }
	second.lease = time.Minute
	if _, err := first.RegisterRoute("feishu:chat:root:first"); err != nil {
		t.Fatal(err)
	}
	if _, err := second.RegisterRoute("feishu:chat:root:second"); err != nil {
		t.Fatal(err)
	}
	if _, err := second.RegisterRoute("feishu:other:root:ignored"); err != nil {
		t.Fatal(err)
	}

	routes, err := first.ActiveThreadRoutes("feishu:chat")
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 2 || routes[0].SessionKey != "feishu:chat:root:first" ||
		routes[1].SessionKey != "feishu:chat:root:second" {
		t.Fatalf("active routes = %#v", routes)
	}

	first.now = func() time.Time { return now.Add(2 * time.Minute) }
	routes, err = first.ActiveThreadRoutes("feishu:chat")
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 0 {
		t.Fatalf("expired routes = %#v", routes)
	}
}

func TestSessionHostMainChatIsSilentlyConsumedWithAnyThreadCount(t *testing.T) {
	for _, routeCount := range []int{0, 1, 2} {
		t.Run(fmt.Sprintf("routes_%d", routeCount), func(t *testing.T) {
			dataDir := t.TempDir()
			for i := 0; i < routeCount; i++ {
				router := NewSessionHostRouter(
					dataDir, fmt.Sprintf("project-%d", i), filepath.Join(t.TempDir(), fmt.Sprintf("route-%d.sock", i)))
				if _, err := router.RegisterRoute(fmt.Sprintf("feishu:chat:root:thread-%d", i)); err != nil {
					t.Fatal(err)
				}
			}
			platform := &mainChatRecordingPlatform{routedTestPlatform: routedTestPlatform{name: "feishu"}}
			agent := &hostLifecycleTestAgent{
				events: make(chan HostSessionLifecycle), collaboration: make(chan HostSessionCollaboration),
			}
			engine := NewEngine("controller", agent, []Platform{platform}, "", LangEnglish)
			defer engine.Stop()
			engine.SetSessionHostRouter(NewSessionHostRouter(
				dataDir, "controller", filepath.Join(t.TempDir(), "controller.sock")))
			messages := []*Message{
				{SessionKey: "feishu:chat", MessageID: "main-text", Content: "hello", ReplyCtx: "main"},
				{SessionKey: "feishu:chat", MessageID: "main-slash", Content: "/resume", ReplyCtx: "main"},
				{SessionKey: "feishu:chat", MessageID: "main-file", Content: "see file", ReplyCtx: "main",
					Files: []FileAttachment{{FileName: "note.txt", MimeType: "text/plain", Data: []byte("x")}}},
			}
			for _, msg := range messages {
				if !engine.routeBaseChatToHostThread(platform, msg) {
					t.Fatalf("Session Host main-chat message %q was not consumed", msg.MessageID)
				}
				if !engine.routeBaseChatToHostThread(platform, msg) {
					t.Fatalf("duplicate Session Host main-chat message %q was not consumed", msg.MessageID)
				}
			}
			platform.mu.Lock()
			defer platform.mu.Unlock()
			if len(platform.replies) != 0 {
				t.Fatalf("main-chat replies = %#v, want silent discard", platform.replies)
			}
		})
	}
}

func TestSessionHostRouterStaleGenerationRejectsRoutedInputAndCard(t *testing.T) {
	dataDir := t.TempDir()
	now := time.Unix(1_700_000_000, 0)
	oldRouter := NewSessionHostRouter(dataDir, "project-a", filepath.Join(t.TempDir(), "old.sock"))
	oldRouter.now = func() time.Time { return now }
	oldRouter.lease = time.Minute
	const key = "test:chat:root:stale"
	if _, err := oldRouter.RegisterRoute(key); err != nil {
		t.Fatal(err)
	}

	agent := &stubSessionEffortAgent{current: "auto"}
	engine := NewEngine("project-a", agent, []Platform{&routedTestPlatform{name: "test"}}, "", LangEnglish)
	engine.SetSessionHostRouter(oldRouter)

	now = now.Add(2 * time.Minute)
	newRouter := NewSessionHostRouter(dataDir, "project-a", filepath.Join(t.TempDir(), "new.sock"))
	newRouter.now = func() time.Time { return now }
	newRouter.lease = time.Minute
	if _, err := newRouter.RegisterRoute(key); err != nil {
		t.Fatal(err)
	}

	outcome, err := engine.DispatchRoutedMessage("test", &Message{
		SessionKey: key, MessageID: "stale-input", Content: "do not run",
	})
	if err != nil || !outcome.Stale {
		t.Fatalf("stale routed input outcome = %#v, %v", outcome, err)
	}
	card := engine.handleCardNavLocal("act:/reasoning 2", key)
	if agent.setCalls != 0 || card == nil || card.Header == nil || card.Header.Color != "red" {
		t.Fatalf("stale card mutated owner: calls=%d card=%#v", agent.setCalls, card)
	}
}

func TestUnsolicitedOutputStopsAfterRouteGenerationTakeover(t *testing.T) {
	dataDir := t.TempDir()
	var nowUnix atomic.Int64
	nowUnix.Store(1_700_000_000)
	clock := func() time.Time { return time.Unix(nowUnix.Load(), 0) }
	oldRouter := NewSessionHostRouter(dataDir, "project-a", filepath.Join(t.TempDir(), "old.sock"))
	oldRouter.now = clock
	oldRouter.lease = 30 * time.Millisecond
	const key = "test:chat:root:output"
	route, err := oldRouter.RegisterRoute(key)
	if err != nil {
		t.Fatal(err)
	}

	events := make(chan Event, 2)
	agentSession := &routeOwnedAgentSession{events: events}
	platform := &stubPlatformEngine{n: "test"}
	engine := NewEngine("project-a", &stubAgent{}, []Platform{platform}, "", LangEnglish)
	defer engine.cancel()
	engine.SetSessionHostRouter(oldRouter)
	session := engine.sessions.GetOrCreateActive(key)
	state := &interactiveState{platform: platform, replyCtx: key, agentSession: agentSession,
		sessionHostRouteGeneration: route.Generation}
	engine.startUnsolicitedReader(state, session, engine.sessions, key, "")

	newNow := time.Unix(nowUnix.Load()+60, 0)
	newRouter := NewSessionHostRouter(dataDir, "project-a", filepath.Join(t.TempDir(), "new.sock"))
	newRouter.now = func() time.Time { return newNow }
	newRouter.lease = time.Minute
	if _, err := newRouter.RegisterRoute(key); err != nil {
		t.Fatal(err)
	}
	events <- Event{Type: EventText, Content: "must-not-leak"}
	events <- Event{Type: EventResult, Content: "must-not-leak", Done: true}
	time.Sleep(80 * time.Millisecond)
	if sent := platform.getSent(); len(sent) != 0 {
		t.Fatalf("stale owner relayed unsolicited output: %#v", sent)
	}
}

func TestSessionHostRouterForwardsInteractionToOwningAPIServer(t *testing.T) {
	dataDir := t.TempDir()
	socketDir, err := os.MkdirTemp("/tmp", "cc-route-test-")
	if err != nil {
		t.Fatalf("MkdirTemp() error = %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	ownerSocket := filepath.Join(socketDir, "owner.sock")
	const key = "test:thread-owner"

	platform := &routedTestPlatform{name: "test"}
	agentSession := &recordingAgentSession{}
	engine := NewEngine("project-a", &stubAgent{}, []Platform{platform}, "", LangEnglish)
	state := &interactiveState{agentSession: agentSession, platform: platform, replyCtx: key,
		pending: &pendingPermission{RequestID: "req-1", ToolInput: map[string]any{"command": "pwd"}, Resolved: make(chan struct{})}}
	engine.interactiveStates[key] = state

	server, err := NewAPIServerAt(dataDir, ownerSocket)
	if err != nil {
		t.Fatalf("NewAPIServerAt() error = %v", err)
	}
	server.RegisterEngine("project-a", engine)
	server.Start()
	defer server.Stop()

	owner := NewSessionHostRouter(dataDir, "project-a", ownerSocket)
	if err := owner.Register(key); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	other := NewSessionHostRouter(dataDir, "project-a", filepath.Join(t.TempDir(), "other.sock"))
	route, err := other.Lookup(key)
	if err != nil {
		t.Fatalf("Lookup() error = %v", err)
	}
	outcome, err := other.Forward(context.Background(), route, &Message{
		SessionKey: key, Platform: "test", Content: "allow",
		IsPermissionResponse: true, IsInteractionResponse: true, InteractionRequestID: "req-1",
	})
	if err != nil {
		t.Fatalf("Forward() error = %v", err)
	}
	if !outcome.Accepted || agentSession.calls != 1 || agentSession.lastID != "req-1" {
		t.Fatalf("outcome=%#v calls=%d id=%q", outcome, agentSession.calls, agentSession.lastID)
	}
}

func TestSessionHostRouterKeepsConcurrentHostInteractionsOnTheirOwningProcesses(t *testing.T) {
	dataDir := t.TempDir()
	socketDir, err := os.MkdirTemp("/tmp", "cc-interaction-route-")
	if err != nil {
		t.Fatalf("MkdirTemp() error = %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })

	const (
		keyA     = "feishu:oc_chat:root:thread-a"
		keyB     = "feishu:oc_chat:root:thread-b"
		requestA = "permission-shared-id"
		requestB = "permission-shared-id"
		hostA    = "java-session-a"
		hostB    = "java-session-b"
	)

	newProcess := func(socket, key, requestID, hostSessionID string) (
		*Engine, *SessionHostRouter, *recordingHostAgentSession, *APIServer,
	) {
		platform := &routedTestPlatform{name: "feishu"}
		agentSession := &recordingHostAgentSession{hostSessionID: hostSessionID}
		engine := NewEngine("project-a", &stubAgent{}, []Platform{platform}, "", LangEnglish)
		router := NewSessionHostRouter(dataDir, "project-a", socket)
		engine.SetSessionHostRouter(router)
		route, registerErr := router.RegisterRoute(key)
		if registerErr != nil {
			t.Fatalf("RegisterRoute(%s) error = %v", key, registerErr)
		}
		engine.interactiveStates[key] = &interactiveState{
			agentSession: agentSession, platform: platform, replyCtx: key,
			sessionHostRouteKey: key, sessionHostRouteGeneration: route.Generation,
			pending: &pendingPermission{
				RequestID: requestID, HostSessionID: hostSessionID,
				ToolInput: map[string]any{"command": "pwd"}, Resolved: make(chan struct{}),
			},
		}
		if _, registerErr = router.RegisterInteraction(
			requestID, hostSessionID, key, route.Generation); registerErr != nil {
			t.Fatalf("RegisterInteraction(%s) error = %v", requestID, registerErr)
		}
		server, serverErr := NewAPIServerAt(dataDir, socket)
		if serverErr != nil {
			t.Fatalf("NewAPIServerAt(%s) error = %v", socket, serverErr)
		}
		server.RegisterEngine("project-a", engine)
		server.Start()
		return engine, router, agentSession, server
	}

	engineA, _, sessionA, serverA := newProcess(
		filepath.Join(socketDir, "a.sock"), keyA, requestA, hostA)
	defer serverA.Stop()
	engineB, _, sessionB, serverB := newProcess(
		filepath.Join(socketDir, "b.sock"), keyB, requestB, hostB)
	defer serverB.Stop()

	click := func(receiver *Engine, key, requestID, hostSessionID string) InteractionOutcome {
		result := make(chan InteractionOutcome, 1)
		receiver.handleMessage(&routedTestPlatform{name: "feishu"}, &Message{
			SessionKey: key, Platform: "feishu", Content: "allow",
			IsPermissionResponse: true, IsInteractionResponse: true,
			InteractionRequestID: requestID, InteractionSessionID: hostSessionID,
			InteractionResult: result,
		})
		select {
		case outcome := <-result:
			return outcome
		case <-time.After(2 * time.Second):
			t.Fatalf("interaction %s timed out", requestID)
			return InteractionOutcome{}
		}
	}

	if outcome := click(engineA, keyB, requestB, hostA); !outcome.Stale {
		t.Fatalf("mismatched Java session outcome = %#v, want stale", outcome)
	}
	if sessionA.calls != 0 || sessionB.calls != 0 {
		t.Fatalf("mismatched Java session reached an agent: A=%d B=%d",
			sessionA.calls, sessionB.calls)
	}

	if outcome := click(engineA, keyB, requestB, hostB); !outcome.Accepted {
		t.Fatalf("process A receiving process B click outcome = %#v", outcome)
	}
	if sessionA.calls != 0 || sessionB.calls != 1 || sessionB.lastID != requestB {
		t.Fatalf("B click crossed sessions: A calls=%d, B calls=%d id=%q",
			sessionA.calls, sessionB.calls, sessionB.lastID)
	}

	if outcome := click(engineB, keyA, requestA, hostA); !outcome.Accepted {
		t.Fatalf("process B receiving process A click outcome = %#v", outcome)
	}
	if sessionA.calls != 1 || sessionA.lastID != requestA || sessionB.calls != 1 {
		t.Fatalf("A click crossed sessions: A calls=%d id=%q, B calls=%d",
			sessionA.calls, sessionA.lastID, sessionB.calls)
	}
}

func TestSessionHostRouterForwardsCompactToOwningSession(t *testing.T) {
	dataDir := t.TempDir()
	socketDir, err := os.MkdirTemp("/tmp", "cc-compact-route-")
	if err != nil {
		t.Fatalf("MkdirTemp() error = %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	ownerSocket := filepath.Join(socketDir, "owner.sock")
	const key = "test:compact-owner"

	platform := &routedTestPlatform{name: "test"}
	agent := &stubSessionCompactorAgent{}
	engine := NewEngine("project-a", agent, []Platform{platform}, "", LangEnglish)
	session := engine.sessions.GetOrCreateActive(key)
	session.SetAgentSessionID("java-session", "sessionhost")
	server, err := NewAPIServerAt(dataDir, ownerSocket)
	if err != nil {
		t.Fatalf("NewAPIServerAt() error = %v", err)
	}
	server.RegisterEngine("project-a", engine)
	server.Start()
	defer server.Stop()

	owner := NewSessionHostRouter(dataDir, "project-a", ownerSocket)
	if err := owner.Register(key); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	other := NewSessionHostRouter(dataDir, "project-a", filepath.Join(socketDir, "other.sock"))
	route, _ := other.Lookup(key)
	if _, err := other.Forward(context.Background(), route, &Message{
		SessionKey: key, Platform: "test", UserID: "admin", Content: "/compact keep decisions",
	}); err != nil {
		t.Fatalf("Forward(/compact) error = %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		sessionID, instructions := agent.compactCall()
		if sessionID == "java-session" && instructions == "keep decisions" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("compact target=%q instructions=%q", sessionID, instructions)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSessionHostRouterForwardsCardActionToOwningProcess(t *testing.T) {
	dataDir := t.TempDir()
	socketDir, err := os.MkdirTemp("/tmp", "cc-card-route-")
	if err != nil {
		t.Fatalf("MkdirTemp() error = %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	ownerSocket := filepath.Join(socketDir, "owner.sock")
	otherSocket := filepath.Join(socketDir, "other.sock")
	const key = "feishu:oc_chat:root:om_effort_owner"

	ownerAgent := &stubSessionEffortAgent{current: "auto"}
	ownerEngine := NewEngine("project-a", ownerAgent, nil, "", LangEnglish)
	ownerEngine.sessions.GetOrCreateActive(key).SetAgentSessionID("java-session", "sessionhost")
	ownerRouter := NewSessionHostRouter(dataDir, "project-a", ownerSocket)
	ownerEngine.SetSessionHostRouter(ownerRouter)

	server, err := NewAPIServerAt(dataDir, ownerSocket)
	if err != nil {
		t.Fatalf("NewAPIServerAt() error = %v", err)
	}
	server.RegisterEngine("project-a", ownerEngine)
	server.Start()
	defer server.Stop()
	if err := ownerRouter.Register(key); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	otherAgent := &stubSessionEffortAgent{current: "auto"}
	otherEngine := NewEngine("project-a", otherAgent, nil, "", LangEnglish)
	otherEngine.sessions.GetOrCreateActive(key).SetAgentSessionID("java-session", "sessionhost")
	otherEngine.SetSessionHostRouter(NewSessionHostRouter(dataDir, "project-a", otherSocket))

	card := otherEngine.handleCardNav("act:/reasoning 2", key)

	if ownerAgent.current != "low" || ownerAgent.setCalls != 1 {
		t.Fatalf("owner effort current=%q setCalls=%d, want low/1", ownerAgent.current, ownerAgent.setCalls)
	}
	if otherAgent.current != "auto" || otherAgent.setCalls != 0 {
		t.Fatalf("non-owner effort current=%q setCalls=%d, must remain auto/0", otherAgent.current, otherAgent.setCalls)
	}
	if card == nil || !strings.Contains(card.RenderText(), "low") {
		t.Fatalf("forwarded effort card = %#v", card)
	}
}

func TestSessionHostRouterForwardsResumeCardActionToOwningProcess(t *testing.T) {
	dataDir := t.TempDir()
	socketDir, err := os.MkdirTemp("/tmp", "cc-resume-route-")
	if err != nil {
		t.Fatalf("MkdirTemp() error = %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	ownerSocket := filepath.Join(socketDir, "owner.sock")
	otherSocket := filepath.Join(socketDir, "other.sock")
	const (
		key      = "feishu:oc_chat:root:om_resume_owner"
		oldID    = "java-session-old"
		targetID = "java-session-target"
	)

	oldRemote := newResumeHostTestSession(oldID)
	targetRemote := newResumeHostTestSession(targetID)
	ownerAgent := &resumeHostTestAgent{
		events: make(chan HostSessionLifecycle),
		sessions: []AgentSessionInfo{
			resumeSessionInfo(oldID, "Current", 2),
			resumeSessionInfo(targetID, "Target", 8),
		},
		prepared: map[string]*resumeHostTestSession{targetID: targetRemote},
		gens:     map[string]uint64{targetID: 15},
	}
	ownerEngine := NewEngine("project-a", ownerAgent, nil, "", LangEnglish)
	defer ownerEngine.Stop()
	ownerEngine.sessions.GetOrCreateActive(key).SetAgentSessionID(oldID, ownerAgent.Name())
	ownerEngine.interactiveStates[key] = &interactiveState{agentSession: oldRemote}
	ownerRouter := NewSessionHostRouter(dataDir, "project-a", ownerSocket)
	ownerEngine.SetSessionHostRouter(ownerRouter)

	server, err := NewAPIServerAt(dataDir, ownerSocket)
	if err != nil {
		t.Fatalf("NewAPIServerAt() error = %v", err)
	}
	server.RegisterEngine("project-a", ownerEngine)
	server.Start()
	defer server.Stop()
	if err := ownerRouter.Register(key); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	otherAgent := &resumeHostTestAgent{events: make(chan HostSessionLifecycle)}
	otherEngine := NewEngine("project-a", otherAgent, nil, "", LangEnglish)
	defer otherEngine.Stop()
	otherEngine.sessions.GetOrCreateActive(key).SetAgentSessionID(oldID, otherAgent.Name())
	otherEngine.SetSessionHostRouter(NewSessionHostRouter(dataDir, "project-a", otherSocket))

	card := otherEngine.handleCardNav("act:/resume "+targetID, key)
	if card == nil || !strings.Contains(card.RenderText(), "Resumed") {
		t.Fatalf("forwarded resume card = %#v", card)
	}
	if got := ownerEngine.sessions.GetOrCreateActive(key).GetAgentSessionID(); got != targetID {
		t.Fatalf("owner active session = %q", got)
	}
	if ownerAgent.callCount() != 1 || otherAgent.callCount() != 0 {
		t.Fatalf("resume calls owner=%d receiver=%d",
			ownerAgent.callCount(), otherAgent.callCount())
	}
	if !oldRemote.isClosed() || targetRemote.isClosed() {
		t.Fatalf("owner attachments old=%v target=%v",
			oldRemote.isClosed(), targetRemote.isClosed())
	}
}

func TestSessionHostRouterForwardsCardActionWithoutActivatingReceivingProcess(t *testing.T) {
	dataDir := t.TempDir()
	socketDir, err := os.MkdirTemp("/tmp", "cc-card-route-")
	if err != nil {
		t.Fatalf("MkdirTemp() error = %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	ownerSocket := filepath.Join(socketDir, "owner.sock")
	otherSocket := filepath.Join(socketDir, "other.sock")
	const key = "feishu:oc_chat:root:om_owner"
	const sessionID = "java-session-owner"

	ownerAgent := &stubSessionModelAgent{states: map[string]SessionModelState{
		sessionID: {Current: "old", Models: []ModelOption{{Name: "new-model"}}},
	}}
	ownerEngine := NewEngine("project-a", ownerAgent, nil, "", LangEnglish)
	ownerEngine.sessions.GetOrCreateActive(key).SetAgentSessionID(sessionID, "sessionhost")
	ownerRouter := NewSessionHostRouter(dataDir, "project-a", ownerSocket)
	ownerEngine.SetSessionHostRouter(ownerRouter)

	server, err := NewAPIServerAt(dataDir, ownerSocket)
	if err != nil {
		t.Fatalf("NewAPIServerAt() error = %v", err)
	}
	server.RegisterEngine("project-a", ownerEngine)
	server.Start()
	defer server.Stop()
	if err := ownerRouter.Register(key); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	otherAgent := &stubSessionModelAgent{states: map[string]SessionModelState{
		sessionID: {Current: "wrong-process", Models: []ModelOption{{Name: "new-model"}}},
	}}
	otherEngine := NewEngine("project-a", otherAgent, nil, "", LangEnglish)
	// Both processes read the same persisted cc-connect session mapping. Before
	// cross-process card routing, this is enough for the callback receiver to
	// open the owner's Java session ID against its own TUI.
	otherEngine.sessions.GetOrCreateActive(key).SetAgentSessionID(sessionID, "sessionhost")
	otherEngine.SetSessionHostRouter(NewSessionHostRouter(dataDir, "project-a", otherSocket))

	card := otherEngine.handleCardNav("act:/model new-model", key)
	if card == nil || !strings.Contains(card.RenderText(), "new-model") {
		t.Fatalf("card = %#v", card)
	}
	buttons := card.CollectButtons()
	if len(buttons) != 1 || len(buttons[0]) != 1 || buttons[0][0].Data != "nav:/model" {
		t.Fatalf("routed result card buttons = %#v", buttons)
	}
	ownerAgent.mu.Lock()
	ownerSet, ownerModel := ownerAgent.lastSet, ownerAgent.lastName
	ownerAgent.mu.Unlock()
	otherAgent.mu.Lock()
	otherSet := otherAgent.lastSet
	otherAgent.mu.Unlock()
	if ownerSet != sessionID || ownerModel != "new-model" {
		t.Fatalf("owner set session=%q model=%q", ownerSet, ownerModel)
	}
	if otherSet != "" {
		t.Fatalf("receiving process activated owner session %q", otherSet)
	}
}

func TestSessionHostRouterCorruptRouteDoesNotMutateReceivingProcess(t *testing.T) {
	dataDir := t.TempDir()
	const key = "feishu:oc_chat:root:om_corrupt_route"
	router := NewSessionHostRouter(dataDir, "project-a", filepath.Join(t.TempDir(), "local.sock"))
	if err := os.MkdirAll(router.routeDir, 0o700); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(router.routePath(key), []byte("{not-json"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	agent := &stubSessionEffortAgent{current: "auto"}
	engine := NewEngine("project-a", agent, nil, "", LangEnglish)
	engine.sessions.GetOrCreateActive(key).SetAgentSessionID("java-session", "sessionhost")
	engine.SetSessionHostRouter(router)

	card := engine.handleCardNav("act:/reasoning 2", key)

	if agent.current != "auto" || agent.setCalls != 0 {
		t.Fatalf("corrupt route mutated receiving process: current=%q setCalls=%d",
			agent.current, agent.setCalls)
	}
	if card == nil || card.Header == nil || card.Header.Color != "red" {
		t.Fatalf("corrupt route card = %#v, want explicit failure", card)
	}
}

func TestHostSessionEndedDoesNotNotifyThreadOwnedByAnotherProcess(t *testing.T) {
	dataDir := t.TempDir()
	const (
		key       = "feishu:oc_chat:root:om_remote_owner"
		sessionID = "java-session-owner"
	)
	ownerRouter := NewSessionHostRouter(
		dataDir, "project-a", filepath.Join(t.TempDir(), "owner.sock"))
	if err := ownerRouter.Register(key); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	platform := &routedRecordingPlatform{routedTestPlatform: routedTestPlatform{name: "feishu"}}
	engine := NewEngine("project-a", &stubAgent{}, []Platform{platform}, "", LangEnglish)
	engine.SetSessionHostRouter(NewSessionHostRouter(
		dataDir, "project-a", filepath.Join(t.TempDir(), "other.sock")))
	engine.sessions.GetOrCreateActive(key).SetAgentSessionID(sessionID, "sessionhost")
	engine.interactiveStates[key] = &interactiveState{
		platform: platform, replyCtx: key, agentSession: &recordingAgentSession{},
	}
	acknowledged := false

	engine.handleHostSessionEnded(HostSessionLifecycle{
		SessionID: sessionID, Ended: true, Reason: "terminal_exit",
		Acknowledge: func() { acknowledged = true },
	})

	if !acknowledged {
		t.Fatal("terminal end was not acknowledged")
	}
	if len(platform.sent) != 0 {
		t.Fatalf("non-owner sent terminal notifications: %#v", platform.sent)
	}
}

type routedTestPlatform struct{ name string }

func (p *routedTestPlatform) Name() string                             { return p.name }
func (p *routedTestPlatform) Start(MessageHandler) error               { return nil }
func (p *routedTestPlatform) Reply(context.Context, any, string) error { return nil }
func (p *routedTestPlatform) Send(context.Context, any, string) error  { return nil }
func (p *routedTestPlatform) Stop() error                              { return nil }
func (p *routedTestPlatform) ReconstructReplyCtx(sessionKey string) (any, error) {
	return sessionKey, nil
}

type routedRecordingPlatform struct {
	routedTestPlatform
	sent []string
}

type mainChatRecordingPlatform struct {
	routedTestPlatform
	mu      sync.Mutex
	replies []string
}

func (p *mainChatRecordingPlatform) Reply(_ context.Context, _ any, content string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.replies = append(p.replies, content)
	return nil
}

type routeOwnedAgentSession struct{ events chan Event }

type recordingHostAgentSession struct {
	recordingAgentSession
	hostSessionID string
}

func (s *recordingHostAgentSession) CurrentSessionID() string { return s.hostSessionID }

func (s *routeOwnedAgentSession) Send(string, string, []ImageAttachment, []FileAttachment) error {
	return nil
}
func (s *routeOwnedAgentSession) RespondPermission(string, PermissionResult) error { return nil }
func (s *routeOwnedAgentSession) Events() <-chan Event                             { return s.events }
func (s *routeOwnedAgentSession) CurrentSessionID() string                         { return "host-session" }
func (s *routeOwnedAgentSession) Alive() bool                                      { return true }
func (s *routeOwnedAgentSession) Close() error                                     { return nil }

func (p *routedRecordingPlatform) Send(_ context.Context, _ any, content string) error {
	p.sent = append(p.sent, content)
	return nil
}
