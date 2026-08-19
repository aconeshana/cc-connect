package sessionhost

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

func TestNewRequiresTokenFromEnvironment(t *testing.T) {
	t.Setenv(defaultAuthTokenEnv, "")
	_, err := New(map[string]any{"endpoint": "/tmp/session-link.sock"})
	if err == nil || !strings.Contains(err.Error(), defaultAuthTokenEnv) {
		t.Fatalf("New error = %v, want missing token environment error", err)
	}
}

func TestNewValidatesLocalEndpointAndLimits(t *testing.T) {
	t.Setenv(defaultAuthTokenEnv, "secret")
	tests := []struct {
		name string
		opts map[string]any
		want string
	}{
		{name: "missing endpoint", opts: map[string]any{}, want: "endpoint is required"},
		{name: "tcp rejected", opts: map[string]any{"endpoint": "tcp://127.0.0.1:9000"}, want: "only local"},
		{name: "relative socket rejected", opts: map[string]any{"endpoint": "run/session.sock"}, want: "absolute path"},
		{name: "bad token env", opts: map[string]any{"endpoint": "/tmp/session.sock", "auth_token_env": "BAD-NAME"}, want: "invalid auth_token_env"},
		{name: "small frame limit", opts: map[string]any{"endpoint": "/tmp/session.sock", "max_frame_bytes": 100}, want: "max_frame_bytes"},
		{name: "fractional timeout", opts: map[string]any{"endpoint": "/tmp/session.sock", "request_timeout_seconds": 1.5}, want: "must be an integer"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := New(test.opts)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("New error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestNewUsesUnixEndpointAndWorkDir(t *testing.T) {
	t.Setenv("SESSION_HOST_TEST_TOKEN", "secret")
	raw, err := New(map[string]any{
		"endpoint":        "unix:///tmp/session.sock",
		"auth_token_env":  "SESSION_HOST_TEST_TOKEN",
		"work_dir":        "/workspace/a",
		"max_frame_bytes": int64(8192),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	agent := raw.(*Agent)
	if agent.Name() != "sessionhost" || agent.cfg.network != "unix" || agent.cfg.address != "/tmp/session.sock" || agent.GetWorkDir() != "/workspace/a" {
		t.Fatalf("agent config = %#v", agent.cfg)
	}
	agent.SetWorkDir("/workspace/b")
	if got := agent.GetWorkDir(); got != "/workspace/b" {
		t.Fatalf("GetWorkDir = %q", got)
	}
}

func TestNewAllowsJavaRuntimeEnvironmentToInjectEndpointAndWorkDir(t *testing.T) {
	t.Setenv("SESSION_HOST_TEST_TOKEN", "secret")
	t.Setenv("CC_SESSION_LINK_ENDPOINT", "unix:///tmp/injected-session.sock")
	t.Setenv("CC_SESSION_WORK_DIR", "/workspace/injected")
	raw, err := New(map[string]any{
		"auth_token_env":   "SESSION_HOST_TEST_TOKEN",
		"bind_session_key": "feishu:oc_chat:ou_owner",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	agent := raw.(*Agent)
	if agent.cfg.address != "/tmp/injected-session.sock" || agent.GetWorkDir() != "/workspace/injected" {
		t.Fatalf("injected config = %#v", agent.cfg)
	}
	if agent.HostSessionBindingTarget() != "feishu:oc_chat:ou_owner" {
		t.Fatalf("binding target = %q", agent.HostSessionBindingTarget())
	}
	channels := agent.HostSessionCollaborationChannels()
	if !reflect.DeepEqual(channels, []string{"feishu"}) {
		t.Fatalf("collaboration channels = %#v", channels)
	}
	if got := agent.HostSessionBindingTargetFor("feishu"); got != "feishu:oc_chat:ou_owner" {
		t.Fatalf("feishu binding target = %q", got)
	}
}

func TestNewParsesMultipleCollaborationTargets(t *testing.T) {
	t.Setenv(defaultAuthTokenEnv, "secret")
	raw, err := New(map[string]any{
		"endpoint": "/tmp/session.sock",
		"collaboration_targets": map[string]any{
			"slack":  "slack:C123:U1",
			"feishu": "feishu:oc_chat:ou_owner",
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	agent := raw.(*Agent)
	if got := agent.HostSessionCollaborationChannels(); !reflect.DeepEqual(got, []string{"feishu", "slack"}) {
		t.Fatalf("channels = %#v", got)
	}
	if agent.HostSessionBindingTargetFor("slack") != "slack:C123:U1" {
		t.Fatalf("slack target = %q", agent.HostSessionBindingTargetFor("slack"))
	}
}

func TestCollaborationChangedAcceptsCompleteHostSessionMetadata(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &linkClient{
		ctx:           ctx,
		cancel:        cancel,
		sessions:      make(map[string]*Session),
		earlyEvents:   make(map[string][]frame),
		collaboration: make(chan core.HostSessionCollaboration, 1),
	}
	payload, err := json.Marshal(map[string]any{
		"id":            "session-1",
		"work_dir":      "/workspace/project",
		"summary":       "Fix collaboration",
		"message_count": 3,
		"modified_at":   "2026-08-12T17:00:00Z",
		"git_branch":    "main",
		"channel":       "feishu",
		"enabled":       true,
		"origin":        "local",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.deliverEvent(frame{
		Protocol: protocolName, Version: protocolVersion, Kind: frameKindEvent,
		Name: eventCollaborationChanged, SessionID: "session-1", Payload: payload,
	}); err != nil {
		t.Fatalf("deliverEvent: %v", err)
	}
	select {
	case change := <-client.collaboration:
		if change.SessionID != "session-1" || change.Channel != "feishu" || !change.Enabled {
			t.Fatalf("collaboration change = %#v", change)
		}
	case <-time.After(time.Second):
		t.Fatal("collaboration change was not delivered")
	}
}

func TestSessionEndedIsDeliveredAndAcknowledgedAfterHandling(t *testing.T) {
	clientConn, hostConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = hostConn.Close()
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &linkClient{
		codec:        newFrameCodec(clientConn, defaultMaxFrameBytes),
		ctx:          ctx,
		cancel:       cancel,
		hostSessions: make(chan core.HostSessionLifecycle, 1),
	}
	// Java builds session.ended from the same SessionHostInfo payload as
	// session.activated, so a normal terminal exit includes modified_at.
	payload, err := json.Marshal(map[string]any{
		"id": "session-1", "work_dir": "/workspace", "origin": "local",
		"reason": "terminal_exit", "modified_at": "2026-08-14T09:26:00Z",
		"notification_id": "end-notification-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.deliverEvent(frame{
		Protocol: protocolName, Version: protocolVersion, Kind: frameKindEvent,
		Name: eventSessionEnded, SessionID: "session-1", Payload: payload,
	}); err != nil {
		t.Fatalf("deliverEvent: %v", err)
	}
	event := <-client.hostSessions
	if !event.Ended || event.SessionID != "session-1" || event.Reason != "terminal_exit" {
		t.Fatalf("terminal-end event = %#v", event)
	}
	acknowledged := make(chan frame, 1)
	go func() {
		value, _ := newFrameCodec(hostConn, defaultMaxFrameBytes).read()
		acknowledged <- value
	}()
	event.Acknowledge()
	select {
	case ack := <-acknowledged:
		if ack.Name != "session.ended.ack" || ack.SessionID != "session-1" || ack.Kind != frameKindRequest {
			t.Fatalf("terminal-end acknowledgement = %#v", ack)
		}
		var body struct {
			NotificationID string `json:"notification_id"`
		}
		if err := json.Unmarshal(ack.Payload, &body); err != nil || body.NotificationID != "end-notification-1" {
			t.Fatalf("terminal-end acknowledgement payload = %s, err=%v", ack.Payload, err)
		}
	case <-time.After(time.Second):
		t.Fatal("terminal-end acknowledgement was not written")
	}
}

func TestAgentSessionLifecycleOverSessionLink(t *testing.T) {
	clientConn, hostConn := net.Pipe()
	t.Cleanup(func() { _ = hostConn.Close() })

	agent := newAgent(agentConfig{
		endpoint:  "/tmp/session-link-test.sock",
		authToken: "test-secret",
		workDir:   "/workspace/project",
	}, func(context.Context, string, string) (net.Conn, error) {
		return clientConn, nil
	})

	hostErr := make(chan error, 1)
	go func() {
		hostErr <- runLifecycleHost(hostConn)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rawSession, err := agent.StartSession(ctx, "resume-me")
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	session := rawSession.(*Session)
	if got := session.CurrentSessionID(); got != "host-session-1" {
		t.Fatalf("CurrentSessionID = %q, want host-session-1", got)
	}

	if err := session.Send("fix the tests", "om_456", nil, nil); err != nil {
		t.Fatalf("Send: %v", err)
	}

	textEvent := receiveEvent(t, session.Events())
	if textEvent.Type != core.EventText || textEvent.Content != "working" || textEvent.SessionID != "host-session-1" {
		t.Fatalf("text event = %#v", textEvent)
	}

	interactionEvent := receiveEvent(t, session.Events())
	if interactionEvent.Type != core.EventPermissionRequest || interactionEvent.RequestID != "ask-1" ||
		interactionEvent.ToolName != "AskUserQuestion" || len(interactionEvent.Questions) != 1 ||
		interactionEvent.Questions[0].Options[0].Label != "Proceed" {
		t.Fatalf("interaction event = %#v", interactionEvent)
	}

	if err := session.RespondPermission("ask-1", core.PermissionResult{
		Behavior:     "allow",
		UpdatedInput: map[string]any{"answers": map[string]any{"Continue?": "Proceed"}},
	}); err != nil {
		t.Fatalf("RespondPermission: %v", err)
	}

	resultEvent := receiveEvent(t, session.Events())
	if resultEvent.Type != core.EventResult || !resultEvent.Done || resultEvent.OutputTokens != 7 {
		t.Fatalf("result event = %#v", resultEvent)
	}

	if err := session.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if session.Alive() {
		t.Fatal("session remains alive after Close")
	}
	if err := agent.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	select {
	case err := <-hostErr:
		if err != nil {
			t.Fatalf("fake host: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fake host did not finish")
	}
}

func TestAgentListSessionsMapsHostMetadata(t *testing.T) {
	clientConn, hostConn := net.Pipe()
	t.Cleanup(func() { _ = hostConn.Close() })

	agent := newAgent(agentConfig{
		endpoint:  "/tmp/session-link-test.sock",
		authToken: "test-secret",
		workDir:   "/workspace/project",
	}, func(context.Context, string, string) (net.Conn, error) {
		return clientConn, nil
	})

	hostErr := make(chan error, 1)
	go func() {
		codec := newFrameCodec(hostConn, defaultMaxFrameBytes)
		if err := acceptHello(codec); err != nil {
			hostErr <- err
			return
		}
		req, err := codec.read()
		if err != nil {
			hostErr <- err
			return
		}
		if req.Name != messageSessionList {
			hostErr <- unexpectedFrame(req, messageSessionList)
			return
		}
		modified := time.Date(2026, 8, 11, 12, 30, 0, 0, time.UTC)
		hostErr <- writeResponse(codec, req, sessionListResult{Sessions: []wireSessionInfo{{
			ID:           "session-a",
			Summary:      "Implement IM bridge",
			MessageCount: 12,
			ModifiedAt:   modified.Format(time.RFC3339Nano),
			GitBranch:    "codex/session-host",
		}}})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sessions, err := agent.ListSessions(ctx)
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(sessions) != 1 || sessions[0].ID != "session-a" || sessions[0].MessageCount != 12 ||
		sessions[0].GitBranch != "codex/session-host" || sessions[0].ModifiedAt.IsZero() {
		t.Fatalf("sessions = %#v", sessions)
	}
	_ = agent.Stop()

	if err := <-hostErr; err != nil {
		t.Fatalf("fake host: %v", err)
	}
}

func TestResumeHostSessionCarriesCausalActivationMetadata(t *testing.T) {
	clientConn, hostConn := net.Pipe()
	t.Cleanup(func() { _ = hostConn.Close() })
	agent := newAgent(agentConfig{
		endpoint: "/tmp/session-link-test.sock", authToken: "test-secret",
		workDir: "/workspace/project", requestTimeout: 5 * time.Second,
	}, func(context.Context, string, string) (net.Conn, error) {
		return clientConn, nil
	})
	defer agent.Stop()

	hostErr := make(chan error, 1)
	go func() {
		codec := newFrameCodec(hostConn, defaultMaxFrameBytes)
		if err := acceptHello(codec); err != nil {
			hostErr <- err
			return
		}
		open, err := codec.read()
		if err != nil {
			hostErr <- err
			return
		}
		payload, err := decodePayload[sessionOpenPayload](open.Payload)
		if err != nil || open.Name != messageSessionOpen ||
			payload.RequestedSessionID != "session-target" ||
			payload.ActivationID != "activation-go-1" {
			hostErr <- unexpectedPayload(payload)
			return
		}
		if err := writeResponse(codec, open, sessionOpenResult{
			SessionID: "session-target", ActivationID: "activation-go-1",
			ActivationGeneration: 42,
		}); err != nil {
			hostErr <- err
			return
		}
		detach, err := codec.read()
		if err != nil {
			hostErr <- err
			return
		}
		if detach.Name != messageSessionDetach || detach.SessionID != "session-target" {
			hostErr <- unexpectedFrame(detach, messageSessionDetach)
			return
		}
		hostErr <- writeResponse(codec, detach, struct{}{})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	activation, err := agent.ResumeHostSession(ctx, "session-target", "activation-go-1")
	if err != nil {
		t.Fatalf("ResumeHostSession: %v", err)
	}
	if activation.SessionID != "session-target" || activation.ActivationID != "activation-go-1" ||
		activation.ActivationGeneration != 42 || activation.Session == nil {
		t.Fatalf("activation = %#v", activation)
	}
	if err := activation.Session.Close(); err != nil {
		t.Fatalf("Close resumed session: %v", err)
	}
	if err := <-hostErr; err != nil {
		t.Fatalf("fake host: %v", err)
	}
}

func TestResumeHostSessionRejectsUntrustedIdentifiersBeforeDial(t *testing.T) {
	agent := newAgent(agentConfig{endpoint: "/tmp/test.sock", authToken: "test-secret"},
		func(context.Context, string, string) (net.Conn, error) {
			t.Fatal("invalid resume input reached the Session Link dialer")
			return nil, errors.New("unexpected dial")
		})
	for _, test := range []struct {
		sessionID    string
		activationID string
	}{
		{sessionID: "", activationID: "activation-1"},
		{sessionID: "session\nother", activationID: "activation-1"},
		{sessionID: "session", activationID: "bad activation"},
		{sessionID: "session", activationID: strings.Repeat("x", 129)},
	} {
		if _, err := agent.ResumeHostSession(
			context.Background(), test.sessionID, test.activationID,
		); err == nil {
			t.Fatalf("ResumeHostSession(%q, %q) unexpectedly succeeded",
				test.sessionID, test.activationID)
		}
	}
}

func TestSessionActivatedLifecyclePreservesGenerationAndActivationID(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &linkClient{
		ctx: ctx, cancel: cancel, sessions: make(map[string]*Session),
		earlyEvents: make(map[string][]frame), hostSessions: make(chan core.HostSessionLifecycle, 1),
	}
	payload, err := json.Marshal(sessionActivatedPayload{
		ID: "session-2", Origin: "local", ActivationID: "activation-local-2",
		ActivationGeneration: 77,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.deliverEvent(frame{
		Protocol: protocolName, Version: protocolVersion, Kind: frameKindEvent,
		Name: eventSessionActivated, SessionID: "session-2", Payload: payload,
	}); err != nil {
		t.Fatalf("deliverEvent: %v", err)
	}
	event := <-client.hostSessions
	if event.SessionID != "session-2" || event.ActivationID != "activation-local-2" ||
		event.ActivationGeneration != 77 {
		t.Fatalf("lifecycle = %#v", event)
	}
}

func TestSessionActivatedLifecycleCapturesPreviousActiveSession(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &linkClient{
		ctx: ctx, cancel: cancel, sessions: make(map[string]*Session),
		earlyEvents: make(map[string][]frame), hostSessions: make(chan core.HostSessionLifecycle, 1),
		activeSession: "session-before-resume", activeActivationGeneration: 76,
	}
	payload, err := json.Marshal(sessionActivatedPayload{
		ID: "session-after-resume", Origin: "local", ActivationGeneration: 77,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.deliverEvent(frame{
		Protocol: protocolName, Version: protocolVersion, Kind: frameKindEvent,
		Name: eventSessionActivated, SessionID: "session-after-resume", Payload: payload,
	}); err != nil {
		t.Fatalf("deliverEvent: %v", err)
	}
	event := <-client.hostSessions
	if event.PreviousSessionID != "session-before-resume" {
		t.Fatalf("previous session = %q", event.PreviousSessionID)
	}
}

func TestSessionUpdatedLifecycleIsMetadataOnly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &linkClient{
		ctx: ctx, cancel: cancel, sessions: make(map[string]*Session),
		earlyEvents: make(map[string][]frame), hostSessions: make(chan core.HostSessionLifecycle, 1),
	}
	payload, err := json.Marshal(sessionUpdatedPayload{
		ID: "session-2", WorkDir: "/workspace/project", Summary: "Readable title", Origin: "local",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.deliverEvent(frame{
		Protocol: protocolName, Version: protocolVersion, Kind: frameKindEvent,
		Name: eventSessionUpdated, SessionID: "session-2", Payload: payload,
	}); err != nil {
		t.Fatalf("deliverEvent: %v", err)
	}
	event := <-client.hostSessions
	if !event.MetadataOnly || event.SessionID != "session-2" || event.Summary != "Readable title" {
		t.Fatalf("metadata lifecycle = %#v", event)
	}
}

func TestSessionModelCapabilityUsesAttachedHostSession(t *testing.T) {
	clientConn, hostConn := net.Pipe()
	t.Cleanup(func() { _ = hostConn.Close() })

	agent := newAgent(agentConfig{
		endpoint: "/tmp/session-link-test.sock", authToken: "test-secret",
		workDir: "/workspace/project", requestTimeout: 5 * time.Second,
	}, func(context.Context, string, string) (net.Conn, error) {
		return clientConn, nil
	})
	defer agent.Stop()

	hostErr := make(chan error, 1)
	go func() {
		codec := newFrameCodec(hostConn, defaultMaxFrameBytes)
		if err := acceptHello(codec); err != nil {
			hostErr <- err
			return
		}
		open, err := codec.read()
		if err != nil {
			hostErr <- err
			return
		}
		if err := writeResponse(codec, open, sessionOpenResult{SessionID: "model-session"}); err != nil {
			hostErr <- err
			return
		}
		reactivate, err := codec.read()
		if err != nil {
			hostErr <- err
			return
		}
		if reactivate.Name != messageSessionOpen {
			hostErr <- unexpectedFrame(reactivate, messageSessionOpen)
			return
		}
		if err := writeResponse(codec, reactivate, sessionOpenResult{SessionID: "model-session"}); err != nil {
			hostErr <- err
			return
		}
		get, err := codec.read()
		if err != nil {
			hostErr <- err
			return
		}
		if get.Name != messageModelGet || get.SessionID != "model-session" {
			hostErr <- unexpectedFrame(get, messageModelGet)
			return
		}
		if err := writeResponse(codec, get, modelStateResult{
			Current: "sonnet",
			Models: []wireModelOption{
				{Name: "sonnet", Label: "Sonnet 4.6", Description: "Balanced"},
				{Name: "opus", Label: "Opus 4.6", Description: "Most capable"},
			},
		}); err != nil {
			hostErr <- err
			return
		}
		set, err := codec.read()
		if err != nil {
			hostErr <- err
			return
		}
		payload, err := decodePayload[modelSetPayload](set.Payload)
		if err != nil || set.Name != messageModelSet || set.SessionID != "model-session" || payload.Model != "opus" {
			hostErr <- unexpectedPayload(payload)
			return
		}
		hostErr <- writeResponse(codec, set, modelStateResult{
			Current: "opus",
			Models:  []wireModelOption{{Name: "sonnet"}, {Name: "opus"}},
		})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	raw, err := agent.StartSession(ctx, "")
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	session := raw.(*Session)
	session.client.setActiveSession("another-session")

	state, err := session.ModelState(ctx)
	if err != nil {
		t.Fatalf("ModelState: %v", err)
	}
	if state.Current != "sonnet" || len(state.Models) != 2 || state.Models[1].Name != "opus" {
		t.Fatalf("model state = %#v", state)
	}
	changed, err := session.SetModel(ctx, "opus")
	if err != nil {
		t.Fatalf("SetModel: %v", err)
	}
	if changed.Current != "opus" || session.GetModel() != "opus" {
		t.Fatalf("changed state = %#v current=%q", changed, session.GetModel())
	}

	if err := <-hostErr; err != nil {
		t.Fatalf("fake host: %v", err)
	}
}

func TestSessionModelCapabilityReactivatesPersistedUnattachedSession(t *testing.T) {
	clientConn, hostConn := net.Pipe()
	t.Cleanup(func() { _ = hostConn.Close() })

	agent := newAgent(agentConfig{
		endpoint: "/tmp/session-link-test.sock", authToken: "test-secret",
		workDir: "/workspace/project", requestTimeout: 5 * time.Second,
	}, func(context.Context, string, string) (net.Conn, error) {
		return clientConn, nil
	})
	agent.SetSessionEnv([]string{"CC_SESSION_KEY=feishu:thread-42"})
	defer agent.Stop()

	hostErr := make(chan error, 1)
	go func() {
		codec := newFrameCodec(hostConn, defaultMaxFrameBytes)
		if err := acceptHello(codec); err != nil {
			hostErr <- err
			return
		}
		open, err := codec.read()
		if err != nil {
			hostErr <- err
			return
		}
		payload, err := decodePayload[sessionOpenPayload](open.Payload)
		if err != nil || open.Name != messageSessionOpen ||
			payload.RequestedSessionID != "persisted-session" ||
			payload.WorkDir != "/workspace/project" || payload.CollaborationChannel != "feishu" {
			hostErr <- unexpectedPayload(payload)
			return
		}
		if err := writeResponse(codec, open, sessionOpenResult{SessionID: "persisted-session"}); err != nil {
			hostErr <- err
			return
		}
		set, err := codec.read()
		if err != nil {
			hostErr <- err
			return
		}
		setPayload, err := decodePayload[modelSetPayload](set.Payload)
		if err != nil || set.Name != messageModelSet || set.SessionID != "persisted-session" ||
			setPayload.Model != "opus" {
			hostErr <- unexpectedPayload(setPayload)
			return
		}
		hostErr <- writeResponse(codec, set, modelStateResult{
			Current: "opus",
			Models:  []wireModelOption{{Name: "sonnet"}, {Name: "opus", Alias: "opus"}},
		})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	state, err := agent.SetSessionModel(ctx, "persisted-session", "opus")
	if err != nil {
		t.Fatalf("SetSessionModel: %v", err)
	}
	if state.Current != "opus" || len(state.Models) != 2 || state.Models[1].Alias != "opus" {
		t.Fatalf("state = %#v", state)
	}
	if got := agent.client.attachedSession("persisted-session"); got != nil {
		t.Fatalf("unattached model call registered session = %#v", got)
	}
	if err := <-hostErr; err != nil {
		t.Fatalf("fake host: %v", err)
	}
}

func TestSessionEffortCapabilityUsesAddressedLiveSession(t *testing.T) {
	clientConn, hostConn := net.Pipe()
	t.Cleanup(func() { _ = hostConn.Close() })
	agent := newAgent(agentConfig{
		endpoint: "/tmp/session-link-test.sock", authToken: "test-secret",
		workDir: "/workspace/project", requestTimeout: 5 * time.Second,
	}, func(context.Context, string, string) (net.Conn, error) {
		return clientConn, nil
	})
	defer agent.Stop()

	hostErr := make(chan error, 1)
	go func() {
		codec := newFrameCodec(hostConn, defaultMaxFrameBytes)
		if err := acceptHello(codec); err != nil {
			hostErr <- err
			return
		}
		open, err := codec.read()
		if err != nil {
			hostErr <- err
			return
		}
		if err := writeResponse(codec, open, sessionOpenResult{SessionID: "session-effort"}); err != nil {
			hostErr <- err
			return
		}
		set, err := codec.read()
		if err != nil {
			hostErr <- err
			return
		}
		var payload map[string]string
		if err := json.Unmarshal(set.Payload, &payload); err != nil || set.Name != "effort.set" ||
			set.SessionID != "session-effort" || payload["effort"] != "none" {
			hostErr <- unexpectedPayload(payload)
			return
		}
		hostErr <- writeResponse(codec, set, map[string]any{
			"current": "none", "effective": "none",
			"efforts": []string{"auto", "none", "minimal", "low", "medium", "high", "xhigh", "max"},
		})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	state, err := agent.SetSessionReasoningEffort(ctx, "session-effort", "none")
	if err != nil {
		t.Fatalf("SetSessionReasoningEffort: %v", err)
	}
	if state.Current != "none" || state.Effective != "none" || len(state.Efforts) != 8 {
		t.Fatalf("state = %#v", state)
	}
	if err := <-hostErr; err != nil {
		t.Fatalf("fake host: %v", err)
	}
}

func TestSessionCompactCapabilityUsesAddressedLiveSessionAndInstructions(t *testing.T) {
	clientConn, hostConn := net.Pipe()
	t.Cleanup(func() { _ = hostConn.Close() })
	agent := newAgent(agentConfig{
		endpoint: "/tmp/session-link-test.sock", authToken: "test-secret",
		workDir: "/workspace/project", requestTimeout: 5 * time.Second,
	}, func(context.Context, string, string) (net.Conn, error) {
		return clientConn, nil
	})
	defer agent.Stop()

	hostErr := make(chan error, 1)
	go func() {
		codec := newFrameCodec(hostConn, defaultMaxFrameBytes)
		if err := acceptHello(codec); err != nil {
			hostErr <- err
			return
		}
		open, err := codec.read()
		if err != nil {
			hostErr <- err
			return
		}
		if err := writeResponse(codec, open, sessionOpenResult{SessionID: "session-compact"}); err != nil {
			hostErr <- err
			return
		}
		run, err := codec.read()
		if err != nil {
			hostErr <- err
			return
		}
		var payload map[string]string
		if err := json.Unmarshal(run.Payload, &payload); err != nil || run.Name != "compact.run" ||
			run.SessionID != "session-compact" || payload["instructions"] != "keep decisions" {
			hostErr <- unexpectedPayload(payload)
			return
		}
		hostErr <- writeResponse(codec, run, map[string]any{"message": "Compacted"})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := agent.CompactSession(ctx, "session-compact", "keep decisions")
	if err != nil {
		t.Fatalf("CompactSession: %v", err)
	}
	if result.Message != "Compacted" {
		t.Fatalf("result = %#v", result)
	}
	if err := <-hostErr; err != nil {
		t.Fatalf("fake host: %v", err)
	}
}

func TestCompactInstructionsRejectControlCharactersAndOversizeInput(t *testing.T) {
	if _, err := validateCompactInstructions("bad\x00instruction"); err == nil {
		t.Fatal("control character should be rejected")
	}
	if _, err := validateCompactInstructions(strings.Repeat("x", 32*1024+1)); err == nil {
		t.Fatal("oversize instructions should be rejected")
	}
}

func TestSessionLinkMapsAllSemanticEvents(t *testing.T) {
	clientConn, hostConn := net.Pipe()
	t.Cleanup(func() { _ = hostConn.Close() })

	agent := newAgent(agentConfig{
		endpoint:  "/tmp/session-link-test.sock",
		authToken: "test-secret",
		workDir:   "/workspace/project",
	}, func(context.Context, string, string) (net.Conn, error) {
		return clientConn, nil
	})

	hostErr := make(chan error, 1)
	go func() {
		codec := newFrameCodec(hostConn, defaultMaxFrameBytes)
		if err := acceptHello(codec); err != nil {
			hostErr <- err
			return
		}
		req, err := codec.read()
		if err != nil {
			hostErr <- err
			return
		}
		if err := writeResponse(codec, req, sessionOpenResult{SessionID: "semantic-1"}); err != nil {
			hostErr <- err
			return
		}
		events := []struct {
			name    string
			payload any
		}{
			{eventOutputThinking, outputThinkingPayload{Content: "reasoning"}},
			{eventToolStarted, toolStartedPayload{Name: "Read", Input: "README.md", InputRaw: map[string]any{"file_path": "README.md"}}},
			{eventToolCompleted, toolCompletedPayload{Name: "Read", Result: "contents", Status: "completed", Success: boolPtr(true), ExitCode: intPtr(0)}},
			{eventSessionError, sessionErrorPayload{Message: "boom"}},
		}
		for _, event := range events {
			if err := writeEvent(codec, event.name, "semantic-1", event.payload); err != nil {
				hostErr <- err
				return
			}
		}
		detach, err := codec.read()
		if err != nil {
			hostErr <- err
			return
		}
		hostErr <- writeResponse(codec, detach, struct{}{})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rawSession, err := agent.StartSession(ctx, "")
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	session := rawSession.(*Session)

	thinking := receiveEvent(t, session.Events())
	toolStarted := receiveEvent(t, session.Events())
	toolCompleted := receiveEvent(t, session.Events())
	hostError := receiveEvent(t, session.Events())
	if thinking.Type != core.EventThinking || thinking.Content != "reasoning" {
		t.Fatalf("thinking = %#v", thinking)
	}
	if toolStarted.Type != core.EventToolUse || toolStarted.ToolName != "Read" || toolStarted.ToolInputRaw["file_path"] != "README.md" {
		t.Fatalf("tool started = %#v", toolStarted)
	}
	if toolCompleted.Type != core.EventToolResult || toolCompleted.ToolResult != "contents" || toolCompleted.ToolSuccess == nil || !*toolCompleted.ToolSuccess {
		t.Fatalf("tool completed = %#v", toolCompleted)
	}
	if hostError.Type != core.EventError || hostError.Error == nil || !strings.Contains(hostError.Error.Error(), "boom") {
		t.Fatalf("host error = %#v", hostError)
	}

	if err := session.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_ = agent.Stop()
	if err := <-hostErr; err != nil {
		t.Fatalf("fake host: %v", err)
	}
}

func TestMapWireEventPreservesHostTurnAndPermissionContext(t *testing.T) {
	userRaw, _ := json.Marshal(turnStartedPayload{
		DisplayText: "检查下载目录", PermissionMode: "default", Origin: "remote",
		ModelOverride: "claude-opus-4-6", EffortOverride: "high",
	})
	user, known, err := mapWireEvent(frame{
		Protocol: protocolName, Version: protocolVersion, Kind: frameKindEvent,
		Name: eventTurnStarted, SessionID: "s1", Payload: userRaw,
	})
	if err != nil || !known || user.Type != core.EventUserInput || user.Content != "检查下载目录" || user.InputOrigin != "remote" ||
		user.PermissionMode != "default" || user.Model != "claude-opus-4-6" || user.ReasoningEffort != "high" {
		t.Fatalf("turn started event = %#v known=%v err=%v", user, known, err)
	}

	modelRaw, _ := json.Marshal(turnModelPayload{Model: "claude-sonnet-4-5", MessageCount: 4})
	model, known, err := mapWireEvent(frame{
		Protocol: protocolName, Version: protocolVersion, Kind: frameKindEvent,
		Name: eventTurnModel, SessionID: "s1", Payload: modelRaw,
	})
	if err != nil || !known || model.Type != core.EventModel || model.Model != "claude-sonnet-4-5" {
		t.Fatalf("turn model event = %#v known=%v err=%v", model, known, err)
	}

	permissionRaw, _ := json.Marshal(interactionRequestedPayload{
		RequestID: "ask-1", ToolName: "Bash", ToolInput: "rm generated.tmp",
		DecisionReasonType: "rule", DecisionReasonDetail: "Bash(rm:*)",
		DestructiveWarning: "May delete files", BlockedPath: "/private",
		CustomMessage: "Review deletion", ToolDescription: "Runs a shell command",
	})
	permission, known, err := mapWireEvent(frame{
		Protocol: protocolName, Version: protocolVersion, Kind: frameKindEvent,
		Name: eventInteractionRequested, SessionID: "s1", Payload: permissionRaw,
	})
	if err != nil || !known || permission.Type != core.EventPermissionRequest ||
		permission.DecisionReasonType != "rule" || permission.DecisionReasonDetail != "Bash(rm:*)" ||
		permission.DestructiveWarning != "May delete files" || permission.BlockedPath != "/private" ||
		permission.CustomMessage != "Review deletion" || permission.ToolDescription != "Runs a shell command" {
		t.Fatalf("permission event = %#v known=%v err=%v", permission, known, err)
	}
}

func TestSessionSendEncodesAttachments(t *testing.T) {
	clientConn, hostConn := net.Pipe()
	t.Cleanup(func() { _ = hostConn.Close() })

	agent := newAgent(agentConfig{endpoint: "/tmp/test.sock", authToken: "test-secret", workDir: "/workspace"}, func(context.Context, string, string) (net.Conn, error) {
		return clientConn, nil
	})
	hostErr := make(chan error, 1)
	go func() {
		codec := newFrameCodec(hostConn, defaultMaxFrameBytes)
		if err := acceptHello(codec); err != nil {
			hostErr <- err
			return
		}
		open, err := codec.read()
		if err != nil {
			hostErr <- err
			return
		}
		if err := writeResponse(codec, open, sessionOpenResult{SessionID: "attachments-1"}); err != nil {
			hostErr <- err
			return
		}
		turn, err := codec.read()
		if err != nil {
			hostErr <- err
			return
		}
		payload, err := decodePayload[turnSubmitPayload](turn.Payload)
		if err != nil {
			hostErr <- err
			return
		}
		if len(payload.Images) != 1 || len(payload.Attachments) != 1 ||
			!reflect.DeepEqual(payload.Images[0].Data, []byte{1, 2}) ||
			!reflect.DeepEqual(payload.Attachments[0].Data, []byte("hello")) {
			hostErr <- unexpectedPayload(payload)
			return
		}
		if err := writeResponse(codec, turn, struct{}{}); err != nil {
			hostErr <- err
			return
		}
		detach, err := codec.read()
		if err != nil {
			hostErr <- err
			return
		}
		hostErr <- writeResponse(codec, detach, struct{}{})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rawSession, err := agent.StartSession(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	session := rawSession.(*Session)
	err = session.Send("with files", "om-files", []core.ImageAttachment{{MimeType: "image/png", FileName: "a.png", Data: []byte{1, 2}}}, []core.FileAttachment{{MimeType: "text/plain", FileName: "a.txt", Data: []byte("hello")}})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	_ = agent.Stop()
	if err := <-hostErr; err != nil {
		t.Fatalf("fake host: %v", err)
	}
}

func TestHostErrorResponsePropagatesWithoutToken(t *testing.T) {
	clientConn, hostConn := net.Pipe()
	t.Cleanup(func() { _ = hostConn.Close() })
	agent := newAgent(agentConfig{endpoint: "/tmp/test.sock", authToken: "do-not-leak", workDir: "/workspace"}, func(context.Context, string, string) (net.Conn, error) {
		return clientConn, nil
	})

	go func() {
		codec := newFrameCodec(hostConn, defaultMaxFrameBytes)
		req, err := codec.read()
		if err != nil {
			return
		}
		_ = codec.write(frame{
			Protocol: protocolName,
			Version:  protocolVersion,
			Kind:     frameKindError,
			Name:     req.Name,
			ReplyTo:  req.ID,
			Error:    &wireError{Code: "unauthorized", Message: "authentication failed"},
		})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := agent.StartSession(ctx, "")
	if err == nil || !strings.Contains(err.Error(), "authentication failed") || strings.Contains(err.Error(), "do-not-leak") {
		t.Fatalf("StartSession error = %v", err)
	}
}

func TestSessionRejectsUnknownPermissionBehavior(t *testing.T) {
	client := &linkClient{}
	client.alive.Store(true)
	session := &Session{id: "s1", client: client, events: make(chan core.Event, 1)}
	session.alive.Store(true)
	err := session.RespondPermission("r1", core.PermissionResult{Behavior: "maybe"})
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("RespondPermission error = %v", err)
	}
}

func TestSessionMapsStaleInteractionToAlreadyResolved(t *testing.T) {
	clientConn, hostConn := net.Pipe()
	t.Cleanup(func() { _ = hostConn.Close() })
	agent := newAgent(agentConfig{
		endpoint: "/tmp/test.sock", authToken: "test-secret", workDir: "/workspace",
	}, func(context.Context, string, string) (net.Conn, error) {
		return clientConn, nil
	})
	defer agent.Stop()

	hostErr := make(chan error, 1)
	go func() {
		codec := newFrameCodec(hostConn, defaultMaxFrameBytes)
		if err := acceptHello(codec); err != nil {
			hostErr <- err
			return
		}
		openReq, err := codec.read()
		if err != nil {
			hostErr <- err
			return
		}
		if err := writeResponse(codec, openReq, sessionOpenResult{SessionID: "s-stale"}); err != nil {
			hostErr <- err
			return
		}
		respondReq, err := codec.read()
		if err != nil {
			hostErr <- err
			return
		}
		hostErr <- codec.write(frame{
			Protocol: protocolName, Version: protocolVersion, Kind: frameKindError,
			Name: respondReq.Name, ReplyTo: respondReq.ID,
			Error: &wireError{Code: "stale_interaction", Message: "interaction is no longer pending"},
		})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	raw, err := agent.StartSession(ctx, "")
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	err = raw.RespondPermission("req-stale", core.PermissionResult{Behavior: "allow"})
	if !errors.Is(err, core.ErrInteractionAlreadyResolved) {
		t.Fatalf("RespondPermission error = %v, want ErrInteractionAlreadyResolved", err)
	}
	if err := <-hostErr; err != nil {
		t.Fatalf("fake host: %v", err)
	}
}

func TestAgentStopIsIdempotentAndPreventsRestart(t *testing.T) {
	agent := newAgent(agentConfig{endpoint: "/tmp/test.sock", authToken: "secret"}, func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("should not dial")
	})
	if err := agent.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := agent.Stop(); err != nil {
		t.Fatal(err)
	}
	_, err := agent.StartSession(context.Background(), "")
	if err == nil || !strings.Contains(err.Error(), "stopped") {
		t.Fatalf("StartSession error = %v", err)
	}
}

func TestAgentPropagatesDialFailure(t *testing.T) {
	agent := newAgent(agentConfig{endpoint: "/tmp/test.sock", authToken: "secret"}, func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("socket unavailable")
	})
	_, err := agent.StartSession(context.Background(), "")
	if err == nil || !strings.Contains(err.Error(), "socket unavailable") {
		t.Fatalf("StartSession error = %v", err)
	}
}

func TestListSessionsRejectsInvalidTimestamp(t *testing.T) {
	clientConn, hostConn := net.Pipe()
	t.Cleanup(func() { _ = hostConn.Close() })
	agent := newAgent(agentConfig{endpoint: "/tmp/test.sock", authToken: "test-secret"}, func(context.Context, string, string) (net.Conn, error) {
		return clientConn, nil
	})
	go func() {
		codec := newFrameCodec(hostConn, defaultMaxFrameBytes)
		if acceptHello(codec) != nil {
			return
		}
		req, err := codec.read()
		if err != nil {
			return
		}
		_ = writeResponse(codec, req, sessionListResult{Sessions: []wireSessionInfo{{ID: "bad-time", ModifiedAt: "yesterday"}}})
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := agent.ListSessions(ctx)
	if err == nil || !strings.Contains(err.Error(), "modified_at") {
		t.Fatalf("ListSessions error = %v", err)
	}
	_ = agent.Stop()
}

func TestStartSessionRejectsEmptyHostSessionID(t *testing.T) {
	clientConn, hostConn := net.Pipe()
	t.Cleanup(func() { _ = hostConn.Close() })
	agent := newAgent(agentConfig{endpoint: "/tmp/test.sock", authToken: "test-secret"}, func(context.Context, string, string) (net.Conn, error) {
		return clientConn, nil
	})
	go func() {
		codec := newFrameCodec(hostConn, defaultMaxFrameBytes)
		if acceptHello(codec) != nil {
			return
		}
		req, err := codec.read()
		if err != nil {
			return
		}
		_ = writeResponse(codec, req, sessionOpenResult{})
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := agent.StartSession(ctx, "")
	if err == nil || !strings.Contains(err.Error(), "empty session ID") {
		t.Fatalf("StartSession error = %v", err)
	}
	_ = agent.Stop()
}

func TestSessionConcurrentEventAndFinishIsSafe(t *testing.T) {
	client := &linkClient{sessions: make(map[string]*Session)}
	client.alive.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	session := &Session{
		id:     "race-safe",
		client: client,
		ctx:    ctx,
		cancel: cancel,
		events: make(chan core.Event),
	}
	session.alive.Store(true)
	client.sessions[session.id] = session

	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(started)
		session.emit(core.Event{Type: core.EventText, Content: "late"})
		close(done)
	}()
	<-started
	session.finish()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent emit did not exit after finish")
	}
}

func TestNewSessionBuffersBurstWithoutImmediateHostBackpressure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &linkClient{ctx: ctx, workDir: "/project"}
	session := newSession(ctx, client, "s-burst", time.Second)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1024; i++ {
			session.emit(core.Event{Type: core.EventText, Content: "x"})
		}
	}()

	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("a normal output burst backpressured Session Link before the IM consumer ran")
	}
}

func TestRegisterSessionReplaysEarlyEventsInOrder(t *testing.T) {
	first, err := json.Marshal(outputTextPayload{Content: "first"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := json.Marshal(outputTextPayload{Content: "second"})
	if err != nil {
		t.Fatal(err)
	}
	client := &linkClient{
		sessions: make(map[string]*Session),
		earlyEvents: map[string][]frame{
			"ordered": {
				{Name: eventOutputText, SessionID: "ordered", Payload: first},
				{Name: eventOutputText, SessionID: "ordered", Payload: second},
			},
		},
	}
	client.alive.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	session := &Session{
		id:     "ordered",
		client: client,
		ctx:    ctx,
		cancel: cancel,
		events: make(chan core.Event, 2),
	}
	session.alive.Store(true)
	if err := client.registerSession(session); err != nil {
		t.Fatalf("registerSession: %v", err)
	}
	if got := receiveEvent(t, session.Events()).Content; got != "first" {
		t.Fatalf("first event = %q", got)
	}
	if got := receiveEvent(t, session.Events()).Content; got != "second" {
		t.Fatalf("second event = %q", got)
	}
	session.finish()
}

func runLifecycleHost(conn net.Conn) error {
	codec := newFrameCodec(conn, defaultMaxFrameBytes)
	if err := acceptHello(codec); err != nil {
		return err
	}

	openReq, err := codec.read()
	if err != nil {
		return err
	}
	if openReq.Name != messageSessionOpen {
		return unexpectedFrame(openReq, messageSessionOpen)
	}
	var open sessionOpenPayload
	if err := json.Unmarshal(openReq.Payload, &open); err != nil {
		return err
	}
	if open.RequestedSessionID != "resume-me" || open.WorkDir != "/workspace/project" {
		return unexpectedPayload(open)
	}
	if err := writeResponse(codec, openReq, sessionOpenResult{SessionID: "host-session-1"}); err != nil {
		return err
	}

	turnReq, err := codec.read()
	if err != nil {
		return err
	}
	if turnReq.Name != messageTurnSubmit || turnReq.SessionID != "host-session-1" {
		return unexpectedFrame(turnReq, messageTurnSubmit)
	}
	var turn turnSubmitPayload
	if err := json.Unmarshal(turnReq.Payload, &turn); err != nil {
		return err
	}
	if turn.Prompt != "fix the tests" || turn.MessageID != "om_456" {
		return unexpectedPayload(turn)
	}
	if err := writeResponse(codec, turnReq, struct{}{}); err != nil {
		return err
	}

	if err := writeEvent(codec, eventOutputText, "host-session-1", outputTextPayload{Content: "working"}); err != nil {
		return err
	}
	if err := writeEvent(codec, eventInteractionRequested, "host-session-1", interactionRequestedPayload{
		RequestID: "ask-1",
		ToolName:  "AskUserQuestion",
		Questions: []wireQuestion{{
			Question: "Continue?",
			Header:   "Decision",
			Options: []wireQuestionOption{{
				Label:       "Proceed",
				Description: "Continue the implementation",
			}},
		}},
	}); err != nil {
		return err
	}

	permissionReq, err := codec.read()
	if err != nil {
		return err
	}
	if permissionReq.Name != messageInteractionRespond || permissionReq.SessionID != "host-session-1" {
		return unexpectedFrame(permissionReq, messageInteractionRespond)
	}
	var response interactionRespondPayload
	if err := json.Unmarshal(permissionReq.Payload, &response); err != nil {
		return err
	}
	if response.RequestID != "ask-1" || response.Behavior != "allow" || response.UpdatedInput == nil {
		return unexpectedPayload(response)
	}
	if err := writeResponse(codec, permissionReq, struct{}{}); err != nil {
		return err
	}

	if err := writeEvent(codec, eventTurnCompleted, "host-session-1", turnCompletedPayload{
		Done:         true,
		OutputTokens: 7,
	}); err != nil {
		return err
	}

	detachReq, err := codec.read()
	if err != nil {
		return err
	}
	if detachReq.Name != messageSessionDetach || detachReq.SessionID != "host-session-1" {
		return unexpectedFrame(detachReq, messageSessionDetach)
	}
	return writeResponse(codec, detachReq, struct{}{})
}

func acceptHello(codec *frameCodec) error {
	req, err := codec.read()
	if err != nil {
		return err
	}
	if req.Name != messageLinkHello {
		return unexpectedFrame(req, messageLinkHello)
	}
	var hello linkHelloPayload
	if err := json.Unmarshal(req.Payload, &hello); err != nil {
		return err
	}
	if hello.AuthToken != "test-secret" || hello.Client != clientName {
		return unexpectedPayload(hello)
	}
	return writeResponse(codec, req, linkHelloResult{Host: "claude-code-java"})
}

func writeResponse(codec *frameCodec, req frame, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return codec.write(frame{
		Protocol: protocolName,
		Version:  protocolVersion,
		Kind:     frameKindResponse,
		Name:     req.Name,
		ReplyTo:  req.ID,
		Payload:  raw,
	})
}

func writeEvent(codec *frameCodec, name, sessionID string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return codec.write(frame{
		Protocol:  protocolName,
		Version:   protocolVersion,
		Kind:      frameKindEvent,
		Name:      name,
		SessionID: sessionID,
		Payload:   raw,
	})
}

func receiveEvent(t *testing.T, events <-chan core.Event) core.Event {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for session event")
		return core.Event{}
	}
}

func boolPtr(value bool) *bool { return &value }

func intPtr(value int) *int { return &value }

type testError string

func (e testError) Error() string { return string(e) }

func unexpectedFrame(got frame, want string) error {
	raw, _ := json.Marshal(got)
	return testError("unexpected frame for " + want + ": " + string(raw))
}

func unexpectedPayload(got any) error {
	raw, _ := json.Marshal(got)
	return testError("unexpected payload: " + string(raw))
}
