package sessionhost

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

// TestJavaSessionLinkInterop is driven by the Java runtime module's integration
// test. It intentionally uses the production Unix-socket client and protocol.
func TestJavaSessionLinkInterop(t *testing.T) {
	endpoint := os.Getenv("CC_JAVA_INTEROP_ENDPOINT")
	token := os.Getenv("CC_JAVA_INTEROP_TOKEN")
	sessionID := os.Getenv("CC_JAVA_INTEROP_SESSION_ID")
	if endpoint == "" || token == "" || sessionID == "" {
		t.Skip("Java Session Link fixture is not running")
	}
	t.Setenv("CC_JAVA_INTEROP_AUTH", token)
	raw, err := New(map[string]any{
		"endpoint": endpoint, "auth_token_env": "CC_JAVA_INTEROP_AUTH",
		"work_dir": "/interop", "bind_session_key": "feishu:interop:owner",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	agent := raw.(*Agent)
	t.Cleanup(func() { _ = agent.Stop() })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rawSession, err := agent.StartSession(ctx, sessionID)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	session := rawSession.(*Session)
	models, err := session.ModelState(ctx)
	if err != nil {
		t.Fatalf("ModelState: %v", err)
	}
	if models.Current != "sonnet" || len(models.Models) != 2 || models.Models[0].Alias != "sol" {
		t.Fatalf("model state = %#v", models)
	}
	changed, err := session.SetModel(ctx, "opus")
	if err != nil {
		t.Fatalf("SetModel: %v", err)
	}
	if changed.Current != "opus" || session.GetModel() != "opus" {
		t.Fatalf("changed model state = %#v cache=%q", changed, session.GetModel())
	}
	if err := session.Send("cross-language", "go-message-1", nil, nil); err != nil {
		t.Fatalf("Send: %v", err)
	}

	text := receiveEvent(t, session.Events())
	if text.Type != core.EventText || text.Content != "java:cross-language" {
		t.Fatalf("text event = %#v", text)
	}
	permission := receiveEvent(t, session.Events())
	if permission.Type != core.EventPermissionRequest || permission.ToolName != "Bash" {
		t.Fatalf("permission event = %#v", permission)
	}
	if err := session.RespondPermission(permission.RequestID, core.PermissionResult{
		Behavior: "allow", UpdatedInput: permission.ToolInputRaw,
	}); err != nil {
		t.Fatalf("RespondPermission: %v", err)
	}
	result := receiveEvent(t, session.Events())
	if result.Type != core.EventResult || !result.Done {
		t.Fatalf("result event = %#v", result)
	}
	if err := session.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	activation, err := agent.ResumeHostSession(ctx, sessionID, "interop-activation-1")
	if err != nil {
		t.Fatalf("ResumeHostSession: %v", err)
	}
	if activation.SessionID != sessionID ||
		activation.ActivationID != "interop-activation-1" ||
		activation.ActivationGeneration == 0 || activation.Session == nil {
		t.Fatalf("resume activation = %#v", activation)
	}
	if err := activation.Session.Close(); err != nil {
		t.Fatalf("Close resumed session: %v", err)
	}
}
