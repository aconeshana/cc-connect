package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/config"
	"github.com/chenhg5/cc-connect/core"
)

func TestResolveFeishuSetupInputs_AutoModeWithoutCredentialsUsesNew(t *testing.T) {
	mode, appID, appSecret, err := resolveFeishuSetupInputs(feishuSetupModeAuto, "", "", "")
	if err != nil {
		t.Fatalf("resolveFeishuSetupInputs returned error: %v", err)
	}
	if mode != feishuSetupModeNew {
		t.Fatalf("mode = %q, want %q", mode, feishuSetupModeNew)
	}
	if appID != "" || appSecret != "" {
		t.Fatalf("credentials should be empty, got appID=%q appSecret=%q", appID, appSecret)
	}
}

func TestResolveFeishuSetupInputs_AutoModeWithAppUsesBind(t *testing.T) {
	mode, appID, appSecret, err := resolveFeishuSetupInputs(feishuSetupModeAuto, "cli_xxx:sec_xxx", "", "")
	if err != nil {
		t.Fatalf("resolveFeishuSetupInputs returned error: %v", err)
	}
	if mode != feishuSetupModeBind {
		t.Fatalf("mode = %q, want %q", mode, feishuSetupModeBind)
	}
	if appID != "cli_xxx" || appSecret != "sec_xxx" {
		t.Fatalf("credentials = (%q, %q), want (%q, %q)", appID, appSecret, "cli_xxx", "sec_xxx")
	}
}

func TestResolveFeishuSetupInputs_BindRequiresCredentials(t *testing.T) {
	_, _, _, err := resolveFeishuSetupInputs(feishuSetupModeBind, "", "", "")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestResolveFeishuSetupInputs_RejectsMixedCredentialFlags(t *testing.T) {
	_, _, _, err := resolveFeishuSetupInputs(feishuSetupModeAuto, "cli_xxx:sec_xxx", "cli_xxx", "sec_xxx")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestParseAppPair_SecretCanContainColon(t *testing.T) {
	appID, appSecret, err := parseAppPair("cli_xxx:sec:yyy")
	if err != nil {
		t.Fatalf("parseAppPair returned error: %v", err)
	}
	if appID != "cli_xxx" || appSecret != "sec:yyy" {
		t.Fatalf("result = (%q, %q), want (%q, %q)", appID, appSecret, "cli_xxx", "sec:yyy")
	}
}

func TestSaveQRCodeImage_CreatesPNG(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-qr.png")

	if err := saveQRCodeImage("https://example.com/test", path); err != nil {
		t.Fatalf("saveQRCodeImage failed: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read output file: %v", err)
	}
	if len(data) < 100 {
		t.Fatalf("PNG file too small: %d bytes", len(data))
	}
	// PNG magic bytes
	if data[0] != 0x89 || data[1] != 'P' || data[2] != 'N' || data[3] != 'G' {
		t.Fatal("output file is not a valid PNG")
	}
}

func TestSaveQRCodeImage_InvalidPath(t *testing.T) {
	err := saveQRCodeImage("https://example.com", "/nonexistent/dir/qr.png")
	if err == nil {
		t.Fatal("expected error for invalid path, got nil")
	}
}

func TestWriteTerminalQRCodeFitsFeishuTUI(t *testing.T) {
	// The Feishu onboarding endpoint currently returns a 56-character
	// verification_uri_complete. Keep the fixture opaque so this test never
	// creates or exposes a real device code.
	verificationURL := "https://accounts.feishu.cn/" + strings.Repeat("x", 29)
	if len(verificationURL) != 56 {
		t.Fatalf("fixture length = %d, want 56", len(verificationURL))
	}

	var output bytes.Buffer
	writeTerminalQRCode(&output, verificationURL)

	lines := strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n")
	if len(lines) > 21 {
		t.Fatalf("QR height = %d rows, want at most 21", len(lines))
	}
	for row, line := range lines {
		if width := len([]rune(line)); width > 41 {
			t.Fatalf("QR row %d width = %d columns, want at most 41", row, width)
		}
	}
}

func TestReadFeishuAppCredentialsFromStdinKeepsSecretOutOfArgv(t *testing.T) {
	appID, appSecret, err := readFeishuAppCredentials(strings.NewReader(
		`{"app_id":"cli_existing","app_secret":"sec:with:colons"}`))
	if err != nil {
		t.Fatalf("readFeishuAppCredentials returned error: %v", err)
	}
	if appID != "cli_existing" || appSecret != "sec:with:colons" {
		t.Fatalf("credentials = (%q, %q)", appID, appSecret)
	}
}

func TestReadFeishuAppCredentialsFromStdinRejectsOversizedOrMissingValues(t *testing.T) {
	if _, _, err := readFeishuAppCredentials(strings.NewReader(`{"app_id":"","app_secret":"x"}`)); err == nil {
		t.Fatal("missing app_id unexpectedly accepted")
	}
	oversized := strings.NewReader(`{"app_id":"cli_x","app_secret":"` + strings.Repeat("x", 70<<10) + `"}`)
	if _, _, err := readFeishuAppCredentials(oversized); err == nil {
		t.Fatal("oversized credential payload unexpectedly accepted")
	}
}

func TestLoadFeishuResumeCredentialsFromIncompleteSessionHostConfig(t *testing.T) {
	t.Setenv("FEISHU_APP_ID", "cli_saved")
	t.Setenv("FEISHU_APP_SECRET", "secret_saved")
	path := filepath.Join(t.TempDir(), "cc-connect.toml")
	err := os.WriteFile(path, []byte(`
[[projects]]
name = "demo"
[projects.agent]
type = "sessionhost"
[projects.agent.options]
work_dir = "/tmp/demo"
[[projects.platforms]]
type = "feishu"
[projects.platforms.options]
app_id = "${FEISHU_APP_ID}"
app_secret = "${FEISHU_APP_SECRET}"
`), 0o600)
	if err != nil {
		t.Fatal(err)
	}
	previous := config.ConfigPath
	config.ConfigPath = path
	t.Cleanup(func() { config.ConfigPath = previous })

	platformType, appID, appSecret, err := loadFeishuResumeCredentials("demo")
	if err != nil {
		t.Fatalf("loadFeishuResumeCredentials returned error: %v", err)
	}
	if platformType != "feishu" || appID != "cli_saved" || appSecret != "secret_saved" {
		t.Fatalf("credentials = (%q, %q, %q)", platformType, appID, appSecret)
	}
}

type discoveryPlatform struct {
	message *core.Message
}

func (p *discoveryPlatform) Name() string { return "feishu" }
func (p *discoveryPlatform) Start(handler core.MessageHandler) error {
	go handler(p, p.message)
	return nil
}
func (p *discoveryPlatform) Reply(context.Context, any, string) error { return nil }
func (p *discoveryPlatform) Send(context.Context, any, string) error  { return nil }
func (p *discoveryPlatform) Stop() error                              { return nil }

func TestDiscoverFeishuTargetUsesTheChatWhereTheUserMessagesTheBot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	target, err := discoverFeishuTarget(ctx, &discoveryPlatform{message: &core.Message{
		SessionKey: "feishu:oc_selected_chat:ou_owner", UserID: "ou_owner",
	}})
	if err != nil {
		t.Fatalf("discoverFeishuTarget returned error: %v", err)
	}
	if target.SessionKey != "feishu:oc_selected_chat:ou_owner" ||
		target.ChatID != "oc_selected_chat" || target.UserID != "ou_owner" {
		t.Fatalf("target = %#v", target)
	}
}

func TestDiscoverFeishuTargetHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := discoverFeishuTarget(ctx, &discoveryPlatform{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context cancellation", err)
	}
}
