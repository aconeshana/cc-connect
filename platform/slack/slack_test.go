package slack

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/chenhg5/cc-connect/core"
	"github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"
)

func TestPlatformImplementsSessionThreadBinder(t *testing.T) {
	var platform any = &Platform{}
	if _, ok := platform.(core.SessionThreadBinder); !ok {
		t.Fatal("slack should expose native thread binding to Session Host")
	}
	if _, ok := platform.(core.FreshSessionThreadBinder); !ok {
		t.Fatal("slack should expose fresh native thread creation to Session Host")
	}
}

func TestCreateSessionThreadFromExistingThreadCreatesSiblingRoot(t *testing.T) {
	posts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posts++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok": true, "channel": "C1", "ts": "1700000000.0002",
		})
	}))
	defer srv.Close()
	p := &Platform{client: slack.New("xoxb-test", slack.OptionAPIURL(srv.URL+"/"))}

	key, _, err := p.CreateSessionThread(
		context.Background(), "slack:C1:t:1700000000.0001", "Sibling")
	if err != nil {
		t.Fatalf("CreateSessionThread: %v", err)
	}
	if key != "slack:C1:t:1700000000.0002" || posts != 1 {
		t.Fatalf("key=%q posts=%d", key, posts)
	}
}

func TestBindSessionThreadCreatesProactiveRoot(t *testing.T) {
	var postedText string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat.postMessage" {
			t.Fatalf("unexpected Slack API path %q", r.URL.Path)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("ParseForm: %v", err)
		}
		postedText = r.FormValue("text")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok": true, "channel": "C1", "ts": "1700000000.0001",
		})
	}))
	defer srv.Close()
	p := &Platform{client: slack.New("xoxb-test", slack.OptionAPIURL(srv.URL+"/"))}

	key, rawCtx, err := p.BindSessionThread(
		context.Background(), "slack:C1:U1", "session-1", "Build gateway")
	if err != nil {
		t.Fatalf("BindSessionThread: %v", err)
	}
	if key != "slack:C1:t:1700000000.0001" {
		t.Fatalf("session key = %q", key)
	}
	rc := rawCtx.(replyContext)
	if rc.channel != "C1" || rc.timestamp != "1700000000.0001" {
		t.Fatalf("reply context = %#v", rc)
	}
	if postedText != "Build gateway" {
		t.Fatalf("posted root = %q", postedText)
	}
}

func TestBindSessionThreadRestoresExistingThreadWithoutPosting(t *testing.T) {
	p := &Platform{}
	key, rawCtx, err := p.BindSessionThread(
		context.Background(), "slack:C1:t:1700000000.0001", "session-1", "ignored")
	if err != nil {
		t.Fatalf("BindSessionThread: %v", err)
	}
	if key != "slack:C1:t:1700000000.0001" {
		t.Fatalf("session key = %q", key)
	}
	rc := rawCtx.(replyContext)
	if rc.timestamp != "1700000000.0001" {
		t.Fatalf("reply context = %#v", rc)
	}
}

func TestStripAppMentionText(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "strips bot mention prefix",
			in:   "<@U0BOT123> run tests",
			want: "run tests",
		},
		{
			name: "empty mention becomes empty text",
			in:   "<@U0BOT123> ",
			want: "",
		},
		{
			name: "plain text remains unchanged",
			in:   "run tests",
			want: "run tests",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stripAppMentionText(tt.in); got != tt.want {
				t.Fatalf("stripAppMentionText(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestDownloadSlackFile_HTMLDetection(t *testing.T) {
	// Test that we detect HTML responses (Slack login page) and return an error
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate Slack returning HTML login page when auth is missing
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("<!DOCTYPE html><html><body>Please login</body></html>"))
	}))
	defer ts.Close()

	p := &Platform{botToken: "xoxb-test-token"}
	_, err := p.downloadSlackFile(ts.URL)
	if err == nil {
		t.Fatal("expected error for HTML response, got nil")
	}
	// Should detect HTML prefix
	if err != nil && err.Error() == "" {
		t.Fatal("expected non-empty error message")
	}
}

func TestDownloadSlackFile_MissingAuth(t *testing.T) {
	// Test that we return an error for non-200 status codes
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte("unauthorized"))
	}))
	defer ts.Close()

	p := &Platform{botToken: "xoxb-test-token"}
	_, err := p.downloadSlackFile(ts.URL)
	if err == nil {
		t.Fatal("expected error for 401 response, got nil")
	}
}

func TestDownloadSlackFile_Success(t *testing.T) {
	// Test successful binary download
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify Authorization header is set
		auth := r.Header.Get("Authorization")
		if auth != "Bearer xoxb-test-token" {
			t.Errorf("expected Authorization header 'Bearer xoxb-test-token', got %q", auth)
		}
		w.Header().Set("Content-Type", "image/png")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("\x89PNG\r\n\x1a\n")) // PNG magic bytes
	}))
	defer ts.Close()

	p := &Platform{botToken: "xoxb-test-token"}
	data, err := p.downloadSlackFile(ts.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(data) != 8 {
		t.Errorf("expected 8 bytes, got %d", len(data))
	}
}

func TestDownloadSlackFile_EmptyURL(t *testing.T) {
	p := &Platform{botToken: "xoxb-test-token"}
	_, err := p.downloadSlackFile("")
	if err == nil {
		t.Fatal("expected error for empty URL, got nil")
	}
}

func TestParseSlackInnerEventFiles(t *testing.T) {
	raw := json.RawMessage(`{"type":"app_mention","user":"U1","text":"<@B> hi","files":[{"id":"F1","name":"a.pdf","mimetype":"application/pdf","url_private_download":"http://example/f"}]}`)
	files := parseSlackInnerEventFiles(&raw)
	if len(files) != 1 {
		t.Fatalf("len(files) = %d, want 1", len(files))
	}
	if files[0].Name != "a.pdf" || files[0].Mimetype != "application/pdf" {
		t.Fatalf("unexpected file: %+v", files[0])
	}
}

func TestProcessSlackFileShares_GenericFile(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("%PDF-1.4 minimal"))
	}))
	defer ts.Close()

	p := &Platform{botToken: "xoxb-test"}
	images, audio, docs := p.processSlackFileShares([]slackevents.File{
		{
			ID:                 "Fpdf",
			Name:               "doc.pdf",
			Mimetype:           "application/pdf",
			URLPrivateDownload: ts.URL,
		},
	})
	if len(images) != 0 || audio != nil {
		t.Fatalf("expected only doc file, got images=%d audio=%v", len(images), audio)
	}
	if len(docs) != 1 {
		t.Fatalf("len(docs) = %d, want 1", len(docs))
	}
	if docs[0].FileName != "doc.pdf" || docs[0].MimeType != "application/pdf" {
		t.Fatalf("unexpected doc: %+v", docs[0])
	}
	if string(docs[0].Data) != "%PDF-1.4 minimal" {
		t.Fatalf("unexpected data %q", docs[0].Data)
	}
}

func TestProcessSlackFileShares_ImageVsDoc(t *testing.T) {
	imgSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("fakepng"))
	}))
	defer imgSrv.Close()
	txtSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("hello"))
	}))
	defer txtSrv.Close()

	p := &Platform{botToken: "xoxb-test"}
	images, audio, docs := p.processSlackFileShares([]slackevents.File{
		{ID: "1", Name: "x.png", Mimetype: "image/png", URLPrivateDownload: imgSrv.URL},
		{ID: "2", Name: "n.txt", Mimetype: "text/plain", URLPrivateDownload: txtSrv.URL},
	})
	if audio != nil {
		t.Fatal("unexpected audio")
	}
	if len(images) != 1 || len(docs) != 1 {
		t.Fatalf("want 1 image 1 doc, got images=%d docs=%d", len(images), len(docs))
	}
	if images[0].MimeType != "image/png" {
		t.Errorf("image mime: %q", images[0].MimeType)
	}
	if docs[0].MimeType != "text/plain" || string(docs[0].Data) != "hello" {
		t.Errorf("unexpected text file: %+v", docs[0])
	}
}

func TestProcessSlackFileShares_EmptyMimeBecomesOctetStream(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte{0, 1, 2})
	}))
	defer ts.Close()

	p := &Platform{botToken: "xoxb-test"}
	_, _, docs := p.processSlackFileShares([]slackevents.File{
		{ID: "z", Name: "blob.bin", Mimetype: "", URLPrivateDownload: ts.URL},
	})
	if len(docs) != 1 || docs[0].MimeType != "application/octet-stream" {
		t.Fatalf("got %+v", docs)
	}
}
