package core

import (
	"strings"
	"testing"
)

func TestInteractionUnsupportedMessageIsLocalizedAndStandalone(t *testing.T) {
	languages := []Language{
		LangEnglish, LangChinese, LangTraditionalChinese, LangJapanese, LangSpanish,
	}
	for _, lang := range languages {
		engine := &Engine{i18n: NewI18n(lang)}
		message := engine.interactionUnsupportedMessage("sudo_password")
		if strings.TrimSpace(message) == "" || !strings.Contains(message, "TUI") {
			t.Fatalf("language %q message = %q", lang, message)
		}
		if strings.Contains(message, "sudo launchctl") {
			t.Fatalf("language %q leaked request content: %q", lang, message)
		}
	}
}

func TestInteractionUnsupportedUnknownKindUsesGenericLabel(t *testing.T) {
	engine := &Engine{i18n: NewI18n(LangEnglish)}
	message := engine.interactionUnsupportedMessage("future_interaction")
	if !strings.Contains(message, "This interaction") || strings.Contains(message, "future_interaction") {
		t.Fatalf("message = %q", message)
	}
}
