package core

import "testing"

func TestInteractionActionsRoundTripRequestID(t *testing.T) {
	permission := PermissionAction("req-123", "allow_all")
	requestID, decision, ok := ParsePermissionAction(permission)
	if !ok || requestID != "req-123" || decision != "allow_all" {
		t.Fatalf("ParsePermissionAction(%q) = %q, %q, %v", permission, requestID, decision, ok)
	}

	ask := AskQuestionAction("req-456", 2, 3)
	requestID, question, option, ok := ParseAskQuestionAction(ask)
	if !ok || requestID != "req-456" || question != 2 || option != 3 {
		t.Fatalf("ParseAskQuestionAction(%q) = %q, %d, %d, %v", ask, requestID, question, option, ok)
	}

	other := AskQuestionOtherAction("req-789", 1)
	requestID, question, option, ok = ParseAskQuestionAction(other)
	if !ok || requestID != "req-789" || question != 1 || option != 0 {
		t.Fatalf("ParseAskQuestionAction(%q) = %q, %d, %d, %v", other, requestID, question, option, ok)
	}
}

func TestInteractionActionsAcceptLegacyPayloads(t *testing.T) {
	requestID, decision, ok := ParsePermissionAction("perm:deny")
	if !ok || requestID != "" || decision != "deny" {
		t.Fatalf("legacy permission parse = %q, %q, %v", requestID, decision, ok)
	}
	requestID, question, option, ok := ParseAskQuestionAction("askq:0:2")
	if !ok || requestID != "" || question != 0 || option != 2 {
		t.Fatalf("legacy ask parse = %q, %d, %d, %v", requestID, question, option, ok)
	}
	requestID, question, option, ok = ParseAskQuestionAction("askq:0:other")
	if !ok || requestID != "" || question != 0 || option != 0 {
		t.Fatalf("legacy AskUserQuestion Other parse = %q, %d, %d, %v", requestID, question, option, ok)
	}
}
