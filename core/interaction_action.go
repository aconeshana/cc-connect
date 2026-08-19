package core

import (
	"fmt"
	"strconv"
	"strings"
)

// PermissionAction builds a card/button payload tied to one interaction request.
// The legacy payload is retained for callers that do not have a request ID.
func PermissionAction(requestID, decision string) string {
	if strings.TrimSpace(requestID) == "" {
		return "perm:" + decision
	}
	return "perm:" + requestID + ":" + decision
}

// ParsePermissionAction accepts both perm:<decision> and
// perm:<request-id>:<decision>. The returned request ID is empty for legacy
// actions, allowing receivers to consume old cards as stale instead of routing
// their literal payload into a new agent turn.
func ParsePermissionAction(value string) (requestID, decision string, ok bool) {
	parts := strings.Split(value, ":")
	switch len(parts) {
	case 2:
		if parts[0] == "perm" && validPermissionDecision(parts[1]) {
			return "", parts[1], true
		}
	case 3:
		if parts[0] == "perm" && parts[1] != "" && validPermissionDecision(parts[2]) {
			return parts[1], parts[2], true
		}
	}
	return "", "", false
}

func validPermissionDecision(decision string) bool {
	return decision == "allow" || decision == "deny" || decision == "allow_all"
}

// AskQuestionAction builds an option callback tied to one interaction request.
func AskQuestionAction(requestID string, questionIndex, optionIndex int) string {
	if strings.TrimSpace(requestID) == "" {
		return fmt.Sprintf("askq:%d:%d", questionIndex, optionIndex)
	}
	return fmt.Sprintf("askq:%s:%d:%d", requestID, questionIndex, optionIndex)
}

// AskQuestionOtherAction builds the explicit custom-answer callback for one
// question. ParseAskQuestionAction reports this sentinel as option index 0 so
// callers can distinguish it from the 1-based predefined options.
func AskQuestionOtherAction(requestID string, questionIndex int) string {
	if strings.TrimSpace(requestID) == "" {
		return fmt.Sprintf("askq:%d:other", questionIndex)
	}
	return fmt.Sprintf("askq:%s:%d:other", requestID, questionIndex)
}

// ParseAskQuestionAction accepts both askq:<question>:<option> and
// askq:<request-id>:<question>:<option>.
func ParseAskQuestionAction(value string) (requestID string, questionIndex, optionIndex int, ok bool) {
	parts := strings.Split(value, ":")
	var questionPart, optionPart string
	switch len(parts) {
	case 3:
		if parts[0] != "askq" {
			return "", 0, 0, false
		}
		questionPart, optionPart = parts[1], parts[2]
	case 4:
		if parts[0] != "askq" || parts[1] == "" {
			return "", 0, 0, false
		}
		requestID = parts[1]
		questionPart, optionPart = parts[2], parts[3]
	default:
		return "", 0, 0, false
	}
	questionIndex, err := strconv.Atoi(questionPart)
	if err != nil || questionIndex < 0 {
		return "", 0, 0, false
	}
	if optionPart == "other" {
		return requestID, questionIndex, 0, true
	}
	optionIndex, err = strconv.Atoi(optionPart)
	if err != nil || optionIndex < 1 {
		return "", 0, 0, false
	}
	return requestID, questionIndex, optionIndex, true
}
