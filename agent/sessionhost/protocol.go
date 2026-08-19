package sessionhost

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

const (
	protocolName    = "session-link"
	protocolVersion = 1
	clientName      = "cc-connect"

	defaultMaxFrameBytes = 16 << 20
	maxMaxFrameBytes     = 64 << 20

	frameKindRequest  = "request"
	frameKindResponse = "response"
	frameKindEvent    = "event"
	frameKindError    = "error"

	messageLinkHello          = "link.hello"
	messageSessionOpen        = "session.open"
	messageSessionList        = "session.list"
	messageSessionDetach      = "session.detach"
	messageTurnSubmit         = "turn.submit"
	messageInteractionRespond = "interaction.respond"
	messageModelGet           = "model.get"
	messageModelSet           = "model.set"
	messageEffortGet          = "effort.get"
	messageEffortSet          = "effort.set"
	messageCompactRun         = "compact.run"

	eventOutputText           = "output.text"
	eventOutputThinking       = "output.thinking"
	eventTurnStarted          = "turn.started"
	eventTurnModel            = "turn.model"
	eventToolStarted          = "tool.started"
	eventToolCompleted        = "tool.completed"
	eventInteractionRequested = "interaction.requested"
	eventInteractionResolved  = "interaction.resolved"
	eventTurnCompleted        = "turn.completed"
	eventSessionError         = "session.error"
	eventSessionActivated     = "session.activated"
	eventSessionUpdated       = "session.updated"
	eventSessionEnded         = "session.ended"
	eventCollaborationChanged = "collaboration.changed"
)

type frame struct {
	Protocol  string          `json:"protocol"`
	Version   int             `json:"version"`
	Kind      string          `json:"kind"`
	Name      string          `json:"name"`
	ID        string          `json:"id,omitempty"`
	ReplyTo   string          `json:"reply_to,omitempty"`
	SessionID string          `json:"session_id,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Error     *wireError      `json:"error,omitempty"`
}

type wireError struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message"`
}

type linkHelloPayload struct {
	Client                string   `json:"client"`
	AuthToken             string   `json:"auth_token"`
	CollaborationChannels []string `json:"collaboration_channels,omitempty"`
}

type linkHelloResult struct {
	Host string `json:"host"`
}

type sessionOpenPayload struct {
	RequestedSessionID   string `json:"requested_session_id,omitempty"`
	WorkDir              string `json:"work_dir"`
	CollaborationChannel string `json:"collaboration_channel,omitempty"`
	ActivationID         string `json:"activation_id,omitempty"`
}

type sessionOpenResult struct {
	SessionID            string `json:"session_id"`
	ActivationID         string `json:"activation_id,omitempty"`
	ActivationGeneration uint64 `json:"activation_generation,omitempty"`
}

type sessionListResult struct {
	Sessions []wireSessionInfo `json:"sessions"`
}

type wireSessionInfo struct {
	ID           string `json:"id"`
	WorkDir      string `json:"work_dir,omitempty"`
	Summary      string `json:"summary,omitempty"`
	MessageCount int    `json:"message_count,omitempty"`
	ModifiedAt   string `json:"modified_at,omitempty"`
	GitBranch    string `json:"git_branch,omitempty"`
}

type sessionActivatedPayload struct {
	ID                   string `json:"id"`
	WorkDir              string `json:"work_dir,omitempty"`
	Summary              string `json:"summary,omitempty"`
	MessageCount         int    `json:"message_count,omitempty"`
	ModifiedAt           string `json:"modified_at,omitempty"`
	GitBranch            string `json:"git_branch,omitempty"`
	Origin               string `json:"origin,omitempty"`
	ActivationID         string `json:"activation_id,omitempty"`
	ActivationGeneration uint64 `json:"activation_generation,omitempty"`
}

type sessionUpdatedPayload struct {
	ID           string `json:"id,omitempty"`
	WorkDir      string `json:"work_dir,omitempty"`
	Summary      string `json:"summary,omitempty"`
	MessageCount int    `json:"message_count,omitempty"`
	ModifiedAt   string `json:"modified_at,omitempty"`
	GitBranch    string `json:"git_branch,omitempty"`
	Origin       string `json:"origin,omitempty"`
}

type sessionEndedPayload struct {
	ID             string `json:"id,omitempty"`
	WorkDir        string `json:"work_dir,omitempty"`
	Summary        string `json:"summary,omitempty"`
	MessageCount   int    `json:"message_count,omitempty"`
	ModifiedAt     string `json:"modified_at,omitempty"`
	GitBranch      string `json:"git_branch,omitempty"`
	Origin         string `json:"origin,omitempty"`
	Reason         string `json:"reason,omitempty"`
	NotificationID string `json:"notification_id"`
}

type sessionEndedAckPayload struct {
	NotificationID string `json:"notification_id"`
}

type collaborationChangedPayload struct {
	ID           string `json:"id,omitempty"`
	WorkDir      string `json:"work_dir,omitempty"`
	Summary      string `json:"summary,omitempty"`
	MessageCount int    `json:"message_count,omitempty"`
	ModifiedAt   string `json:"modified_at,omitempty"`
	GitBranch    string `json:"git_branch,omitempty"`
	Channel      string `json:"channel"`
	Enabled      bool   `json:"enabled"`
	Origin       string `json:"origin,omitempty"`
}

type turnSubmitPayload struct {
	Prompt      string           `json:"prompt"`
	MessageID   string           `json:"message_id,omitempty"`
	Images      []wireAttachment `json:"images,omitempty"`
	Attachments []wireAttachment `json:"attachments,omitempty"`
}

type wireAttachment struct {
	MimeType string `json:"mime_type,omitempty"`
	FileName string `json:"file_name,omitempty"`
	Data     []byte `json:"data"`
}

type interactionRespondPayload struct {
	RequestID    string         `json:"request_id"`
	Behavior     string         `json:"behavior"`
	UpdatedInput map[string]any `json:"updated_input,omitempty"`
	Message      string         `json:"message,omitempty"`
}

type modelSetPayload struct {
	Model string `json:"model"`
}

type modelStateResult struct {
	Current string            `json:"current"`
	Models  []wireModelOption `json:"models"`
}

type effortSetPayload struct {
	Effort string `json:"effort"`
}

type effortStateResult struct {
	Current   string   `json:"current"`
	Effective string   `json:"effective,omitempty"`
	Efforts   []string `json:"efforts"`
}

type compactRunPayload struct {
	Instructions string `json:"instructions,omitempty"`
}

type compactRunResult struct {
	Message string `json:"message,omitempty"`
}

type wireModelOption struct {
	Name        string `json:"name"`
	Label       string `json:"label,omitempty"`
	Description string `json:"description,omitempty"`
	Alias       string `json:"alias,omitempty"`
	Default     bool   `json:"default,omitempty"`
}

type outputTextPayload struct {
	Content   string         `json:"content"`
	Synthetic bool           `json:"synthetic,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

type outputThinkingPayload struct {
	Content string `json:"content"`
}

type turnStartedPayload struct {
	DisplayText    string `json:"display_text"`
	PermissionMode string `json:"permission_mode,omitempty"`
	Origin         string `json:"origin,omitempty"`
	ModelOverride  string `json:"model_override,omitempty"`
	EffortOverride string `json:"effort_override,omitempty"`
}

type turnModelPayload struct {
	Model        string `json:"model"`
	MessageCount int    `json:"message_count,omitempty"`
}

type toolStartedPayload struct {
	Name     string         `json:"name"`
	Input    string         `json:"input,omitempty"`
	InputRaw map[string]any `json:"input_raw,omitempty"`
}

type toolCompletedPayload struct {
	Name     string `json:"name"`
	Result   string `json:"result,omitempty"`
	Status   string `json:"status,omitempty"`
	ExitCode *int   `json:"exit_code,omitempty"`
	Success  *bool  `json:"success,omitempty"`
}

type interactionRequestedPayload struct {
	RequestID             string         `json:"request_id"`
	ToolName              string         `json:"tool_name"`
	ToolInput             string         `json:"tool_input,omitempty"`
	InputRaw              map[string]any `json:"input_raw,omitempty"`
	Questions             []wireQuestion `json:"questions,omitempty"`
	DecisionReasonType    string         `json:"decision_reason_type,omitempty"`
	DecisionReasonDetail  string         `json:"decision_reason_detail,omitempty"`
	SuggestionRuleContent string         `json:"suggestion_rule_content,omitempty"`
	SuggestionLabel       string         `json:"suggestion_label,omitempty"`
	WorkerID              string         `json:"worker_id,omitempty"`
	WorkerColor           string         `json:"worker_color,omitempty"`
	DestructiveWarning    string         `json:"destructive_warning,omitempty"`
	BlockedPath           string         `json:"blocked_path,omitempty"`
	CustomMessage         string         `json:"custom_message,omitempty"`
	ToolDescription       string         `json:"tool_description,omitempty"`
}

type interactionResolvedPayload struct {
	RequestID    string         `json:"request_id"`
	Behavior     string         `json:"behavior"`
	Origin       string         `json:"origin,omitempty"`
	UpdatedInput map[string]any `json:"updated_input,omitempty"`
	Message      string         `json:"message,omitempty"`
}

type wireQuestion struct {
	Question    string               `json:"question"`
	Header      string               `json:"header,omitempty"`
	Options     []wireQuestionOption `json:"options,omitempty"`
	MultiSelect bool                 `json:"multi_select,omitempty"`
}

type wireQuestionOption struct {
	Label       string `json:"label"`
	Description string `json:"description,omitempty"`
}

type turnCompletedPayload struct {
	Content                  string         `json:"content,omitempty"`
	Done                     bool           `json:"done"`
	InputTokens              int            `json:"input_tokens,omitempty"`
	OutputTokens             int            `json:"output_tokens,omitempty"`
	CacheCreationInputTokens int            `json:"cache_creation_input_tokens,omitempty"`
	CacheReadInputTokens     int            `json:"cache_read_input_tokens,omitempty"`
	Metadata                 map[string]any `json:"metadata,omitempty"`
}

type sessionErrorPayload struct {
	Message string `json:"message"`
}

func encodeFrame(value frame) ([]byte, error) {
	if err := validateFrame(value); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("session-host: encode frame: %w", err)
	}
	return raw, nil
}

func decodeFrame(raw []byte, maxBytes int) (frame, error) {
	if maxBytes <= 0 {
		maxBytes = defaultMaxFrameBytes
	}
	if len(raw) > maxBytes {
		return frame{}, fmt.Errorf("session-host: frame size %d exceeds limit %d", len(raw), maxBytes)
	}

	var value frame
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return frame{}, fmt.Errorf("session-host: decode frame: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return frame{}, err
	}
	if err := validateFrame(value); err != nil {
		return frame{}, err
	}
	return value, nil
}

func decodePayload[T any](raw json.RawMessage) (T, error) {
	var value T
	if len(raw) == 0 {
		return value, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return value, fmt.Errorf("session-host: decode %T payload: %w", value, err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return value, err
	}
	return value, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("session-host: multiple JSON values in frame")
		}
		return fmt.Errorf("session-host: trailing JSON data: %w", err)
	}
	return nil
}

func validateFrame(value frame) error {
	if value.Protocol != protocolName {
		return fmt.Errorf("session-host: unsupported protocol %q", value.Protocol)
	}
	if value.Version != protocolVersion {
		return fmt.Errorf("session-host: unsupported version %d", value.Version)
	}
	if strings.TrimSpace(value.Name) == "" || len(value.Name) > 128 {
		return fmt.Errorf("session-host: invalid message name")
	}
	if len(value.ID) > 128 || len(value.ReplyTo) > 128 || len(value.SessionID) > 1024 {
		return fmt.Errorf("session-host: frame identifier exceeds limit")
	}

	switch value.Kind {
	case frameKindRequest:
		if value.ID == "" || value.ReplyTo != "" || value.Error != nil {
			return fmt.Errorf("session-host: invalid request envelope")
		}
	case frameKindResponse:
		if value.ReplyTo == "" || value.Error != nil {
			return fmt.Errorf("session-host: invalid response envelope")
		}
	case frameKindEvent:
		if value.ID != "" || value.ReplyTo != "" || value.Error != nil {
			return fmt.Errorf("session-host: invalid event envelope")
		}
	case frameKindError:
		if value.ReplyTo == "" || value.Error == nil || strings.TrimSpace(value.Error.Message) == "" {
			return fmt.Errorf("session-host: invalid error envelope")
		}
	default:
		return fmt.Errorf("session-host: unsupported frame kind %q", value.Kind)
	}
	return nil
}
