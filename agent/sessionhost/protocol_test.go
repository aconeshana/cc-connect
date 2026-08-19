package sessionhost

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestFrameRoundTripPreservesTypedEnvelope(t *testing.T) {
	payload, err := json.Marshal(turnSubmitPayload{
		Prompt:    "inspect the failing test",
		MessageID: "om_123",
	})
	if err != nil {
		t.Fatal(err)
	}

	want := frame{
		Protocol:  protocolName,
		Version:   protocolVersion,
		Kind:      frameKindRequest,
		Name:      messageTurnSubmit,
		ID:        "cc-7",
		SessionID: "session-42",
		Payload:   payload,
	}

	encoded, err := encodeFrame(want)
	if err != nil {
		t.Fatalf("encodeFrame: %v", err)
	}
	wantJSON := `{"protocol":"session-link","version":1,"kind":"request","name":"turn.submit","id":"cc-7","session_id":"session-42","payload":{"prompt":"inspect the failing test","message_id":"om_123"}}`
	if string(encoded) != wantJSON {
		t.Fatalf("encoded frame = %s, want %s", encoded, wantJSON)
	}
	got, err := decodeFrame(encoded, defaultMaxFrameBytes)
	if err != nil {
		t.Fatalf("decodeFrame: %v", err)
	}

	if got.Protocol != want.Protocol || got.Version != want.Version || got.Kind != want.Kind ||
		got.Name != want.Name || got.ID != want.ID || got.SessionID != want.SessionID {
		t.Fatalf("decoded envelope = %#v, want %#v", got, want)
	}

	var decodedPayload turnSubmitPayload
	if err := json.Unmarshal(got.Payload, &decodedPayload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if decodedPayload.Prompt != "inspect the failing test" || decodedPayload.MessageID != "om_123" {
		t.Fatalf("decoded payload = %#v", decodedPayload)
	}
}

func TestDecodeFrameRejectsUnknownEnvelopeFields(t *testing.T) {
	raw := []byte(`{"protocol":"session-link","version":1,"kind":"event","name":"output.text","surprise":true}`)
	_, err := decodeFrame(raw, defaultMaxFrameBytes)
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("decodeFrame error = %v, want unknown field rejection", err)
	}
}

func TestDecodeFrameRejectsUnsupportedProtocolVersion(t *testing.T) {
	raw := []byte(`{"protocol":"session-link","version":2,"kind":"event","name":"output.text"}`)
	_, err := decodeFrame(raw, defaultMaxFrameBytes)
	if err == nil || !strings.Contains(err.Error(), "unsupported version") {
		t.Fatalf("decodeFrame error = %v, want unsupported version", err)
	}
}

func TestDecodeFrameRejectsOversizedFrame(t *testing.T) {
	raw := []byte(`{"protocol":"session-link","version":1,"kind":"event","name":"output.text"}`)
	_, err := decodeFrame(raw, len(raw)-1)
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("decodeFrame error = %v, want frame size rejection", err)
	}
}

func TestDecodePayloadRejectsUnknownFieldsAndTrailingValues(t *testing.T) {
	_, err := decodePayload[outputTextPayload]([]byte(`{"content":"ok","extra":true}`))
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("decodePayload error = %v, want unknown field", err)
	}
	_, err = decodePayload[outputTextPayload]([]byte(`{"content":"ok"} {"content":"again"}`))
	if err == nil || !strings.Contains(err.Error(), "multiple JSON values") {
		t.Fatalf("decodePayload error = %v, want multiple values", err)
	}
}

func TestEncodeFrameRejectsInvalidEnvelope(t *testing.T) {
	tests := []frame{
		{Protocol: "other", Version: protocolVersion, Kind: frameKindEvent, Name: eventOutputText},
		{Protocol: protocolName, Version: protocolVersion, Kind: "mystery", Name: eventOutputText},
		{Protocol: protocolName, Version: protocolVersion, Kind: frameKindRequest, Name: messageTurnSubmit},
		{Protocol: protocolName, Version: protocolVersion, Kind: frameKindEvent, Name: ""},
	}
	for _, test := range tests {
		if _, err := encodeFrame(test); err == nil {
			t.Fatalf("encodeFrame(%#v) unexpectedly succeeded", test)
		}
	}
}

func TestMapWireEventIgnoresUnknownEventAndRejectsBadPayload(t *testing.T) {
	unknown, known, err := mapWireEvent(frame{Name: "future.event", SessionID: "s1"})
	if err != nil || known || unknown.Type != "" {
		t.Fatalf("unknown mapping = (%#v, %v, %v)", unknown, known, err)
	}
	_, known, err = mapWireEvent(frame{
		Name:      eventOutputText,
		SessionID: "s1",
		Payload:   []byte(`{"content":"ok","unexpected":true}`),
	})
	if !known || err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("bad payload mapping = (known=%v, err=%v)", known, err)
	}
}
