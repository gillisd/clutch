package ws

import (
	"encoding/json"
	"fmt"
)

// request is an internal envelope pairing an outbound message with a response channel.
type request struct {
	id     uint64
	raw    json.RawMessage
	respCh chan json.RawMessage // closed on shutdown
}

// injectID sets the correlation ID field in a raw JSON message.
func injectID(idField string, data json.RawMessage, id uint64) (json.RawMessage, error) {
	var m map[string]json.RawMessage
	if len(data) == 0 || string(data) == "null" {
		m = make(map[string]json.RawMessage)
	} else {
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, fmt.Errorf("unmarshal for ID injection: %w", err)
		}
	}
	idBytes, err := json.Marshal(id)
	if err != nil {
		return nil, fmt.Errorf("marshal ID: %w", err)
	}
	m[idField] = json.RawMessage(idBytes)
	out, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("marshal after ID injection: %w", err)
	}
	return json.RawMessage(out), nil
}

// extractID reads the correlation ID field from a raw JSON message.
func extractID(idField string, data json.RawMessage) (uint64, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return 0, fmt.Errorf("unmarshal for ID extraction: %w", err)
	}
	raw, ok := m[idField]
	if !ok {
		return 0, fmt.Errorf("missing ID field %q", idField)
	}
	var id uint64
	if err := json.Unmarshal(raw, &id); err != nil {
		return 0, fmt.Errorf("parse ID field %q: %w", idField, err)
	}
	return id, nil
}
