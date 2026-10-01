package researchreplay

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode"
)

var forbiddenReplayKeys = map[string]bool{
	"authorization": true, "proxyauthorization": true, "cookie": true, "setcookie": true,
	"password": true, "passphrase": true, "privatekey": true, "secret": true,
	"apikey": true, "accesskey": true, "accesstoken": true, "refreshtoken": true,
	"bottoken": true, "telegrambottoken": true, "signature": true, "credential": true,
}

var forbiddenReplayBytePatterns = [][]byte{
	[]byte("-----begin private key"), []byte("-----begin rsa private key"),
	[]byte("-----begin encrypted private key"), []byte("kalshi-access-signature"),
	[]byte("kalshi-access-key"), []byte("proxy-authorization"), []byte("authorization: bearer"),
	[]byte("bearer "),
}

func normalizedSecretKey(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return unicode.ToLower(r)
		}
		return -1
	}, s)
}

func forbiddenReplayKey(key string) bool {
	key = normalizedSecretKey(key)
	if forbiddenReplayKeys[key] || strings.Contains(key, "authorization") || strings.Contains(key, "cookie") {
		return true
	}
	for _, suffix := range []string{"password", "passphrase", "privatekey", "secret", "apikey",
		"accesskey", "accesstoken", "refreshtoken", "bottoken", "signature", "credential"} {
		if strings.HasSuffix(key, suffix) {
			return true
		}
	}
	return false
}

func scanReplaySecrets(v any, depth int) error {
	if depth > 32 {
		return fmt.Errorf("research replay payload nesting exceeds 32")
	}
	switch x := v.(type) {
	case map[string]any:
		for key, value := range x {
			if forbiddenReplayKey(key) {
				return fmt.Errorf("%w: forbidden key %q", ErrSecretMaterial, key)
			}
			if err := scanReplaySecrets(value, depth+1); err != nil {
				return err
			}
		}
	case []any:
		for _, value := range x {
			if err := scanReplaySecrets(value, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func canonicalPayload(payload any) (json.RawMessage, error) {
	var raw []byte
	var err error
	switch v := payload.(type) {
	case json.RawMessage:
		raw = append([]byte(nil), v...)
	case []byte:
		raw = append([]byte(nil), v...)
	default:
		raw, err = json.Marshal(v)
		if err != nil {
			return nil, err
		}
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, fmt.Errorf("research replay payload is empty")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var decoded any
	if err := dec.Decode(&decoded); err != nil {
		return nil, fmt.Errorf("research replay payload JSON: %w", err)
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("research replay payload has trailing JSON")
		}
		return nil, fmt.Errorf("research replay payload trailing JSON: %w", err)
	}
	if err := scanReplaySecrets(decoded, 0); err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(decoded)
	if err != nil {
		return nil, err
	}
	lower := bytes.ToLower(canonical)
	for _, pattern := range forbiddenReplayBytePatterns {
		if bytes.Contains(lower, pattern) {
			return nil, fmt.Errorf("%w: forbidden credential byte pattern", ErrSecretMaterial)
		}
	}
	return canonical, nil
}

func validateReplayLabel(name, value string, required bool) error {
	value = strings.TrimSpace(value)
	if required && value == "" {
		return fmt.Errorf("research replay %s is required", name)
	}
	if len(value) > 512 {
		return fmt.Errorf("research replay %s exceeds 512 bytes", name)
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return fmt.Errorf("research replay %s contains a control character", name)
		}
	}
	lower := []byte(strings.ToLower(value))
	for _, pattern := range forbiddenReplayBytePatterns {
		if bytes.Contains(lower, pattern) {
			return fmt.Errorf("%w in %s", ErrSecretMaterial, name)
		}
	}
	return nil
}

func validateAndCopyFrame(in Frame, maxBytes int) (Frame, int, error) {
	for _, field := range []struct {
		name, value string
		required    bool
	}{
		{"source", in.Source, true}, {"schema_version", in.SchemaVersion, true},
		{"kind", in.Kind, true}, {"entity_id", in.EntityID, true}, {"selection", in.Selection, true},
		{"canonical_event_id", in.CanonicalEventID, false}, {"opportunity_id", in.OpportunityID, false},
	} {
		if err := validateReplayLabel(field.name, field.value, field.required); err != nil {
			return Frame{}, 0, err
		}
	}
	if !in.ResearchOnly || in.Funded || in.PaperAuthority || in.LiveAuthority {
		return Frame{}, 0, fmt.Errorf("research replay frame attempted trading authority")
	}
	if in.SequenceGap < 0 {
		return Frame{}, 0, fmt.Errorf("research replay sequence gap cannot be negative")
	}
	if in.SourceSequence != nil && *in.SourceSequence < 0 {
		return Frame{}, 0, fmt.Errorf("research replay source sequence cannot be negative")
	}
	if in.PriorSourceSequence != nil && *in.PriorSourceSequence < 0 {
		return Frame{}, 0, fmt.Errorf("research replay prior source sequence cannot be negative")
	}
	payload, err := canonicalPayload(in.Payload)
	if err != nil {
		return Frame{}, 0, err
	}
	if len(payload) > maxBytes {
		return Frame{}, 0, fmt.Errorf("research replay frame payload=%d exceeds max=%d", len(payload), maxBytes)
	}
	out := in
	out.Source, out.SchemaVersion = strings.TrimSpace(in.Source), strings.TrimSpace(in.SchemaVersion)
	out.Kind, out.EntityID, out.Selection = strings.TrimSpace(in.Kind), strings.TrimSpace(in.EntityID), strings.TrimSpace(in.Selection)
	out.CanonicalEventID, out.OpportunityID = strings.TrimSpace(in.CanonicalEventID), strings.TrimSpace(in.OpportunityID)
	if out.ObservedAt.IsZero() {
		out.ObservedAt = time.Now().UTC()
	} else {
		out.ObservedAt = out.ObservedAt.UTC()
	}
	if !out.SourceAt.IsZero() {
		out.SourceAt = out.SourceAt.UTC()
	}
	if out.SourceClock == "" {
		if out.SourceAt.IsZero() {
			out.SourceClock = "not_applicable"
		} else {
			out.SourceClock = "venue"
		}
	}
	switch out.SourceClock {
	case "venue":
		if out.SourceAt.IsZero() {
			return Frame{}, 0, fmt.Errorf("research replay venue clock is missing source_at")
		}
	case "arrival_only", "missing", "not_applicable":
		if (out.SourceClock == "arrival_only" || out.SourceClock == "missing") && !out.SourceAt.IsZero() {
			return Frame{}, 0, fmt.Errorf("research replay %s clock cannot claim source_at", out.SourceClock)
		}
		if out.SourceClock == "not_applicable" && !out.SourceAt.IsZero() {
			return Frame{}, 0, fmt.Errorf("research replay not_applicable clock cannot claim source_at")
		}
	default:
		return Frame{}, 0, fmt.Errorf("research replay source clock %q is invalid", out.SourceClock)
	}
	derivedState, derivedGap := replaySequenceTruth(out.SourceSequence, out.PriorSourceSequence)
	if out.SequenceState == "" || out.SequenceState == "not_applicable" && out.SourceSequence != nil {
		out.SequenceState = derivedState
	}
	if out.SequenceGap == 0 && derivedGap > 0 {
		out.SequenceGap = derivedGap
	}
	switch out.SequenceState {
	case "not_applicable":
		if out.SourceSequence != nil || out.PriorSourceSequence != nil || out.SequenceGap != 0 {
			return Frame{}, 0, fmt.Errorf("research replay not_applicable sequence carries sequence truth")
		}
	case "missing":
		if out.SourceSequence != nil || out.PriorSourceSequence != nil || out.SequenceGap != 0 {
			return Frame{}, 0, fmt.Errorf("research replay missing sequence carries a sequence")
		}
	case "baseline":
		if out.SourceSequence == nil || out.PriorSourceSequence != nil || out.SequenceGap != 0 {
			return Frame{}, 0, fmt.Errorf("research replay baseline sequence is inconsistent")
		}
	case "contiguous":
		if out.SourceSequence == nil || out.PriorSourceSequence == nil || derivedState != "contiguous" || out.SequenceGap != 0 {
			return Frame{}, 0, fmt.Errorf("research replay contiguous sequence is inconsistent")
		}
	case "gap":
		if out.SourceSequence == nil || out.PriorSourceSequence == nil || derivedState != "gap" || out.SequenceGap != derivedGap {
			return Frame{}, 0, fmt.Errorf("research replay sequence gap is inconsistent")
		}
	case "replay_or_reset":
		if out.SourceSequence == nil || out.PriorSourceSequence == nil || derivedState != "replay_or_reset" || out.SequenceGap != 0 {
			return Frame{}, 0, fmt.Errorf("research replay reset/replay sequence is inconsistent")
		}
	default:
		return Frame{}, 0, fmt.Errorf("research replay sequence state %q is invalid", out.SequenceState)
	}
	out.Payload = append(json.RawMessage(nil), payload...)
	encoded, _ := json.Marshal(out)
	return out, len(encoded), nil
}

func replaySequenceTruth(current, prior *int64) (string, int64) {
	if current == nil {
		return "missing", 0
	}
	if prior == nil {
		return "baseline", 0
	}
	if *current == *prior+1 {
		return "contiguous", 0
	}
	if *current > *prior+1 {
		return "gap", *current - *prior - 1
	}
	return "replay_or_reset", 0
}

// SequenceTruth derives the only accepted sequence state/gap from actual adjacent source values.
// It never infers that unobserved values between periodic receipts were received.
func SequenceTruth(current, prior *int64) (string, int64) {
	return replaySequenceTruth(current, prior)
}
