// Command kalshi-forwarder watches a folder for new Kalshi briefing files and
// pushes each new one to your phone via ntfy, Telegram, or a generic webhook.
//
// Standard library only — build with `go build` and no dependency downloads.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type NtfyConfig struct {
	Server string `json:"server"`
	Topic  string `json:"topic"`
}
type TelegramConfig struct {
	BotToken string `json:"bot_token"`
	ChatID   string `json:"chat_id"`
}
type WebhookConfig struct {
	URL string `json:"url"`
}

type Config struct {
	WatchDir               string         `json:"watch_dir"`
	PollSeconds            int            `json:"poll_seconds"`
	Channel                string         `json:"channel"` // ntfy | telegram | webhook
	ForwardExistingOnStart bool           `json:"forward_existing_on_start"`
	StateFile              string         `json:"state_file"`
	Ntfy                   NtfyConfig     `json:"ntfy"`
	Telegram               TelegramConfig `json:"telegram"`
	Webhook                WebhookConfig  `json:"webhook"`
}

const maxBodyRunes = 3500

func main() {
	cfgPath := flag.String("config", "forwarder-config.json", "path to config JSON")
	flag.Parse()

	cfg, err := loadConfig(*cfgPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	if cfg.PollSeconds <= 0 {
		cfg.PollSeconds = 15
	}
	if cfg.StateFile == "" {
		cfg.StateFile = "forwarder-state.json"
	}
	if cfg.WatchDir == "" {
		log.Fatal("config: watch_dir is required")
	}
	if err := os.MkdirAll(cfg.WatchDir, 0o755); err != nil {
		log.Fatalf("create watch dir: %v", err)
	}

	seen, err := loadState(cfg.StateFile)
	if err != nil {
		log.Printf("warning: could not load state (%v); starting fresh", err)
		seen = map[string]bool{}
	}

	// On the very first run, optionally skip the existing backlog so you don't
	// get spammed with old briefings the moment you start the forwarder.
	if len(seen) == 0 && !cfg.ForwardExistingOnStart {
		for _, f := range listBriefings(cfg.WatchDir) {
			seen[f] = true
		}
		_ = saveState(cfg.StateFile, seen)
		log.Printf("first run: marked %d existing file(s) as already-seen (backlog not forwarded)", len(seen))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	log.Printf("kalshi-forwarder: watching %q every %ds, channel=%s", cfg.WatchDir, cfg.PollSeconds, cfg.Channel)

	ticker := time.NewTicker(time.Duration(cfg.PollSeconds) * time.Second)
	defer ticker.Stop()

	scan(cfg, seen) // scan once immediately
	for {
		select {
		case <-ctx.Done():
			log.Println("shutting down")
			return
		case <-ticker.C:
			scan(cfg, seen)
		}
	}
}

func scan(cfg Config, seen map[string]bool) {
	for _, name := range listBriefings(cfg.WatchDir) {
		if seen[name] {
			continue
		}
		content, err := os.ReadFile(filepath.Join(cfg.WatchDir, name))
		if err != nil {
			log.Printf("read %s: %v", name, err)
			continue
		}
		title := firstLine(string(content), name)
		if err := send(cfg, title, string(content), name); err != nil {
			log.Printf("send %s: %v (will retry next poll)", name, err)
			continue // do NOT mark as seen, so it retries
		}
		seen[name] = true
		if err := saveState(cfg.StateFile, seen); err != nil {
			log.Printf("save state: %v", err)
		}
		log.Printf("forwarded %s", name)
	}
}

func listBriefings(dir string) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(strings.ToLower(e.Name()), ".md") {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out
}

func firstLine(content, fallback string) string {
	for _, ln := range strings.Split(content, "\n") {
		ln = strings.TrimSpace(strings.TrimLeft(strings.TrimSpace(ln), "#"))
		if ln != "" {
			return clip(ln, 120)
		}
	}
	return fallback
}

func send(cfg Config, title, body, filename string) error {
	body = clip(body, maxBodyRunes)
	switch strings.ToLower(cfg.Channel) {
	case "ntfy":
		return sendNtfy(cfg.Ntfy, title, body)
	case "telegram":
		return sendTelegram(cfg.Telegram, title, body)
	case "webhook":
		return sendWebhook(cfg.Webhook, title, body, filename)
	default:
		return fmt.Errorf("unknown channel %q (want ntfy | telegram | webhook)", cfg.Channel)
	}
}

func sendNtfy(c NtfyConfig, title, body string) error {
	if c.Topic == "" {
		return fmt.Errorf("ntfy.topic is empty")
	}
	server := c.Server
	if server == "" {
		server = "https://ntfy.sh"
	}
	endpoint := strings.TrimRight(server, "/") + "/" + url.PathEscape(c.Topic)
	req, err := http.NewRequest(http.MethodPost, endpoint, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Title", asciiOnly(title))
	req.Header.Set("Tags", "chart_with_upwards_trend")
	return doReq(req)
}

func sendTelegram(c TelegramConfig, title, body string) error {
	if c.BotToken == "" || c.ChatID == "" {
		return fmt.Errorf("telegram.bot_token and telegram.chat_id are required")
	}
	payload := map[string]any{
		"chat_id":                  c.ChatID,
		"text":                     clip(title+"\n\n"+body, 4000),
		"disable_web_page_preview": true,
	}
	b, _ := json.Marshal(payload)
	endpoint := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", c.BotToken)
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	return doReq(req)
}

func sendWebhook(c WebhookConfig, title, body, filename string) error {
	if c.URL == "" {
		return fmt.Errorf("webhook.url is required")
	}
	payload := map[string]string{"title": title, "filename": filename, "body": body}
	b, _ := json.Marshal(payload)
	req, err := http.NewRequest(http.MethodPost, c.URL, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	return doReq(req)
}

func doReq(req *http.Request) error {
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 500))
		return fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return nil
}

// clip truncates to n runes (UTF-8 safe).
func clip(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "\n…(truncated — see the full file)"
}

func asciiOnly(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= 32 && r < 127 {
			b.WriteRune(r)
		}
	}
	return strings.TrimSpace(b.String())
}

func loadConfig(path string) (Config, error) {
	var c Config
	b, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	if err := json.Unmarshal(b, &c); err != nil {
		return c, fmt.Errorf("parse %s: %w", path, err)
	}
	return c, nil
}

func loadState(path string) (map[string]bool, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]bool{}, nil
		}
		return nil, err
	}
	m := map[string]bool{}
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return m, nil
}

func saveState(path string, m map[string]bool) error {
	b, _ := json.MarshalIndent(m, "", "  ")
	return os.WriteFile(path, b, 0o644)
}
