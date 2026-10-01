// Package pmx is the Polymarket US EXCHANGE gRPC client (grpc-api.prod.polymarketexchange.com) —
// the venue's fastest documented feed: binary protobuf streams, 20 concurrent streams/firm, and
// crucially egress that does NOT count against the retail 20 req/s REST budget.
//
// AUTH IS INSTITUTIONAL (R22, docs-verified 2026-07-02): OAuth2 client-credentials with an RS256
// JWT client assertion against Polymarket's Auth0 — a DIFFERENT credential than the retail
// Ed25519 key from polymarket.us/developer. Tokens live 3 MINUTES; the TokenSource here refreshes
// 30s early, automatically, forever. Until onboarding issues these credentials
// (support@polymarket.us / onboarding@polymarket.us), this package stays dark and the retail
// WebSockets carry the suite.
//
// Activate with environment variables:
//
//	PMX_AUTH0_DOMAIN    e.g. pmx-prod.us.auth0.com
//	PMX_CLIENT_ID       Auth0 client id (issued at onboarding)
//	PMX_AUDIENCE        API audience URL (issued at onboarding)
//	PMX_KEY_FILE        path to the RS256 private key PEM (public half registered with Polymarket)
//	PMX_PARTICIPANT_ID  e.g. firms/YOUR_FIRM/users/YOUR_USER
//	PMX_GRPC_ADDR       optional; default grpc-api.prod.polymarketexchange.com:443
//
// Then run `kalshi-suite.exe pmx-probe` — it verifies token → health → identity → instruments →
// a 10s live market-data stream, and prints the venue's symbology so the cache integration can be
// built against OBSERVED shapes (probe-first).
package pmx

import (
	"context"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"

	pmxv1 "github.com/kalshi-suite/kalshi-suite/internal/pmxgen/polymarket/v1"
)

// Config holds the onboarding-issued credentials. FromEnv returns ok=false when unset (normal
// for retail-only operation — callers must treat that as "feature dark", not an error).
type Config struct {
	Auth0Domain   string
	ClientID      string
	Audience      string
	KeyFile       string
	ParticipantID string
	Addr          string
}

// FromEnv loads the PMX_* variables. ok=false ⇒ gRPC stack not configured.
func FromEnv() (Config, bool) {
	c := Config{
		Auth0Domain:   os.Getenv("PMX_AUTH0_DOMAIN"),
		ClientID:      os.Getenv("PMX_CLIENT_ID"),
		Audience:      os.Getenv("PMX_AUDIENCE"),
		KeyFile:       os.Getenv("PMX_KEY_FILE"),
		ParticipantID: os.Getenv("PMX_PARTICIPANT_ID"),
		Addr:          os.Getenv("PMX_GRPC_ADDR"),
	}
	if c.Addr == "" {
		c.Addr = "grpc-api.prod.polymarketexchange.com:443"
	}
	ok := c.Auth0Domain != "" && c.ClientID != "" && c.Audience != "" && c.KeyFile != ""
	return c, ok
}

// TokenSource mints and caches 3-minute Auth0 access tokens, refreshing 30s before expiry.
type TokenSource struct {
	cfg  Config
	priv *rsa.PrivateKey

	mu    sync.Mutex
	tok   string
	expAt time.Time
}

// NewTokenSource parses the RS256 key and returns a refreshing token source.
func NewTokenSource(cfg Config) (*TokenSource, error) {
	b, err := os.ReadFile(cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("read PMX key: %w", err)
	}
	block, _ := pem.Decode(b)
	if block == nil {
		return nil, errors.New("PMX key: no PEM block")
	}
	var priv *rsa.PrivateKey
	if k, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		priv = k
	} else if kAny, err2 := x509.ParsePKCS8PrivateKey(block.Bytes); err2 == nil {
		var ok bool
		if priv, ok = kAny.(*rsa.PrivateKey); !ok {
			return nil, errors.New("PMX key: not RSA")
		}
	} else {
		return nil, fmt.Errorf("PMX key: parse failed (PKCS1: %v, PKCS8: %v)", err, err2)
	}
	return &TokenSource{cfg: cfg, priv: priv}, nil
}

// Token returns a live access token, minting/refreshing as needed.
// KEY DETAIL (docs): the assertion's `aud` is the TOKEN ENDPOINT URL; the API audience goes in
// the token request body's `audience` field. Getting these backwards → invalid_client.
func (t *TokenSource) Token(ctx context.Context) (string, error) {
	t.mu.Lock()
	if t.tok != "" && time.Until(t.expAt) > 30*time.Second {
		tok := t.tok
		t.mu.Unlock()
		return tok, nil
	}
	t.mu.Unlock()

	tokenURL := "https://" + t.cfg.Auth0Domain + "/oauth/token"
	now := time.Now()
	claims := jwt.MapClaims{
		"iss": t.cfg.ClientID,
		"sub": t.cfg.ClientID,
		"aud": tokenURL,
		"iat": now.Unix(),
		"exp": now.Add(5 * time.Minute).Unix(),
		"jti": fmt.Sprintf("%d-%d", now.UnixNano(), os.Getpid()),
	}
	assertion, err := jwt.NewWithClaims(jwt.SigningMethodRS256, claims).SignedString(t.priv)
	if err != nil {
		return "", fmt.Errorf("sign PMX assertion: %w", err)
	}
	body, _ := json.Marshal(map[string]string{
		"grant_type":            "client_credentials",
		"client_id":             t.cfg.ClientID,
		"client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
		"client_assertion":      assertion,
		"audience":              t.cfg.Audience,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(string(body)))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("PMX token request: %w", err)
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("PMX token: status %d: %s", resp.StatusCode, truncate(string(rb), 300))
	}
	var tr struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.Unmarshal(rb, &tr); err != nil || tr.AccessToken == "" {
		return "", fmt.Errorf("PMX token: unparseable: %s", truncate(string(rb), 200))
	}
	t.mu.Lock()
	t.tok = tr.AccessToken
	t.expAt = time.Now().Add(time.Duration(tr.ExpiresIn) * time.Second)
	t.mu.Unlock()
	return tr.AccessToken, nil
}

// perRPC injects Bearer + x-participant-id metadata on every call, minting fresh tokens as the
// 3-minute expiry rolls — so even a days-long stream keeps authenticating.
type perRPC struct {
	ts   *TokenSource
	part string
}

func (p perRPC) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	tok, err := p.ts.Token(ctx)
	if err != nil {
		return nil, err
	}
	md := map[string]string{"authorization": "Bearer " + tok}
	if p.part != "" {
		md["x-participant-id"] = p.part
	}
	return md, nil
}
func (p perRPC) RequireTransportSecurity() bool { return true }

// Client is the dialed gRPC connection plus typed service stubs.
type Client struct {
	cfg  Config
	conn *grpc.ClientConn

	Health  pmxv1.HealthAPIClient
	Refdata pmxv1.RefDataAPIClient
	MktData pmxv1.MarketDataSubscriptionAPIClient
	Orders  pmxv1.OrderEntryAPIClient
}

// Dial connects with TLS + auto-refreshing bearer credentials and gRPC keepalive pings (the ALB
// idle-kills quiet streams at ~1h; keepalive + the app-level KeepAliveCommand cover both layers).
func Dial(ctx context.Context, cfg Config) (*Client, error) {
	ts, err := NewTokenSource(cfg)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(cfg.Addr,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})),
		grpc.WithPerRPCCredentials(perRPC{ts: ts, part: cfg.ParticipantID}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 30 * time.Second, Timeout: 10 * time.Second, PermitWithoutStream: true}),
	)
	if err != nil {
		return nil, fmt.Errorf("PMX dial %s: %w", cfg.Addr, err)
	}
	return &Client{
		cfg: cfg, conn: conn,
		Health:  pmxv1.NewHealthAPIClient(conn),
		Refdata: pmxv1.NewRefDataAPIClient(conn),
		MktData: pmxv1.NewMarketDataSubscriptionAPIClient(conn),
		Orders:  pmxv1.NewOrderEntryAPIClient(conn),
	}, nil
}

// Close tears down the connection.
func (c *Client) Close() error { return c.conn.Close() }

// Probe is the credential-arrival smoke test: token → health → whoami-equivalent (instruments) →
// 10 seconds of live market data, printing the venue's SYMBOLOGY so the slug↔symbol mapping and
// cache integration get built against observed truth, never guesses.
func (c *Client) Probe(ctx context.Context, w io.Writer) error {
	fmt.Fprintf(w, "PMX probe → %s (participant %q)\n", c.cfg.Addr, c.cfg.ParticipantID)
	hctx, hcancel := context.WithTimeout(ctx, 15*time.Second)
	defer hcancel()
	if hr, err := c.Health.HealthCheck(hctx, &pmxv1.HealthCheckRequest{}); err != nil {
		return fmt.Errorf("health check: %w", err)
	} else {
		fmt.Fprintf(w, "health: %v\n", hr)
	}
	ictx, icancel := context.WithTimeout(ctx, 20*time.Second)
	defer icancel()
	insts, err := c.Refdata.ListInstruments(ictx, &pmxv1.ListInstrumentsRequest{PageSize: 10})
	if err != nil {
		return fmt.Errorf("list instruments (auth check): %w", err)
	}
	symbols := []string{}
	for _, in := range insts.GetInstruments() {
		fmt.Fprintf(w, "instrument: symbol=%q state=%v qty_scale=%d minimum_qty=%g\n",
			in.GetSymbol(), in.GetState(), InstrumentQuantityScale(in), InstrumentMinimumTradeQty(in))
		if len(symbols) < 3 && in.GetSymbol() != "" {
			symbols = append(symbols, in.GetSymbol())
		}
	}
	if len(symbols) == 0 {
		fmt.Fprintln(w, "no instruments returned — stream test skipped")
		return nil
	}
	sctx, scancel := context.WithTimeout(ctx, 12*time.Second)
	defer scancel()
	stream, err := c.MktData.CreateMarketDataSubscription(sctx, &pmxv1.CreateMarketDataSubscriptionRequest{
		Symbols: symbols, Depth: 1, SlowConsumerSkipToHead: true,
	})
	if err != nil {
		return fmt.Errorf("market data subscribe: %w", err)
	}
	fmt.Fprintf(w, "streaming %v for ~10s…\n", symbols)
	n := 0
	for {
		msg, err := stream.Recv()
		if err != nil {
			if sctx.Err() != nil {
				break // timeout = normal end of probe
			}
			return fmt.Errorf("stream recv after %d msgs: %w", n, err)
		}
		n++
		if n <= 5 {
			fmt.Fprintf(w, "  msg %d: %s\n", n, truncate(fmt.Sprintf("%v", msg), 240))
		}
		if n >= 50 {
			break
		}
	}
	fmt.Fprintf(w, "stream delivered %d messages. PMX gRPC is LIVE — next step: wire symbology into the price/order caches.\n", n)
	return nil
}

// MDUpdate is one simplified market-data tick handed to the integration callback.
type MDUpdate struct {
	Symbol string
	Raw    *pmxv1.MarketDataUpdate
}

// StreamMarketData runs the bidirectional stream with app-level keepalives (ALB idle-timeout
// countermeasure, per the proto docs) and auto-reconnect, invoking cb per update. Symbol-set is
// fixed per call; reconnects resubscribe. Runs until ctx ends.
func (c *Client) StreamMarketData(ctx context.Context, symbols []string, cb func(MDUpdate)) {
	backoff := 2 * time.Second
	for ctx.Err() == nil {
		started := time.Now()
		err := c.streamMDOnce(ctx, symbols, cb)
		if ctx.Err() != nil {
			return
		}
		if time.Since(started) > time.Minute {
			backoff = 2 * time.Second
		} else if backoff < time.Minute {
			backoff *= 2
		}
		jitter := time.Duration(time.Now().UnixNano() % int64(backoff/2+1))
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff/2 + jitter):
		}
		_ = err
	}
}

func (c *Client) streamMDOnce(ctx context.Context, symbols []string, cb func(MDUpdate)) error {
	stream, err := c.MktData.BiDirectionalStreamMarketData(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&pmxv1.BiDirectionalStreamMarketDataRequest{
		Command: &pmxv1.BiDirectionalStreamMarketDataRequest_Subscribe{Subscribe: &pmxv1.SubscribeCommand{Symbols: symbols}},
		Depth:   1, SlowConsumerSkipToHead: true,
	}); err != nil {
		return err
	}
	done := make(chan struct{})
	defer close(done)
	go func() { // app-level keepalive ~5min (ALB idle_timeout ~1h; stay far under)
		t := time.NewTicker(5 * time.Minute)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				_ = stream.Send(&pmxv1.BiDirectionalStreamMarketDataRequest{
					Command: &pmxv1.BiDirectionalStreamMarketDataRequest_Keepalive{Keepalive: &pmxv1.KeepAliveCommand{}},
				})
			}
		}
	}()
	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}
		if up := msg.GetUpdate(); up != nil {
			cb(MDUpdate{Symbol: up.GetSymbol(), Raw: up})
		}
	}
}

// note: metadata import kept for future unary helpers that need explicit contexts.
var _ = metadata.MD{}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
