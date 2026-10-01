package kalshi

import (
	"context"
	"net/http"
)

// GetAccountLimits returns GET /account/limits — your current usage tier, token refill rate, bucket
// capacity (read + write), and active grants. Authenticated (uses the loaded prod key). Read-only.
func (c *Client) GetAccountLimits(ctx context.Context) (map[string]any, error) {
	var out map[string]any
	if err := c.do(ctx, http.MethodGet, "/account/limits", &out, true); err != nil {
		return nil, err
	}
	return out, nil
}

// UpgradeTier requests the permanent Advanced API usage-level grant (POST /account/api_usage_level/
// upgrade), raising the rate cap (Basic 200/100 -> Advanced 300/300 tokens/s ≈ 20 -> 30 req/s at
// the default 10-token cost). Kalshi REQUIRES that at least one of your last 100 orders was
// API-created, else it rejects. This is an ACCOUNT action, not an order, so the demo-only order
// lock does not apply. Sent with an empty JSON body: the endpoint 400s invalid_content_type on a
// body-less POST (no Content-Type header).
func (c *Client) UpgradeTier(ctx context.Context) (map[string]any, error) {
	var out map[string]any
	if err := c.doWithBody(ctx, http.MethodPost, "/account/api_usage_level/upgrade", map[string]any{}, &out, true); err != nil {
		return nil, err
	}
	return out, nil
}
