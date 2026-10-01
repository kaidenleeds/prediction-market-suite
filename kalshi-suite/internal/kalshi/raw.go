package kalshi

import "context"

// RawGET returns an authenticated GET's raw body — a truth-probe for verifying live response
// shapes before coding parsers against them (kept small + permanent: shape drift keeps happening).
func (c *Client) RawGET(ctx context.Context, path string) ([]byte, error) {
	var raw rawCapture
	if err := c.do(ctx, "GET", path, &raw, true); err != nil {
		return nil, err
	}
	return raw.b, nil
}

// rawCapture satisfies json.Unmarshaler by keeping the bytes verbatim.
type rawCapture struct{ b []byte }

func (r *rawCapture) UnmarshalJSON(b []byte) error {
	r.b = append([]byte(nil), b...)
	return nil
}
