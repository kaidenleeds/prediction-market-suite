module github.com/kalshi-suite/kalshi-suite

go 1.25.0

// Dependencies are intentionally not pinned here. Run `go mod tidy` once to
// fetch and lock them. The only direct external dependencies are:
//
//   modernc.org/sqlite     -- pure-Go SQLite driver (no CGO, single static binary)
//   golang.org/x/crypto    -- Argon2id key derivation for the encrypted credential store
//
// Everything else is the Go standard library.

require (
	github.com/golang-jwt/jwt/v5 v5.3.1
	github.com/gorilla/websocket v1.5.3
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.29.0
	github.com/jchv/go-webview2 v0.0.0-20260205173254-56598839c808
	golang.org/x/crypto v0.53.0
	golang.org/x/sys v0.46.0
	google.golang.org/genproto/googleapis/api v0.0.0-20260630182238-925bb5da69e7
	google.golang.org/grpc v1.82.0
	google.golang.org/protobuf v1.36.11
	modernc.org/sqlite v1.52.0
)

require (
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jchv/go-winloader v0.0.0-20250406163304-c1995be93bd1 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/text v0.38.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260622175928-b703f567277d // indirect
	modernc.org/libc v1.72.3 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
)
