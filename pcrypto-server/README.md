# pcrypto-server

This folder contains a small paper-trading service for short-horizon Polymarket crypto markets. It uses the Go standard library and runs on its own.

Each scan reads the current Up and Down prices for BTC, ETH, SOL, XRP, and DOGE markets. The service picks the favored side inside a configured price band. For 15-minute markets, it can compare the direction with Kalshi. Accepted candidates go into a paper ledger and settle after the market closes.

The dashboard shows open paper positions, recent results, and the activity log. State can persist between runs.

## Run

```text
go run .
go run . -port 127.0.0.1:9000
go run . -coins btc,eth -durations 15m,1h
go run . -min-conf 0.5
go run . -live
```

`-live` reads live market data and logs proposed orders. It does not submit orders. All displayed profit and loss comes from the paper ledger.

Build a binary with:

```text
go build -o pcrypto-server .
```

## Settings

Copy `pcrypto-settings.example.json` to `pcrypto-settings.json` before changing the defaults. The local settings file and paper state are ignored by Git.

The main flags are:

| Flag | Default | Purpose |
|---|---:|---|
| `-port` | `127.0.0.1:8799` | Dashboard address |
| `-coins` | `btc,eth,sol,xrp,doge` | Coins to scan |
| `-durations` | `15m,1h,4h,1d` | Market windows |
| `-interval` | `30s` | Time between scans |
| `-bankroll` | `100` | Starting paper balance |
| `-stake-pct` | `0.05` | Paper stake per candidate |
| `-min-entry` | `0.52` | Lowest favored-side price |
| `-max-entry` | `0.90` | Highest favored-side price |
| `-min-conf` | `0` | Minimum research score |
| `-cross-x` | `2.0` | Size multiplier when Kalshi agrees |
| `-state` | `pcrypto-state.json` | Paper-state file |
