# Prediction Market Suite

A local research and trading system for prediction markets. It collects market data, looks for possible price gaps, tests ideas with paper trades, stores the full decision trail in SQLite, and shows the results in a browser dashboard.

The repo includes a safe demo that runs with generated data. It does not need an exchange account, API key, Python, or a separate database install.

## Try the demo

Install [Go 1.25 or newer](https://go.dev/dl/), then run:

```powershell
git clone https://github.com/kaidenleeds/prediction-market-suite.git
cd prediction-market-suite\kalshi-suite
go run ./cmd/showcase
```

The dashboard opens at `http://127.0.0.1:8787`. The first run downloads the Go packages, creates `demo-data/showcase.db`, and fills it with sample markets, signals, paper trades, and research results.

The demo updates on its own. Use **Pause** to stop the clock or **Run one step** to advance it manually. Delete the `demo-data` folder or add `-reset` to start with fresh sample data.

```powershell
go run ./cmd/showcase -reset
```

## What happens in the demo

Suppose other markets imply a 47% chance while a YES contract is available for 41¢. The 6¢ gap becomes a candidate. The suite then checks the current price, fees, available size, market status, and account limits. A passing candidate becomes a paper trade and is saved for later scoring.

The dashboard shows each part of that flow:

1. Read current markets and reference prices.
2. Create signals from price gaps, market movement, or order flow.
3. Check fees, size, timing, and risk limits.
4. Record simulated fills in SQLite.
5. Compare settled results across older and newer samples.

All names, prices, trades, and results in `cmd/showcase` are synthetic.

## Main features

- Kalshi and Polymarket market feeds
- Order-book rebuilding with stale-data and sequence-gap checks
- Market matching across venues
- Signal families for value gaps, spot lag, order flow, weather, crypto, and other research ideas
- Paper portfolios with fills, fees, settlement, and P&L
- SQLite ledgers for decisions, market snapshots, orders, and research
- Python models for book-based scoring
- Local dashboard and JSON API
- Explicit arming and risk checks around authenticated order routes

The public snapshot keeps the full source tree. Credentials, personal account data, live databases, private logs, machine paths, and operator notes are excluded.

## Project layout

```text
kalshi-suite/
  cmd/showcase/           safe first-run demo
  cmd/kalshi-suite/       main service and command-line app
  internal/kalshi/        Kalshi data, signing, fees, and orders
  internal/polymarketus/  Polymarket US data, books, and orders
  internal/polymarket/    international research feeds
  internal/server/        dashboard, strategies, paper trading, and order checks
  internal/storage/       SQLite schemas, ledgers, archives, and reports
  ml/                     Python training and scoring
forwarder/                optional phone notifications
pcrypto-server/           standalone paper-research service
```

## Run the full test suite

From `kalshi-suite`:

```powershell
$env:GOEXPERIMENT = 'jsonv2'
go test ./...
go vet ./...
```

The demo has its own focused test:

```powershell
go test ./cmd/showcase
```

## Full local service

The full service runs on public feeds without private keys. Add your own exchange credentials to load private account data and authenticated feeds.

```powershell
cd kalshi-suite
Copy-Item config.example.json config.json
$env:GOEXPERIMENT = 'jsonv2'
go run ./cmd/kalshi-suite serve
```

Open `http://127.0.0.1:8787` if the browser does not open on its own.

The example settings start in paper mode. Automatic trading and live order routes are off. See [Full setup](docs/FULL_SETUP.md) for credential files, environment-variable options, startup checks, and the Python model service.

Keep `config.json`, key files, databases, logs, and runtime state on your machine. The included `.gitignore` blocks their normal paths.

## Safety

Prediction markets involve financial risk. Paper results, backtests, model scores, and visible prices do not prove future profit. Read [SECURITY.md](SECURITY.md) before adding authenticated access.
