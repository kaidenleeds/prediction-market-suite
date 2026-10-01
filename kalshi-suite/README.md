# Kalshi Suite

The main service for the Prediction Market Research & Execution Suite.

It provides venue clients, validated order books, strategy research, paper portfolios, guarded execution, durable evidence storage, a local dashboard, and an optional Python model sidecar.

## Build and test

```powershell
Copy-Item config.example.json config.json
$env:GOEXPERIMENT = 'jsonv2'
go test ./...
go vet ./...
go build -o kalshi-suite.exe ./cmd/kalshi-suite
```

Run the server:

```powershell
.\kalshi-suite.exe serve
```

The dashboard listens on `127.0.0.1:8787` by default.

## Safe configuration

The example configuration starts with live allocation disabled. Real credentials belong in the encrypted local store, environment variables, or files outside this repository. `config.json`, key files, databases, logs, and runtime state are ignored by Git.

Useful commands:

```text
kalshi-suite serve             start the local dashboard and API
kalshi-suite ping              check public and authenticated connectivity
kalshi-suite markets           list current markets
kalshi-suite set-credentials   write encrypted Kalshi credentials locally
```

Review the root [README](../README.md) for architecture and safety notes.
