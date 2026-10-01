# Contributing

Changes should preserve the project's evidence and safety boundaries.

1. Do not commit credentials, account data, logs, databases, or generated runtime state.
2. Keep demo, paper, and live behavior explicitly separated.
3. Treat current executable prices, depth, fees, and lifecycle state as the source of truth for order decisions.
4. Add regression coverage for money, identity, timing, reconciliation, or restart behavior.
5. Run the full Go test suite and `go vet ./...` from `kalshi-suite` before opening a pull request.

Bug reports should include a minimal reproduction with secrets and account identifiers removed.
