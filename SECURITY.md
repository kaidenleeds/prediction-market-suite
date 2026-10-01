# Security policy

## Safe local use

- Bind the dashboard to localhost unless you add authentication and TLS.
- Begin with demo or read-only access.
- Keep live arming, automatic execution, and live allocation disabled by default.
- Store credentials outside the repository or in the encrypted local credential store.
- Treat Telegram tokens, chat identifiers, webhook URLs, private keys, API secrets, databases, and logs as sensitive.
- Review venue permissions and use keys with the minimum access required.

The repository ignores the normal credential and runtime paths, but ignore rules are not a substitute for reviewing staged files before every commit.

## Reporting a vulnerability

Please use GitHub's private vulnerability-reporting feature for the repository. Do not open a public issue containing credentials, account data, exploitable details, or private market activity.

## Financial-risk notice

Passing tests and safety gates does not establish that a strategy is profitable. Paper fills, modeled fills, backtests, visible liquidity, and model scores are not equivalent to authenticated exchange execution.
