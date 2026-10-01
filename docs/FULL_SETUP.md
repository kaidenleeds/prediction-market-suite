# Full setup

This guide runs the main service with real market feeds. It starts in paper mode. You choose which accounts and optional services to connect.

## What you need

- Go 1.25 or newer
- Git
- A Kalshi API key if you want authenticated Kalshi data
- A Polymarket US API key if you want its authenticated feed
- Python 3.11 or newer if you want to run the optional model service

The dashboard and public data routes still run when no exchange keys are present.

## 1. Create your local settings

From the repository root:

```powershell
cd kalshi-suite
Copy-Item config.example.json config.json
```

On macOS or Linux:

```bash
cd kalshi-suite
cp config.example.json config.json
```

`config.json` is ignored by Git. The example file binds the dashboard to your own computer, keeps automatic trading off, and leaves every live-order switch off.

The example uses `"environment": "demo"`. Change it to `"environment": "prod"` before loading a production Kalshi key. Leave it as `demo` only when you are using Kalshi's demo environment and demo credentials.

## 2. Add your own credentials

You can skip this section for a public-feed run.

### Kalshi

Save your RSA private key outside the repository. Set these two fields in `config.json`:

```json
"kalshi_key_file": "C:\\path\\outside\\the-repo\\kalshi.pem",
"kalshi_key_id": "your-api-key-id"
```

Use `/home/you/keys/kalshi.pem` style paths on macOS or Linux. The service reads the private key from that file. It does not print the key or place it in the database.

You may use environment variables instead:

```powershell
$env:KALSHI_PROD_KEY_ID = 'your-api-key-id'
$env:KALSHI_PROD_KEY_FILE = 'C:\path\outside\the-repo\kalshi.pem'
```

Those two names are for production. For a demo account, use `KALSHI_DEMO_KEY_ID` and `KALSHI_DEMO_KEY_FILE` and keep the config environment set to `demo`.

### Polymarket US

Save the base64 secret supplied with your API key as a one-line text file outside the repository. Set:

```json
"polyus_key_file": "C:\\path\\outside\\the-repo\\polyus-secret.txt",
"polyus_key_id": "your-api-key-id"
```

The environment-variable version is:

```powershell
$env:POLY_US_KEY_ID = 'your-api-key-id'
$env:POLY_US_SECRET_FILE = 'C:\path\outside\the-repo\polyus-secret.txt'
```

File paths are easier to keep out of shell history than raw secrets. Never commit either key file.

## 3. Start the service

PowerShell:

```powershell
$env:GOEXPERIMENT = 'jsonv2'
go run ./cmd/kalshi-suite serve
```

macOS or Linux:

```bash
export GOEXPERIMENT=jsonv2
go run ./cmd/kalshi-suite serve
```

The first run downloads Go packages and creates local SQLite files under `data`. Open `http://127.0.0.1:8787` to use the dashboard.

Leave the terminal open while the service runs. Press `Ctrl+C` for a clean shutdown.

## 4. Check the connection

The top of the dashboard shows feed and account status. These local endpoints provide the same checks in JSON:

```text
http://127.0.0.1:8787/api/status
http://127.0.0.1:8787/api/ready
```

With no keys, the service reports that authenticated connections are unavailable and continues with public data. With keys, confirm that each account and feed you enabled is healthy before using its data.

## Optional Python model service

The main Go service runs without Python. To use the model code, create a virtual environment and install the included requirements:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r ml\requirements.txt
```

On macOS or Linux, activate it with `source .venv/bin/activate` and use `ml/requirements.txt`.

The Go service starts the model process when the configured model files and Python environment are available. Its status appears in `/api/ready` and on the dashboard.

## Local files

These files stay on your machine:

- `config.json`
- `data/`
- `demo-data/`
- key files
- logs and model output

The repository's ignore rules cover those paths. Check `git status` before every commit in case you store a private file somewhere unusual.

## Live orders

The example settings do not allow live orders. Live use requires valid credentials, healthy feeds, enabled account limits, and a separate arm action in the dashboard. Review every limit with a small account before enabling that path. Paper trading is the normal starting point.
