# Kalshi Briefing Forwarder

A tiny, dependency-free Go program that watches the `kalshi-briefings/` folder and
pushes each new briefing to your phone. A scheduled task drops a briefing file into
that folder; this program notices it and forwards it.

```
scheduled task  ──writes──▶  kalshi-briefings/briefing-*.md  ──watched by──▶  forwarder  ──push──▶  your phone
```

## Build

```bash
cd forwarder
go build -o kalshi-forwarder.exe .
```

No `go mod tidy` step is needed. The forwarder uses the standard library.

## Configure

```bash
copy config.example.json forwarder-config.json
```

Then edit `forwarder-config.json` and pick **one** channel.

### Option A: ntfy
1. Install the **ntfy** app on your phone (iOS/Android), or use the web app.
2. Pick a long, secret topic name and put it in `ntfy.topic`. Anyone with that name can read your briefings.
3. In the app, **subscribe** to that same topic.
4. Set `"channel": "ntfy"`. Done.

### Option B: Telegram
1. In Telegram, message **@BotFather** → `/newbot` → copy the **bot token** into `telegram.bot_token`.
2. Send any message to your new bot, then visit
   `https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates` and copy the `chat.id` into `telegram.chat_id`.
3. Set `"channel": "telegram"`.

### Option C: generic webhook
Set `"channel": "webhook"` and put the URL in `webhook.url`. The program POSTs JSON
`{"title","filename","body"}`. (For Discord/Slack you may want a tiny adapter, since
they expect a specific field like `content`/`text`.)

## Run

```bash
./kalshi-forwarder.exe -config forwarder-config.json
```

Leave it running. It polls every `poll_seconds` and forwards any new `.md` file.

- On first start it marks existing briefings as already-seen (no backlog spam). Set
  `"forward_existing_on_start": true` if you'd rather get the backlog once.
- If a send fails, that file is retried on the next poll (nothing is lost).
- Sent files are tracked in `forwarder-state.json`.

### Keep it running automatically (Windows)
Run it in a terminal you leave open, or register it as a background service (e.g. with
[nssm](https://nssm.cc/) or Windows Task Scheduler set to "run at logon").

## Good to know
- The forwarder only sends files while it is running. Keep the scheduler and forwarder
  running for continuous delivery.
- The main suite also has a built-in notifier. This small service remains useful when the
  briefing process runs separately.
