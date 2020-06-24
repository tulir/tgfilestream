# tgfilestream
A Telegram bot that can stream Telegram files to users over HTTP.

## Setup
Install dependencies (see [requirements.txt](/requirements.txt)), configure
environment variables (see below) and run with `python3 -m tgfilestream`.

A reverse proxy is recommended to add TLS. When using a reverse proxy, keep
`HOST` as-is, but add the publicly accessible URL to `PUBLIC_URL`. The URL
should include the protocol, e.g. `https://example.com`.

### Environment variables
* `TG_API_ID` (required) - Your Telegram API ID.
* `TG_API_HASH` (required) - Your Telegram API hash.
* `PORT` (defaults to `8080`) - The port to listen at.
* `HOST` (defaults to `localhost`) - The host to listen at.
* `PUBLIC_URL` (defaults to `http://localhost:8080`) - The prefix for links that the bot gives.
* `TRUST_FORWARD_HEADERS` (defaults to false) - Whether or not to trust X-Forwarded-For headers when logging requests.
* `DEBUG` (defaults to false) - Whether or not to enable extra prints.
* `LOG_CONFIG` - Path to a Python basic log config. Overrides `DEBUG`.
* `REQUEST_LIMIT` (default 5) - The maximum number of requests a single IP can have active at a time.
* `CONNECTION_LIMIT` (default 20) - The maximum number of connections to a single Telegram datacenter.
* `TG_START_MESG` - The message that should be shown in Telegram chat, in case of non-media message.
* `TG_G_C_MESG` - The message that should be shown in a Telegram Group chat.
* `TG_SESSION_NAME` (defaults to `tgfilestream`) - The name of the Telethon session file to use.
* `TG_BOT_FATHER_TOKEN` (defaults to None) - This option is mutually exclusive to `TG_SESSION_NAME`, and if set, the client will login as a bot, instead of an user.
