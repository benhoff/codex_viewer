# Codex Session Viewer

A small FastAPI app that centralizes Codex rollout sessions into a server-owned SQLite database and serves a mobile-friendly web viewer with export endpoints.

## Features

- Imports real Codex rollout files from `~/.codex/sessions` by default
- Stores normalized session metadata and timeline events in SQLite
- Groups sessions by inferred GitHub remote when available, or by host-aware directory fallback
- Includes a manual override interface for project grouping, organization, repository, and remote URL
- Supports a simple agent-to-server sync flow where daemons push normalized session data to the server
- Exposes remote agent health, version drift, and sync protocol status
- Server-rendered HTML viewer for grouped projects, project detail, and session detail pages
- Export each session as raw JSONL, normalized JSON, or Markdown
- Uses Tailwind for the UI

## Run

0. Create a local env file if you are not using systemd:

   ```bash
   cp .env.example .env
   ```

   For a development-oriented setup, you can start from:

   ```bash
   cp .env.development.example .env
   ```

1. Install Python dependencies into the repo:

   ```bash
   python3 -m pip install --target .deps -r requirements.txt
   ```

2. Install Tailwind locally and build the CSS:

   ```bash
   npm install
   npm run build:css
   ```

3. Start the app:

   ```bash
   PYTHONPATH=.deps python3 -m codex_session_viewer serve --host 0.0.0.0 --port 8000
   ```

4. Open `http://localhost:8000`

## Commands

Sync the SQLite database:

```bash
PYTHONPATH=.deps python3 -m codex_session_viewer sync
```

Force a full rebuild after parser or schema changes:

```bash
PYTHONPATH=.deps python3 -m codex_session_viewer sync --rebuild
```

Export one session from SQLite:

```bash
PYTHONPATH=.deps python3 -m codex_session_viewer export SESSION_ID --format markdown
```

Run the background sync daemon:

```bash
PYTHONPATH=.deps python3 -m codex_session_viewer daemon --interval 30
```

In `remote` mode, the daemon does not write rollout data into local SQLite. It asks the server for a per-host manifest and uploads any missing or mismatched sessions back to the server.

The wrapper scripts also load local env files now, so you can run:

```bash
./scripts/start-server.sh
./scripts/start-agent-daemon.sh
```

without relying on the systemd env file.

## Configuration

Env files are loaded in this order:

- `.env`
- `.env.<CODEX_VIEWER_ENV>`
- `.env.local`
- `.env.<CODEX_VIEWER_ENV>.local`

Shell or systemd-provided environment variables still win over values from these files.

- `CODEX_SESSION_ROOTS`: comma-separated list of rollout roots to index
- `CODEX_VIEWER_ENV`: optional env profile name such as `development`
- `CODEX_VIEWER_SYNC_MODE`: `local` for direct SQLite import, `remote` for daemon-to-server upload
- `CODEX_VIEWER_DB`: override the SQLite database path
- `CODEX_VIEWER_DATA_DIR`: override the data directory
- `CODEX_VIEWER_PAGE_SIZE`: sessions shown on the home page
- `CODEX_VIEWER_SYNC_ON_START=0`: disable automatic sync during server startup
- `CODEX_VIEWER_HOST`: bind address for the FastAPI server
- `CODEX_VIEWER_PORT`: bind port for the FastAPI server
- `CODEX_VIEWER_SERVER_URL`: base URL for remote daemon uploads, such as `http://viewer.internal:8000`
- `CODEX_VIEWER_SYNC_API_TOKEN`: bearer token the daemon sends for `/api/sync/*`; create this token from the server's `/settings` page
- `CODEX_VIEWER_APP_VERSION`: version string the current server or agent reports for itself
- `CODEX_VIEWER_API_VERSION`: sync protocol version shared by the server and agents
- `CODEX_VIEWER_EXPECTED_AGENT_VERSION`: version the server expects agents to be running
- `CODEX_VIEWER_MIN_AGENT_VERSION`: floor the server can advertise for manual ops
- `CODEX_VIEWER_AGENT_UPDATE_COMMAND`: optional local command the agent runs when the server advertises a different agent version
- `CODEX_VIEWER_SYNC_INTERVAL`: daemon polling interval in seconds
- `CODEX_VIEWER_REMOTE_TIMEOUT`: HTTP timeout for remote sync requests
- `CODEX_VIEWER_LOG_LEVEL`: log verbosity for the server and daemon
- `CODEX_VIEWER_SOURCE_HOST`: host label written onto imported sessions
- `CODEX_VIEWER_DEV_RELOAD=1`: restart the child process when watched project files change
- `CODEX_VIEWER_DEV_RELOAD_INTERVAL`: polling interval, in seconds, for dev reload

## Remote Sync Model

- The server owns SQLite and exposes `/api/sync/manifest` and `/api/sync/session`.
- The server also exposes `/api/sync/heartbeat` and a `/remotes` UI page for agent health.
- The server validates sync requests only against managed tokens created in `/settings`.
- Each daemon asks the server for the manifest for its own `CODEX_VIEWER_SOURCE_HOST`.
- The manifest includes `source_path`, file size, file mtime, content hash, declared `event_count`, actual `stored_event_count`, the current server app version, the expected agent version, and the sync API version.
- The daemon re-uploads a full session when the server is missing it, when file metadata changed, when the server is missing the stored hash, or when `stored_event_count` does not match `event_count`.
- The daemon sends heartbeats with its `agent_version`, `sync_api_version`, last sync stats, and update state.
- If the server advertises a different agent version and `CODEX_VIEWER_AGENT_UPDATE_COMMAND` is configured, the daemon runs that local command and restarts itself.
- If the sync API version differs, the daemon reports a protocol mismatch so you can manually reinstall or update the remote.

This is intentionally simple. It handles server purges and partial remote data without local SQLite. If you later want stronger integrity checks for unchanged local files, the next step would be a small local checksum cache or periodic full-hash validation.

## Systemd

Systemd unit files and wrapper scripts live under [deploy/systemd](/home/wulfuser/codex_viewer/deploy/systemd) and [scripts](/home/wulfuser/codex_viewer/scripts).

The current units are set up for this machine:

- `codex-session-viewer.service`: FastAPI web server
- `codex-session-agent.service`: background sync daemon

Install them with:

```bash
sudo cp /home/wulfuser/codex_viewer/deploy/systemd/codex-session-*.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now codex-session-agent.service codex-session-viewer.service
```

If you move the repo or want to run under a different user, edit the unit files and [deploy/systemd/codex-session-viewer.env](/home/wulfuser/codex_viewer/deploy/systemd/codex-session-viewer.env) first.

For non-systemd local runs, use `.env` instead.

For development, the wrapper scripts support separate reload modes for the web server and the agent daemon:

- `CODEX_VIEWER_SERVER_DEV_RELOAD=1`: restarts the web server on Python, template, and asset changes
- `CODEX_VIEWER_AGENT_DEV_RELOAD=1`: restarts the daemon only on agent-relevant Python and env changes

The older `CODEX_VIEWER_DEV_RELOAD` variables still act as a fallback, but the split settings are the intended configuration now.

Changes to systemd unit files still require a normal `systemctl restart`.
