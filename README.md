# Codex Session Viewer

A small FastAPI app that centralizes Codex rollout sessions into a server-owned SQLite database and serves a mobile-friendly web viewer with export endpoints.

## Features

- Server-first deployment where remote agents upload rollout data into a server-owned SQLite database
- Stores normalized session metadata and timeline events in SQLite
- Groups sessions by inferred GitHub remote when available, or by host-aware directory fallback
- Includes a manual override interface for project grouping, organization, repository, and remote URL
- Supports a simple agent-to-server sync flow where daemons push normalized session data to the server
- Exposes remote agent health, version drift, and sync protocol status
- Server-rendered HTML viewer for grouped projects, project detail, and session detail pages
- Export each session as raw JSONL, normalized JSON, or Markdown
- Uses Tailwind for the UI

## Docker

The repo now includes a container path for the web server:

```bash
docker compose up --build -d
```

This default container setup is intentionally server-first:

- runs the FastAPI web server on port `8000`
- persists SQLite in a Docker volume mounted at `/app/data`
- assumes remote agents will upload sessions into the server
- does not depend on Node at runtime because the built CSS is already committed

The container defaults are defined in [compose.yml](/home/wulfuser/codex_viewer/compose.yml) and [Dockerfile](/home/wulfuser/codex_viewer/Dockerfile).

Single-box local import is still supported, but it is an advanced path rather than the default deployment model.

If you want to run the server in single-box local import mode:

1. change `CODEX_VIEWER_SYNC_MODE` to `local`
2. mount your Codex session root into the container
3. point `CODEX_SESSION_ROOTS` at that mounted path
4. enable startup sync from `/settings` after the first boot

Example override:

```yaml
services:
  viewer:
    volumes:
      - viewer-data:/app/data
      - /home/you/.codex/sessions:/sessions:ro
    environment:
      CODEX_VIEWER_SYNC_MODE: local
      CODEX_SESSION_ROOTS: /sessions
```

If you prefer transparent host-side backups instead of a named Docker volume, replace:

```yaml
volumes:
  - viewer-data:/app/data
```

with:

```yaml
volumes:
  - ./data:/app/data
```

## Run

0. Create the role-specific env file you need if you are not using systemd:

   ```bash
   cp .env.server.example .env
   ```

   This is the normal server path. The viewer defaults to a server-owned SQLite database and expects remotes to push data in.

   For an agent-only host:

   ```bash
   cp .env.agent.example .env
   ```

   For local repo development:

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

   If browser auth is enabled, the first visit will redirect to `/setup` so you can create the initial local admin account.

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

Role-specific examples:

- `.env.server.example`: normal viewer server setup
- `.env.agent.example`: remote agent daemon setup
- `.env.development.example`: local repo development with server and daemon reload enabled

Normal server setup:

- `CODEX_VIEWER_DATA_DIR`: override the data directory
- `CODEX_VIEWER_HOST`: bind address for the FastAPI server
- `CODEX_VIEWER_PORT`: bind port for the FastAPI server
- `CODEX_VIEWER_LOG_LEVEL`: log verbosity for the server and daemon
- `CODEX_VIEWER_AUTH_MODE`: `none`, `password`, `proxy`, or `password_or_proxy`
- `CODEX_VIEWER_SESSION_SECRET`: optional override for the login session signing secret; if unset and UI auth is enabled, the viewer generates one and stores it in `data/.session-secret`
- `CODEX_VIEWER_AUTH_PROXY_USER_HEADER`: trusted reverse-proxy header used for SSO identity, default `X-Forwarded-User`
- `CODEX_VIEWER_AUTH_PROXY_NAME_HEADER`: trusted reverse-proxy display-name header, default `X-Forwarded-Name`
- `CODEX_VIEWER_AUTH_PROXY_EMAIL_HEADER`: trusted reverse-proxy email header, default `X-Forwarded-Email`
- `CODEX_VIEWER_AUTH_PROXY_LOGIN_URL`: optional SSO entrypoint URL for proxy-only deployments
- `CODEX_VIEWER_AUTH_PROXY_LOGOUT_URL`: optional IdP or proxy logout URL
- `CODEX_VIEWER_AUTH_COOKIE_SECURE=1`: mark auth session cookies as secure

Normal agent setup:

- `CODEX_VIEWER_SERVER_URL`: base URL for remote daemon uploads, such as `http://viewer.internal:8000`
- `CODEX_VIEWER_SYNC_API_TOKEN`: bearer token the daemon sends for `/api/sync/*`; create this token from the server's `/settings` page
- `CODEX_VIEWER_SYNC_INTERVAL`: daemon polling interval in seconds

Advanced or single-box overrides:

- `CODEX_VIEWER_ENV`: optional env profile name such as `development`
- `CODEX_VIEWER_SYNC_MODE`: `remote` by default; set `local` only for direct single-box SQLite import
- `CODEX_SESSION_ROOTS`: comma-separated rollout roots for local import or customized agent scan paths
- `CODEX_VIEWER_DB`: override the exact SQLite database path
- `CODEX_VIEWER_SOURCE_HOST`: override the host label written onto imported sessions
- `CODEX_VIEWER_REMOTE_TIMEOUT`: HTTP timeout for remote sync requests
- `CODEX_VIEWER_DAEMON_REBUILD_ON_START`: force a full daemon rebuild on startup
- `CODEX_VIEWER_AGENT_UPDATE_COMMAND`: optional local command the agent runs when the server advertises a different agent version

## UI Auth

The viewer now supports two practical self-hosted auth modes:

- built-in password login
- reverse-proxy header SSO

### Lightweight auth

Use built-in password auth when you want a single local login without another dependency:

```env
CODEX_VIEWER_AUTH_MODE=password
```

Then open the app and complete `/setup` once to create the first local admin account in SQLite. On first boot, the viewer will generate and persist a session signing secret in `data/.session-secret`. If you prefer to supply your own fixed secret, you can still set `CODEX_VIEWER_SESSION_SECRET`.

### SSO behind a reverse proxy

Use proxy mode when an upstream reverse proxy or auth gateway injects a trusted user header:

```env
CODEX_VIEWER_AUTH_MODE=proxy
CODEX_VIEWER_AUTH_PROXY_USER_HEADER=X-Forwarded-User
CODEX_VIEWER_AUTH_PROXY_NAME_HEADER=X-Forwarded-Name
CODEX_VIEWER_AUTH_PROXY_EMAIL_HEADER=X-Forwarded-Email
CODEX_VIEWER_AUTH_PROXY_LOGIN_URL=https://sso.example.com
CODEX_VIEWER_AUTH_PROXY_LOGOUT_URL=https://sso.example.com/logout
CODEX_VIEWER_AUTH_COOKIE_SECURE=1
```

If you do not want the generated secret file for proxy mode either, you can still set `CODEX_VIEWER_SESSION_SECRET` explicitly.

Important: proxy auth should only be used behind a trusted reverse proxy that strips and rewrites these headers. Do not expose header-based SSO directly to the internet without a proxy boundary.

If you want both a local emergency login and proxy SSO, use:

```env
CODEX_VIEWER_AUTH_MODE=password_or_proxy
```

In `password_or_proxy` mode, the first-run setup still creates the local admin account. After that, users can authenticate through either the local password form or your trusted reverse proxy.

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

Changes to systemd unit files still require a normal `systemctl restart`.

## Product Notes

There is now a short product-direction doc for the self-hosted audience in [docs/selfhosted-launch-priorities.md](/home/wulfuser/codex_viewer/docs/selfhosted-launch-priorities.md).

## License

This project is licensed under the GNU Affero General Public License v3.0 only
(`AGPL-3.0-only`). See [LICENSE](/home/wulfuser/codex_viewer/LICENSE).
